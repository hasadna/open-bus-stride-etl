import os
import json
import shutil
import zipfile
import datetime
import tempfile
from pprint import pprint
from textwrap import dedent
from collections import defaultdict

import pytz
import dataflows as DF

from ..common import now
from open_bus_stride_db import db
from .common import is_file_exists, upload_file, download_file


SQL_TEMPLATE = dedent('''
    select
        svl.id,
        svl.bearing,svl.distance_from_journey_start,svl.distance_from_siri_ride_stop_meters,svl.lat,svl.lon,
        svl.recorded_at_time,svl.velocity,
        srs."order" siri_stop_order,
        sr.scheduled_start_time siri_scheduled_start_time,sr.duration_minutes siri_duration_minutes,
        sr.journey_ref siri_journey_ref,sr.vehicle_ref siri_vehicle_ref,
        st.code siri_stop_code,
        srt.operator_ref siri_operator_ref,srt.line_ref siri_line_ref,
        sn.snapshot_id as siri_snapshot_id,
        gr.journey_ref gtfs_journey_ref,
        gr.start_time gtfs_start_time,
        gr.end_time gtfs_end_time,
        gs.code gtfs_stop_code, gs.lat gtfs_stop_lat, gs.lon gtfs_stop_lon, gs.city gtfs_stop_city, gs.name gtfs_stop_name,
        grs.arrival_time gtfs_arrival_time, grs.departure_time gtfs_departure_time,
        grs.drop_off_type gtfs_drop_off_type, grs.pickup_type gtfs_pickup_type,
        grs.shape_dist_traveled gtfs_shape_dist_traveled,
        grs.stop_sequence gtfs_stop_sequence,
        grt.line_ref gtfs_line_ref, grt.operator_ref gtfs_operator_ref, grt.agency_name gtfs_agency_name,
        grt.route_short_name gtfs_route_short_name, grt.route_long_name gtfs_route_long_name,
        grt.route_type gtfs_route_type, grt.route_alternative gtfs_route_alternative, grt.route_direction gtfs_route_direction,
        grt.route_mkt gtfs_route_mkt
    from
        siri_vehicle_location svl
        join siri_ride_stop srs on srs.id = svl.siri_ride_stop_id
        join siri_ride sr on sr.id = srs.siri_ride_id
        join siri_stop st on st.id = srs.siri_stop_id
        join siri_route srt on srt.id = sr.siri_route_id
        join siri_snapshot sn on sn.id = svl.siri_snapshot_id
        left join gtfs_ride gr on gr.id = sr.gtfs_ride_id
        left join gtfs_stop gs on gs.id = srs.gtfs_stop_id and gs.date >= '{min_date_utc}' and gs.date < '{max_date_utc}'
        left join gtfs_ride_stop grs on grs.gtfs_ride_id = gr.id and grs.gtfs_stop_id = srs.gtfs_stop_id
        left join gtfs_route grt on grt.id = gr.gtfs_route_id and grt.date >= '{min_date_utc}' and grt.date < '{max_date_utc}'
    where
        svl.recorded_at_time >= '{min_time_utc}'
        and svl.recorded_at_time < '{max_time_utc}'
        and sr.updated_duration_minutes is not null
    order by
        sr.id, svl.recorded_at_time
''')


def get_row(row):
    for k, v in row.items():
        if isinstance(v, str):
            continue
        if v is None:
            row[k] = ''
        elif isinstance(v, datetime.datetime):
            row[k] = v.isoformat()
        else:
            row[k] = str(v)
    return row


def db_datetime(dt):
    return dt.astimezone(pytz.UTC).replace(tzinfo=None).strftime('%Y-%m-%d %H:%M:%S')


def db_date(dt):
    return dt.astimezone(pytz.UTC).replace(tzinfo=None).strftime('%Y-%m-%d')


def sql_iterator(stats, min_time: datetime.datetime, max_time: datetime.datetime, verbose=False):
    start_time = now()
    if verbose:
        print(f'{start_time} iterating over sql from {min_time} to {max_time}')
    sql = SQL_TEMPLATE.format(
        min_date_utc=db_date(min_time - datetime.timedelta(days=2)),
        max_date_utc=db_date(max_time + datetime.timedelta(days=2)),
        min_time_utc=db_datetime(min_time),
        max_time_utc=db_datetime(max_time),
    )
    with db.get_session() as session:
        for sql_row in session.execute(sql, execution_options={'stream_results': True}):
            stats['rows'] += 1
            if verbose and stats['rows'] == 1:
                print(f'{(now()-start_time).total_seconds()}s: got first row from DB')
            yield get_row(dict(sql_row))
    if verbose:
        print(f'{(now() - start_time).total_seconds()}s: finished processing last row from DB ({stats["rows"]} rows)')


def sql_iterator_timedelta(stats, start_time: datetime.datetime, timedelta_units, timedelta_amount):
    end_time = start_time + datetime.timedelta(**{timedelta_units: timedelta_amount})
    time = start_time
    print(f'iterating timedeltas from {start_time} to {end_time}')
    while time < end_time:
        next_time = time + datetime.timedelta(**{timedelta_units: 1})
        yield from sql_iterator(stats, time, next_time)
        time = next_time
        stats['timedeltas'] += 1


def save_package_timedelta(stats, start_time, timedelta_units, timedelta_amount, output_path):
    print(f'Packaging siri data {start_time} -> {timedelta_units}={timedelta_amount} -> {output_path}')
    DF.Flow(
        sql_iterator_timedelta(stats, start_time, timedelta_units, timedelta_amount),
        DF.dump_to_path(output_path),
    ).process()


def save_package(stats, start_time, end_time, output_path, verbose=False):
    if verbose:
        print(f'Packaging siri data {start_time} -> {end_time} -> {output_path}')
    DF.Flow(
        sql_iterator(stats, start_time, end_time, verbose),
        DF.dump_to_path(output_path),
    ).process()


def get_existing_package_hash(package_path, base_filename):
    print(f'Getting existing package hash {package_path} ({base_filename})')
    with tempfile.TemporaryDirectory() as temp_dir:
        filename = os.path.join(temp_dir, 'package.zip')
        download_file(package_path, filename)
        with zipfile.ZipFile(filename, 'r') as zf:
            with zf.open(f'{base_filename}-metadata.json') as f:
                return json.load(f)['hash']


def upload_package(tmpdir, package_path, base_filename, verbose=False):
    if verbose:
        print(f"Uploading package {tmpdir} -> {package_path}")
        print(f'Creating package file...')
    os.rename(os.path.join(tmpdir, 'package', 'res_1.csv'), os.path.join(tmpdir, 'package', f'{base_filename}.csv'))
    os.rename(os.path.join(tmpdir, 'package', 'datapackage.json'), os.path.join(tmpdir, 'package', f'{base_filename}-metadata.json'))
    shutil.make_archive(os.path.join(tmpdir, base_filename), 'zip', os.path.join(tmpdir, 'package'))
    filename = os.path.join(tmpdir, f'{base_filename}.zip')
    if verbose:
        print(f"Uploading package file {filename} -> {package_path}")
    upload_file(filename, package_path)


def update_package(stats, start_datetimehour: datetime.datetime, force_update=False, verbose=False):
    assert start_datetimehour.minute == 0 and start_datetimehour.second == 0 and start_datetimehour.microsecond == 0
    stats['all_packages'] += 1
    base_filename = start_datetimehour.strftime('%Y-%m-%d.%H')
    package_path = start_datetimehour.strftime('stride-etl-packages/siri/%Y/%m/') + base_filename + '.zip'
    package_exists = is_file_exists(package_path)
    if package_exists:
        if force_update:
            if verbose:
                print(f'Package exists, but forcing update: {package_path}')
            stats['package_forced_update'] += 1
        else:
            if verbose:
                print(f'Package already exists: {package_path}')
            stats['package_skipped'] += 1
            return
    else:
        if verbose:
            print(f'Package does not exist: {package_path}')
        stats['package_create'] += 1
    print(f"Updating package {package_path} (force_update={force_update})")
    with tempfile.TemporaryDirectory() as tmpdir:
        save_package(stats, start_datetimehour, start_datetimehour + datetime.timedelta(hours=1), os.path.join(tmpdir, 'package'), verbose=verbose)
        if verbose:
            pprint(dict(stats))
        if package_exists:
            with open(os.path.join(tmpdir, 'package', 'datapackage.json')) as f:
                new_package_hash = json.load(f)['hash']
            existing_package_hash = get_existing_package_hash(package_path, base_filename)
            if new_package_hash == existing_package_hash:
                if verbose:
                    print(f'Package hash is the same, skipping upload: {new_package_hash}')
                stats['skipped_upload_packages'] += 1
                return
        upload_package(tmpdir, package_path, base_filename, verbose=verbose)


def hourly_update_packages(stats=None, verbose=False):
    if stats is None:
        stats = defaultdict(int)
    start_time = datetime.datetime.now()
    start_datehour = now().replace(minute=0, second=0, microsecond=0).astimezone(pytz.timezone('israel'))
    end_datehour = datetime.datetime(2022, 3, 15, 0).astimezone(pytz.timezone('israel'))
    print(f'Updating packages from {start_datehour} to {end_datehour} for up to 10 hours')
    current_datehour = start_datehour
    while current_datehour >= end_datehour and (datetime.datetime.now() - start_time).total_seconds() < 60 * 60 * 10:
        if not (
            current_datehour > start_datehour - datetime.timedelta(days=1)
            or start_datehour - datetime.timedelta(days=10) < current_datehour < start_datehour - datetime.timedelta(days=9)
        ):
            current_datehour -= datetime.timedelta(hours=1)
            continue
        force_update = current_datehour > (start_datehour - datetime.timedelta(days=5))
        print(f'{datetime.datetime.now()} Updating package: {current_datehour} (force_update={force_update})')
        update_package(stats, current_datehour, force_update, verbose)
        current_datehour -= datetime.timedelta(hours=1)
    pprint(dict(stats))
