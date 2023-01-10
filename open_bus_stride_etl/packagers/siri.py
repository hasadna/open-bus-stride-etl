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
from .common import get_file_last_modified, upload_file, download_file, download_legacy_file, iterate_keys

# can use this to force update after code changes
FORCE_UPDATE_IF_FILE_LAST_MODIFIED_BEFORE = pytz.timezone('israel').localize(datetime.datetime(2023, 1, 10, 20, 30))

UPDATE_PACKAGE_RES_PACKAGE_EXISTS = 'package_exists'
UPDATE_PACKAGE_RES_SAME_HASH = 'same_hash'
UPDATE_PACKAGE_RES_LEGACY_NOT_EXISTS = 'legacy_not_exists'
STRIDE_FIRST_DATETIME = datetime.datetime(2022, 3, 15, 0).astimezone(pytz.timezone('israel'))
LEGACY_FIRST_DATETIME = datetime.datetime(2019, 3, 4, 0).astimezone(pytz.timezone('israel'))
LEGACY_LAST_DATETIME = datetime.datetime(2021, 8, 9, 0).astimezone(pytz.timezone('israel'))
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
            row[k] = v.replace(tzinfo=pytz.UTC).astimezone(pytz.timezone('israel')).isoformat()
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
            row = get_row(dict(sql_row))
            yield row
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
    return upload_file(filename, package_path)


def update_package(stats, start_datetimehour: datetime.datetime, force_update=False, verbose=False):
    assert start_datetimehour.minute == 0 and start_datetimehour.second == 0 and start_datetimehour.microsecond == 0
    stats['all_packages'] += 1
    base_filename = start_datetimehour.strftime('%Y-%m-%d.%H')
    package_path = start_datetimehour.strftime('stride-etl-packages/siri/%Y/%m/') + base_filename + '.zip'
    file_last_modified = get_file_last_modified(package_path)
    package_exists = file_last_modified is not None
    if package_exists:
        if file_last_modified < FORCE_UPDATE_IF_FILE_LAST_MODIFIED_BEFORE:
            if verbose:
                print(f'Package {package_path} exists and is old, forcing update')
            stats['package_forced_update_old'] += 1
        elif force_update:
            if verbose:
                print(f'Package exists, but forcing update: {package_path}')
            stats['package_forced_update'] += 1
        else:
            if verbose:
                print(f'Package already exists: {package_path}')
            stats['package_skipped'] += 1
            return UPDATE_PACKAGE_RES_PACKAGE_EXISTS
    else:
        if verbose:
            print(f'Package does not exist: {package_path}')
        stats['package_create'] += 1
    if verbose:
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
                return UPDATE_PACKAGE_RES_SAME_HASH
        return upload_package(tmpdir, package_path, base_filename, verbose=verbose)


def hourly_update_packages(stats=None, verbose=False, max_packages_per_type=None, start_datehour=None):
    if stats is None:
        stats = defaultdict(int)
    start_time = datetime.datetime.now()
    if not start_datehour:
        start_datehour = now().replace(minute=0, second=0, microsecond=0).astimezone(pytz.timezone('israel'))
    end_datehour = STRIDE_FIRST_DATETIME
    # end_datehour = LEGACY_FIRST_DATETIME
    print(f'Updating packages from {start_datehour} to {end_datehour} for up to 10 hours')
    current_datehour = start_datehour + datetime.timedelta(hours=1)
    num_is_stride_force, num_is_stride, num_is_legacy = 0, 0, 0
    while current_datehour >= end_datehour and (datetime.datetime.now() - start_time).total_seconds() < 60 * 60 * 10:
        current_datehour -= datetime.timedelta(hours=1)
        is_stride = current_datehour >= STRIDE_FIRST_DATETIME
        is_legacy = current_datehour <= LEGACY_LAST_DATETIME
        if is_stride or is_legacy:
            force_update = current_datehour > (start_datehour - datetime.timedelta(days=5))
            if max_packages_per_type not in [None, 'None', 0, '0']:
                max_packages_per_type = int(max_packages_per_type)
                if (
                    (is_stride and force_update and num_is_stride_force >= max_packages_per_type)
                    or (is_stride and not force_update and num_is_stride >= max_packages_per_type)
                    or (is_legacy and num_is_legacy >= max_packages_per_type)
                ):
                    continue
            print(f'{datetime.datetime.now()} Updating package: {current_datehour} (force_update={force_update},is_stride={is_stride},is_legacy={is_legacy})')
            if is_stride:
                update_package_res = update_package(stats, current_datehour, force_update, verbose)
            else:
                raise Exception('Legacy packages are not supported')
                # assert is_legacy
                # update_package_res = update_package_legacy(stats, current_datehour, verbose)
            if update_package_res in [UPDATE_PACKAGE_RES_PACKAGE_EXISTS, UPDATE_PACKAGE_RES_LEGACY_NOT_EXISTS]:
                pass
            else:
                stats_str = ','.join([f'{k}={stats[k]}' for k in sorted(stats.keys())])
                if update_package_res == UPDATE_PACKAGE_RES_SAME_HASH:
                    print(f'No change ({stats_str})')
                else:
                    print(f'Uploaded ({stats_str}): {update_package_res}')
            if is_stride and force_update:
                num_is_stride_force += 1
            elif is_stride:
                num_is_stride += 1
            elif is_legacy:
                num_is_legacy += 1
    pprint(dict(stats))


def legacy_get_datetime_field(row, date_fields=None, time_fields=None):
    date_fields = date_fields or []
    time_fields = time_fields or []
    date_value = None
    time_value = None
    for date_field in date_fields:
        date_value = row.get(date_field)
        if date_value:
            break
    for time_field in time_fields:
        time_value = row.get(time_field)
        if time_value:
            break
    assert date_value and time_value, f'Could not find date or time in row: {row}'
    return pytz.timezone('israel').localize(datetime.datetime.strptime(f'{date_value} {time_value}', '%Y-%m-%d %H:%M:%S')).isoformat()


# def legacy_get_siri_journey_ref(date, service_id):
#     service_id = service_id.strip() or '0'
#     return f'{date}-{service_id}'


# def legacy_process_row(key, i, row):
#     return {
#         'id': f'{key}_{i}',
#         'lat': row['lat'],
#         'lon': row['lon'],
#         'recorded_at_time': legacy_get_datetime_field(row, ['date_recorded', 'date'], ['time_recorded']),
#         'siri_scheduled_start_time': legacy_get_datetime_field(row, ['planned_start_date', 'date'], ['planned_start_time']),
#         'siri_journey_ref': legacy_get_siri_journey_ref(row['date'], row.get('service_id')),
#         'siri_vehicle_ref': row['bus_id'],
#         'siri_stop_code': row.get('stop_point_ref'),
#         'siri_operator_ref': row['agency_id'],
#         'siri_line_ref': row['route_id'],
#         'siri_snapshot_id': key,
#         'gtfs_route_short_name': row['route_short_name'],
#         'predicted_end_time': legacy_get_datetime_field(row, ['predicted_end_date', 'date'], ['predicted_end_time']),
#         'date': row['date'],
#         'num_duplicates': row.get('num_duplicates') or '',
#     }


# def legacy_package_iterator(key, filename):
#     for res in DF.Flow(
#             DF.load(filename, cast_strategy=DF.load.CAST_TO_STRINGS, infer_strategy=DF.load.INFER_STRINGS)
#     ).datastream().res_iter.get_iterator():
#         for i, row in enumerate(res):
#             yield legacy_process_row(key, i, row)


# def update_package_legacy(stats, current_datehour, verbose):
#     key = current_datehour.strftime("%Y-%m-%d.%-H")
#     if verbose:
#         print(f'update_package_legacy({current_datehour})')
#     with tempfile.TemporaryDirectory() as tmpdir:
#         filename = os.path.join(tmpdir, 'legacy.csv.gz')
#         bucket = 'obus-do1'
#         file_key = f'SiriForSplunk/{current_datehour.strftime("%Y/%m/%d")}/siri_rt_data_v2.{key}.csv.gz'
#         if download_legacy_file(bucket, file_key, filename):
#             output_path = os.path.join(tmpdir, 'package')
#             if verbose:
#                 print(f'Packaging siri data {file_key} -> {output_path}')
#             DF.Flow(
#                 legacy_package_iterator(key, filename),
#                 DF.dump_to_path(output_path)
#             ).process()
#             if verbose:
#                 pprint(dict(stats))
#             base_filename = current_datehour.strftime('%Y-%m-%d.%H')
#             package_path = current_datehour.strftime('stride-etl-packages/siri/%Y/%m/') + base_filename + '.zip'
#             return upload_package(tmpdir, package_path, base_filename, verbose=verbose)
#         else:
#             if verbose:
#                 print("Legacy package does not exist")
#             return UPDATE_PACKAGE_RES_LEGACY_NOT_EXISTS


def iterate_legacy_packages_index():
    for keynum, key in enumerate(iterate_keys('obus-do1', 'SiriForSplunk')):
        if keynum+1 > 2:
            break
        print(f'Processing key {keynum+1}: {key}')
        max_recorded_at_time = None
        min_recorded_at_time = None
        num_rows = 0
        assert key.endswith('.csv.gz')
        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, 'legacy.csv.gz')
            download_legacy_file('obus-do1', key, filename)
            for res in DF.Flow(
                    DF.load(filename, cast_strategy=DF.load.CAST_TO_STRINGS, infer_strategy=DF.load.INFER_STRINGS)
            ).datastream().res_iter.get_iterator():
                for i, row in enumerate(res):
                    num_rows += 1
                    recorded_at_time = legacy_get_datetime_field(row, ['date_recorded', 'date'], ['time_recorded'])
                    if not max_recorded_at_time or recorded_at_time > max_recorded_at_time:
                        max_recorded_at_time = recorded_at_time
                    if not min_recorded_at_time or recorded_at_time < min_recorded_at_time:
                        min_recorded_at_time = recorded_at_time
        yield {
            'key': key,
            'num_rows': num_rows,
            'max_recorded_at_time': max_recorded_at_time,
            'min_recorded_at_time': min_recorded_at_time,
        }


def create_legacy_packages_index():
    with tempfile.TemporaryDirectory() as tmpdir:
        DF.Flow(
            iterate_legacy_packages_index(),
            DF.printer(),
            DF.dump_to_path(os.path.join(tmpdir, 'package'))
        ).process()
        upload_package(tmpdir, 'stride-etl-packages/siri/legacy-packages-index.zip', 'legacy-packages-index')
