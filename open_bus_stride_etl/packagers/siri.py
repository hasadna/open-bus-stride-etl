import os
import json
import shutil
import zipfile
import datetime
import tempfile
import traceback
from pprint import pprint
from textwrap import dedent
from collections import defaultdict

import pytz
import dataflows as DF

from ..common import now
from open_bus_stride_db import db
from .common import get_file_last_modified, upload_file, download_file, download_legacy_file, iterate_keys

# can use this to force update after code changes
FORCE_UPDATE_IF_FILE_LAST_MODIFIED_BEFORE = None

UPDATE_PACKAGE_RES_PACKAGE_EXISTS = 'package_exists'
UPDATE_PACKAGE_RES_SAME_HASH = 'same_hash'
UPDATE_PACKAGE_RES_LEGACY_NOT_EXISTS = 'legacy_not_exists'
STRIDE_FIRST_DATETIME = datetime.datetime(2022, 3, 15, 0).astimezone(pytz.timezone('israel'))
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
        if FORCE_UPDATE_IF_FILE_LAST_MODIFIED_BEFORE and file_last_modified < FORCE_UPDATE_IF_FILE_LAST_MODIFIED_BEFORE:
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
    print(f'Updating packages from {start_datehour} to {end_datehour} for up to 10 hours')
    current_datehour = start_datehour + datetime.timedelta(hours=1)
    num_is_stride_force, num_is_stride = 0, 0
    while current_datehour >= end_datehour and (datetime.datetime.now() - start_time).total_seconds() < 60 * 60 * 10:
        current_datehour -= datetime.timedelta(hours=1)
        force_update = current_datehour > (start_datehour - datetime.timedelta(days=5))
        if max_packages_per_type not in [None, 'None', 0, '0']:
            max_packages_per_type = int(max_packages_per_type)
            if (
                (force_update and num_is_stride_force >= max_packages_per_type)
                or (not force_update and num_is_stride >= max_packages_per_type)
            ):
                continue
        print(f'{datetime.datetime.now()} Updating package: {current_datehour} (force_update={force_update})')
        update_package_res = update_package(stats, current_datehour, force_update, verbose)
        if update_package_res in [UPDATE_PACKAGE_RES_PACKAGE_EXISTS, UPDATE_PACKAGE_RES_LEGACY_NOT_EXISTS]:
            pass
        else:
            stats_str = ','.join([f'{k}={stats[k]}' for k in sorted(stats.keys())])
            if update_package_res == UPDATE_PACKAGE_RES_SAME_HASH:
                print(f'No change ({stats_str})')
            else:
                print(f'Uploaded ({stats_str}): {update_package_res}')
        if force_update:
            num_is_stride_force += 1
        else:
            num_is_stride += 1
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


def legacy_get_siri_journey_ref(date, service_id):
    service_id = service_id.strip() or '0'
    return f'{date}-{service_id}'


def legacy_process_row(key, i, row, with_recorded_at_time=True):
    return {
        'id': f'{key}_{i}',
        'lat': row['lat'],
        'lon': row['lon'],
        **(
            {'recorded_at_time': legacy_get_datetime_field(row, ['date_recorded', 'date'], ['time_recorded'])}
            if with_recorded_at_time else {}
        ),
        'siri_scheduled_start_time': legacy_get_datetime_field(row, ['planned_start_date', 'date'], ['planned_start_time']),
        'siri_journey_ref': legacy_get_siri_journey_ref(row['date'], row.get('service_id')),
        'siri_vehicle_ref': row['bus_id'],
        'siri_stop_code': row.get('stop_point_ref'),
        'siri_operator_ref': row['agency_id'],
        'siri_line_ref': row['route_id'],
        'siri_snapshot_id': key,
        'gtfs_route_short_name': row['route_short_name'],
        'predicted_end_time': legacy_get_datetime_field(row, ['predicted_end_date', 'date'], ['predicted_end_time']),
        'date': row['date'],
        'num_duplicates': row.get('num_duplicates') or '',
    }


def iterate_legacy_packages_index_rows(index_path=None):
    with tempfile.TemporaryDirectory() as tmpdir:
        if not index_path:
            zip_path = os.path.join(tmpdir, 'package.zip')
            download_file('stride-etl-packages/siri/legacy-packages-index.zip', zip_path)
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                zip_ref.extractall(tmpdir)
            os.rename(os.path.join(tmpdir, 'legacy-packages-index.csv'), os.path.join(tmpdir, 'res_1.csv'))
            index_path = os.path.join(tmpdir, 'legacy-packages-index-metadata.json')
        for res in DF.Flow(
            DF.load(index_path, format='datapackage'),
        ).datastream().res_iter.get_iterator():
            for row in res:
                yield row


def iterate_legacy_packages_index(only_keys=None):
    existing_keys = {row['key']: row for row in iterate_legacy_packages_index_rows()}
    print(f'Found {len(existing_keys)} existing keys')
    keys_iterator = only_keys if only_keys else iterate_keys('obus-do1', 'SiriForSplunk')
    for keynum, key in enumerate(keys_iterator):
        if key in existing_keys:
            row = existing_keys[key]
            if row['key_processing_error']:
                print('reprocessing key due to key_processing_error')
            else:
                yield row
                continue
        print(f'Processing key {keynum+1}: {key}')
        max_recorded_at_time = None
        min_recorded_at_time = None
        num_rows = 0
        num_recorded_at_time_exceptions = 0
        num_row_processing_exceptions = 0
        key_processing_error = False
        # noinspection PyBroadException
        try:
            assert key.endswith('.csv.gz')
            with tempfile.TemporaryDirectory() as tmpdir:
                filename = os.path.join(tmpdir, 'legacy.csv.gz')
                download_legacy_file('obus-do1', key, filename)
                for res in DF.Flow(
                    DF.load(filename, cast_strategy=DF.load.CAST_TO_STRINGS, infer_strategy=DF.load.INFER_STRINGS, encoding='utf-8')
                ).datastream().res_iter.get_iterator():
                    for i, row in enumerate(res):
                        num_rows += 1
                        # noinspection PyBroadException
                        try:
                            legacy_process_row(key, i, row, with_recorded_at_time=False)
                        except Exception:
                            traceback.print_exc()
                            print(f'Error legacy processing row {i}: {row}')
                            num_row_processing_exceptions += 1
                        recorded_at_time = None
                        # noinspection PyBroadException
                        try:
                            recorded_at_time = legacy_get_datetime_field(row, ['date_recorded', 'date'], ['time_recorded'])
                        except Exception:
                            traceback.print_exc()
                            print(f'Error getting recorded_at_time for row {i}: {row}')
                            num_recorded_at_time_exceptions += 1
                        if recorded_at_time:
                            if not max_recorded_at_time or recorded_at_time > max_recorded_at_time:
                                max_recorded_at_time = recorded_at_time
                            if not min_recorded_at_time or recorded_at_time < min_recorded_at_time:
                                min_recorded_at_time = recorded_at_time
        except Exception:
            traceback.print_exc()
            print(f'Error processing key')
            key_processing_error = True
        yield {
            'key': key,
            'num_rows': num_rows,
            'max_recorded_at_time': max_recorded_at_time,
            'min_recorded_at_time': min_recorded_at_time,
            'num_recorded_at_time_exceptions': num_recorded_at_time_exceptions,
            'num_row_processing_exceptions': num_row_processing_exceptions,
            'key_processing_error': key_processing_error,
        }


def create_legacy_packages_index(only_keys=None, dump_to_path=False):
    if only_keys is not None:
        only_keys = [k.strip() for k in only_keys.split(',') if k.strip()]
        if not only_keys:
            only_keys = None
    print(f'create_legacy_packages_index(only_keys={only_keys}, dump_to_path={dump_to_path})')
    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = os.path.join('.data', 'legacy_packages_index') if dump_to_path else os.path.join(tmpdir, 'package')
        DF.Flow(
            iterate_legacy_packages_index(only_keys),
            DF.printer(),
            DF.dump_to_path(output_path)
        ).process()
        if not dump_to_path:
            upload_package(tmpdir, 'stride-etl-packages/siri/legacy-packages-index.zip', 'legacy-packages-index')


def legacy_update_package_hour(hour, min_recorded_at_times):
    num_row_errors = 0
    num_keys = 0
    num_rows = 0
    num_hour_rows = 0
    got_exception = False
    url = ''
    already_exists = False
    base_filename = hour.strftime('%Y-%m-%d.%H')
    package_path = hour.strftime('stride-etl-packages/siri/%Y/%m/') + base_filename + '.zip'
    if get_file_last_modified(package_path) is None:
        print(f'legacy_update_package_hour: {hour}')
        all_keys = set()
        for max_recorded_at_times in min_recorded_at_times.values():
            for keys in max_recorded_at_times.values():
                all_keys.update(keys)
        # noinspection PyBroadException
        try:
            all_rows_by_time = {}
            for key in all_keys:
                num_keys += 1
                with tempfile.TemporaryDirectory() as tmpdir:
                    download_legacy_file('obus-do1', key, os.path.join(tmpdir, 'legacy.csv.gz'))
                    for res in DF.Flow(
                        DF.load(os.path.join(tmpdir, 'legacy.csv.gz'), cast_strategy=DF.load.CAST_TO_STRINGS, infer_strategy=DF.load.INFER_STRINGS, encoding='utf-8'),
                    ).datastream().res_iter.get_iterator():
                        for i, row in enumerate(res):
                            num_rows += 1
                            outrow = None
                            # noinspection PyBroadException
                            try:
                                outrow = legacy_process_row(key, i, row)
                            except Exception:
                                num_row_errors += 1
                                traceback.print_exc()
                                print(f'Error legacy processing row {i}: {row}')
                            if outrow:
                                recorded_at_time = datetime.datetime.fromisoformat(outrow['recorded_at_time'])
                                if hour <= recorded_at_time < hour + datetime.timedelta(hours=1):
                                    if recorded_at_time not in all_rows_by_time:
                                        all_rows_by_time[recorded_at_time] = []
                                    all_rows_by_time[recorded_at_time].append({
                                        k: v for k, v in outrow.items() if k != 'recorded_at_time'
                                    })

            def iterator():
                for recorded_at_time in sorted(all_rows_by_time.keys()):
                    for row in all_rows_by_time[recorded_at_time]:
                        yield {
                            'recorded_at_time': recorded_at_time.isoformat(),
                            **row,
                        }

            with tempfile.TemporaryDirectory() as tmpdir:
                DF.Flow(
                    iterator(),
                    DF.dump_to_path(os.path.join(tmpdir, 'package'))
                ).process()
                url = upload_package(tmpdir, package_path, base_filename)
        except Exception:
            traceback.print_exc()
            got_exception = True
    else:
        already_exists = True
    return {
        'hour': hour,
        'num_row_errors': num_row_errors,
        'num_keys': num_keys,
        'num_rows': num_rows,
        'num_hour_rows': num_hour_rows,
        'got_exception': got_exception,
        'url': url,
        'already_exists': already_exists,
    }


def legacy_update_packages_from_index(index_from_path=False):
    hour_keys = {}
    num_keys = 0
    with tempfile.TemporaryDirectory() as tmpdir:
        if index_from_path:
            index_path = os.path.join('.data', 'legacy_packages_index', 'datapackage.json')
            report_path = os.path.join('.data', 'siri_legacy_packages_report')
        else:
            index_path = None
            report_path = os.path.join(tmpdir, 'report')
        for row in iterate_legacy_packages_index_rows(index_path):
            if row['key_processing_error']:
                continue
            num_keys += 1
            min_recorded_at_time = datetime.datetime.fromisoformat(row['min_recorded_at_time'])
            max_recorded_at_time = datetime.datetime.fromisoformat(row['max_recorded_at_time'])
            current_hour = min_recorded_at_time.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=1)
            end_hour = max_recorded_at_time.replace(minute=0, second=0, microsecond=0) - datetime.timedelta(hours=1)
            while current_hour <= end_hour:
                current_hour += datetime.timedelta(hours=1)
                if current_hour not in hour_keys:
                    hour_keys[current_hour] = {}
                if min_recorded_at_time not in hour_keys[current_hour]:
                    hour_keys[current_hour][min_recorded_at_time] = {}
                if max_recorded_at_time not in hour_keys[current_hour][min_recorded_at_time]:
                    hour_keys[current_hour][min_recorded_at_time][max_recorded_at_time] = []
                hour_keys[current_hour][min_recorded_at_time][max_recorded_at_time].append(row['key'])
        print(f'Processing {num_keys} keys')
        DF.Flow(
            (legacy_update_package_hour(hour, hour_keys[hour]) for hour in sorted(hour_keys.keys())),
            DF.dump_to_path(os.path.join(report_path, 'package'))
        ).process()
        if not index_from_path:
            upload_package(report_path, f'stride-etl-packages/siri/legacy-packages-update-report-{now().isoformat()}.zip', 'legacy-packages-update-report')
