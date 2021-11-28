import datetime
from pprint import pprint
from textwrap import dedent
from collections import defaultdict

import pytz
from sqlalchemy.engine import ResultProxy, Row

from open_bus_stride_db.db import session_decorator, Session
from open_bus_stride_db.model import SiriRide
from open_bus_stride_etl import common


GET_FIRST_LAST_SQL_QUERY_TEMPLATE = dedent("""
    select siri_vehicle_location.id, siri_vehicle_location.recorded_at_time
    from siri_vehicle_location, siri_ride_stop, siri_ride
    where siri_vehicle_location.siri_ride_stop_id = siri_ride_stop.id
        and siri_ride_stop.siri_ride_id = siri_ride.id
        and siri_ride.id = {siri_ride_id}
    order by siri_vehicle_location.recorded_at_time {order_by} nulls last
    limit 1
""")


def get_first_last_row(session, siri_ride_id, order_by):
    result: ResultProxy = session.execute(GET_FIRST_LAST_SQL_QUERY_TEMPLATE.format(
        siri_ride_id=siri_ride_id, order_by=order_by)
    )
    row: Row = result.one_or_none()
    return row


def update_first_last_vehicle_locations(siri_ride, first_row, last_row, stats):
    is_updated = False
    if first_row and first_row.id != siri_ride.first_vehicle_location_id:
        siri_ride.first_vehicle_location_id = first_row.id
        is_updated = True
        stats['num_rows_first_vehicle_location_updated'] += 1
    if last_row and last_row.id != siri_ride.last_vehicle_location_id:
        siri_ride.last_vehicle_location_id = last_row.id
        is_updated = True
        stats['num_rows_last_vehicle_location_updated'] += 1
    if not siri_ride.updated_first_last_vehicle_locations:
        is_updated = True
        stats['num_rows_missing_first_last_last_updated'] += 1
    if is_updated:
        siri_ride.updated_first_last_vehicle_locations = common.now()


def update_duration_minutes(siri_ride, first_row, last_row, stats):
    is_updated = False
    if (
            first_row and last_row
            and first_row.recorded_at_time
            and last_row.recorded_at_time
            and common.utc(first_row.recorded_at_time) < common.utc(last_row.recorded_at_time) < common.now_minus(hours=6)
    ):
        siri_ride.duration_minutes = round((last_row.recorded_at_time - first_row.recorded_at_time).total_seconds() / 60)
        stats['num_rows_updated_duration_minutes'] += 1
        is_updated = True
    elif siri_ride.updated_first_last_vehicle_locations < common.now_minus(days=2):
        siri_ride.duration_minutes = 0
        stats['num_rows_too_old_not_updated_duration_minutes'] += 1
        is_updated = True
    if is_updated:
        siri_ride.updated_duration_minutes = common.now()


@session_decorator
def main(session: Session):
    stats = defaultdict(int)
    query = session.query(SiriRide).filter(SiriRide.updated_duration_minutes == None)
    total_rows = query.count()
    print("Total rows to update: {}".format(total_rows))
    siri_ride: SiriRide
    for siri_ride in query:
        first_row = get_first_last_row(session, siri_ride.id, 'asc')
        last_row = get_first_last_row(session, siri_ride.id, 'desc')
        update_first_last_vehicle_locations(siri_ride, first_row, last_row, stats)
        update_duration_minutes(siri_ride, first_row, last_row, stats)
        stats['num_rows'] += 1
        if stats['num_rows'] % 10000 == 0:
            pprint(dict(stats))
            print("Processed {} / {} rows..".format(stats['num_rows'], total_rows))
            session.commit()
    pprint(dict(stats))
    print("Processed {} / {} rows..".format(stats['num_rows'], total_rows))
    session.commit()
