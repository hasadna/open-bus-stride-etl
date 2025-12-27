from pprint import pprint
from textwrap import dedent
from collections import defaultdict

from sqlalchemy.engine import ResultProxy, Row

from open_bus_stride_db.db import session_decorator, Session
from open_bus_stride_db.model import SiriRide
from open_bus_stride_etl import common


# we don't use a limit in this query due to a known PostgreSQL bug: https://www.postgresql.org/message-id/flat/CA%2BU5nMLbXfUT9cWDHJ3tpxjC3bTWqizBKqTwDgzebCB5bAGCgg%40mail.gmail.com
# because this query always returns a low number of rows, we limit the number of rows in the code
GET_FIRST_LAST_SQL_QUERY_TEMPLATE = dedent("""
    select siri_vehicle_location.id, siri_vehicle_location.recorded_at_time
    from siri_vehicle_location, siri_ride_stop, siri_ride
    where siri_vehicle_location.siri_ride_stop_id = siri_ride_stop.id
        and siri_ride_stop.siri_ride_id = siri_ride.id
        and siri_ride.id = {siri_ride_id}
    order by siri_vehicle_location.recorded_at_time {order_by} nulls last
""")


def get_first_and_last_rows(session, siri_ride_id):
    result: ResultProxy = session.execute(GET_FIRST_LAST_SQL_QUERY_TEMPLATE.format(
        siri_ride_id=siri_ride_id, order_by="asc"
    ))
    rows = result.fetchall()
    if not rows:
        return None, None

    first = rows[0]
    last = rows[-1]

    def make_dict(row):
        return {
            'id': int(row.id) if row.id else None,
            'recorded_at_time': common.utc(row.recorded_at_time) if row.recorded_at_time else None
        }

    return make_dict(first), make_dict(last)

def update_first_last_vehicle_locations(siri_ride, first_row, last_row, stats):
    is_updated = False
    if first_row and first_row['id'] != siri_ride.first_vehicle_location_id:
        siri_ride.first_vehicle_location_id = first_row['id']
        is_updated = True
        stats['num_rows_first_vehicle_location_updated'] += 1
    if last_row and last_row['id'] != siri_ride.last_vehicle_location_id:
        siri_ride.last_vehicle_location_id = last_row['id']
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
            and first_row['recorded_at_time']
            and last_row['recorded_at_time']
            and first_row['recorded_at_time'] < last_row['recorded_at_time'] < common.now_minus(hours=6)
    ):
        siri_ride.duration_minutes = round((last_row['recorded_at_time'] - first_row['recorded_at_time']).total_seconds() / 60)
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

    # Time window: last 7 hours
    end_time = common.now()
    start_time = end_time - timedelta(hours=7)

    # Process in 14 × 30-minute chunks
    chunk_size = timedelta(minutes=30)
    chunk_start = start_time

    while chunk_start < end_time:
        chunk_end = chunk_start + chunk_size

        print(f"Processing rides between {chunk_start} and {chunk_end}")

        query = session.query(SiriRide).filter(
            SiriRide.updated_duration_minutes == None,
            SiriRide.scheduled_start_time >= chunk_start,
            SiriRide.scheduled_start_time < chunk_end
        )

        total_rows = query.count()
        print(f"Total rows in this chunk: {total_rows}")

        for siri_ride in query:
            first_row, last_row = get_first_and_last_rows(session, siri_ride.id)
            update_first_last_vehicle_locations(siri_ride, first_row, last_row, stats)
            update_duration_minutes(siri_ride, first_row, last_row, stats)
            stats['num_rows'] += 1

        session.commit()         # commit changes for this chunk
        session.expunge_all()    # free memory for this chunk

        chunk_start = chunk_end  # move to next slice

    pprint(dict(stats))
    print(f"Processed total rows: {stats['num_rows']}")
