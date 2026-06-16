import datetime
from pprint import pprint
from textwrap import dedent
from collections import defaultdict

import pytz
from sqlalchemy.engine import ResultProxy, Row

from open_bus_stride_db.db import session_decorator, Session
from open_bus_stride_db.model import SiriRide
from open_bus_stride_etl import common


# Number of siri_ride rows fetched and processed per batch. We must NOT load the
# whole result set at once: with psycopg2 + SQLAlchemy<2 a plain `for x in query`
# uses a client-side buffered cursor that materializes every matching row (plus
# its ORM object) in memory. That OOM is why the task was disabled in Nov 2024
# (commit d75da16, "uses a lot of RAM"); the disable in turn silently broke the
# whole SIRI->GTFS enrichment chain, the bug this branch fixes by re-enabling the
# task (https://github.com/hasadna/open-bus-stride-etl/issues/22). The naive load
# was fine for a 4-day window, fatal once a backlog built up, so we paginate by
# primary key (keyset) instead: memory stays bounded to BATCH_SIZE rows regardless
# of how far behind the task is. A server-side cursor (stream_results) is not an
# option here because we commit mid-iteration, which would invalidate the cursor.
BATCH_SIZE = 1000


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


def get_first_last_row(session, siri_ride_id, order_by):
    result: ResultProxy = session.execute(GET_FIRST_LAST_SQL_QUERY_TEMPLATE.format(
        siri_ride_id=siri_ride_id, order_by=order_by)
    )
    row: Row = result.first()
    return {
        'id': int(row.id) if row.id else None,
        'recorded_at_time': common.utc(row.recorded_at_time) if row.recorded_at_time else None
    } if row else None


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


def get_scheduled_start_time_filters(min_date, max_date, num_days):
    """Return (orm_filters, sql_where) restricting siri_ride by scheduled_start_time.

    Mirrors the other siri tasks: the window is always [min_date, max_date] via
    parse_min_max_date_strs (min_date defaults to today-num_days, max_date to today).
    Like those tasks, the bounds are day boundaries (00:00), so the max_date day
    itself is excluded -- e.g. the default window ends at the start of today, the
    same way update_rides_gtfs etc. process up to (not including) today. Making the
    window inclusive / same-day is a separate change planned across all four siri
    tasks together. The cutoff datetimes are computed once so the ORM keyset query
    and the raw id-range lookup use the exact same bounds.

    (scheduled_start_time is timezone-aware, so we compare against tz-aware
    datetimes; the sibling tasks pass date strings to raw SQL, which Postgres
    interprets as the same UTC midnight given the connection's utc timezone.)"""
    min_date, max_date = common.parse_min_max_date_strs(min_date, max_date, num_days)
    print('min_date={} max_date={}'.format(min_date, max_date))
    min_dt = pytz.UTC.localize(datetime.datetime.combine(min_date, datetime.time.min))
    max_dt = pytz.UTC.localize(datetime.datetime.combine(max_date, datetime.time.min))
    return [SiriRide.scheduled_start_time >= min_dt, SiriRide.scheduled_start_time <= max_dt], \
        "scheduled_start_time >= '{}' and scheduled_start_time <= '{}'".format(min_dt.isoformat(), max_dt.isoformat())


@session_decorator
def main(session: Session, min_date=None, max_date=None, num_days=4):
    stats = defaultdict(int)
    sched_filters, sched_sql = get_scheduled_start_time_filters(min_date, max_date, num_days)
    # Find the TRUE id range of the window cheaply. A plain `min(id) WHERE
    # scheduled_start_time ...` makes the planner walk the pk index from the
    # bottom (scanning every older row), because the date filter and the pk are
    # different columns. The MATERIALIZED CTE forces it to first pull the window's
    # ids via the scheduled_start_time index, then take min/max over that small set.
    id_range = session.execute(dedent("""
        with window_rows as materialized (
            select id from siri_ride where {}
        )
        select min(id) min_id, max(id) max_id from window_rows
    """).format(sched_sql)).first()
    if id_range.min_id is None:
        print("No siri_rides in the requested window")
        return
    min_id, max_id = id_range.min_id, id_range.max_id
    print("Window id range: {}..{}".format(min_id, max_id))
    # Keyset pagination over [min_id, max_id], fetching at most BATCH_SIZE rows per
    # batch so memory stays bounded regardless of how many rows match. Seeding
    # last_id at min_id-1 keeps the first batch from scanning all the older rows
    # below the window; bounding by `id <= max_id` keeps the final batch
    # from scanning newer rows above it (matters for a past-dated backfill). The
    # `id > last_id` bound guarantees forward progress, so there's no infinite loop
    # even for rows that legitimately stay updated_duration_minutes=NULL this run.
    last_id = min_id - 1
    siri_ride: SiriRide
    while True:
        rides = session.query(SiriRide).filter(
            SiriRide.updated_duration_minutes == None,
            SiriRide.id > last_id,
            SiriRide.id <= max_id,
            *sched_filters
        ).order_by(SiriRide.id).limit(BATCH_SIZE).all()
        if not rides:
            break
        for siri_ride in rides:
            last_id = siri_ride.id
            first_row = get_first_last_row(session, siri_ride.id, 'asc')
            last_row = get_first_last_row(session, siri_ride.id, 'desc')
            update_first_last_vehicle_locations(siri_ride, first_row, last_row, stats)
            update_duration_minutes(siri_ride, first_row, last_row, stats)
            stats['num_rows'] += 1
        session.commit()
        pprint(dict(stats))
        print("Processed {} rows (up to id {})..".format(stats['num_rows'], last_id))
    pprint(dict(stats))
    print("Processed {} rows total".format(stats['num_rows']))
