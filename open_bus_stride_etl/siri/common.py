from textwrap import dedent

from .. import common

from open_bus_stride_db import db


# A ride's SIRI data keeps growing while the ride is in progress - siri-etl appends
# siri_ride_stop / siri_vehicle_location rows for as long as snapshots for it keep arriving - so
# the GTFS-matching jobs may only touch rides which are already over, otherwise they leave the
# ride's later stops unmatched. 12 hours after the scheduled start covers the longest scheduled
# ride plus SIRI ingestion lag.
#
# scheduled_start_time is "timestamp without time zone" holding UTC (DateTimeWithTimeZone strips
# the offset on write), so now() is converted to naive UTC as well: a bare now() is a timestamptz,
# and comparing it against this column casts it using the session's TimeZone setting, which shifts
# the interval by the local UTC offset.
RIDE_DATA_SETTLED_HOURS = 12
RIDE_DATA_SETTLED_SQL = (
    "siri_ride.scheduled_start_time < (now() at time zone 'utc') - interval '{} hours'".format(
        RIDE_DATA_SETTLED_HOURS
    )
)


def iterate_siri_route_id_dates(where_sql=None, extra_from_sql=None):
    if where_sql:
        where_sql = 'where {}'.format(where_sql)
    else:
        where_sql = ''
    if extra_from_sql:
        extra_from_sql = ', {}'.format(extra_from_sql)
    else:
        extra_from_sql = ''
    date_siri_route_ids = {}
    with common.print_memory_usage("Getting siri_route_ids / dates..."):
        with db.get_session() as session:
            sql = dedent("""
                select date_trunc('day', siri_ride.scheduled_start_time) scheduled_start_date, siri_ride.siri_route_id
                from siri_ride {}
                {}
                group by date_trunc('day', siri_ride.scheduled_start_time), siri_ride.siri_route_id
                order by date_trunc('day', siri_ride.scheduled_start_time), siri_ride.siri_route_id
            """).format(extra_from_sql, where_sql)
            print(sql)
            for row in session.execute(sql):
                date_siri_route_ids.setdefault(row.scheduled_start_date.strftime('%Y-%m-%d'), set()).add(row.siri_route_id)
    if len(date_siri_route_ids) > 0:
        print("Date: num siri route ids")
        for date, siri_route_ids in date_siri_route_ids.items():
            print("{}: {}".format(date, len(siri_route_ids)))
        print("Iterating over date / siri route ids")
        for date, siri_route_ids in date_siri_route_ids.items():
            with common.print_memory_usage("Processing date {} ({} route ids)".format(date, len(siri_route_ids))):
                yield date, siri_route_ids
    else:
        print("No relevant date/siri route ids found")
