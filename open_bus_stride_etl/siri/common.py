from textwrap import dedent

from .. import common

from open_bus_stride_db import db


def iterate_siri_route_id_dates(extra_where_sql=None):
    if extra_where_sql:
        extra_where_sql = 'and {}'.format(extra_where_sql)
    else:
        extra_where_sql = ''
    date_siri_route_ids = {}
    with common.print_memory_usage("Getting siri_route_ids / dates..."):
        with db.get_session() as session:
            for row in session.execute(dedent("""
                        select date_trunc('day', siri_ride.scheduled_start_time) scheduled_start_date, siri_ride.siri_route_id
                        from siri_ride_stop, siri_ride
                        where siri_ride.id = siri_ride_stop.siri_ride_id
                        {}
                        group by date_trunc('day', siri_ride.scheduled_start_time), siri_ride.siri_route_id
                        order by date_trunc('day', siri_ride.scheduled_start_time), siri_ride.siri_route_id
                    """).format(extra_where_sql)):
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
