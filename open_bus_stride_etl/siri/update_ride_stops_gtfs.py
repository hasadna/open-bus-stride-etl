import datetime
from pprint import pprint
from textwrap import dedent
from collections import defaultdict

from sqlalchemy.engine import ResultProxy

from open_bus_stride_db import db

from .common import iterate_siri_route_id_dates


def main():
    stats = defaultdict(int)
    for date, siri_route_ids in iterate_siri_route_id_dates(
        extra_from_sql='gtfs_stop, siri_stop, siri_ride_stop',
        where_sql=dedent("""
            siri_ride_stop.siri_stop_id = siri_stop.id
            and siri_ride_stop.siri_ride_id = siri_ride.id
            and siri_ride.updated_duration_minutes is not null
            and siri_ride_stop.gtfs_stop_id is null
            and gtfs_stop.code = siri_stop.code
            and gtfs_stop.date >= '{min_date}'
            and siri_ride.scheduled_start_time >= '{min_date}'
            and gtfs_stop.date = date_trunc('day', siri_ride.scheduled_start_time)
        """).format(min_date=(datetime.datetime.now() - datetime.timedelta(days=5)).strftime('%Y-%m-%d'))
    ):
        for siri_route_id in siri_route_ids:
            stats['updated_siri_routes'] += 1
            with db.get_session() as session:
                res: ResultProxy = session.execute(dedent("""
                    update siri_ride_stop
                    set gtfs_stop_id = gtfs_stop.id
                    from siri_stop, siri_ride, gtfs_stop
                    where siri_ride_stop.siri_stop_id = siri_stop.id
                    and siri_ride_stop.siri_ride_id = siri_ride.id
                    and siri_ride.updated_duration_minutes is not null
                    and siri_ride_stop.gtfs_stop_id is null
                    and gtfs_stop.code = siri_stop.code
                    and gtfs_stop.date = '{}'
                    and siri_ride.siri_route_id = {}
                """).format(date, siri_route_id))
                stats['updated_ride_stops'] += res.rowcount
                session.commit()
                pprint(dict(stats))
