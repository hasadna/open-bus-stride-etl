from pprint import pprint
from textwrap import dedent
from collections import defaultdict

from sqlalchemy.engine import ResultProxy

from open_bus_stride_db import db, model

from .. import common


@db.session_decorator
def main(session: db.Session):
    with common.print_memory_usage('updating gtfs_stop_id in siri_ride_stop table...'):
        res: ResultProxy = session.execute(dedent("""
            update siri_ride_stop
            set gtfs_stop_id = gtfs_stop.id
            from siri_stop, siri_ride, gtfs_stop
            where siri_ride_stop.siri_stop_id = siri_stop.id
            and siri_ride_stop.siri_Ride_id = siri_ride.id
            and siri_ride.updated_duration_minutes is not null
            and siri_ride_stop.gtfs_stop_id is null
            and gtfs_stop.code = siri_stop.code
            and gtfs_stop.date = date_trunc('day', siri_ride.scheduled_start_time)
        """))
        print('updated {} rows'.format(res.rowcount))
    with common.print_memory_usage('committing...'):
        session.commit()
