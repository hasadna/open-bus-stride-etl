import datetime
from pprint import pprint
from textwrap import dedent
from collections import defaultdict

from open_bus_stride_db import db

from .common import iterate_siri_route_id_dates
from ..common import parse_min_max_date_strs, get_db_date_str


UPDATE_ROUTE_GTFS_RIDE_SQL_TEMPLATE = dedent("""
    update siri_ride
    set route_gtfs_ride_id = gtfs_ride.id
    from gtfs_ride, gtfs_route, siri_route
    where 
    gtfs_route.id = gtfs_ride.gtfs_route_id
    and gtfs_route.operator_ref = siri_route.operator_ref
    and gtfs_route.line_ref = siri_route.line_ref
    and siri_route.id = siri_ride.siri_route_id
    and gtfs_route.date = '{date}'
    and siri_ride.scheduled_start_time > gtfs_ride.start_time - '{minutes} minutes'::interval
    and siri_ride.scheduled_start_time < gtfs_ride.start_time + '{minutes} minutes'::interval
    -- if we have updated_duration_minutes it means we updated the duration of the ride
    -- so we have all the ride stops data which we must ensure before making these updates
    and siri_ride.updated_duration_minutes is not null
    {extra_where}
""")

def main(min_date, max_date, num_days):
    min_date, max_date = parse_min_max_date_strs(min_date, max_date, num_days)
    print(f'min_date={min_date}')
    print(f'max_date={max_date}')
    stats = defaultdict(int)
    for date, siri_route_ids in iterate_siri_route_id_dates(
        where_sql=dedent("""
            siri_ride.gtfs_ride_id is null
            and siri_ride.scheduled_start_time >= '{min_date}'
            and siri_ride.scheduled_start_time <= '{max_date}'
            -- if we have updated_duration_minutes it means we updated the duration of the ride
            -- so we have all the ride stops data which we must ensure before making these updates
            and siri_ride.updated_duration_minutes is not null
        """).format(min_date=get_db_date_str(min_date), max_date=get_db_date_str(max_date))
    ):
        updated_journey_gtfs_ride_ids = 0
        updated_route_gtfs_ride_ids = 0
        updated_gtfs_ride_ids_by_route = 0
        updated_gtfs_ride_ids_by_journey = 0
        with db.get_session() as session:
            res = session.execute(dedent("""
                set local synchronous_commit to off;
                update siri_ride
                set journey_gtfs_ride_id = gtfs_ride.id
                from gtfs_ride, gtfs_route
                where gtfs_ride.journey_ref = split_part(siri_ride.journey_ref, '-', 4) || '_' || split_part(siri_ride.journey_ref, '-', 3) || split_part(siri_ride.journey_ref, '-', 2) || substr(split_part(siri_ride.journey_ref, '-', 1), 3)
                and gtfs_route.id = gtfs_ride.gtfs_route_id
                and gtfs_route.date = '{}'
                -- if we have updated_duration_minutes it means we updated the duration of the ride
                -- so we have all the ride stops data which we must ensure before making these updates
                and siri_ride.updated_duration_minutes is not null;
            """).format(date))
            updated_journey_gtfs_ride_ids += res.rowcount
            updated_route_gtfs_ride_ids += session.execute(
                UPDATE_ROUTE_GTFS_RIDE_SQL_TEMPLATE.format(
                    date=date, minutes='1',
                    extra_where=''
                )
            ).rowcount
            updated_route_gtfs_ride_ids += session.execute(
                UPDATE_ROUTE_GTFS_RIDE_SQL_TEMPLATE.format(
                    date=date, minutes='3',
                    extra_where='and siri_ride.route_gtfs_ride_id is null'
                )
            ).rowcount
            updated_route_gtfs_ride_ids += session.execute(
                UPDATE_ROUTE_GTFS_RIDE_SQL_TEMPLATE.format(
                    date=date, minutes='5',
                    extra_where='and siri_ride.route_gtfs_ride_id is null'
                )
            ).rowcount
            updated_gtfs_ride_ids_by_route += session.execute(dedent("""
                update siri_ride
                set gtfs_ride_id = gtfs_ride.id
                from gtfs_ride, gtfs_route
                where gtfs_ride.id = siri_ride.route_gtfs_ride_id
                and gtfs_route.id = gtfs_ride.gtfs_route_id
                and gtfs_route.date = '{}'
                and siri_ride.journey_gtfs_ride_id is null
            """).format(date)).rowcount
            updated_gtfs_ride_ids_by_journey += session.execute(dedent("""
                update siri_ride
                set gtfs_ride_id = gtfs_ride.id
                from gtfs_ride, gtfs_route
                where gtfs_ride.id = siri_ride.journey_gtfs_ride_id
                and gtfs_route.id = gtfs_ride.gtfs_route_id
                and gtfs_route.date = '{}'
            """).format(date)).rowcount
            session.commit()
        print(f"Updated route gtfs ride ids: {updated_route_gtfs_ride_ids}")
        print(f"Updated journey gtfs ride ids: {updated_journey_gtfs_ride_ids}")
        print(f"Updated gtfs ride ids by journey: {updated_gtfs_ride_ids_by_journey}")
        print(f"Updated gtfs ride ids by route: {updated_gtfs_ride_ids_by_route}")
        stats['updated_route_gtfs_ride_ids'] += updated_route_gtfs_ride_ids
        stats['updated_journey_gtfs_ride_ids'] += updated_journey_gtfs_ride_ids
        stats['updated_gtfs_ride_ids_by_journey'] += updated_gtfs_ride_ids_by_journey
        stats['updated_gtfs_ride_ids_by_route'] += updated_gtfs_ride_ids_by_route
        pprint(dict(stats))
