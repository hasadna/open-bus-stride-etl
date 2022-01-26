import datetime
from pprint import pprint
from textwrap import dedent
from collections import defaultdict

from geopy.distance import distance

from open_bus_stride_db import db

from .common import iterate_siri_route_id_dates


def process_ride_rows(rows, session, stats):
    vehicle_location_distance = {}
    ride_stop_nearest_distance = {}
    ride_stop_nearest_vehicle_location = {}
    for row in rows:
        distance_meters = distance(
            (row.siri_vehicle_location_lat, row.siri_vehicle_location_lon),
            (row.gtfs_stop_lat, row.gtfs_stop_lon)
        ).m
        vehicle_location_distance[row.siri_vehicle_location_id] = distance_meters
        if (
            row.siri_ride_stop_id not in ride_stop_nearest_distance
            or distance_meters < ride_stop_nearest_distance[row.siri_ride_stop_id]
        ):
            ride_stop_nearest_distance[row.siri_ride_stop_id] = distance_meters
            ride_stop_nearest_vehicle_location[row.siri_ride_stop_id] = row.siri_vehicle_location_id
    updates = [
        'set local synchronous_commit to off;'
    ]
    for vehicle_location_id, distance_meters in vehicle_location_distance.items():
        stats['updated_vehicle_locations'] += 1
        updates.append(dedent("""
            update siri_vehicle_location 
            set distance_from_siri_ride_stop_meters={}
            where id={}
        """).format(round(distance_meters), vehicle_location_id))
    for ride_stop_id, vehicle_location_id in ride_stop_nearest_vehicle_location.items():
        stats['updated_ride_stops'] += 1
        updates.append(dedent("""
            update siri_ride_stop
            set nearest_siri_vehicle_location_id={}
            where id={}
        """).format(vehicle_location_id, ride_stop_id))
    session.execute(";\n".join(updates))


def main():
    stats = defaultdict(int)
    for date, siri_route_ids in iterate_siri_route_id_dates(
        extra_from_sql='siri_ride_stop',
        where_sql=dedent("""
            siri_ride.id = siri_ride_stop.siri_ride_id
            and siri_ride_stop.nearest_siri_vehicle_location_id is null
            and siri_ride_stop.gtfs_stop_id is not null
            and siri_ride.scheduled_start_time >= '{min_date}'
        """).format(min_date=(datetime.datetime.now() - datetime.timedelta(days=5)).strftime('%Y-%m-%d'))
    ):
        for siri_route_id in siri_route_ids:
            with db.get_session() as session:
                current_ride = {}
                for row in session.execute(dedent("""
                    select siri_ride.id siri_ride_id, 
                        siri_ride_stop.id siri_ride_stop_id,
                        siri_vehicle_location.id siri_vehicle_location_id,
                        siri_vehicle_location.lon siri_vehicle_location_lon, 
                        siri_vehicle_location.lat siri_vehicle_location_lat, 
                        gtfs_stop.lon gtfs_stop_lon, 
                        gtfs_stop.lat gtfs_stop_lat
                    from siri_ride, siri_ride_stop, siri_vehicle_location, gtfs_stop
                    where siri_ride.siri_route_id = {}
                    and date_trunc('day', siri_ride.scheduled_start_time) = '{}'
                    and siri_ride_stop.siri_ride_id = siri_ride.id
                    and siri_vehicle_location.siri_ride_stop_id = siri_ride_stop.id
                    and gtfs_stop.id = siri_ride_stop.gtfs_stop_id
                    order by siri_ride.id, siri_vehicle_location.recorded_at_time
                """.format(siri_route_id, date))):
                    if current_ride.get('id') == row.siri_ride_id:
                        current_ride['rows'].append(row)
                    else:
                        if current_ride.get('rows'):
                            process_ride_rows(current_ride['rows'], session, stats)
                        current_ride['id'] = row.siri_ride_id
                        current_ride['rows'] = []
                if current_ride.get('rows'):
                    process_ride_rows(current_ride['rows'], session, stats)
                session.commit()
                pprint(dict(stats))
