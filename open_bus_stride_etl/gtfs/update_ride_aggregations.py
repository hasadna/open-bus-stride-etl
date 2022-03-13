from pprint import pprint
from collections import defaultdict

from open_bus_stride_db import model, db

from .. import common

@db.session_decorator
def main(session: db.Session, date):
    date = common.parse_date_str(date)
    print("Updating ride aggregations for date {}".format(date))
    stats = defaultdict(int)
    for gtfs_ride in session.query(model.GtfsRide).join(model.GtfsRoute.gtfs_rides).where(model.GtfsRoute.date == date):
        stats['total rides'] += 1
        gtfs_ride_stops = sorted(gtfs_ride.gtfs_ride_stops,
                                 key=lambda gtfs_ride_stop: gtfs_ride_stop.stop_sequence)
        if len(gtfs_ride_stops) > 0:
            gtfs_ride.first_gtfs_ride_stop_id = gtfs_ride_stops[0].id
            gtfs_ride.last_gtfs_ride_stop_id = gtfs_ride_stops[-1].id
            if gtfs_ride.first_gtfs_ride_stop_id == gtfs_ride.last_gtfs_ride_stop_id:
                stats['rides with same first/last stop'] += 1
            else:
                stats['rides with valid first/last stops'] += 1
            gtfs_ride.start_time = session.query(model.GtfsRideStop).get(gtfs_ride.first_gtfs_ride_stop_id).departure_time
            gtfs_ride.end_time = session.query(model.GtfsRideStop).get(gtfs_ride.last_gtfs_ride_stop_id).arrival_time
        else:
            stats['rides without first/last stops'] +=1
            gtfs_ride.first_gtfs_ride_stop_id = None
            gtfs_ride.last_gtfs_ride_stop_id = None
            gtfs_ride.start_time = None
            gtfs_ride.end_time = None
    session.commit()
    pprint(dict(stats))

