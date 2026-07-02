import datetime

import pytz
from sqlalchemy import text

from open_bus_stride_db.db import session_decorator, Session
from open_bus_stride_etl import common


UPDATE_FIRST_LAST_VEHICLE_LOCATIONS_AND_DURATION = """
    UPDATE siri_ride sr
    SET 
        first_vehicle_location_id = locations.first_id,
        last_vehicle_location_id = locations.last_id,
        duration_minutes = ROUND(EXTRACT(EPOCH FROM (locations.max_time - locations.min_time)) / 60),
        updated_first_last_vehicle_locations = NOW(),
        updated_duration_minutes = NOW()
    FROM (
        SELECT 
            sr.id,
            FIRST_VALUE(svl.id) OVER (PARTITION BY sr.id ORDER BY svl.recorded_at_time ASC) as first_id,
            FIRST_VALUE(svl.id) OVER (PARTITION BY sr.id ORDER BY svl.recorded_at_time DESC) as last_id,
            MIN(svl.recorded_at_time) OVER (PARTITION BY sr.id) as min_time,
            MAX(svl.recorded_at_time) OVER (PARTITION BY sr.id) as max_time
        FROM siri_ride sr
        INNER JOIN siri_ride_stop srs ON sr.id = srs.siri_ride_id
        INNER JOIN siri_vehicle_location svl ON svl.siri_ride_stop_id = srs.id
        WHERE sr.scheduled_start_time >= :min_dt
            AND sr.scheduled_start_time < :max_dt
            AND sr.updated_duration_minutes IS NULL
    ) locations
    WHERE sr.id = locations.id
        AND locations.max_time < NOW() - INTERVAL '6 hours';
"""

SET_DURATION_OLD_TRIPS = """
    UPDATE siri_ride
    SET 
        duration_minutes = 0,
        updated_duration_minutes = NOW()
    WHERE scheduled_start_time >= :min_dt
        AND scheduled_start_time < :max_dt
        AND updated_first_last_vehicle_locations < NOW() - INTERVAL '2 days'
        AND updated_duration_minutes IS NULL;
"""


@session_decorator
def main(session: Session, min_date=None, max_date=None, num_days=4):
    stats = {
        "num_rows_updated_duration_minutes": 0,
        "num_rows_too_old_not_updated_duration_minutes": 0,
    }

    min_date, max_date = common.parse_min_max_date_strs(min_date, max_date, num_days)
    print("min_date={} max_date={}".format(min_date, max_date))
    min_dt = pytz.UTC.localize(datetime.datetime.combine(min_date, datetime.time.min))
    max_dt = pytz.UTC.localize(datetime.datetime.combine(max_date, datetime.time.min))

    result = session.execute(
        text(UPDATE_FIRST_LAST_VEHICLE_LOCATIONS_AND_DURATION),
        {"min_dt": min_dt, "max_dt": max_dt},
    )
    stats["num_rows_updated_duration_minutes"] = result.rowcount
    session.commit()
    print(f"Query 1: Updated {result.rowcount} rows with duration and location data")

    result_cleanup = session.execute(
        text(SET_DURATION_OLD_TRIPS), {"min_dt": min_dt, "max_dt": max_dt}
    )
    stats["num_rows_too_old_not_updated_duration_minutes"] = result_cleanup.rowcount
    session.commit()
    print(f"Query 2: Set {result_cleanup.rowcount} rows to duration=0")
    print(f"\nFinal stats: {stats}")
