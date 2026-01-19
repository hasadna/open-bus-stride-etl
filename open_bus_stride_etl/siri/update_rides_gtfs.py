from datetime import date, timedelta
from textwrap import dedent

from open_bus_stride_db import db



def parse_date_str(date):
    """Parses a date string in ISO format (YYYY-MM-DD)."""
    if isinstance(date, datetime.date):
        return date
    return datetime.date.fromisoformat(date)


UPDATE_ROUTE_GTFS_RIDE_QUERY = """
UPDATE siri_ride AS srd
SET route_gtfs_ride_id = grd.id
FROM siri_route AS sr
JOIN gtfs_route AS gr ON sr.line_ref = gr.line_ref
JOIN gtfs_ride AS grd ON gr.id = grd.gtfs_route_id
WHERE srd.scheduled_start_time = grd.start_time
AND srd.scheduled_start_time BETWEEN :input_date AND :input_date + INTERVAL '1 day';
"""


def main(input_date=None):
    input_date = date.today() if not input_date else parse_date_str(input_date)
    print(f"{input_date=}")

    with db.get_session() as session:
        sql = dedent(UPDATE_ROUTE_GTFS_RIDE_QUERY)
        params = {"input_date": input_date}
        res = session.execute(sql, params)
        updated_route_gtfs_ride_ids = res.rowcount
        print(f"{updated_route_gtfs_ride_ids=}")
