import datetime
from textwrap import dedent

from open_bus_stride_db import db


UPDATE_ROUTE_GTFS_RIDE_QUERY = """
UPDATE siri_ride AS srd
SET route_gtfs_ride_id = grd.id
FROM siri_route AS sr
JOIN gtfs_route AS gr ON sr.line_ref = gr.line_ref
JOIN gtfs_ride AS grd ON gr.id = grd.gtfs_route_id
WHERE srd.scheduled_start_time = grd.start_time
AND srd.scheduled_start_time BETWEEN :date AND :date + INTERVAL '1 day';
"""


def parse_date(date):
    """Parses a date string in ISO format (YYYY-MM-DD)."""
    if isinstance(date, datetime.date):
        return date
    return datetime.date.fromisoformat(date)

def main(date=None):
    date = datetime.date.today() if not date else parse_date(date)
    print(f"{date=}")

    with db.get_session() as session:
        sql = dedent(UPDATE_ROUTE_GTFS_RIDE_QUERY)
        params = {"date": date}
        res = session.execute(sql, params)
        updated_route_gtfs_ride_ids = res.rowcount
        print(f"{updated_route_gtfs_ride_ids=}")
