import datetime

import pytz


def parse_siri_snapshot_id(snapshot_id):
    return datetime.datetime.strptime(snapshot_id + 'z+0000', '%Y/%m/%d/%H/%Mz%z')


def now():
    return datetime.datetime.now(pytz.UTC)


def now_minus(**kwargs):
    return now() - datetime.timedelta(**kwargs)


def utc(dt: datetime.datetime):
    return dt.astimezone(pytz.UTC)
