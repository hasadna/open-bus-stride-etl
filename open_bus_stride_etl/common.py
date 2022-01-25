import datetime
from contextlib import contextmanager

import pytz
import psutil


def parse_siri_snapshot_id(snapshot_id):
    return datetime.datetime.strptime(snapshot_id + 'z+0000', '%Y/%m/%d/%H/%Mz%z')


def now():
    return datetime.datetime.now(pytz.UTC)


def now_minus(**kwargs):
    return now() - datetime.timedelta(**kwargs)


def utc(dt: datetime.datetime):
    return dt.astimezone(pytz.UTC)


def parse_date_str(date):
    """Parses a date string in format %Y-%m-%d with default of today if empty"""
    if isinstance(date, datetime.date):
        return date
    if not date or date == 'None':
        return datetime.date.today()
    return datetime.datetime.strptime(date, '%Y-%m-%d').date()


@contextmanager
def print_memory_usage(start_msg, end_msg="Done"):
    print(start_msg)
    yield
    print("{}. Resident memory: {}mb".format(end_msg, psutil.Process().memory_info().rss / (1024 * 1024)))
