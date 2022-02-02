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


def parse_date_str(date, num_days=None):
    """Parses a date string in format %Y-%m-%d with default of today if empty
    if num_days is not None - will use a default of today minus given num_days
    """
    if isinstance(date, datetime.date):
        return date
    elif not date or date is 'None':
        return datetime.date.today() if num_days is None else datetime.date.today() - datetime.timedelta(days=int(num_days))
    else:
        return datetime.datetime.strptime(date, '%Y-%m-%d').date()


def parse_min_max_date_strs(min_date, max_date, num_days=None):
    """Parses min/max date strings in format %Y-%m-%d
     min_date default = today minus num_days
     max_date default = today"""
    min_date, max_date = parse_date_str(min_date, num_days), parse_date_str(max_date)
    assert min_date <= max_date
    return min_date, max_date


def get_db_date_str(d):
    return d.strftime('%Y-%m-%d')


@contextmanager
def print_memory_usage(start_msg, end_msg="Done"):
    print(start_msg)
    yield
    print("{}. Resident memory: {}mb".format(end_msg, psutil.Process().memory_info().rss / (1024 * 1024)))
