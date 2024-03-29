import datetime
from pprint import pprint
from collections import defaultdict

import pytz
import click

from . import siri
from ..common import parse_date_str


def parse_time(time_str):
    if isinstance(time_str, datetime.datetime):
        return time_str
    return pytz.timezone('israel').localize(datetime.datetime.strptime(time_str, '%Y-%m-%d %H:%M'))


@click.group()
def packagers():
    """Package data for downloda"""
    pass


@packagers.command()
@click.argument('start_time', type=str)
@click.option('--end-time', type=str)
@click.option('--timedelta-units', type=str)
@click.option('--timedelta-amount', type=int)
@click.option('--output-path', type=str)
def siri_save_package(start_time, end_time, timedelta_units, timedelta_amount, output_path):
    """Package SIRI data"""
    assert start_time
    if not output_path:
        output_path = '.data/packagers/siri'
    stats = defaultdict(int)
    if end_time:
        assert not timedelta_units
        assert not timedelta_amount
        siri.save_package(stats, parse_time(start_time), parse_time(end_time), output_path)
    else:
        assert timedelta_units
        assert timedelta_amount
        siri.save_package_timedelta(stats, parse_time(start_time), timedelta_units, timedelta_amount, output_path)
    pprint(dict(stats))
    print(f'data saved to {output_path}')
    print("OK")


@packagers.command()
@click.option('--verbose', is_flag=True)
@click.option('--max-packages-per-type', type=int)
@click.option('--start-date-hour', type=str)
def siri_hourly_update_packages(**kwargs):
    start_date_hour = kwargs.pop('start_date_hour', None)
    if start_date_hour:
        date, hour = start_date_hour.split(' ')
        kwargs['start_datehour'] = pytz.timezone('israel').localize(datetime.datetime.strptime(date, '%Y-%m-%d').replace(hour=int(hour)))
    siri.hourly_update_packages(**kwargs)
    print("OK")


@packagers.command()
@click.argument('DATE', type=str)
@click.argument('HOUR', type=int)
@click.option('--force-update', is_flag=True)
@click.option('--verbose', is_flag=True)
def siri_update_package(date, hour, force_update, verbose):
    stats = defaultdict(int)
    start_datetimehour = datetime.datetime.combine(parse_date_str(date), datetime.datetime.min.time().replace(hour=hour))
    siri.update_package(stats, start_datetimehour, force_update, verbose)
    pprint(dict(stats))
    print("OK")


# we already created the index, no need to create it again
# @packagers.command()
# @click.option('--only-keys', type=str)
# @click.option('--dump-to-path', is_flag=True)
# def siri_create_legacy_packages_index(**kwargs):
#     siri.create_legacy_packages_index(**kwargs)


@packagers.command()
@click.option('--index-from-path', is_flag=True)
def siri_legacy_update_packages_from_index(**kwargs):
    siri.legacy_update_packages_from_index(**kwargs)
