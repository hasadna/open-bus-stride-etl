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
def siri_hourly_update_packages(verbose):
    siri.hourly_update_packages(verbose=verbose)
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
