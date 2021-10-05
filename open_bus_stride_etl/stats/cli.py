import click
import datetime

import pytz

from . import api


@click.group()
def stats():
    """Aggregate and show stats about the Stride data"""
    pass


@stats.command()
@click.option('--latest-siri-snapshots-limit', default=10)
@click.option('--last-days-limit', default=5)
@click.option('--last-days-from')
@click.option('--validate', is_flag=True)
def collect(latest_siri_snapshots_limit, last_days_limit, last_days_from, validate):
    """Collect and show current stats data"""
    if last_days_from:
        last_days_from = pytz.timezone('israel').localize(datetime.datetime.strptime(last_days_from, '%Y-%m-%d'))
    exit(0 if api.collect(
        latest_siri_snapshots_limit=latest_siri_snapshots_limit,
        last_days_limit=last_days_limit,
        last_days_from=last_days_from,
        validate=validate,
        print_results=True
    ) else 1)
