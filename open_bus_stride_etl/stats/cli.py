import click
import datetime

import pytz
from ruamel import yaml

from . import api


@click.group()
def stats():
    """Aggregate and show stats about the Stride data"""
    pass


@stats.command()
@click.option('--latest-siri-snapshots-limit', default=10)
@click.option('--last-days-limit', default=5)
@click.option('--last-days-from')
def collect(latest_siri_snapshots_limit, last_days_limit, last_days_from):
    """Collect and show current stats data"""
    if last_days_from:
        last_days_from = pytz.timezone('israel').localize(datetime.datetime.strptime(last_days_from, '%Y-%m-%d'))
    res = api.collect(
        latest_siri_snapshots_limit=latest_siri_snapshots_limit,
        last_days_limit=last_days_limit,
        last_days_from=last_days_from
    )
    print('total_siri_snapshots: {}'.format(res['num_siri_snapshots']))
    print('last_days:  # last {} days'.format(last_days_limit))
    for last_day_stats in res['last_day_stats_iterator']:
        print(yaml.safe_dump([last_day_stats]).strip())
    print('latest_siri_snapshots:  # latest {} siri snapshots'.format(latest_siri_snapshots_limit))
    for siri_snapshot in res['siri_snapshots_iterator']:
        print(yaml.safe_dump([siri_snapshot]).strip())
