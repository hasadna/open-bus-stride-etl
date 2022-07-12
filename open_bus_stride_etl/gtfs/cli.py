import click


@click.group()
def gtfs():
    """Handle processing of GTFS data"""
    pass


@gtfs.command()
@click.option('--date', help='Date string (%Y-%m-%d) specifying the date to process. Defaults to today if not provided.')
@click.option('--date-to', help='If provided, will process a date range from date to date-to')
@click.option('--idempotent', is_flag=True, help='If set, will ensure all dates are processed.')
@click.option('--check-missing-dates', is_flag=True, help='If set, will verify missing dates before processing.')
def update_ride_aggregations(**kwargs):
    """update aggregations on gtfs_ride data"""
    from .update_ride_aggregations import main
    main(**kwargs)
