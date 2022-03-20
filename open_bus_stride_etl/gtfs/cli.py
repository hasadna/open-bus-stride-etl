import click


@click.group()
def gtfs():
    """Handle processing of GTFS data"""
    pass


@gtfs.command()
@click.option('--date', help='Date string (%Y-%m-%d) specifying the date to process. Defaults to today if not provided.')
@click.option('--date-to', help='If provided, will process a date range from date to date-to')
def update_ride_aggregations(**kwargs):
    """update aggregations on gtfs_ride data"""
    from .update_ride_aggregations import main
    main(**kwargs)
