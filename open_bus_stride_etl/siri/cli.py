import click


@click.group()
def siri():
    """Handle processing of SIRI data"""
    pass


@siri.command()
def add_ride_durations():
    """add duration of rides based on vehicle locations to siri_ride table"""
    from .add_ride_durations import main
    main()


@siri.command()
@click.option('--min-date', help='Date string (%Y-%m-%d) specifying the min date to process. Defaults to today minus num_days if not provided.')
@click.option('--max-date', help='Date string (%Y-%m-%d) specifying the max date to process. Defaults to today if not provided.')
@click.option('--num-days', default=1, show_default=True, help='min_date defaults to today minus num_days if not provided')
def update_ride_stops_gtfs(**kwargs):
    """update siri_ride_stop table with the related gtfs_stop data"""
    from .update_ride_stops_gtfs import main
    main(**kwargs)


@siri.command()
@click.option('--min-date', help='Date string (%Y-%m-%d) specifying the min date to process. Defaults to today minus num_days if not provided.')
@click.option('--max-date', help='Date string (%Y-%m-%d) specifying the max date to process. Defaults to today if not provided.')
@click.option('--num-days', default=1, show_default=True, help='min_date defaults to today minus num_days if not provided')
def update_ride_stops_vehicle_locations(**kwargs):
    """update ride_stops with vehicle_location nearest each stop by gtfs lon/lat"""
    from .update_ride_stops_vehicle_locations import main
    main(**kwargs)


@siri.command()
@click.option('--min-date', help='Date string (%Y-%m-%d) specifying the min date to process. Defaults to today minus num_days if not provided.')
@click.option('--max-date', help='Date string (%Y-%m-%d) specifying the max date to process. Defaults to today if not provided.')
@click.option('--num-days', default=1, show_default=True, help='min_date defaults to today minus num_days if not provided')
def update_rides_gtfs(**kwargs):
    """Update siri rides data with related gtfs data"""
    from .update_rides_gtfs import main
    main(**kwargs)
