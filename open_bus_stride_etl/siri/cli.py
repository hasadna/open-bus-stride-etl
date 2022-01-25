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
def update_ride_stops_gtfs():
    """update siri_ride_stop table with the related gtfs_stop data"""
    from .update_ride_stops_gtfs import main
    main()


@siri.command()
def update_ride_stops_vehicle_locations():
    """update ride_stops with vehicle_location nearest each stop by gtfs lon/lat"""
    from .update_ride_stops_vehicle_locations import main
    main()
