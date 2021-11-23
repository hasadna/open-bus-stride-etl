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
