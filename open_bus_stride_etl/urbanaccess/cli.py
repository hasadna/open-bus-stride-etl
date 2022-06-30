import click


@click.group()
def urbanaccess():
    """Handle processing of UrbanAccess data"""
    pass


@urbanaccess.command()
@click.option('--only-area')
@click.option('--only-hours')
@click.option('--limit-stop-times')
@click.option('--limit-fake-gtfs-processed')
def update_areas_fake_gtfs(**kwargs):
    """Update the UrbanAccess areas fake gtfs data"""
    from .update_areas_fake_gtfs import main
    main(**kwargs)
