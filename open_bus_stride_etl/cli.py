import click
import dotenv

from .stats.cli import stats
from .siri.cli import siri
from .gtfs.cli import gtfs
from .db.cli import db
from .artifacts.cli import artifacts


@click.group(context_settings={'max_content_width': 200})
@click.option('--load-dotenv', is_flag=True)
def main(load_dotenv):
    """Open Bus Stride Data Enrichment ETLs"""
    if load_dotenv:
        dotenv.load_dotenv()
    pass


main.add_command(stats)
main.add_command(siri)
main.add_command(gtfs)
main.add_command(db)
main.add_command(artifacts)


if __name__ == "__main__":
    main()
