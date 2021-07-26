import click

from .stats.cli import stats


@click.group(context_settings={'max_content_width': 200})
def main():
    """Open Bus Stride Data Enrichment ETLs"""
    pass


main.add_command(stats)
