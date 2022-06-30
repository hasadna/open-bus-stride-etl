import click
from ruamel import yaml


@click.group()
def artifacts():
    """Manage artifacts"""
    pass


@artifacts.command()
@click.argument('SOURCE_FILE_PATH')
@click.argument('TARGET_FILE_PREFIX')
@click.argument('TARGET_FILE_SUFFIX')
@click.option('--name')
@click.option('--description')
@click.option('--is-directory', is_flag=True)
def upload(**kwargs):
    """Upload an artifact"""
    from .common import upload_artifact
    upload_artifact(**kwargs)


@artifacts.command()
@click.argument('NAME_PREFIX')
@click.option('--limit')
def list(**kwargs):
    from .common import iterate_artifacts
    for artifact in iterate_artifacts(**kwargs):
        print(yaml.safe_dump([artifact], default_flow_style=False))
