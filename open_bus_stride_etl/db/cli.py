import click


@click.group()
def db():
    """stride DB tasks"""
    pass


@db.command()
def copy_backup_to_s3():
    """Copy the latest DB backup to S3"""
    from .copy_backup_to_s3 import main
    main()

