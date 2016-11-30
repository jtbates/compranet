import logging

import click

from . import database
from . import settings
from .xlsx import fetch_xlsx, load_xlsx, pull_xlsx


@click.group()
@click.option('--email-log', is_flag=True)
def cli(email_log):
    if email_log:
        logger = logging.getLogger('compranet')
        logger.addHandler(settings.smtp_handler)

@click.command('fetch_xlsx',
               short_help="Check for updated xlsx files and download them")
def fetch_xlsx_cmd():
    """Check the timestamps of the Excel archive URLs to see if they have been
    updated. When there is a newer version, download the zip file and extract
    the XLSX file."""
    fetch_xlsx()

@click.command('load_xlsx',
               short_help="Process and load updates from Excel file")
@click.argument('path', type=click.Path(exists=True, dir_okay=False))
def load_xlsx_cmd(path):
    """Read the data from the Excel file located at PATH, calculate the
    changes, and import these to the database."""
    load_xlsx(path)

@click.command('pull_xlsx',
               short_help="Download, process, and load updated Excel files")
def pull_xlsx_cmd():
    """Check the timestamps of the Excel archive URLs to see if they have been
    updated. When there is a newer version, download the zip file and extract
    the XLSX file. Read the extracted Excel files, calculate the changes for
    each file, and import them to the database."""
    pull_xlsx()

@click.command()
def create_db():
    """Create the database tables if they do not exist"""
    database.create_all()

@click.command()
def test_log():
    logger = logging.getLogger('compranet')
    logger.error('Test smtp')
    logger.info('test2')


cli.add_command(fetch_xlsx_cmd)
cli.add_command(load_xlsx_cmd)
cli.add_command(pull_xlsx_cmd)
cli.add_command(create_db)
cli.add_command(test_log)

if __name__ == '__main__':
    cli()
