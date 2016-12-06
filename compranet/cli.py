import logging
import os

import click
from crontab import CronTab, CronSlices

from . import database
from . import settings
from .xlsx import fetch_xlsx, load_xlsx, pull_xlsx


@click.group()
@click.option('--email-log', is_flag=True,
              help='Send logs to email.')
def cli(email_log):
    if email_log:
        logger = logging.getLogger('compranet')
        logger.addHandler(settings.smtp_handler)

@click.command('fetch_xlsx',
               short_help="Check for updated xlsx files and download them.")
def fetch_xlsx_cmd():
    """Check the timestamps of the Excel archive URLs to see if they have been
    updated. When there is a newer version, download the zip file and extract
    the XLSX file."""
    fetch_xlsx()

@click.command('load_xlsx',
               short_help="Process and load updates from Excel file.")
@click.argument('paths', metavar='[FILE]...', nargs=-1,
                type=click.Path(exists=True, dir_okay=False))
def load_xlsx_cmd(paths):
    """Read the data from the Excel file located at FILE, calculate the
    changes, and import these to the database."""
    for path in paths:
        load_xlsx(path)

@click.command('pull_xlsx',
               short_help="Download, process, and load updated Excel files.")
def pull_xlsx_cmd():
    """Check the timestamps of the Excel archive URLs to see if they have been
    updated. When there is a newer version, download the zip file and extract
    the XLSX file. Read the extracted Excel files, calculate the changes for
    each file, and import them to the database."""
    pull_xlsx()

@click.command()
def create_db():
    """Create database tables if they do not exist."""
    database.create_all()

@click.command(short_help="Manage a compranet tracker crontab.")
@click.option('-l', is_flag=True,
              help="Display the current compranet tracker crontab.")
@click.option('-r', is_flag=True,
              help="Remove the current compranet tracker crontab.")
@click.argument('action', nargs=-1)
def crontab(l, r, action):
    """Allow the user to manage crontab entries for the compranet tracker.
    The -l option lists the compranet tracker crontab entries and -r removes
    them. Two actions are supported, ADD and REMOVE.

    \b
    To ADD a crontab entry use the following syntax:
        compranet-cli crontab add [time] -- [command]
    where the time argument is a CRON expression (e.g. "0 0 * * 0" or weekly)
    and command is the compranet-cli command to execute.
    Example:
        compranet-cli crontab add "0 2 * * *" -- "--email-log pull_xlsx"

    \b
    To REMOVE a crontab entries use the following syntax:
        compranet-cli crontab remove [command]
    All crontab entries which contain the command argument will be removed.
    Example:
        compranet-cli crontab remove pull_xlsx
    """
    cron = CronTab(user=True)
    if l:
        for job in cron.find_comment('compranet_tracker'):
            print(job)
    if r:
        for job in cron.find_comment('compranet_tracker'):
            cron.remove(job)
        cron.write()
    if len(action) == 0 and not l and not r:
        print(click.get_current_context().get_help())
    if len(action) > 0:
        if action[0].upper() == 'ADD':
            venv_path = settings.VIRTUALENV_PATH
            cli_path = os.path.join(venv_path, 'bin', 'compranet-cli')
            if len(action) != 3:
                raise click.BadParameter("Wrong number of arguments",
                                         param_hint='ACTION')
            time = action[1]
            if not CronSlices.is_valid(time):
                raise click.BadParameter("Invalid CRON expression",
                                         param_hint='ACTION')
            cli_opts = ' '.join(action[2:])
            command = ' '.join([cli_path, cli_opts])
            job = cron.new(command=command, comment='compranet_tracker')
            job.setall(time)
            cron.write()
        elif action[0].upper() == 'REMOVE':
            if len(action) != 2:
                raise click.BadParameter("Wrong number of arguments",
                                         param_hint='ACTION')
            cmd = ' '.join(action[1:])
            for job in cron.find_command(cmd):
                print("Removing entry {}".format(job))
                cron.remove(job)
            cron.write()
        else:
            raise click.BadParameter("Unrecognized action argument",
                                     param_hint='ACTION')

@click.command()
def test_log():
    logger = logging.getLogger('compranet')
    logger.error('Test smtp')
    logger.info('test2')



cli.add_command(fetch_xlsx_cmd)
cli.add_command(load_xlsx_cmd)
cli.add_command(pull_xlsx_cmd)
cli.add_command(create_db)
cli.add_command(crontab)
cli.add_command(test_log)

if __name__ == '__main__':
    cli()
