import configparser
from logging.config import fileConfig
import os

from .utils import BufferingSMTPHandler


BASE_DIR = os.path.dirname(os.path.dirname(__file__))

config = configparser.ConfigParser()
config_path = os.path.join(BASE_DIR, 'config.ini')
config.read(config_path)
main_cfg = config['compranet']

def rel2abs(path, base_dir=BASE_DIR):
    if os.path.isabs(path):
        return(path)
    else:
        return(os.path.join(BASE_DIR, path))

RAW_DIR = rel2abs(main_cfg['raw_dir'])
INTERIM_DIR = rel2abs(main_cfg['interim_dir'])
SQLITE_DB_PATH = rel2abs(main_cfg['sqlite_db_path'])
DB_URI = "sqlite:///{}".format(SQLITE_DB_PATH)
ALEMBIC_INI_PATH = rel2abs(main_cfg['alembic_ini_path'])
ALEMBIC_SCRIPT_LOCATION = rel2abs(main_cfg['alembic_script_location']))

# Set up logging
fileConfig(config_path)
smtp_handler = BufferingSMTPHandler(**config['email'])
