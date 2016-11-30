import configparser
from logging.config import fileConfig
import os

from .utils import BufferingSMTPHandler


BASE_DIR = os.path.dirname(os.path.dirname(__file__))

config = configparser.ConfigParser()
config_path = os.path.join(BASE_DIR, 'config.ini')
config.read(config_path)

def rel2abs(path, base_dir=BASE_DIR):
    if os.path.isabs(path):
        return(path)
    else:
        return(os.path.join(BASE_DIR, path))

RAW_DIR = rel2abs(config['compranet']['raw_dir'])
INTERIM_DIR = rel2abs(config['compranet']['interim_dir'])
SQLITE_DB_PATH = rel2abs(config['compranet']['sqlite_db_path'])
DB_URI = "sqlite:///{}".format(SQLITE_DB_PATH)

# Set up logging
fileConfig(config_path)
smtp_handler = BufferingSMTPHandler(**config['email'])
