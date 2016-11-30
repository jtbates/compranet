import logging
import os

from dotenv import find_dotenv, load_dotenv


DOTENV_PATH = find_dotenv()
load_dotenv(DOTENV_PATH)

BASE_DIR = os.path.dirname(DOTENV_PATH)

def rel2abs(path, base_dir=BASE_DIR):
    if os.path.isabs(path):
        return(path)
    else:
        return(os.path.join(BASE_DIR, path))

RAW_DIR = rel2abs(os.environ.get('RAW_DIR'))
INTERIM_DIR = rel2abs(os.environ.get('INTERIM_DIR'))
SQLITE_DB_PATH = rel2abs(os.environ.get('SQLITE_DB_PATH'))
DB_URI = "sqlite:///{}".format(SQLITE_DB_PATH)
ALEMBIC_INI_PATH = rel2abs(os.environ.get('ALEMBIC_INI_PATH'))
ALEMBIC_SCRIPT_LOCATION = rel2abs(os.environ.get('ALEMBIC_SCRIPT_LOCATION'))

LOG_LEVEL = os.environ.get('LOG_LEVEL')
numeric_level = getattr(logging, LOG_LEVEL.upper())
if not isinstance(numeric_level, int):
    raise ValueError("Invalid log level: {}".format(loglevel))
logging.basicConfig()
logger = logging.getLogger('compranet')
logger.setLevel(numeric_level)

