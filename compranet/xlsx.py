from datetime import datetime
import hashlib
import logging
import os
import re
import shutil
import zipfile

import dateutil.tz
from dateutil.relativedelta import relativedelta
import pandas as pd
import numpy as np
import requests
from send2trash import send2trash

from .database import get_session
from .database import SourceXls, ContratoXls, ContratoXlsHistorial
from . import settings


requests.packages.urllib3.disable_warnings()

logger = logging.getLogger('compranet.xlsx')

session = get_session()

RAW_DIR = settings.RAW_DIR
URL_PREFIX = 'http://upcp.funcionpublica.gob.mx/descargas/'
ZIP_FORMAT = "Contratos{year}.zip"
DATETIME_ARGS = ['year', 'month', 'day', 'hour', 'minute', 'second']
DATETIME_RE_STR = ''.join("(?P<{}>\d{{2}})".format(e) for e in DATETIME_ARGS)
XLSX_RE_STR = 'Contratos(?P<datayr>\d{4}(_\d{4})?)_' + DATETIME_RE_STR
XLSX_RE = re.compile(XLSX_RE_STR)
#ISO_FORMAT = "20{y}-{m}-{d} {H}:{M}:{S}"
#LAST_MOD_STRFTIME = "%a, %d %b %Y %H:%M:%S %Z"
TZ_CDMX = dateutil.tz.gettz('Mexico/General')

NOW_YEAR = datetime.now(TZ_CDMX).year
ALL_YEARS = (str(year) for year in range(2015, NOW_YEAR))


def find_xlsx(years=ALL_YEARS, base_dir=RAW_DIR):
    """
    Returns dict containing a list of paths for each source year
    """
    years_dict = {year: [] for year in years}
    for filename in sorted(os.listdir(base_dir)):
        match = XLSX_RE.match(filename)
        if match:
            datayr = match.group('datayr')
            if datayr in years:
                pathname = os.path.join(base_dir, filename)
                years_dict[datayr].append(pathname)
    return years_dict

def latest_xlsx(years=ALL_YEARS):
    """
    Returns a list of paths to the most recent contracts Excel files
    """
    return [found[-1] for found in find_xlsx(years).values() if found]

def parse_pathname(pathname):
    dirname, filename = os.path.split(pathname)
    match = XLSX_RE.match(filename)
    dt_args = match.groupdict()
    source = dt_args.pop('datayr')
    dt_args['year'] = int('20'+dt_args['year'])
    dt_args = {f: int(v) for f,v in dt_args.items()}
    dt_args['tzinfo'] = TZ_CDMX
    return((dirname, filename, source, datetime(**dt_args)))

def pathname_to_updatetime(pathname):
    _, _, _, dt = parse_pathname(pathname)
    return(dt)

def fetch_xlsx(years=ALL_YEARS, raw_dir=RAW_DIR, rm_zip=True):
    """
    Download new versions of the contracts Excel files if they are newer

    Find the newest versions of the contracts Excel files in the raw directory
    for each source year and then check this against the timestamp at the
    download URL. If there is a newer version, download the archive and
    extract it.

    Parameters
    ----------
    years : list of str
        The source years to download
    raw_dir : str
        The path of the directory containing the contracts Excel files
    rm_zip : bool
        Whether to delete the zip archive after extracting the Excel file

    Returns
    -------
    list of str
        A list of paths to the downloaded files
    """

    downloaded = []

    for year in years:
        try:
            zip_name = ZIP_FORMAT.format(year=year)
            url = URL_PREFIX + zip_name

            # Check if there is a previously downloaded file
            latest_list = latest_xlsx([year])
            if latest_list:
                # Find the time that the latest local file was generated
                loc_time = pathname_to_updatetime(latest_list[-1])

                # Find the last modified time of file on server
                resp = requests.get(url, stream=True, verify=False)
                last_mod = resp.headers['Last-Modified']
                srv_time = dateutil.parser.parse(last_mod)

                msg = ("The server version of {} was modified at {}, "
                       "{} after the local file was generated")
                logger.info(msg.format(zip_name, srv_time, srv_time - loc_time))

                # There is some delay between generating the zip file and
                # uploading it to the server, usually less than 30 minutes.
                # Let's skip downloading the file if the last modified time
                # on the server is less than 6 hours after the time that our
                # local file was generated.
                if srv_time < (loc_time + relativedelta(hours=+6)):
                    logger.info("Skipping the download of {}".format(zip_name))
                    continue

            # There is a newer version or force is True, so we download
            print("Downloading {}...".format(zip_name))
            zip_path = os.path.join(raw_dir, zip_name)
            response = requests.get(url, stream=True, verify=False)
            if response.status_code == 200:
                with open(zip_path, 'wb') as f:
                    shutil.copyfileobj(response.raw, f)

            # Extract the xlsx from the zip file
            logger.info("Extracting {}...".format(zip_name))
            with zipfile.ZipFile(zip_path, 'r') as zf:
                for member in zf.namelist():
                    zf.extract(member, raw_dir)
                    downloaded.append(os.path.join(raw_dir, member))
                zf.extractall(raw_dir)

            if rm_zip:
                # Trash the zip file after extracting
                logger.info("Deleting {}...".format(zip_name))
                send2trash(zip_path)
        except:
            logger.exception("Error while downloading {}".format(zip_name))

    return(downloaded)

def parse_pathname(pathname):
    """
    Extract relevant information from the path to the contracts Excel file.

    Take the path to the contracts Excel file and parse the directory,
    filename, source year string, and export time.

    Parameters
    ----------
    pathname : str
        The path to a CompraNet contracts Excel file

    Returns
    -------
    (str, str, str, datetime.datetime)
        Tuple with the directory, filename, source year string, and export time
    """
    dirname, filename = os.path.split(pathname)
    match = XLSX_RE.match(filename)
    dt_args = match.groupdict()
    source = dt_args.pop('datayr')
    dt_args['year'] = int('20'+dt_args['year'])
    dt_args = {f: int(v) for f,v in dt_args.items()}
    dt_args['tzinfo'] = TZ_CDMX
    return((dirname, filename, source, datetime(**dt_args)))

def drop_dup(df):
    """
    Drop duplicate rows, ignoring CONVENIO_MODIFICATORIO.

    This function will drop rows that are exact duplicates. For rows that are
    duplicates in all columns except CONVENIO_MODIFICATORIO, it will keep the
    row where CONVENIO_MODIFICATORIO has value 1 and drop the other.

    Parameters
    ----------
    df : pandas.DataFrame
        A DataFrame read from a CompraNet contracts Excel file with xlsx2df

    Returns
    -------
    pandas.DataFrame
        A DataFrame where CODIGO_CONTRATO is unique
    """
    df = df[~df.duplicated()]

    df = df.sort_values(['CODIGO_CONTRATO', 'CONVENIO_MODIFICATORIO'])
    cols = set(df.columns).difference({'CONVENIO_MODIFICATORIO'})
    df = df[~df.duplicated(cols, keep='last')]

    # confirm that we now have no duplicate CODIGO_CONTRATO values
    assert not df['CODIGO_CONTRATO'].duplicated().any()

    return(df)

def xlsx2df(pathname):
    """
    Reads an Excel spreadsheet into a dataframe, dropping duplicate rows.

    Parameters
    ----------
    pathname : str
        The path of a Contratos Excel file that was retrieved from CompraNet

    Returns
    -------
    pandas.DataFrame
    """
    raw_dir, filename, source, updated = parse_pathname(pathname)
    converters = {'RAMO': str}
    df = pd.read_excel(pathname, converters=converters)
    # The 2010_2012 Excel has duplicate rows, so we drop them.
    if source == '2010_2012':
        df = drop_dup(df)
    assert not df['CODIGO_CONTRATO'].duplicated().any()
    df = df.assign(_SOURCE=source, _UPDATED=updated)
    return(df)

def row2dict(row):
    """
    Converts a ContratosXls object into a dictionary
    """
    d = {}
    for column in row.__table__.columns:
        d[column.name] = getattr(row, column.name)

    return(d)

def series2dict(series):
    """
    Converts a row from a Contratos DataFrame to a dictionary

    This function takes a row from a contracts DataFrame and converts it to a
    dict that is suitable to pass to ContratosXls by replacing NaN with None
    and converting integers where appropriate.

    Parameters
    ----------
    series : pandas.Series
        A row from a contracts DataFrame

    Returns
    -------
    dict
    """
    s = series.to_dict()
    # pandas uses NaN for null value, sqlalchemy uses None
    for key, val in s.items():
        if pd.isnull(val):
            s[key] = None

    # convert int cols from pandas (which are float64 if nullable, otherwise int64)
    int_cols = ['CODIGO_EXPEDIENTE', 'CODIGO_CONTRATO', 'CONVENIO_MODIFICATORIO',
                'CONTRATO_MARCO', 'COMPRA_CONSOLIDADA', 'PLURIANUAL', 'FOLIO_RUPC',
                'C_EXTERNO']
    float_cols = ['IMPORTE_CONTRATO', 'APORTACION_FEDERAL']
    for col in int_cols:
        if col in s:
            fval = s[col]
            if fval is not None:
                assert np.floor(fval) == fval
                s[col] = int(fval)

    return(s)

def dict_diff(d1, d2, ignore_keys={'_UPDATED'}):
    comp_keys = set(d1.keys()).difference(ignore_keys)
    assert comp_keys == set(d2.keys()).difference(ignore_keys)
    diff = {}
    for key in comp_keys:
        if d1[key] != d2[key]:
            diff[key] = (d1[key], d2[key])

    return(diff)

def dict_equals(d1, d2, ignore_keys=['_UPDATED']):
    diff = dict_diff(d1, d2, ignore_keys)
    return(len(diff) == 0)

def load_source_df(df_new, sha256, session=session):
    """
    Finds changes in the new DataFrame and updates the database accordingly.

    This function takes a contracts DataFrame read with xlsx2df and checks it
    against the database to identify the changes in the corresponding source
    year. The changes can be to add a new contract or modify or delete an
    existing contract. New contracts are simply inserted. When a change is made
    to an existing contract the previous versions are recorded in the
    ContratosXlsHistorial table.

    Parameters
    ----------
    df_new : pandas.DataFrame
        A DataFrame read from a CompraNet contracts Excel file with xlsx2df

    sha256 : str
        The hexadecimal SHA256 hash of the source file

    session: sqlalchemy.orm.session.Session
        An SQLAlchemy session to connect to the database

    Raises
    -------
    ValueError
        Raised if the columns in df_new do not match the columns in the
        ContratosXls table, or if the input dataframe does not correspond
        to a single source and export.
    AssertionError
        Raised if more than one row is retrieved from a given CODIGO_CONTRATO
        and _SOURCE.
    """
    try:
        df_cols = set(df_new.columns)
        tbl_cols = {col.key for col in ContratoXls.__table__.columns}
        msg_args = tbl_cols.difference(df_cols), df_cols.difference(tbl_cols)
        error_msg = "Missing columns {} and added columns {}".format(*msg_args)
        if set(df_new.columns) != tbl_cols:
            raise ValueError(error_msg)

        source = df_new['_SOURCE'].unique()
        updated = df_new['_UPDATED'].unique()
        if (len(source) != 1) or (len(updated) != 1):
            raise ValueError("Input DataFrame is not single source")
        source = source[0]
        updated = updated[0]

        cnt_unchanged = cnt_inserted = cnt_modified = cnt_deleted = 0

        def db_get_cc(cc, source):
            # TODO: replace query with get
            rs = session.query(ContratoXls).filter(ContratoXls.CODIGO_CONTRATO == cc,
                                                   ContratoXls._SOURCE == source)
            assert rs.count() == 1
            return(rs)

        rs = session.query(ContratoXls.CODIGO_CONTRATO).filter(ContratoXls._SOURCE == source)
        old_ids = set(row[0] for row in rs)
        new_ids = set(df_new['CODIGO_CONTRATO'])
        # deleted contracts found in db but not in latest xlsx
        deleted_ids = old_ids.difference(new_ids)
        # inserted contracts found in latest xlsx but not in db
        inserted_ids = new_ids.difference(old_ids)

        # handle deleted contracts
        for cc in deleted_ids:
            db_res = db_get_cc(cc, source)
            db_row = db_res.one()
            db_dict = row2dict(db_row)
            # add row to the hist table for the old version
            hist_row = ContratoXlsHistorial(_REMOVED=False, **db_dict)
            session.add(hist_row)
            # add row to the hist table for the deletion
            del_row = ContratoXlsHistorial(CODIGO_CONTRATO=cc, _REMOVED=True,
                                           _SOURCE=source, _UPDATED=updated)
            session.add(del_row)
            # delete the row in the main contract table
            db_res.delete()
            cnt_deleted += 1

        # handle modified and inserted contracts
        for idx, xls_row in df_new.iterrows():
            cc = xls_row['CODIGO_CONTRATO']
            xls_dict = series2dict(xls_row)
            if cc not in inserted_ids: # contract id found in main table
                db_res = db_get_cc(cc, source)
                db_row = db_res.one()
                db_dict = row2dict(db_row)
                # if there is a change, add old ver to hist table and update
                if not dict_equals(db_dict, xls_dict):
                    # add row to history table for the old version
                    hist_row = ContratoXlsHistorial(_REMOVED=False, **db_dict)
                    session.add(hist_row)
                    # update row in main contract table
                    for key, val in xls_dict.items():
                        setattr(db_row, key, val)
                    cnt_modified += 1
                else: # if there isn't a change, then do nothing
                    cnt_unchanged += 1
            else: # contract id not found in main table
                # add row to the main contract table
                session.add(ContratoXls(**xls_dict))
                cnt_inserted += 1

        session.add(SourceXls(_SOURCE=source, _UPDATED=updated, SHA256=sha256))
        session.commit()
        logger.info("{} rows unchanged".format(cnt_unchanged))
        logger.info("{} rows inserted".format(cnt_inserted))
        logger.info("{} rows modified".format(cnt_modified))
        logger.info("{} rows deleted".format(cnt_deleted))
    except:
        logger.exception("Exception during data import, rolling back all changes")
        session.rollback()
        raise

def hashfile(pathname, hasher=hashlib.sha256(), blocksize=65536):
    with open(pathname, 'rb') as infile:
        buf = infile.read(blocksize)
        while len(buf) > 0:
            hasher.update(buf)
            buf = infile.read(blocksize)
        return(hasher.hexdigest())

def load_xlsx(pathname, session=session):
    """
    Reads a contracts Excel file and imports the changes the database.
    """
    filename = os.path.basename(pathname)
    logger.info("Reading {}...".format(filename))
    sha256 = hashfile(pathname)
    df_new = xlsx2df(pathname)
    logger.info("Beginning data import from {}".format(filename))
    try:
        load_source_df(df_new, sha256, session)
        logger.info("Successfully imported data from {}".format(filename))
    except:
        logger.exception("Failed to import data from {}".format(filename))

def pull_xlsx(years=ALL_YEARS, session=session):
    """
    Download, process, and load updated Excel files
    """
    downloaded = fetch_xlsx(years)
    for pathname in downloaded:
        load_xlsx(pathname, session)

