from cacheManager import redis_connection
from typing import Callable, Dict, List
import os
import multiprocessing
import logging
import pyextract
import pyextract.config as config
import uuid
import chilkat
from datetime import datetime
from zipfile import ZipFile
from pyextract.connect import ABCMessenger
import keyring
import apsw


LOGGER = multiprocessing.get_logger()
LOGGER.setLevel(logging.INFO)

def sqlite_create_credential_table(erp: str, field_definitions: Dict[str, str]) -> str:
    """Return a SQLite statement to create a user credential table."""
    columnstr = ','.join(
        '"{}" {}'.format(field, definition)
        for field, definition in field_definitions.items()
    )
    statement = (
        'CREATE TABLE IF NOT EXISTS {}_CREDENTIALS ({})'
        ).format(erp, columnstr)
    return statement


def sqlite_add_cred_table_field(erp: str, field: str, definition: str) -> str:
    """Return a SQLite statement to add a field to a user credential table."""
    statement = (
        'ALTER TABLE {}_CREDENTIALS ADD COLUMN "{}" {}'
        ).format(erp, field, definition)
    return statement


def _messenger_from_password(password: str, folder: str,
                             filename: str = None) -> ABCMessenger:
    """Translate the password from the GUI into a SQLite messenger.

    This messenger will be the location for pause/resume data during
    data extraction.
    """
    if not filename:
        filename = "Encrypted_Content_TableExtractions.dat"

    if password is None:
        is_zipped = False
        aes256 = False
    elif len(password) == 32:
        is_zipped = True
        aes256 = False
    elif len(password) == 64:
        is_zipped = True
        aes256 = True
    else:
        raise ValueError('len(password) must be 32 or 64')

    output = pyextract.SQLiteMessenger(
        filepath=os.path.join(folder, filename),
        is_zipped=is_zipped,
        aes256=aes256,
        password=password,
    )
    return output


def log_final_record_count(extraction: pyextract.Extraction,
                           resume_extract: bool):
    """After an extraction completes, log and alert user of total time taken."""
    if extraction.rows_read == 0:
        LOGGER.warning(
            'Extraction returned zero records. Ensure you selected the '
            'correct database and that your query is well-formed. '
            'Please submit feedback for PwC to investigate.'
        )
        return

    if extraction.stream.is_stopped():
        template = "Extractions returned {:,} records before pausing."

    elif resume_extract:
        template = "Extractions returned {:,} records since last pause."

    else:
        template = "Extractions returned a total of {:,} records."

    LOGGER.info(template.format(extraction.rows_read))


def _create_data_package(ecf_file: str, output_folder: str) -> pyextract.DataPackage:
    """Package all the output data from an extraction into a ZIP file."""
    package = pyextract.DataPackage(ecf_file)

    # Add all SQLite files from the temp folder to the ZIP package
    for item in os.listdir(output_folder):
        if item.endswith('.dat') or item.endswith('.log'):
            dbpath = os.path.join(output_folder, item)
            package.add_sqlite_file(dbpath)

    # if not os.path.exists(logfile):
    #     LOGGER.warning('No messages were written to the log file.')
    #     # Create an empty logfile before packaging
    #     open(logfile, 'a').close()
    # package.add_text_file(logfile)

    return package


def _extract_id_from_package(name: str) -> str:
    """Return Extract ID from Extract Package name."""
    assert name.startswith('Package_')
    assert name.endswith('.zip')
    assert len(name) == 48
    return name[8:44]


def _unique_sqlite_password(keylen=16) -> str:
    """Generate a unique SQLite password for use in an extraction.

    ARGS:
        keylen: 16 for AES-128 encryption, and 32 for AES-256 encryption.
    """
    assert keylen in (16, 32), 'keylen must be either 16 or 32'
    generator = chilkat.CkPrng()
    return generator.genRandom(keylen, 'hex')


def get_set_keyring_password() -> str:
    """Will check if a password currently exists in the vault and if so, return
        the stored password to unlock the config.db, or will create and set
        set the password in the vault
    """
    if keyring.get_password(config.KEYRING_SYSTEM, config.KEYRING_USER):
        return keyring.get_password(config.KEYRING_SYSTEM, config.KEYRING_USER)
    else:
        password = str(uuid.uuid4())
        keyring.set_password(config.KEYRING_SYSTEM, config.KEYRING_USER, password)
        return password




@redis_connection()
def update_progress(r, extract_key: str, completion_pct: int = 0, increment_by: int = None) -> int:
    """ updates the stored progress values"""
    if increment_by:
        # increments the value at key by the value of increment_by and returns the update value
        return r.incrby("extract:progress:{}".format(extract_key), increment_by)
    else:
        # sets the value of key to the absolute value of completion_pct
        r.set("extract:progress:" + extract_key, completion_pct)
        return completion_pct


def unzip_package(filepath: str, output_path: str, filetype: str = None):
    """Unzip a data package from previous extraction.

    ARGS:
        filepath: Location of the data package on local disk.
        output_path: Location to put unzipped data from package.
        filetype: If provided, only unzip files of the type provided.
    """
    with ZipFile(filepath, 'r') as package:
        # Extract all SQLite databases from the zipped package
        for item in package.namelist():
            if filetype and not item.endswith(filetype):
                continue  # Doesn't match filetype filter, skip
            else:
                package.extract(item, output_path)

@redis_connection()
def update_status(r, extract_key: str, status) -> int:
    """ updates the stored progress values"""
    return r.set("extract:status:{}".format(extract_key), status)


@redis_connection()
def update_control(r, extract_key: str, status) -> int:
    """ updates the stored progress values"""
    return r.lpush("extract:control:{}".format(extract_key), status)