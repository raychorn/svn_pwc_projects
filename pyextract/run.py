import logging
import multiprocessing
import os
import time
import traceback
from typing import Callable, Dict, List
import pyextract.connect.sap
import pyextract.connect.oracle
import pyextract.connect.db2
import pyextract.connect.mysql
import pyextract.connect.mssql
import pyextract.streams.sapstream
import pyextract.streams.sqlstream
from pyextract.connect import ABCMessenger
import pyextract.utils
import chilkat
import base64
import pyextract.utils as utils
import json
from cacheManager import redis_connection
from pyextract import config
import apsw
import datetime
import keyring
import uuid
import threading
from pyextract.connect.sqlite import sqlite_connection
from zipfile import ZipFile
from contextlib import contextmanager
import common
from pyextract.version import EXTRACT_VERSION

BUILD = datetime.datetime.now().strftime('%Y.%m.%d.%H.%M')

DOCKER_VERSION = "v{}_b{}".format(EXTRACT_VERSION, BUILD)

ROOT_DIR = '/usr/pwcextract'
OUT_DIR = os.path.join(ROOT_DIR, 'out')
ECF_DIR = os.path.join(ROOT_DIR, 'ecf')
LOG_DIR = os.path.join(ROOT_DIR, 'log')

os.makedirs(OUT_DIR, exist_ok=True)
os.makedirs(ECF_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

utils.setup_logging()
LOGGER = multiprocessing.get_logger()
LOGGER.setLevel(logging.INFO)


@redis_connection()
def log(r, extract_key: str, message: str, *args):
    msg = message.format(*args) if args else message
    r.zadd("extract:logs:{}".format(extract_key), time.time(), msg)  # sorted set
    print(msg)



def _default_config_path() -> str:
    return '/usr/pwcextract/pyextract/extract.config'


def get_desktop_folder() -> str:
    """Return filepath of the current active user's Desktop."""
    return os.path.join(os.path.expanduser('~'), 'Desktop')


def _check_for_floating_databases(folder: str):
    """Will remove any floating .dat files within the request ID subdirectory
        due to the implementation of encrypted databases, if there is a floating
        database there was an issue with a previous extraction and the password
        to decrypt isn't available, as such it will be removed for a clean
        extraction
    """
    databases_to_be_dropped = []

    if not os.path.exists(folder):
        return
    for file_ in os.listdir(folder):
        if file_.endswith('.dat'):
            databases_to_be_dropped.append(os.path.join(folder, file_))

    [os.remove(db) for db in databases_to_be_dropped]



@redis_connection()
def cleanup(r, extract_key: str) -> None:
    r.lpush("extract:monitor:finished", extract_key)
    # cleanup of redis keys will be handled in ExtractManager


@redis_connection()
def get_ecf_with_key(r, extract_key: str) -> dict:
    """ pulls the ecf configuration from the cache """
    config = r.get("extract:configs:" + extract_key)
    if not config:
        log(extract_key, "Key 'extract:configs:{}' not found".format(extract_key))
        raise Exception("Extract key extract:configs:{} is missing from cache".format(extract_key))
    return json.loads(config.decode("utf-8"))


@redis_connection()
def wait_for_control_cmd(r, extract_key: str) -> dict:

    cmd = r.blpop("extract:control:" + extract_key)
    return cmd


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


@redis_connection()
def update_status(r, extract_key: str, status) -> int:
    """ updates the stored progress values"""
    return r.set("extract:status:{}".format(extract_key), status)


@redis_connection()
def update_control(r, extract_key: str, status) -> int:
    """ updates the stored progress values"""
    return r.lpush("extract:control:{}".format(extract_key), status)


class RedisLogHandler(logging.Handler):

    def __init__(self, package_id):
        logging.Handler.__init__(self)
        self.package_id = package_id

    def emit(self, record: str):
        line = self.format(record) + '\n'
        log(self.package_id, line)


def _run_extraction(stream: pyextract.DataStream,
                    output: ABCMessenger,
                    output_folder: str,
                    package_name: str,
                    worker_timeout: int,
                    ecf_file: str,
                    max_workers: int = 4,
                    callback_error: Callable[[str], None] = None,
                    chunk_results: str = 'db_per_table',
                    resume_extract: bool = False,
                    gauge=None,
                    sqlite_password: str = None,
                    dockerPackageId: str = None):

    """Top-level API/function to run all extractions from external interfaces."""
    utils.setup_multiproc_logger()

    assert os.path.exists(ecf_file), \
        'ECF file does not exist at "{}"'.format(ecf_file)

    # Start timer for extraction, and create a list where
    # warnings will be collected so that they can be displayed
    # as pop-up alerts to the user once Extraction completes
    starttime = time.time()
    warnings = []  # type: List[str]

    # Build logfile filename from Extract ID
    extract_id = common._extract_id_from_package(package_name)
    logname = 'extract_{}.log'.format(extract_id)
    # logging_dir = os.path.join(LOG_DIR, dockerPackageId)
    logfile = os.path.join(LOG_DIR, logname)

    # If an error occurs during extraction, this will still be None,
    # and we can determine which cleanup actions to take based on that
    extraction = None
    errors = False

    try:
        extraction, errors = pyextract.extract_from_ecf(
            ecf=ecf_file, source=stream, output=output, logfile=logfile,
            max_writers=1, worker_timeout=worker_timeout,
            chunk_results=chunk_results, resume_extract=resume_extract,
            extract_id=extract_id, gauge=gauge, dockerPackageId=dockerPackageId
        )

    except AssertionError as error:
        # A known error is occurring, log error and alert user
        LOGGER.error(error)
        pyextract.utils.flush_log_to_file(LOGGER, logfile)
        if callback_error:
            callback_error(str(error))

    except:  # pylint: disable=broad-except
        message = ('An unhandled error occured during extraction. '
                   'See the log file for details:  {}'.format(logfile))
        LOGGER.error(message)
        # Remove the GUI handler if it's enabled before writing traceback
        LOGGER.handlers = [handler for handler in LOGGER.handlers]
        # Write the traceback to the shell and text log
        LOGGER.exception('An unhandled error occured during extraction')
        pyextract.utils.flush_log_to_file(LOGGER, logfile)
        # Call the callback function to alert via GUI if provided
        if callback_error:
            callback_error(message)

    # Package all the output data together and write it to disk
    LOGGER.info('All data pulls complete, packaging data for upload...')
    package = common._create_data_package(ecf_file, output_folder)
    zip_path = os.path.join(output_folder, package_name)
    package.create(zip_path, sqlite_password)
    LOGGER.info('Data packaging complete, data is ready for upload')

    # Report total time and # records extracted to the user
    if extraction:
        if errors:
            LOGGER.warning("Extractions completed WITH ERRORS in {:.2f} seconds."
                           .format(time.time() - starttime))
        else:
            LOGGER.info("Extractions completed in {:.2f} seconds."
                        .format(time.time() - starttime))
        common.log_final_record_count(extraction, False)
    else:
        LOGGER.warning("Extractions completed with errors in {:.2f} seconds."
                       .format(time.time() - starttime))

    # Delete the extraction obj (and children) with stricter logging,
    # to avoid 'sending shutdown to manager' message that occurs during it
    LOGGER.setLevel(logging.WARNING)
    del extraction
    LOGGER.setLevel(logging.INFO)

    # Delete tracking table and close any open DB connections
    # output.drop_table_if_exists("temp_tracker")
    output._conn.close()

    # Delete items that are now packaged
    for item in os.listdir(output_folder):
        if not item.endswith('.zip'):
            while True:
                try:
                    os.remove(os.path.join(output_folder, item))
                    break
                except PermissionError as err:
                    LOGGER.debug("{} still being inserted into package.  Waiting 3 seconds...".format(item))
                    time.sleep(3)


class ExtractThread(threading.Thread):

    def __init__(self, stream, output,output_folder, package_name, worker_timeout, ecf_file, max_workers,
                    callback_error, chunk_results, resume_extract, gauge, sqlite_password, dockerPackageId, stopevent):

        super().__init__()
        self.stream = stream
        self.output = output
        self.output_folder = output_folder
        self.package_name = package_name
        self.worker_timeout = worker_timeout
        self.ecf_file = ecf_file
        self.max_workers = max_workers
        self.callback_error = callback_error
        self.chunk_results = chunk_results
        self.resume_extract = resume_extract
        self.gauge = gauge
        self.sqlite_password = sqlite_password
        self.dockerPackageId = dockerPackageId
        self.stopevent = stopevent

    def run(self):

        if self.resume_extract:
            package_path = os.path.join(self.output_folder, self.package_name)

            if not os.path.exists(package_path):
                LOGGER.warning('Data package to continue from was not found '
                               'at path "{}"'.format(package_path))
                LOGGER.warning('Extraction will be restarted completely.')
            else:
                _check_for_floating_databases(self.output_folder)
                common.unzip_package(package_path, self.output_folder, filetype='.dat')

        _run_extraction(
            stream=self.stream,
            output=self.output,
            output_folder=self.output_folder,
            package_name=self.package_name,
            worker_timeout=self.worker_timeout,
            ecf_file=self.ecf_file,
            max_workers=self.max_workers,
            callback_error=self.callback_error,
            chunk_results=self.chunk_results,
            resume_extract=self.resume_extract,
            gauge=self.gauge,
            sqlite_password=self.sqlite_password,
            dockerPackageId=self.dockerPackageId,

        )
        update_control(self.dockerPackageId, "Complete")
        update_status(self.dockerPackageId, "Completed")


def _upload(package_path, upload_method, sftp_name, lfu_name):

    if upload_method == "MFT_LFU":
        # Upload to PwC using the SFTP method
        if not sftp_name:
            raise Exception("'sftp_name' required. {}".format(traceback.format_exc()))
        creds = config.SFTP_UPLOAD_LOCATIONS[sftp_name]
        try:
            client = pyextract.SFTPClient(creds=creds)
            client.send(package_path)
        except Exception as error:
            response = [creds['HOSTADDRESS'], error]
            success = False
        else:
            response = [creds['HOSTADDRESS']]
            success = True

    elif upload_method == "LFU":

        # Upload to PwC using the REST API method
        if not lfu_name:
            raise Exception("'lfu_name' required. {}".format(traceback.format_exc()))
        kwargs = config.LFU_UPLOAD_LOCATIONS[lfu_name]
        try:
            client = pyextract.LFUClient(**kwargs)
            client.send(package_path, chunk_size=1000000)
        except Exception as error:
            response = [kwargs['host'], error]
            success = False
        else:
            response = [kwargs['host']]
            success = True


def run():

    try:
        stopevent = threading.Event()

        # Step 1:  Get Package ID from Environment Variable
        package_id = os.getenv("PACKAGE_ID", None)
        update_status(package_id, "Running")

        if package_id:
            log(package_id, "Setting up Extraction for package {}", package_id)
        else:
            log("None", "PACKAGE_ID env not set.".format(package_id))
            return

        log(package_id, "Running PyExtract Build {}".format(DOCKER_VERSION))

        # Step 2:  Get Configuration for Package ID from Redis
        pkg_config = get_ecf_with_key(package_id)
        log(package_id, "Received {}", pkg_config["package_id"])

        # Step 3:  Write ECF file.
        ecf_path = os.path.join(ECF_DIR, "pkg-{}.ecf".format(pkg_config["package_id"]))
        with open(ecf_path, 'wb') as ecf_file:
            ecf_file.write(base64.b64decode(pkg_config["ecf_b64_bin"]))
        log(package_id, "Wrote ECF to {}".format(ecf_path))
        actual_ecf = pyextract.read_encrypted_json(ecf_path)
        data_source = actual_ecf["DataSource"]

        # Step 4:  Determine Package Name
        package_name = "".join(["Package_", pkg_config["package_id"], ".zip"])
        output_folder = os.path.join(OUT_DIR, pkg_config["package_id"])
        os.makedirs(output_folder, exist_ok=True)

        # Step 5:  Determine SQLite Password
        if pkg_config["sqlite_password"]:
            sqlite_password = pkg_config["sqlite_password"]
        else:
            sqlite_password = common._unique_sqlite_password(keylen=32)

        # Step 6:  Create SQLiteMessenger to be used for output.
        output = common._messenger_from_password(sqlite_password, output_folder)

        # Step 7:  Create Appropriate Stream
        if data_source["DataConnector"] == "Data Provider for Oracle":
            source_msgr = pyextract.connect.oracle.OracleMessenger(**pkg_config["connection_args"])
            source = pyextract.streams.sqlstream.DataStream(messenger=source_msgr,
                                                            chunk_size=50000,
                                                            stopevent=stopevent)

        elif data_source["DataConnector"] == "RFC":
            if pkg_config["connection_type"] == "Direct Connection":
                source_msgr = pyextract.connect.sap.SAPMessenger(**pkg_config["connection_args"])
                source = pyextract.streams.sapstream.SAPStream(messenger=source_msgr,
                                                               chunk_size=500000,
                                                               batch_size=200,
                                                               stopevent=stopevent,
                                                               chunk_results='db_per_table',
                                                               output=output)

        elif data_source["DataConnector"] == "Data Provider for DB2" or data_source["DataConnector"] == "DB2 RDBMS":
            source_msgr = pyextract.connect.db2.DB2Messenger(**pkg_config["connection_args"])
            source = pyextract.streams.sqlstream.DataStream(messenger=source_msgr)

        elif data_source["DataConnector"] == "Data Provider for SQL Server":
            source_msgr = pyextract.connect.mssql.MSSQLMessenger(**pkg_config["connection_args"])
            source = pyextract.streams.sqlstream.DataStream(messenger=source_msgr,
                                                            stopevent=stopevent)

        elif data_source["DataConnector"] == "Data Provider for MySQL":
            source_msgr = pyextract.connect.mysql.MySQLMessenger(**pkg_config["connection_args"])
            source = pyextract.streams.sqlstream.DataStream(messenger=source_msgr)



        # Step 9:  Setup Logging
        logfmt = "%(asctime)s -- %(levelname)s -- %(message)s"
        datefmt = '%I:%M:%S %p'
        handler = RedisLogHandler(package_id=package_id)
        handler.setFormatter(logging.Formatter(logfmt, datefmt=datefmt))
        LOGGER.addHandler(handler)
        multiprocessing.get_logger().addHandler(handler)

        # Step 10:  Determine if its a pause or resume
        is_resume = False

        # Step 12:  Execute Extraction
        log(package_id, "Starting extraction for package {}.", package_id)

        extraction_parameters = {
            "stream": source,
            "output": output,
            "output_folder": output_folder,
            "package_name": package_name,
            "worker_timeout": 60,
            "ecf_file": ecf_path,
            "max_workers": 4,
            "callback_error": None,
            "chunk_results": 'db_per_table',
            "resume_extract": is_resume,
            "gauge": None,
            "sqlite_password": sqlite_password,
            "dockerPackageId": package_id,
            "stopevent": stopevent
        }

        # Start Extraction Thread
        t = ExtractThread(**extraction_parameters)
        t.start()

        # Wait for control commands...
        cmd_b = wait_for_control_cmd(package_id)
        cmd = cmd_b[1].decode("utf-8")

        if cmd == "Complete":
            update_status(package_id, "Uploading")
            log(package_id, "Starting package upload...", package_id)
            update_progress(package_id, 90)
            if "upload_mft" in pkg_config:
                if pkg_config["upload_mft"]:
                    if "mft_env" in pkg_config:
                        mft_env = pkg_config["mft_env"]
                    else:
                        mft_env = 'STAGE'
                    _upload(os.path.join(OUT_DIR, package_name), actual_ecf["FileUploadMethod"], sftp_name=mft_env, lfu_name=None)
            update_progress(package_id, 100)
            update_status(package_id, "Complete")
        elif cmd == "Pause":
            update_status(package_id, "Paused")
            log(package_id, "Extraction paused by user", package_id)
            stopevent.set()

        log(package_id, "Extraction complete using PyExtract Build {}".format(DOCKER_VERSION))

    except:
        log(package_id, traceback.format_exc())
        update_status(package_id, "Failed")
        cleanup(package_id)


    cleanup(package_id)


if __name__ == '__main__':
    run()

