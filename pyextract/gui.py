"""Graphic interface for the PyExtract program."""
# pylint: disable=no-member,unused-argument
# pylint: disable=too-few-public-methods,invalid-name

######################################################################################
## BEGIN: Search for reusable Python libs and connect them via sys.path
##         This must happen before any other imports...
##         This code only adds the required library if it does not otherwise appear.
######################################################################################

import os, sys
fpath = sys.executable if (hasattr(sys, 'frozen')) else sys.argv[0]
fpath = os.path.dirname(fpath)
for dirname, dirs, dirnames in os.walk(fpath):
    if (len([f for f in dirnames if (f.find('__init__.py') > -1)]) > 0):
        __has__ = len([f for f in sys.path if (f.find(dirname) > -1)]) > 0
        if (not __has__):
            dname = os.path.dirname(dirname)
            i = 0
            for f in sys.path:
                if (f.find(dname) > -1):
                    i += 1
                    break
                i += 1
            sys.path.insert(i, dirname)
            break # only handles the first instance...

#######################################################################
## END!!! Search for reusable Python libs and connect them via sys.path
#######################################################################

from datetime import datetime
import importlib
import logging
import multiprocessing
import os
from pprint import pformat
import shutil
import sys
import threading
import time
import uuid
from zipfile import ZipFile
from typing import Callable, Dict, List
import traceback
from pyextract.connect.sqlite import sqlite_connection
import apsw
import chilkat
import wx
import wx.grid
from wx.lib.newevent import NewEvent

import pyextract
from pyextract import config
from pyextract import version
from pyextract.connect import ABCMessenger
from pyextract.connect.abap import ABAPMessenger, ABAPInputGenerate

import pyextract.utils
from pyextract.utils import DependencyError, NetworkDisconnectError
import common

# Logging setup for normal and multiprocess loggers
pyextract.utils.setup_logging()
pyextract.utils.setup_multiproc_logger()

LOGGER = multiprocessing.get_logger()
LOGGER.setLevel(logging.INFO)

# Global GUI Settings
FONT_TITLES = wx.Font(12, 74, 90, 92, False, "Arial")
FONT_BOLD = wx.Font(wx.FontInfo(9).Bold())

# User config settings for entire GUI
USER_CONFIGS = (
    'working_directory', 'encryption', 'lfu_location', 'sftp_location', 'chunk_size',
    'sap_chunk_size', 'queue_size', 'worker_timeout', 'sap_sdk_folder',
    'oracle_client_folder', 'ibm_dll_folder', 'sftp_port', 'log_level',
    'sap_batch_size', 'auto_upload', 'max_readers', 'rename_wait'
)
USER_CONFIG_NAMES = {
    'encryption': 'Encryption Type',
    'sftp_location': 'SFTP Upload Location',
    'lfu_location': 'LFU Upload Location',
    'chunk_size': 'Non-SAP Rows per request',
    'sap_chunk_size': 'SAP Rows per request',
    'queue_size': 'Max requests in memory',
    'worker_timeout': 'Max writer wait time',
    'sftp_port': 'SFTP Upload Port',
    'log_level': 'Logging Level',
    'sap_batch_size': 'SAP Batch Size',
    'working_directory': 'Working Directory',
    'sap_sdk_folder': 'SAP Netweaver SDK',
    'oracle_client_folder': 'Oracle Client',
    'ibm_dll_folder': 'IBM DB2 Driver',
    'auto_upload': 'Auto Upload Enabled',
    'max_readers': 'Max data read threads',
    'rename_wait': 'Rename wait'
}
USER_CONFIG_CHOICES = {
    'encryption': config.ENCRYPTION_OPTIONS,
    'sftp_location': sorted(config.SFTP_UPLOAD_LOCATIONS),
    'lfu_location': sorted(config.LFU_UPLOAD_LOCATIONS),
    'sftp_port': ['22', '52222'],
    'log_level': ['INFO', 'DEBUG'],
    'auto_upload': ['No', 'Yes'],
}

USER_CONFIG_TOOLTIPS = {
    'encryption': 'This option controls how the data is encrypted when it is extracted.  '
                  'Default value is AES-256 and cannot be changed.',
    'sftp_location': 'Environment to upload data to when using SFTP service',
    'lfu_location': 'Environment to upload data to when using Large File Upload (LFU) service',
    'chunk_size': ''.join([
        'When extracting data from a given table, PwC Extract make numerous calls/requests ',
        'to the database for data returning the data in small {0}chunks{1} until the all the required '.format(chr(226)+chr(8364)+chr(339), chr(226)+chr(8364)+chr(65533)),
        'data is extracted.  Extracting the data in smaller chunks helps control the amount of ',
        'resources (i.e. memory) consumed on the machine where Extract is installed and limits the ',
        'performance impact on the client{0}s system.  This option controls the number of records '.format(chr(226)+chr(8364)+chr(8482)),
        'extracted in each request and impacts only non-SAP (Oracle, MSSQL, DB2, MySQL).  Default Setting: 50,000.'
    ]),
    'sap_chunk_size': ''.join([
        'When extracting data from a given table, PwC Extract make numerous calls/requests ',
        'to the database for data returning the data in small {0}chunks{1} until the all the required '.format(chr(226)+chr(8364)+chr(339), chr(226)+chr(8364)+chr(65533)),
        'data is extracted.  Extracting the data in smaller chunks helps control the amount of ',
        'resources (i.e. memory) consumed on the machine where Extract is installed and limits the ',
        'performance impact on the client{0}s system.  This option controls the number of records '.format(chr(226)+chr(8364)+chr(8482)),
        'extracted in each request and impacts only SAP. Default Setting: 500,000.'
    ]),
    'queue_size': (
        ''.join([
            'PwC Extract uses parallel processing to make the extraction processes more efficient.  ',
            'Reads are treated as separate tasks.  As data is read, it is {0}queued{1} and waits for write '.format(chr(226)+chr(8364)+chr(339), chr(226)+chr(8364)+chr(65533)),
            'process to write the data to disk.  This option controls the number of read requests that ',
            'can be on the queue before PwC Extract will stop making requests for more data, waiting for ',
            'space in the queue to open up.  This also helps control the amount of resources (i.e. memory) ',
            'consumed on the machine where Extract is installed.  Default Setting: 20. '
        ])
    ),
    'worker_timeout': (
        'Maximum number of seconds that writers will wait for data. '
        'If this limit is reached by all writers, the extraction will time '
        'out and need to be restarted by the user. '
        'Leave blank or put "0" for no timeout.'
    ),
    'sftp_port': 'When using SFTP to transfer data packages to PwC, PwC Extract supports 2 different ports.  '
                 'This option determines which port PwC Extract will use.  Options include: 22 (default) and 52222.',
    'log_level': 'During extraction, PwC Extract generates a log.  This log is presented to the user in the '
                 'extraction panel during extraction and is included in the final data package (.zip).  This '
                 'option determines how verbosity of the log.  If preferred and or required for troubleshooting '
                 'purposes, the DEBUG option provides additional logging information that INFO does not.  INFO '
                 'is generally sufficient for most uses.  Options include: INFO (default) and DEBUG.',
    'sap_batch_size': (
        ''.join([
            'When extracting {0}large{1} tables from SAP, generally line item/detail tables that have a corresponding '.format(chr(226)+chr(8364)+chr(339), chr(226)+chr(8364)+chr(65533)),
            'header table, this option provides another method of breaking up the extraction into smaller chunks ',
            'instead of trying to retrieve all the records in the table at one time.  Extracting the data in smaller ',
            'chunks helps control the amount of resources (i.e. memory) consumed on the machine where Extract is ',
            'installed and limits the performance impact on the client{0}s system.  This option impacts only SAP and '.format(chr(226)+chr(8364)+chr(8482)),
            'should not be altered from the default unless instructed by Support.  Default Setting: 200. '
        ])
    ),
    'working_directory': (
        'The directory on the local machine where data will be stored '
        'during and between extractions'
    ),
    'sap_sdk_folder': (
        'The directory on the local machine that contains all components '
        'of the SAP Netweaver SDK'
    ),
    'oracle_client_folder': (
        'The directory on the local machine that contains all components '
        'of the Oracle Client'
    ),
    'ibm_dll_folder': (
        'The directory on the local machine that contains all components '
        'of the IBM DB2 Driver'
    ),
    'auto_upload': ''.join([
        ' This option determines whether or not PwC Extract will automatically upload data after a ',
        'successful extraction.  When set to {0}No,{1} the data upload needs to be manually started.  '.format(chr(226)+chr(8364)+chr(339), chr(226)+chr(8364)+chr(65533)),
        'Options include: No (default) and Yes'
    ]),
    'ashost': 'SAP App Server to pull data from (hostname or IP address)',
    'client': 'SAP Client ID',
    'database': 'Name of the database to connect to',
    'group': 'Group',
    'host': 'Server to pull data from (hostname or IP address)',
    'language': 'SAP Language Code (e.g. "EN" for English)',
    'mshost': 'SAP Message server / load balancer (hostname or IP address)',
    'msserv': 'Service name of the SAP message server (optional)',
    'password': 'Password to authenticate access',
    'port': 'Port to connect to the server / host with',
    'schema': 'Name of the data schema to connect to',
    'snc_lib': 'SAP Secure Network Connection (SNC) library',
    'snc_myname': 'SAP SNC Initiator\'s Name',
    'snc_partnername': 'SNC Partner Name*',
    'snc_qop': 'SAP SNC Quality of Protection',
    'sysid': 'SAP System ID',
    'sysnr': 'SAP System Number',
    'user': 'Username to authenticate access',
    'abap_folder': 'Folder to save SAP ABAP .FIL files in',
    'abap_filename': 'Filename to save for SAP ABAP input generation file',
    'max_readers': ''.join([
        'PwC Extract uses parallel processing to make the extraction processes more efficient.  ',
        'Reads and writes are treated as separate tasks.  This option determines how many read ',
        '{0}workers{1} are used during data read.  Setting the number of workers helps control the '.format(chr(226)+chr(8364)+chr(339), chr(226)+chr(8364)+chr(65533)),
        'amount of resources (i.e. memory) consumed on the machine where Extract is installed.  ',
        'This option should not be altered from the default unless instructed by Support.  ',
        'Default Setting: 4. '
    ]),
    'rename_wait': 'Determines how long PwC Extract will wait to rename uploaded files with _Complete suffix.'
}
USER_CONFIG_DEFAULTS = {
    'working_directory': pyextract.utils.local_appdata_path('Data'),
    'encryption': 'AES-256',
    'lfu_location': 'QA-WEST',
    'sftp_location': 'QA',
    'chunk_size': '50000',
    'sap_chunk_size': '500000',
    'queue_size': '20',
    'worker_timeout': '0',
    'sftp_port': '22',
    'log_level': 'INFO',
    'sap_batch_size': '200',
    'auto_upload': 'No',
    'max_readers': 4,
    'rename_wait': 2
}


class ConfigDatabase(object):
    """Encrypted SQLite database of configuration settings for PyExtract."""

    configs_cols = ('NAME', 'VALUE')
    ecf_cols = ('EXTRACT_ID', 'REQUEST_ID', 'STARTED_DATE',
                'DATA_SERVER', 'DATA_CONNECTOR', 'FILE_PATH',
                'EXTRACTION_PASSWORD')

    def __init__(self, filepath: str = None):
        """Return a new Config object from a SQLite filepath."""
        self._filepath = None
        self.filepath = filepath or pyextract.utils.local_appdata_path('extract.config')
        # Initialize SQLite database tables if they don't exist
        self.create_tables()

    @property
    def filepath(self):
        """filepath propery decorator"""
        return self._filepath

    @filepath.setter
    def filepath(self, path: str):
        """setter method to ensure the current config db is encrypted"""
        # Create intermediate folders if they don't exist
        os.makedirs(os.path.dirname(path), exist_ok=True)

        # Ensure database can be created or opened
        try:
            with sqlite_connection(path) as cursor:
                cursor.execute("SELECT 1 FROM sqlite_master LIMIT 1")
        except (apsw.CantOpenError, apsw.IOError):
            try:
                import wx
                message = (
                    'Unencrypted user config database detected. Creating a new '
                    'encrypted database. Saved credentials / configs will be lost.'
                )
                wx.MessageBox(message, 'Warning', style=wx.ICON_EXCLAMATION)
                os.remove(path)
            except:
                pass

        self._filepath = path

    def create_tables(self):
        """Create all configuration tables in the Config database.

        If any table already exists, no action will be taken for that table.
        """
        erp_field_definitions = {
            'SAP': {
                'name': 'VARCHAR(200) PRIMARY KEY',
                'type': 'VARCHAR(100) NOT NULL',
                'client': 'VARCHAR(100) NULL',
                'user': 'VARCHAR(100) NULL',
                'password': 'VARCHAR(100) NULL',
                'language': 'VARCHAR(100) NULL',
                'ashost': 'VARCHAR(100) NULL',
                'sysnr': 'VARCHAR(100) NULL',
                'mshost': 'VARCHAR(100) NULL',
                'msserv': 'VARCHAR(100) NULL',
                'sysid': 'VARCHAR(100) NULL',
                'group': 'VARCHAR(100) NULL',
                'snc_qop': 'VARCHAR(100) NULL',
                'snc_myname': 'VARCHAR(100) NULL',
                'snc_partnername': 'VARCHAR(100) NULL',
                'snc_lib': 'VARCHAR(300) NULL',
            },
            'ORACLE': {
                'name': 'VARCHAR(200) PRIMARY KEY',
                'type': 'VARCHAR(100) NOT NULL',
                'host': 'VARCHAR(100) NULL',
                'port': 'VARCHAR(10) NULL',
                'system_id': 'VARCHAR(100) NULL',
                'service_name': 'VARCHAR(100) NULL',
                'user': 'VARCHAR(100) NULL',
                'password': 'VARCHAR(100) NULL',
            },
            'MSSQL': {
                'name': 'VARCHAR(200) PRIMARY KEY',
                'type': 'VARCHAR(100) NOT NULL',
                'host': 'VARCHAR(100) NOT NULL',
                'port': 'VARCHAR(100) NULL',
                'schema': 'VARCHAR(100) NULL',
                'database': 'VARCHAR(100) NOT NULL',
                'user': 'VARCHAR(100) NULL',
                'password': 'VARCHAR(100) NULL',
                'driver': 'VARCHAR(100) NULL',
            },
            'DB2': {
                'name': 'VARCHAR(200) PRIMARY KEY',
                'type': 'VARCHAR(100) NOT NULL',
                'host': 'VARCHAR(100) NULL',
                'port': 'VARCHAR(10) NULL',
                'database': 'VARCHAR(100) NULL',
                'user': 'VARCHAR(100) NULL',
                'password': 'VARCHAR(100) NULL',
            },
            'MYSQL': {
                'name': 'VARCHAR(200) PRIMARY KEY',
                'type': 'VARCHAR(100) NOT NULL',
                'host': 'VARCHAR(100) NULL',
                'port': 'VARCHAR(10) NULL',
                'dsn': 'VARCHAR(100) NULL',
                'database': 'VARCHAR(100) NULL',
                'user': 'VARCHAR(100) NULL',
                'password': 'VARCHAR(100) NULL',
                'driver': 'VARCHAR(100) NULL',
            },
        }

        with sqlite_connection(self.filepath) as cursor:
            # Check what ERP tables already exist and what columns they have
            cursor.execute("""
                SELECT tbl_name
                FROM sqlite_master
                WHERE type = "table"
                """)
            tables = [row[0] for row in cursor.fetchall()]
            existing_erps = [table[:-12] for table in tables
                             if table.endswith('_CREDENTIALS')]

            # Create all ERP credential tables that don't exist
            for erp, field_definitions in erp_field_definitions.items():

                # Create new table if it doesn't exist at all
                if erp not in existing_erps:
                    statement = common.sqlite_create_credential_table(erp, field_definitions)
                    cursor.execute(statement)
                    continue

                # Otherwise, add any new columns that don't currently exist
                cursor.execute('PRAGMA table_info({}_CREDENTIALS)'.format(erp))
                existing_fields = [row[1] for row in cursor.fetchall()]

                for field, definition in field_definitions.items():
                    if field not in existing_fields:
                        statement = common.sqlite_add_cred_table_field(erp, field, definition)
                        cursor.execute(statement)

            # Create Config Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS CONFIGS(
                    "NAME" VARCHAR(200) PRIMARY KEY,
                    "VALUE" VARCHAR(400) NOT NULL
                )
                """)

            # Create ECF Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS ECFS(
                    "EXTRACT_ID" VARCHAR(100) PRIMARY KEY,
                    "REQUEST_ID" VARCHAR(100) NOT NULL,
                    "STARTED_DATE" VARCHAR(100) NOT NULL,
                    "DATA_SERVER" VARCHAR(100) NOT NULL,
                    "DATA_CONNECTOR" VARCHAR(100) NOT NULL,
                    "FILE_PATH" VARCHAR(500) NOT NULL,
                    "EXTRACTION_PASSWORD" VARCHAR(100) NULL
                )
                """)

    # Saved Connections / Credentials
    def conn_exists(self, name: str, erp='SAP') -> bool:
        """Return True if a connection is saved in database for an ERP."""
        erp = erp.upper()
        assert erp in config.ERPS_TO_CREDENTIALS
        stmt = "SELECT * FROM {}_CREDENTIALS WHERE NAME = ?".format(erp)
        args = (name,)
        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(stmt.strip(), args)
            data = cursor.fetchall()
        return bool(data)

    def saved_credential_names(self, erp='SAP') -> List[str]:
        """Return names of saved credentials for an ERP."""
        erp = erp.upper()
        assert erp in config.ERPS_TO_CREDENTIALS
        query = "SELECT NAME FROM {}_CREDENTIALS".format(erp)
        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(query)
            names = [row[0] for row in cursor.fetchall()]
        return names

    def get_credentials(self, name: str, erp='SAP') -> Dict[str, str]:
        """Return dict of parameters to values to instantiate a Messenger."""
        erp = erp.upper()
        assert erp in config.ERPS_TO_CREDENTIALS

        # Build SQL statement to select ordered credential data
        erp_credentials = config.ERPS_TO_CREDENTIALS[erp]
        columns = ['"{}"'.format(col) for col in erp_credentials]
        query = """
            SELECT {} FROM {}_CREDENTIALS WHERE NAME = ?
            """.format(','.join(columns), erp)
        args = (name,)

        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(query, args)
            data = cursor.fetchall()

        # Convert table of credential data into kwargs for an ABCMessenger
        kwargs = {}
        for index, column in enumerate(erp_credentials):
            kwargs[column.lower()] = data[0][index]

        return kwargs

    def save_credentials(self, name: str, creds: Dict[str, str],
                         erp='SAP', save_password=False):
        """Save dictionary of credentials to the database for an ERP.
        Will not save password information unless the user requests it.
        """
        creds = creds.copy()
        if 'password' in creds and not save_password:
            del creds['password']

        erp_credentials = config.ERPS_TO_CREDENTIALS[erp]
        for key in creds:
            assert key in erp_credentials

        # Build SQL statement used to insert credential data
        columns = ['"{}"'.format(col) for col in erp_credentials]
        placeholders = ['?'] * len(columns)
        statement = (
            "INSERT INTO {}_CREDENTIALS ({}) VALUES ({})"
            ).format(erp, ','.join(columns), ','.join(placeholders))

        # Transform creds to be saved into row for DB insertion
        creds['name'] = name
        args = tuple(creds.get(key) for key in erp_credentials)

        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(statement, args)

    def delete_credentials(self, name: str, erp='SAP'):
        """Delete saved credentials based on name and ERP."""
        erp = erp.upper()
        assert erp in config.ERPS_TO_CREDENTIALS
        query = "DELETE FROM {}_CREDENTIALS WHERE NAME = ?".format(erp)
        args = (name,)
        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(query, args)

    # Saved User Config Settings
    def does_config_exist(self, name: str) -> bool:
        """Return True if the config exists in this database."""
        statement = "SELECT * FROM CONFIGS WHERE NAME = ?"
        args = (name,)
        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(statement, args)
            data = cursor.fetchall()
        return bool(data)

    def save_config(self, name: str, value: str):
        """Save a the given config to this local database."""
        if self.does_config_exist(name):
            statement = "UPDATE CONFIGS SET VALUE = ? WHERE NAME = ?"
            args = (value, name)
        else:
            cols_str = ", ".join(self.configs_cols)
            placeholders = ", ".join(["?" for each in self.configs_cols])
            statement = (
                "INSERT INTO CONFIGS ({}) VALUES ({})"
                ).format(cols_str, placeholders)
            args = (name, value)

        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(statement, args)

    def get_config_value(self, name: str) -> str:
        """Return the value saved for a given config name."""
        statement = "SELECT VALUE FROM CONFIGS WHERE NAME = ?"
        args = (name,)
        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(statement, args)
            data = cursor.fetchall()

        if data:
            return data[0][0]

        return None

    def get_config_dict(self) -> List[list]:
        """Return table (2D list) of data about all user configs."""
        statement = "SELECT NAME, VALUE FROM CONFIGS"
        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(statement)
            data = {each[0]: each[1] for each in cursor.fetchall()}
        return data

    # Saved Extractions
    def saved_extract_exists(self, extract_id: str) -> bool:
        """Return True if an extraction has been saved to this database."""
        statement = "SELECT 1 FROM ECFS WHERE EXTRACT_ID = ?"
        args = (extract_id,)
        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(statement, args)
            data = cursor.fetchall()
        return bool(data)

    def saved_request_exists(self, request_id: str) -> bool:
        """Return True if ANY extraction(s) have been saved for a request."""
        statement = "SELECT 1 FROM ECFS WHERE REQUEST_ID = ?"
        args = (request_id,)
        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(statement, args)
            data = cursor.fetchall()
        return bool(data)

    def all_saved_extract_data(self) -> List[list]:
        """Return table (2D list) of data about all saved extractions."""
        with sqlite_connection(self.filepath) as cursor:
            cursor.execute("SELECT EXTRACT_ID, REQUEST_ID, STARTED_DATE, \
                           DATA_SERVER, DATA_CONNECTOR, FILE_PATH FROM ECFS")
            data = cursor.fetchall()
        return data

    def save_extract_request(self, extract_id: str, request_id: str,
                             data_server: str, data_connector: str,
                             file_path: str,
                             password: str = None):
        """Save an Extract request to the database to resume later.

        ARGS:
            extract_id: Unique ID for each extraction attempt.
            request_id: RequestId from the ECF.
            data_server: Server where data resides for this extraction.
            data_connector: Connection method used to access source data.
            file_path: Local filepath of ECF used to start this extraction.
            password: random sting for resuming extractions at a later date.
        """
        timestamp = datetime.now().isoformat()
        column_str = ", ".join(self.ecf_cols)
        placeholders = ", ".join(["?" for each in self.ecf_cols])
        statement = """
            INSERT INTO ECFS ({}) VALUES ({})
            """.format(column_str, placeholders)
        args = (extract_id, request_id, timestamp,
                data_server, data_connector, file_path,
                password)
        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(statement.strip(), args)

    def delete_saved_extract(self, extract_id: str):
        """Delete metadata about a saved extraction from this database."""
        statement = "DELETE FROM ECFS WHERE EXTRACT_ID = ?"
        args = (extract_id,)
        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(statement, args)

    def set_default_user_configs(self):
        """Set default values for any user configs not saved previously."""
        saved_config = self.get_config_dict()

        # If saved encryption option is no longer available, remove it
        # (occurs when choosing 'None' in QA build, then using prod build)
        if 'encryption' in saved_config:
            if saved_config['encryption'] not in config.ENCRYPTION_OPTIONS:
                del saved_config['encryption']

        defaults_to_set = {
            key: value for key, value in USER_CONFIG_DEFAULTS.items()
            if key not in sorted(saved_config)
        }

        if not defaults_to_set:
            return  # All settings provided already, no action needed

        # Set defaults for all unset settings in SQLite
        for key, value in defaults_to_set.items():
            self.save_config(key, value)

        if not saved_config:
            return  # First launch of program, do not alert the user

        # Notify user of all settings that were changed
        defaults_str = '\n    '.join(
            ('{}:  {}'.format(USER_CONFIG_NAMES[key], value)
             for key, value in defaults_to_set.items())
        )
        message = (
            'Program settings have been changed.\n\n    {}'
            ).format(defaults_str)
        wx.MessageBox(message, 'Info')

    def get_extraction_password(self, extraction_id: str):
        """Retrievs the unique SQLite password for a given session"""
        statement = "SELECT EXTRACTION_PASSWORD FROM ECFS WHERE EXTRACT_ID = ?"
        args = (extraction_id,)
        with sqlite_connection(self.filepath) as cursor:
            cursor.execute(statement, args)
            data = cursor.fetchall()
        assert data, 'Session unable to continue, password cannot be retrived'
        return data

class DefaultDialog(wx.Dialog):
    """The default format for Dialogs in this App.

    Binds the main app's (parent) Frame to the dialog,
    sets the icon and title for OS-level taskbar / window display,
    allows windows to be resized by the user,
    and sets the background color to pure white.
    """

    def __init__(self, parent: wx.Frame,
                 style=wx.DEFAULT_FRAME_STYLE|wx.CAPTION,
                 *args, **kwargs):
        super().__init__(parent, style=style, *args, **kwargs)
        self.parent = parent
        self.SetIcon(_extract_icon())
        self.SetTitle('Extract')
        self.SetSizeHints(wx.DefaultSize, wx.DefaultSize)
        self.SetBackgroundColour(wx.Colour(255, 255, 255))
        self.SetSize(1130, 741)






class LogBox(wx.TextCtrl):
    """Widget that displays all logging messages."""

    def __init__(self, parent: wx.Panel, app: wx.Frame):
        """Return a new LogBox widget with a handler that emits log records."""
        style = (wx.TE_CHARWRAP|wx.TE_MULTILINE|
                 wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        super().__init__(parent, style=style)

        logfmt = "%(asctime)s\t  %(levelname)-8s\t  %(message)s"
        datefmt = '%Y-%m-%d %H:%M:%S'
        handler = WxTextCtrlHandler(self, app)
        handler.setFormatter(logging.Formatter(logfmt, datefmt=datefmt))
        # Change the name of WARNING to WARN and CRITICAL to ERROR
        logging.addLevelName(30, 'WARN')
        logging.addLevelName(50, 'ERROR')
        # Add handler to root and multiproc loggers
        LOGGER.addHandler(handler)
        multiprocessing.get_logger().addHandler(handler)

    def write_line(self, line: str):
        """Write a new line to the logbox, always at the end.

        Necessary in case the user clicks the box while the log is writing,
        because the cursor must reset before writing each and every line.
        """
        self.SetInsertionPointEnd()
        self.WriteText(line)


class WxTextCtrlHandler(logging.Handler):
    """Logging handler that emits records to a wx Text Control."""

    def __init__(self, logbox: LogBox, app: wx.Frame):
        """Return a new logging.Handler connected to a LogBox."""
        logging.Handler.__init__(self)
        self.logbox = logbox
        self.app = app

    def emit(self, record: str):
        """Log a record to this handlers logbox if app still exists."""
        line = self.format(record) + '\n'
        if self.app:
            wx.CallAfter(self.logbox.write_line, line)


class GuidePanel(wx.Panel):
    """Panel with instructions for user to follow."""

    def __init__(self, parent: wx.Panel, content: str):
        """Return a new instance of a Guide panel."""
        super().__init__(parent, wx.ID_ANY, wx.DefaultPosition,
                         wx.Size(235, 625), wx.RAISED_BORDER|wx.TAB_TRAVERSAL)
        self.SetMinSize(wx.Size(235, 625))
        self.SetMaxSize(wx.Size(235, 625))
        self.sizer = wx.BoxSizer(wx.VERTICAL)

        title = wx.StaticText(self, wx.ID_ANY, 'Instructions')
        title.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92,
                              False, wx.EmptyString))
        self.sizer.Add(title, 0, wx.ALL|wx.ALIGN_CENTER_HORIZONTAL, 5)

        line = wx.StaticLine(self, wx.ID_ANY, wx.DefaultPosition,
                             wx.DefaultSize, wx.LI_HORIZONTAL)
        self.sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        text = wx.StaticText(self, wx.ID_ANY, content)
        text.Wrap(-1)
        self.sizer.Add(text, 1, wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()


class MainPanel(wx.Panel):
    """Raised panel with a consistent size that holds primary content."""
    def __init__(self, parent, size=wx.Size(865, 625)):
        super().__init__(parent, size=size, style=wx.RAISED_BORDER)
        self.SetMinSize(size)
        self.SetMaxSize(size)


class StaticLabel(wx.StaticText):
    """Non-resizable label followed by a colon, associated with a user input."""

    def __init__(self, parent: wx.Dialog, text: str, *args,
                 size=wx.Size(100, -1), **kwargs):
        """Create the label, ensuring a colon exists after text."""
        if not text.endswith(':'):
            text += ':'
        super().__init__(parent, label=text, size=size, **kwargs)
        self.Wrap(-1)
        self.SetMinSize(size)
        self.SetMaxSize(size)


class FolderSelectPanel(wx.Panel):
    """A label, text input, and button for user folder selection."""

    def __init__(self, parent: wx.Panel, label_text: str, *args,
                 tooltip_text: str = None,
                 label_size=wx.Size(130, -1), **kwargs):
        super().__init__(parent, *args, **kwargs)
        sizer = wx.BoxSizer(wx.HORIZONTAL)

        label = StaticLabel(self, text=label_text, size=label_size)
        sizer.Add(label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        self.control = wx.DirPickerCtrl(self, message="Select a folder")
        sizer.Add(self.control, 1, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        self.SetSizer(sizer)
        self.Layout()
        sizer.Fit(self)

        if tooltip_text:
            label.SetToolTip(wx.ToolTip(tooltip_text))


class UserInputPanel(wx.Panel):
    """A label and choice or text input for the ConfigsDialog."""

    def __init__(self, parent: wx.Panel, label_text: str, *args,
                 choices: list = None, tooltip_text: str = None,
                 label_size=wx.Size(100, -1),
                 control_size: wx.Size=None, **kwargs):
        super().__init__(parent, *args, **kwargs)
        sizer = wx.BoxSizer(wx.HORIZONTAL)

        label = StaticLabel(self, text=label_text, size=label_size)
        sizer.Add(label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        if choices:
            self.control = wx.Choice(self, choices=choices)
            self.control.SetSelection(1)
        elif 'password' in label_text.lower():
            self.control = wx.TextCtrl(self, style=wx.TE_PASSWORD)
        else:
            self.control = wx.TextCtrl(self)

        if control_size:
            self.control.SetMaxSize(control_size)
            self.control.SetMinSize(control_size)

        sizer.Add(self.control, 1, wx.ALL|wx.EXPAND, 5)
        if label_text == 'SAP Rows per request':
            sizer.Add(StaticLabel(self, text="Take caution when increasing row size greater than 500K.  If required to "
                                             "avoid row skips, it is suggested you run extractions during non-business "
                                             "hours or even run the extraction in a non-production environment that mimicks "
                                             "production so it can be monitored by your client's administrator before running "
                                             "in production.", size=wx.Size(400, 75)), 0, wx.EXPAND | wx.ALL, 5)
        self.SetSizer(sizer)
        self.Layout()
        sizer.Fit(self)

        if tooltip_text:
            label.SetToolTip(wx.ToolTip(tooltip_text))


class CopyableMessageBox(wx.Dialog):
    """Replication of wx.MessageBox except you can copy the text contents."""
    def __init__(self, parent: wx.Dialog, title: str, text: str):
        super().__init__(parent, title=title,
                         style=wx.DEFAULT_DIALOG_STYLE|wx.RESIZE_BORDER)
        self.SetSizeHints(wx.DefaultSize, wx.DefaultSize)

        textstyle = (wx.TE_CHARWRAP|wx.TE_MULTILINE|
                     wx.TE_READONLY|wx.HSCROLL|wx.VSCROLL)
        textbox = wx.TextCtrl(self, style=textstyle)
        textbox.SetValue(text)
        textbox.SetBackgroundColour(wx.Colour(255, 255, 255))

        self.ShowModal()
        self.Destroy()


class ConfigsDialog(wx.Dialog):
    """Panel for the user to set advanced configuration for the program."""

    def __init__(self, parent: wx.Frame):
        """Return a new instance of the dialog window."""
        super().__init__(parent, title=u"Extract Configuration",
                         size=wx.Size(888, 528),
                         style=wx.DEFAULT_DIALOG_STYLE|wx.RESIZE_BORDER)
        self.config_db = ConfigDatabase()
        self.parent = parent
        self.SetSizeHints(wx.DefaultSize, wx.DefaultSize)

        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.controls = {}  # type: List[wx.Control]

        self.top_panel = wx.Panel(self)
        top_panel_sizer = wx.BoxSizer(wx.VERTICAL)

        # Create and add all the Folder Select inputs
        folder_controls = ('working_directory', 'sap_sdk_folder',
                           'oracle_client_folder', 'ibm_dll_folder')
        for control in folder_controls:
            text = USER_CONFIG_NAMES.get(control, control)
            tooltip_text = USER_CONFIG_TOOLTIPS.get(control)
            panel = FolderSelectPanel(self.top_panel, label_text=text,
                                      tooltip_text=tooltip_text)
            self.controls[control] = panel.control
            top_panel_sizer.Add(panel, 1, wx.EXPAND|wx.ALL, 5)

        # End folder picker section, begin user input panels
        self.top_panel.SetSizer(top_panel_sizer)
        self.top_panel.Layout()
        top_panel_sizer.Fit(self.top_panel)
        self.sizer.Add(self.top_panel, 0, wx.EXPAND|wx.ALL, 5)

        # Add title above Advanced Options section
        adv_opts_title = wx.StaticText(self, label='Advanced Options')
        adv_opts_title.SetFont(FONT_BOLD)
        self.sizer.Add(adv_opts_title, 0, wx.EXPAND|wx.LEFT|wx.TOP, 10)

        # Advanced Options panel with scrolling
        scroll_panel = wx.ScrolledWindow(self, style=wx.SUNKEN_BORDER|wx.VSCROLL)
        scroll_panel.SetScrollRate(5, 5)
        scroll_panel.SetBackgroundColour((215, 215, 215))
        scroll_window_sizer = wx.BoxSizer(wx.VERTICAL)

        adv_opts_panel = wx.Panel(scroll_panel)
        adv_opts_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        # Left side of Adv Opts panel for labels
        adv_opts_left = wx.Panel(adv_opts_panel)
        left_sizer = wx.BoxSizer(wx.VERTICAL)

        # Create and add all the Advanced Option inputs
        controls = ('encryption', 'sftp_location', 'lfu_location', 'chunk_size',
                    'sap_chunk_size', 'queue_size', 'max_readers',
                    'worker_timeout', 'sftp_port',
                    'log_level', 'sap_batch_size', 'auto_upload', 'rename_wait')
        for control in controls:
            label_text = USER_CONFIG_NAMES.get(control, control)
            choices = USER_CONFIG_CHOICES.get(control)
            tooltip_text = USER_CONFIG_TOOLTIPS.get(control)
            panel = UserInputPanel(adv_opts_left, label_text=label_text,
                                   choices=choices, tooltip_text=tooltip_text,
                                   control_size=wx.Size(120, -1),
                                   label_size=wx.Size(150, -1))
            self.controls[control] = panel.control
            left_sizer.Add(panel, 0, wx.EXPAND|wx.ALL, 5)

            # Hide the SFTP and LFU upload location selectors in Prod build,
            # since all uploads will be driven by values in the ECF.
            if (control in ('sftp_location', 'lfu_location')
                    and not config.ALLOW_USER_UPLOAD_LOCATION):
                panel.Hide()

            # Hide the worker_timeout input in Prod build
            # since it will always default to 'None'
            if (control == 'worker_timeout'
                    and not config.ALLOW_WORKER_TIMEOUT_SETTING):
                panel.Hide()

        # Finish Adv Opts left panel layout and sizing
        adv_opts_left.SetSizer(left_sizer)
        adv_opts_left.Layout()
        left_sizer.Fit(adv_opts_left)
        adv_opts_panel_sizer.Add(adv_opts_left, 1, wx.EXPAND|wx.ALL, 0)

        # Right side of Adv Opts panel for user inputs?
        self.right_panel = wx.Panel(adv_opts_panel)
        right_sizer = wx.BoxSizer(wx.VERTICAL)

        self.right_panel.SetSizer(right_sizer)
        self.right_panel.Layout()
        right_sizer.Fit(self.right_panel)
        adv_opts_panel_sizer.Add(self.right_panel, 1, wx.EXPAND|wx.ALL, 5)

        # Finish layout and sizing of the Adv Opts panel with scrollbar
        adv_opts_panel.SetSizer(adv_opts_panel_sizer)
        adv_opts_panel.Layout()
        adv_opts_panel_sizer.Fit(adv_opts_panel)

        scroll_window_sizer.Add(adv_opts_panel, 1, wx.EXPAND|wx.ALL, 5)
        scroll_panel.SetSizer(scroll_window_sizer)
        scroll_panel.Layout()
        scroll_window_sizer.Fit(scroll_panel)
        self.sizer.Add(scroll_panel, 1, wx.EXPAND|wx.ALL, 5)

        # Bottom panel with Manage Conn / Close / Apply buttons
        button_panel = self.create_button_panel()
        self.sizer.Add(button_panel, 0, wx.EXPAND|wx.ALL, 5)

        # Finish layout and fit content to window
        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)

        # Connect Events
        self.manage_conn_button.Bind(wx.EVT_BUTTON, self.show_manage_conn_window)
        self.close_button.Bind(wx.EVT_BUTTON, self.close_button_pressed)
        self.apply_button.Bind(wx.EVT_BUTTON, self.apply_button_pressed)

        # On window load, re-populate input fields from the SQLite config
        self.Bind(wx.EVT_SHOW, self.fill_fields_from_config)

    def create_button_panel(self) -> wx.Panel:
        """Add the bottom panel with Manage Conn / Close / Apply buttons."""
        panel = wx.Panel(self)
        sizer = wx.BoxSizer(wx.HORIZONTAL)

        left_panel = wx.Panel(panel)
        left_sizer = wx.BoxSizer(wx.HORIZONTAL)

        right_panel = wx.Panel(panel)
        right_sizer = wx.BoxSizer(wx.HORIZONTAL)
        right_sizer.AddStretchSpacer()  # Align these buttons to right

        self.manage_conn_button = wx.Button(left_panel,
                                            label="Manage Saved Connections")
        left_sizer.Add(self.manage_conn_button, 0, wx.ALL, 5)

        self.close_button = wx.Button(right_panel, label="Close")
        right_sizer.Add(self.close_button, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        self.apply_button = wx.Button(right_panel, label="Apply")
        right_sizer.Add(self.apply_button, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        # Layout and Size the bottom panels
        left_panel.SetSizer(left_sizer)
        left_panel.Layout()
        left_sizer.Fit(left_panel)
        sizer.Add(left_panel, 1, wx.ALL|wx.ALIGN_LEFT, 0)

        right_panel.SetSizer(right_sizer)
        right_panel.Layout()
        right_sizer.Fit(right_panel)
        sizer.Add(right_panel, 1, wx.ALL|wx.ALIGN_RIGHT, 0)

        panel.SetSizer(sizer)
        panel.Layout()
        sizer.Fit(panel)
        return panel

    def fill_fields_from_config(self, event: wx.Event):
        """Populate user input fields with values currently in config."""
        for field in USER_CONFIGS:
            value = self.config_db.get_config_value(field)
            if value:
                control = self.controls[field]
                _set_wx_control_value(control, value)

    def close_button_pressed(self, event: wx.Event):
        """Close the dialog window when Close button is clicked."""
        self.EndModal(1)

    def apply_button_pressed(self, event: wx.Event):
        """Validate configs and save them to SQLite."""
        folder_inputs = (
            # (setting, filepath, read_only)
            ('Working Directory', 'working_directory', False),
            ('SAP Netweaver SDK', 'sap_sdk_folder', True),
            ('Oracle Client', 'oracle_client_folder', True),
            ('IBM DB2 Driver', 'ibm_dll_folder', True),
        )

        # Validate all filepath values that have been set by the user
        for setting, control, read_only in folder_inputs:
            filepath = self.controls[control].GetPath()

            # If the control is the working directory and it is blank,
            # set it back to the default value.
            # Otherwise, skip filepaths that have not been set.
            if not filepath.strip():
                if control != 'working_directory':
                    continue
                # Set the working dir back to the default value
                filepath = USER_CONFIG_DEFAULTS[control]
                self.controls[control].SetPath(filepath)

            # Raise an error if the user can't access any given folder
            error = validate_folder_access(filepath, read_only)
            if error:
                message = (
                    'Could not save {} value\n\n{}'
                    ).format(setting, error)
                wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
                return

        # Alert user if any ERP Dependencies are invalid
        controls_validations_erps = [
            ('sap_sdk_folder', validate_sap_sdk, 'sap'),
            ('oracle_client_folder', validate_oracle_client, 'oracle'),
            ('ibm_dll_folder', validate_db2_driver, 'db2'),
        ]
        for control, validate_func, erp in controls_validations_erps:
            path = self.controls[control].GetPath()
            if not path:
                continue

            # Validate the contents of the path using a variable function
            try:
                validate_func(path)
            except DependencyError as error:
                wx.MessageBox(error.text, 'Error', style=wx.ICON_ERROR)
                continue

            if control == 'ibm_dll_folder':
                # Update env. vars. so that patched ibm_db can
                # locate the two inner IBM DB2 dependency folders
                os.environ['PWC_IBM_DLL'] = path
                pyextract.utils.update_path(os.path.join(path, 'clidriver', 'bin'))
            else:
                # Update PATH env. var. to include dependency path
                pyextract.utils.update_path(path)

            try:
                self.parent.load_submodules(erp)
            except DependencyError as error:
                # Alert user of failure and don't save configs
                wx.MessageBox(error.text, 'Error', style=wx.ICON_ERROR)
                return

        # Save each user config to SQL if it is in range of approved values
        for field in USER_CONFIGS:
            control = self.controls[field]
            value = _get_wx_control_value(control)
            if not valid_user_config_value(field, value):
                continue  # Alert user, don't save config
            self.config_db.save_config(field, value)

        # Update the new settings in GUI memory, and alert user of success
        self.parent.load_user_configs()
        self.parent.load_user_dependencies()
        wx.MessageBox('User configuration saved', 'Info')

    def show_manage_conn_window(self, event: wx.Event):
        """When user clicks the button, show the ManageSavedConnDialog."""
        self.parent.dialogs['manage_conn'].Show()


class ManageSavedConnDialog(DefaultDialog):
    """Page where user can update and delete saved DB connections."""

    def __init__(self, parent: wx.Frame):
        """Return a new instance of the dialog window."""
        super().__init__(parent)

        self.sizer = wx.BoxSizer(wx.VERTICAL)

        self.delete_button = wx.Button(self, label='Delete Selected')
        self.sizer.Add(self.delete_button, 0, wx.ALL, 5)

        grid = SavedConnGrid(self, parent.config_db)
        grid.fill_values()
        self.sizer.Add(grid, 0, wx.ALL, 5)

        # Fit and layout the grid / sizer
        self.SetSizer(self.sizer)
        self.Layout()
        self.sizer.Fit(self)

        # Connect Events
        self.delete_button.Bind(wx.EVT_BUTTON, grid.delete_selected_rows)


class SavedConnGrid(wx.grid.Grid):
    """Grid of information about user's saved database connections."""

    def __init__(self, parent: wx.Dialog, config_db: ConfigDatabase):
        super().__init__(parent)
        self.parent = parent
        self.config_db = config_db
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.empty_grid_message = 'No saved connection data'

        # Build the grid from data
        self.CreateGrid(0, 2)  # 1 row, 2 columns

        # Columns
        self.SetColSize(0, 150)
        self.SetColSize(1, 250)
        self.SetColLabelValue(0, 'ERP')
        self.SetColLabelValue(1, 'Connection Name')

        # Set generic Grid properties
        self.EnableDragColMove(False)
        self.EnableDragColSize(True)
        self.EnableDragGridSize(False)
        self.EnableDragRowSize(True)
        self.EnableEditing(False)
        self.EnableGridLines(True)
        self.SetColLabelAlignment(wx.ALIGN_LEFT, wx.ALIGN_BOTTOM)
        self.SetColLabelSize(25)
        self.SetDefaultCellAlignment(wx.ALIGN_LEFT, wx.ALIGN_TOP)
        self.SetGridLineColour(wx.SystemSettings.GetColour(wx.SYS_COLOUR_3DDKSHADOW))
        self.SetMargins(0, 0)
        self.SetRowLabelAlignment(wx.ALIGN_CENTRE, wx.ALIGN_CENTRE)
        self.SetRowLabelSize(50)
        self.SetSelectionMode(wx.grid.Grid.GridSelectRows)

        # Fit and layout the grid / sizer
        self.sizer.Add(self, 1, wx.ALL|wx.EXPAND, 5)
        self.SetSizer(self.sizer)
        self.Layout()
        self.sizer.Fit(self)

    def fill_values(self):
        """Fill this grid with all saved user credential data."""

        # Clear the grid of previous data
        rows = self.GetNumberRows()
        if rows:
            self.DeleteRows(0, rows)

        # Add data for each ERP to the grid
        for erp in ['SAP', 'Oracle', 'DB2', 'MSSQL', 'MYSQL']:
            names = self.config_db.saved_credential_names(erp)
            self.add_erp_values(erp, names)

        # If no rows were created, display one row w/ message
        if not self.GetNumberRows():
            self.AppendRows()
            self.SetCellValue(0, 0, self.empty_grid_message)

    def add_erp_values(self, erp: str, names: List[str]):
        """Add saved ERP conn names to the grid, one row at a time."""
        offset = self.GetNumberRows()
        for index, name in enumerate(names):
            self.AppendRows()
            self.SetCellValue(index + offset, 0, erp)
            self.SetCellValue(index + offset, 1, name)

    def delete_selected_rows(self, event: wx.CommandEvent):
        """Prompt user to delete currently selected rows from Grid and DB."""

        selected_rows = self.GetSelectedRows()
        if not selected_rows:
            wx.MessageBox('No row(s) selected for deletion.',
                          'Error', style=wx.ICON_ERROR)
            return
        elif self.GetCellValue(0, 0) == self.empty_grid_message:
            wx.MessageBox('No saved extractions available to delete.',
                          'Error', style=wx.ICON_ERROR)
            return

        # Prompt user to confirm they want to delete the rows
        if len(selected_rows) == 1:
            target = 'the selected connection'
        else:
            target = 'all {} selected connections'.format(len(selected_rows))
        message = 'Are you sure you want to delete {}?'.format(target)

        dialog = wx.MessageDialog(self, message, 'Info',
                                  style=wx.YES_NO|wx.ICON_INFORMATION)
        answer = dialog.ShowModal()
        if answer == wx.ID_NO:
            # User elects not to delete the rows
            return

        # Remove each row from local database then rebuild the Grid
        for row in selected_rows:
            erp = self.GetCellValue(row, 0)
            name = self.GetCellValue(row, 1)
            self.config_db.delete_credentials(name, erp)

        self.fill_values()


class ECFSelectionDialog(DefaultDialog):
    """A panel where the user can select and view contents of an ECF."""

    def __init__(self, parent: wx.Frame):
        """Return a new instance of the dialog window."""
        super().__init__(parent)
        self.sizer = wx.BoxSizer(wx.VERTICAL)

        title = wx.StaticText(self, label="ECF Preview")
        title.SetFont(FONT_TITLES)
        self.sizer.Add(title, 0, wx.EXPAND|wx.ALL, 10)

        line = wx.StaticLine(self)
        self.sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.content_panel = wx.Panel(self)
        content_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.main_panel = MainPanel(self.content_panel)
        main_sizer = wx.BoxSizer(wx.VERTICAL)

        # ECF File Picker bar above main content data
        self.picker_panel = wx.Panel(self.main_panel)
        picker_sizer = wx.BoxSizer(wx.HORIZONTAL)

        picker_label = wx.StaticText(self.picker_panel, label="Select ECF File:")
        picker_label.Wrap(-1)
        picker_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 94, 90))

        picker_sizer.Add(picker_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        self.filepicker = wx.FilePickerCtrl(self.picker_panel,
                                            message="Select a file",
                                            wildcard="*.ecf")
        self.filepicker.SetBackgroundColour(wx.Colour(255, 255, 255))
        picker_sizer.Add(self.filepicker, 1, wx.ALL, 5)

        self.picker_panel.SetSizer(picker_sizer)
        self.picker_panel.Layout()
        picker_sizer.Fit(self.picker_panel)
        main_sizer.Add(self.picker_panel, 0, wx.EXPAND|wx.ALL, 5)

        # Init the dynamic scrolling panel to show current ECF contents
        self.scroll_panel = wx.ScrolledWindow(
            self.main_panel, style=wx.HSCROLL|wx.SUNKEN_BORDER|wx.VSCROLL
        )
        self.scroll_panel.SetScrollRate(5, 5)
        color = wx.SystemSettings.GetColour(wx.SYS_COLOUR_WINDOW)
        self.scroll_panel.SetBackgroundColour(color)
        main_sizer.Add(self.scroll_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.refresh_dynamic_panel()

        self.main_panel.SetSizer(main_sizer)
        self.main_panel.Layout()
        content_panel_sizer.Add(self.main_panel, 1, wx.EXPAND|wx.RIGHT, 5)

        content = (
            "Please browse and select a valid Extraction Configuration "
            "File (ECF) and review the configuration details for accuracy.  "
            "By clicking Next, you confirm the details are correct.\n\n"
            "If there is an error, please contact your PwC Engagement Team "
            "contact for support."
        )
        guidepanel = GuidePanel(parent=self.content_panel, content=content)
        content_panel_sizer.Add(guidepanel, 1, wx.EXPAND|wx.LEFT, 5)

        self.content_panel.SetSizer(content_panel_sizer)
        self.content_panel.Layout()
        content_panel_sizer.Fit(self.content_panel)
        self.sizer.Add(self.content_panel, 1, wx.EXPAND|wx.ALL, 5)

        nav_panel = wx.Panel(self)
        nav_sizer = wx.BoxSizer(wx.VERTICAL)

        button_panel = wx.Panel(nav_panel)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.cancel_button = wx.Button(button_panel, label="Cancel")
        button_sizer.Add(self.cancel_button, 0, wx.ALL, 5)

        self.previous_button = wx.Button(button_panel, label="Previous")
        self.previous_button.Enable(False)

        button_sizer.Add(self.previous_button, 0, wx.ALL, 5)

        self.next_button = wx.Button(button_panel, label="Next")
        self.next_button.Enable(False)

        button_sizer.Add(self.next_button, 0, wx.ALL, 5)

        button_panel.SetSizer(button_sizer)
        button_panel.Layout()
        button_sizer.Fit(button_panel)
        nav_sizer.Add(button_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        nav_panel.SetSizer(nav_sizer)
        nav_panel.Layout()
        nav_sizer.Fit(nav_panel)
        self.sizer.Add(nav_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)

        # Connect Events
        self.cancel_button.Bind(wx.EVT_BUTTON, self.cancel_button_pressed)
        self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)
        self.filepicker.Bind(wx.EVT_FILEPICKER_CHANGED, self.new_ecf_selected)
        self.Bind(wx.EVT_CLOSE, self.parent.confirm_close_extraction)
        self.Bind(wx.EVT_SHOW, self.on_show)

    def new_ecf_selected(self, event: wx.FileDirPickerEvent):
        """When user select another ECF on second screen of pyExtract"""
        self.parent.validate_ecf(event.GetPath())
        self.next_button.Enable(True)

    def cancel_button_pressed(self, event: wx.CommandEvent):
        """When user selects cancel button"""
        self.EndModal(0)

    def next_button_pressed(self, event: wx.CommandEvent):
        """Confirm required submodules are loaded; continue if so."""
        erp = self.parent.dialogs['connection'].erp.lower()
        try:
            self.parent.load_submodules(erp)
        except DependencyError as error:
            # Alert user of failure and redirect to Configs screen
            wx.MessageBox(error.text, 'Error', style=wx.ICON_ERROR)
            if not self.parent.dialogs['config'].IsShown():
                self.parent.dialogs['config'].ShowModal()
        else:
            self.EndModal(1)

    def on_show(self, event: wx.ShowEvent):
        """Populate ECF selector and content from modal selection."""
        if not event.IsShown():
            return  # Hiding the page, no action needed
        elif not self.next_button.IsEnabled():
            self.parent.validate_ecf(self.filepicker.GetPath())
            self.next_button.Enable(True)

    def refresh_dynamic_panel(self, content: dict = None):
        """Refresh ECF Preview content based on current ECF selected."""
        self.scroll_panel.DestroyChildren()
        scroll_panel_sizer = wx.BoxSizer(wx.VERTICAL)

        if content:
            for key, value in content:
                # If value is None, use a blank string for the value
                value = value or ''

                key_value_panel = wx.Panel(self.scroll_panel)
                key_value_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

                # Key Label
                label = wx.StaticText(key_value_panel, label=key,
                                      style=wx.ALIGN_RIGHT)
                label.Wrap(-1)
                label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))
                label.SetMinSize(wx.Size(150, -1))

                key_value_panel_sizer.Add(label, 0, wx.ALL, 5)

                # Key Value
                value = wx.StaticText(key_value_panel, label=value)
                value.Wrap(-1)
                key_value_panel_sizer.Add(value, 0, wx.ALL, 5)

                key_value_panel.SetSizer(key_value_panel_sizer)
                key_value_panel.Layout()
                key_value_panel_sizer.Fit(key_value_panel)
                scroll_panel_sizer.Add(key_value_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.scroll_panel.SetSizer(scroll_panel_sizer)
        self.scroll_panel.Layout()
        scroll_panel_sizer.Fit(self.scroll_panel)
        self.main_panel.Layout()


class BaseConnectionDialog(DefaultDialog):
    """Base class for an ERP Connection Dialog panel."""

    erp = None  # Must be overwritten by subclass
    required_submodules = []
    required_user_inputs = []

    def __init__(self, parent: wx.Frame, *args, **kwargs):
        """Create new instance of a generic connection dialog panel."""
        super().__init__(parent, *args, **kwargs)

        # Validate that subclasses provide required attributes
        assert self.erp in config.ERPS_TO_CREDENTIALS
        for module in self.required_submodules:
            assert module in config.CONNECT_SUBMODULES
        if isinstance(self.required_user_inputs, list):
            for control in self.required_user_inputs:
                assert control in USER_CONFIG_TOOLTIPS, control

        # Primary attributes
        self.config_db = ConfigDatabase()
        self.controls = {}  # type: Dict[str, wx.Control]
        self.busy_info = None  # type: wx.BusyInfo
        self.messenger = None  # type: pyextract.connect.ABCMessenger
        self.all_invalid = False # type bool

        # Various shared panels + sizers for content organization
        self.content_panel = wx.Panel(self)
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.main_panel = MainPanel(self.content_panel)
        self.bottom_panel = wx.Panel(self.main_panel)
        self.save_conn_panel = wx.Panel(self.bottom_panel)

        # User inputs consistent across all connection panels
        self.save_connection = wx.CheckBox(self.save_conn_panel,
                                           label="Save Connection As:")
        self.connection_name = wx.TextCtrl(self.save_conn_panel,
                                           size=wx.Size(200, -1))
        self.save_password = wx.CheckBox(self.save_conn_panel,
                                         label="Save Password?")

        # Bind events for connection testing and query validation
        self.Bind(EVT_QUERY_VALIDATE, self.query_validation_done)
        self.Bind(EVT_GETCONNECTION_DONE, self.connection_established)

    def save_connection_changed(self, event: wx.Event):
        """Action to take when user checks / unchecks 'Save Connection'."""
        if not self.save_connection.GetValue():
            # Not saving connection info, allowed to enter next page
            self.connection_name.Disable()
            self.next_button.Enable()
        else:
            # Saving connection info, allow user to choose name to save
            # as, and enable entry to next page if name is entered
            self.connection_name.Enable()
            self.validate_connection_name(event)

    def validate_connection_name(self, event: wx.Event):
        """Enable 'Next' button if user-provided connection name is valid."""
        if self.connection_name.GetValue():
            self.next_button.Enable()
        else:
            self.next_button.Disable()

    def conn_type_changed(self, event: wx.Event):
        """Rebuild the connection panel whenever connection type is changed."""
        self.init_middle_panel()

    def cancel_button_pressed(self, event: wx.Event):
        """Return to the Extract home page."""
        self.EndModal(0)

    def previous_button_pressed(self, event: wx.Event):
        """Return to the ECF Selection page."""
        self.EndModal(-1)

    def refresh_load_previous(self):
        """Reload the panel with saved credentials available in dropdown."""
        self.load_previous_choice.Clear()
        saved_connections = [""]
        saved_connections += self.config_db.saved_credential_names(self.erp)
        self.load_previous_choice.AppendItems(saved_connections)
        self.Layout()

    def connection_established(self, event: wx.Event):
        """Handle response from attempted connection and update GUI."""
        self.busy_info = None
        self.Enable()

        # If connection failed, notify user and do not continue
        if event.response["status"] == "Error":
            wx.MessageBox(event.response['message'],
                          event.response['status'], style=wx.ICON_ERROR)
            return

        elif event.response["status"] == "Warning":
            wx.MessageBox(event.response['message'],
                          event.response['status'], style=wx.ICON_WARNING)

        # Parse source messenger and connection kwargs from previous screen
        self.messenger = event.response["messenger"]
        kwargs = event.response["connection_args"]
        kwargs["type"] = event.response["connection_type"]

        # Save credentials to local config database if requested
        if self.save_connection.GetValue():
            name = self.connection_name.GetValue()
            if len(name) > 200:
                message = 'Connection names must be shorter than 200 characters'
                wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
                return
            self._save_connection_values(name, kwargs)
            self.refresh_load_previous()

        # Validate queries in this ECF in a separate thread
        self.busy_info = wx.BusyInfo("Validating queries in ECF...")
        thread = ValidateQueriesThread(
            parent=self,
            ecf_meta_data=self.parent.ecf_meta_data,
            data_server=self.parent.ecf_data["DataSource"]["DataServer"],
            data_connector=self.parent.ecf_data["DataSource"]["DataConnector"],
            messenger=self.messenger,
        )
        thread.start()

    def query_validation_done(self, event: wx.Event):
        """Handle response from query validation thread."""
        self.busy_info = None

        # If query validation failed, notify user and do not continue
        if event.response["status"] != "success":
            wx.MessageBox(event.response['status'],
                          'Query Validation Failed', style=wx.ICON_ERROR)
            return

        queries = event.response["queries"]
        errors = event.response["errors"]
        data_server = event.response["data_server"]

        self.parent.dialogs['content_preview'].init_main_panel(queries=queries,
                                                               erp=data_server)

        if len(queries) == len(errors):
            self.all_invalid = True
            wx.MessageBox((
                'WARNING: There are no valid tables/queries in this ECF. '
                'No data will be extracted, please select an ECF '
                'with valid queries/parameters.'
                ), 'Invalid ECF', style=wx.ICON_ERROR)
        elif errors:
            wx.MessageBox((
                'WARNING: Queries/Tables within this ECF will encounter '
                'errors during extraction.\nPlease evaluate the errors '
                'on the content preview screen and determine if '
                'extraction should continue.'
                ), 'Invalid Queries', style=wx.ICON_ERROR)

        self.parent.dialogs['extraction'].clear_logbox()
        self.EndModal(1)

    def _save_connection_values(self, name: str, kwargs: Dict[str, str]):
        """Save connection values by keyword to local ConfigDatabase."""
        save_password = self.save_password.GetValue()

        if self.config_db.conn_exists(name, erp=self.erp):
            # Confirm that the user wants to overwrite existing connection
            message = (
                'A saved "{}" connection with the name "{}" '
                'already exists. Would you like to overwrite it?'
                ).format(self.erp, name)
            dialog = wx.MessageDialog(self, message, 'Info',
                                      style=wx.YES_NO|wx.ICON_INFORMATION)
            answer = dialog.ShowModal()
            if answer == wx.ID_NO:
                return  # Do not save credentials to database at all
            else:
                self.config_db.delete_credentials(name, erp=self.erp)

        self.config_db.save_credentials(name, kwargs, erp=self.erp,
                                        save_password=save_password)

    def add_user_input(self, parent: wx.Panel, sizer: wx.Sizer,
                       attr: str, label: str):
        """Add a user input Panel to another panel on this connection dialog."""
        tooltip = USER_CONFIG_TOOLTIPS.get(attr)
        panel = UserInputPanel(parent, label_text=label, tooltip_text=tooltip)
        # Add an control to this object for direct access to panel's Control
        self.controls[attr] = panel.control
        sizer.Add(panel, 0, wx.EXPAND|wx.ALL, 5)

    def reset_control_listeners(self):
        """Reset the event listeners for all wx.Controls on this panel."""
        self.validate_required_controls()
        for control in self.controls.values():
            if not control:
                continue  # control has been deleted
            control.Bind(wx.EVT_KEY_UP, self.validate_required_controls)
            control.Bind(wx.EVT_FILEPICKER_CHANGED, self.validate_required_controls)


    def validate_required_controls(self, event: wx.Event=None):
        """Validate that all required user inputs on this panel have values.
        If all required controls have values, enable 'Next' button.
        If any required controls do not have values, disable 'Next' button.
        """

        if isinstance(self.required_user_inputs, list):
            required = self.required_user_inputs
        else:
            conn = self.connection_type_choice.GetStringSelection()
            if conn == 'PWC-XTRACT (ABAP)':
                required = []
            else:
                required = self.required_user_inputs[conn]

        can_enable = True
        for rkey in required:
            if rkey in self.controls:
                control = self.controls[rkey]
                if control:
                    value = _get_wx_control_value(control)
                    if value == "":
                        can_enable = False
                        break

        if can_enable:
            self.next_button.Enable()
        else:
            self.next_button.Disable()

    def build_bottom_panel(self):
        """Build the bottom of the connection panel (with Save Conn panel)."""
        panel_sizer = wx.BoxSizer(wx.VERTICAL)
        control_sizer = wx.BoxSizer(wx.HORIZONTAL)

        controls = [self.save_connection, self.connection_name, self.save_password]
        for control in controls:
            control_sizer.Add(control, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        self.save_conn_panel.SetSizer(control_sizer)
        self.save_conn_panel.Layout()

        control_sizer.Fit(self.save_conn_panel)
        panel_sizer.Add(self.save_conn_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        self.bottom_panel.SetSizer(panel_sizer)
        self.bottom_panel.Layout()

        panel_sizer.Fit(self.bottom_panel)


class SAPConnectionDialog(BaseConnectionDialog):
    """Connection dialog window for connecting to SAP."""

    erp = 'SAP'
    required_submodules = ['connect.sap', 'streams.sapstream']
    conntypes_to_controls = {
        'Direct Connection': (
            'client', 'language', 'user', 'password', 'ashost', 'sysnr'
        ),
        'Load Balanced Connection': (
            'client', 'user', 'password', 'language', 'mshost', 'msserv', 'sysid', 'group'
        ),
        'Direct Connection w/SNC': (
            'client', 'language', 'user', 'password', 'ashost', 'sysnr',
            'snc_qop', 'snc_myname', 'snc_partnername', 'snc_lib'
        ),
        'Load Balanced w/SNC': (
            'client', 'user', 'password', 'language', 'mshost', 'msserv', 'sysid', 'group',
            'snc_qop', 'snc_myname', 'snc_partnername', 'snc_lib'
        ),
    }

    required_user_inputs = {
        'Direct Connection': (
            'client', 'language', 'user', 'password', 'ashost', 'sysnr'
        ),
        'Load Balanced Connection': (
            'client', 'user', 'password', 'language', 'mshost', 'sysid', 'group'
        ),
        'Direct Connection w/SNC': (
            'client', 'language', 'user', 'password', 'ashost', 'sysnr',
            'snc_qop', 'snc_myname', 'snc_partnername', 'snc_lib'
        ),
        'Load Balanced w/SNC': (
            'client', 'user', 'password', 'language', 'mshost', 'sysid', 'group',
            'snc_qop', 'snc_myname', 'snc_partnername', 'snc_lib'
        ),
    }
    # required_user_inputs = ['client', 'user', 'password', 'language',
    #                         'ashost', 'sysnr', 'mshost',
    #                         'sysid', 'group', 'snc_qop', 'snc_myname',
    #                         'snc_partnername', 'language', 'snc_lib',
    #                         'abap_folder', 'abap_filename']

    def __init__(self, *args, **kwargs):
        """Return a new instance of the dialog window."""
        super().__init__(*args, **kwargs)

        title = wx.StaticText(self, label="SAP Connection")
        title.SetFont(FONT_TITLES)
        self.sizer.Add(title, 0, wx.EXPAND|wx.ALL, 10)

        line = wx.StaticLine(self)
        self.sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        content_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)
        main_panel_sizer = wx.BoxSizer(wx.VERTICAL)

        self.top_panel = wx.Panel(self.main_panel)
        top_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        # Connection Type
        conntype_panel = wx.Panel(self.top_panel)
        conntype_sizer = wx.BoxSizer(wx.HORIZONTAL)

        conntype_label = wx.StaticText(conntype_panel, label="Connection Type:")
        conntype_label.Wrap(-1)
        conntype_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        conntype_sizer.Add(conntype_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        choices = [
            'Direct Connection',
            'Load Balanced Connection',
            'Direct Connection w/SNC',
            'Load Balanced w/SNC',
            'PWC-XTRACT (ABAP)'
        ]
        self.connection_type_choice = wx.Choice(conntype_panel,
                                                choices=choices)
        self.connection_type_choice.SetSelection(0)
        conntype_sizer.Add(self.connection_type_choice, 0, wx.ALL, 5)

        conntype_panel.SetSizer(conntype_sizer)
        conntype_panel.Layout()
        conntype_sizer.Fit(conntype_panel)
        top_panel_sizer.Add(conntype_panel, 1, wx.EXPAND|wx.ALL, 5)

        load_prev_panel = wx.Panel(self.top_panel)
        load_prev_sizer = wx.BoxSizer(wx.HORIZONTAL)

        load_label = wx.StaticText(load_prev_panel,
                                   label='Load Saved Connection:')
        load_label.Wrap(-1)
        load_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        load_prev_sizer.Add(load_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        # Load Previous
        saved_connections = [""]
        saved_connections += self.config_db.saved_credential_names(self.erp)
        self.load_previous_choice = wx.Choice(load_prev_panel,
                                              choices=saved_connections)
        self.load_previous_choice.SetSelection(0)
        load_prev_sizer.Add(self.load_previous_choice, 1, wx.ALL, 5)

        load_prev_panel.SetSizer(load_prev_sizer)
        load_prev_panel.Layout()
        load_prev_sizer.Fit(load_prev_panel)
        top_panel_sizer.Add(load_prev_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.top_panel.SetSizer(top_panel_sizer)
        self.top_panel.Layout()
        top_panel_sizer.Fit(self.top_panel)
        main_panel_sizer.Add(self.top_panel, 0, wx.EXPAND|wx.ALL, 5)

        line = wx.StaticLine(self.main_panel)
        main_panel_sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.middle_panel = wx.Panel(self.main_panel)
        main_panel_sizer.Add(self.middle_panel, 1, wx.EXPAND|wx.ALL, 5)

        line = wx.StaticLine(self.main_panel)
        main_panel_sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.build_bottom_panel()
        main_panel_sizer.Add(self.bottom_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.main_panel.SetSizer(main_panel_sizer)
        self.main_panel.Layout()
        content_panel_sizer.Add(self.main_panel, 1, wx.EXPAND|wx.RIGHT, 5)

        content = (
            'Please select an existing connection or enter connection '
            'details for the SAP instance to extract data from.\n\n'
            'To connect with a Secure Network Connection (SNC), please '
            'select the "Direct Connection w/SNC" option to display '
            'additional required fields.\n\n'
            'If using an SNC on a load balanced server, please select the '
            '"Load Balanced Connection" option instead.\n\n'
            'To perform a manual data extraction using the PwC ABAP tool, '
            'select the "PWC-XTRACT (ABAP)" option, and follow the '
            'instructions provided on that page.'
        )
        guidepanel = GuidePanel(parent=self.content_panel, content=content)
        content_panel_sizer.Add(guidepanel, 1, wx.EXPAND|wx.LEFT, 5)

        self.content_panel.SetSizer(content_panel_sizer)
        self.content_panel.Layout()
        content_panel_sizer.Fit(self.content_panel)
        self.sizer.Add(self.content_panel, 1, wx.EXPAND|wx.ALL, 5)

        nav_panel = wx.Panel(self)
        nav_sizer = wx.BoxSizer(wx.VERTICAL)

        button_panel = wx.Panel(nav_panel)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.cancel_button = wx.Button(button_panel, label="Cancel")
        button_sizer.Add(self.cancel_button, 0, wx.ALL, 5)

        self.previous_button = wx.Button(button_panel, label="Previous")
        button_sizer.Add(self.previous_button, 0, wx.ALL, 5)

        self.next_button = wx.Button(button_panel, label="Next")
        button_sizer.Add(self.next_button, 0, wx.ALL, 5)

        button_panel.SetSizer(button_sizer)
        button_panel.Layout()
        button_sizer.Fit(button_panel)
        nav_sizer.Add(button_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        nav_panel.SetSizer(nav_sizer)
        nav_panel.Layout()
        nav_sizer.Fit(nav_panel)
        self.sizer.Add(nav_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)

        # Initialize the middle panel for the first time
        self.init_middle_panel()

        # Connect Events
        self.connection_type_choice.Bind(wx.EVT_CHOICE, self.conn_type_changed)
        self.load_previous_choice.Bind(wx.EVT_CHOICE, self.load_previous_selected)
        self.cancel_button.Bind(wx.EVT_BUTTON, self.cancel_button_pressed)
        self.previous_button.Bind(wx.EVT_BUTTON, self.previous_button_pressed)
        self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)
        self.save_connection.Bind(wx.EVT_CHECKBOX, self.save_connection_changed)
        self.connection_name.Bind(wx.EVT_KEY_UP, self.validate_connection_name)
        self.Bind(wx.EVT_CLOSE, self.parent.confirm_close_extraction)

    def load_previous_selected(self, event: wx.Event):
        """Load saved connection data from user config DB into this panel."""
        connection_name = self.load_previous_choice.GetStringSelection()
        if connection_name == "":
            return  # No connection selected to reload

        saved_creds = self.config_db.get_credentials(connection_name, self.erp)

        self.connection_type_choice.SetStringSelection(saved_creds['type'])
        self.init_middle_panel()

        for key in self.conntypes_to_controls[saved_creds['type']]:
            value = saved_creds.get(key) or ''
            _set_wx_control_value(self.controls[key], value)

        self.Layout()
        self.validate_required_controls()

    def next_button_pressed(self, event: wx.Event):

        """Based on connection type selected by user, go to next dialog."""
        connection_type = self.connection_type_choice.GetStringSelection()
        if connection_type == "SAP HANA":
            wx.MessageBox('SAP HANA connection is not currently supported',
                          'Error', style=wx.ICON_ERROR)
            return

        # Generate connection kwargs based on connection type
        connection_args = {}
        for key in self.conntypes_to_controls[connection_type]:
            connection_args[key] = _get_wx_control_value(self.controls[key])

        # Create SAPMessenger while a busy info box is shown
        self.busy_info = wx.BusyInfo("Testing connection...")
        self.Disable()
        thread = GetConnectionThread(self, "SAP Application Server",
                                     connection_type, connection_args)
        thread.start()

    def save_abap_input_file(self, event: wx.Event):
        """Action that occurs when user clicks the 'Save ECF' button."""
        request_id = self.parent.ecf_data["RequestId"]
        if self.config_db.saved_request_exists(request_id):
            msg = (
                'An ECF with Request ID "{}" has already been started. '
                'Please delete the existing ECF before restarting.'
                ).format(request_id)
            wx.MessageBox(msg, 'Warning', style=wx.ICON_EXCLAMATION)
            return

        ecfpath = self.parent.dialogs['ecf'].filepicker.GetPath()
        folder = self.parent.dialogs['sap'].controls['abap_folder'].GetPath()
        filename = self.parent.dialogs['sap'].controls['abap_filename'].GetValue() + '.csv'
        outputpath = os.path.join(folder, filename)

        if os.path.exists(outputpath):
            message = '{} already exists. Do you want to replace it?'.format(filename)
            dialog = wx.MessageDialog(self, message, 'Info',
                                      style=wx.YES_NO|wx.ICON_INFORMATION)
            answer = dialog.ShowModal()
            if answer != wx.ID_YES:
            #     generator = ABAPInputGenerate(ecfpath, outputpath)
            #     warnings = generator.create_parameter_file()
            # else:
                # User elects not to overwrite current ABAP input file, cannot continue
                return
        # else:
        generator = ABAPInputGenerate(ecfpath, outputpath)
        try:
            warnings = generator.create_parameter_file()
            save_successful = True
        except Exception as error:
            error_message = str(traceback.format_exc())
            save_successful = False

        if save_successful:
            self.config_db.save_extract_request(
                extract_id=str(uuid.uuid4()),
                request_id=request_id,
                data_server=self.parent.ecf_data["DataSource"]["DataServer"],
                data_connector=self.parent.ecf_data["DataSource"]["DataConnector"],
                file_path=self.parent.configs["ecf_file_path"],
            )

            wx.MessageBox('Input File generated in the specified folder',
                          'Information', style=wx.ICON_INFORMATION)

            for warning in warnings:
                wx.MessageBox(warning, 'Warning', style=wx.ICON_WARNING)
        else:
            wx.MessageBox(error_message, 'Error', style=wx.ICON_ERROR)

    def finish_button_pressed(self, event: wx.Event):
        """Return to home screen after user has created an ABAP input file."""
        self.EndModal(2)

    def init_middle_panel(self):
        """Instantiate components of the middle panel for this window."""
        # "PwC Xtract (ABAP)"
        if self.connection_type_choice.GetCurrentSelection() == 4:

            self.middle_panel.DestroyChildren()

            middle_panel_sizer = wx.BoxSizer(wx.VERTICAL)

            abap_info = (
                "The PwC-XTRACT program is an Advanced Business Application "
                "Programming (\"ABAP\") report, which extracts "
                "SAP-System-Resident (not archived) SAP tables in output "
                "files on the SAP application server.\n\n"
                "In order to specify which data should be extracted, "
                "an input file specifying the list of tables and fields "
                "and conditions must be provided.  Select a folder and file "
                "name, then click the \"Save\" button to create this input "
                "file based on the selected ECF.\n\n"
                "The PwC-XTRACT program must be executed manually using the "
                "input file generated.  After completion, the output files "
                "will need to be transferred using FTP from the SAP application server onto "
                "the local system where PwC Extract is installed.\n\n"
                "Note: The PwC-XTRACT ABAP report does not support internal "
                "SAP tables, raw strings, references, or structures.'"
            )
            infotext = wx.StaticText(self.middle_panel, label=abap_info,
                                     size=wx.Size(-1, 175))
            infotext.Wrap(-1)
            infotext.SetMinSize(wx.Size(-1, 175))
            infotext.SetMaxSize(wx.Size(-1, 175))
            middle_panel_sizer.Add(infotext, 0, wx.ALL|wx.EXPAND, 5)

            folder_panel = wx.Panel(self.middle_panel)
            folder_sizer = wx.BoxSizer(wx.HORIZONTAL)

            folder_label = StaticLabel(folder_panel, text="Select Folder")
            folder_sizer.Add(folder_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

            self.controls['abap_folder'] = wx.DirPickerCtrl(folder_panel, message="Select a folder")
            self.controls['abap_folder'].SetBackgroundColour(wx.Colour(255, 255, 255))
            folder_sizer.Add(self.controls['abap_folder'], 1, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

            folder_panel.SetSizer(folder_sizer)
            folder_panel.Layout()
            folder_sizer.Fit(folder_panel)
            middle_panel_sizer.Add(folder_panel, 0, wx.EXPAND|wx.ALL, 5)

            filename_panel = wx.Panel(self.middle_panel)
            filename_sizer = wx.BoxSizer(wx.HORIZONTAL)

            filename_label = StaticLabel(filename_panel, text="File Name")
            filename_sizer.Add(filename_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

            self.controls['abap_filename'] = wx.TextCtrl(filename_panel, size=wx.Size(275, -1))
            self.controls['abap_filename'].SetMinSize(wx.Size(275, -1))
            self.controls['abap_filename'].SetMaxSize(wx.Size(275, -1))
            filename_sizer.Add(self.controls['abap_filename'], 0, wx.ALL, 5)

            filetype_label = wx.StaticText(filename_panel, label=".csv")
            filetype_label.Wrap(-1)
            filename_sizer.Add(filetype_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

            filename_panel.SetSizer(filename_sizer)
            filename_panel.Layout()
            filename_sizer.Fit(filename_panel)
            middle_panel_sizer.Add(filename_panel, 0, wx.EXPAND|wx.ALL, 5)

            save_panel = wx.Panel(self.middle_panel)
            save_sizer = wx.BoxSizer(wx.VERTICAL)

            self.save_abap_button = wx.Button(save_panel, label="Save")
            save_sizer.Add(self.save_abap_button, 0, wx.ALL, 5)

            save_panel.SetSizer(save_sizer)
            save_panel.Layout()
            save_sizer.Fit(save_panel)
            middle_panel_sizer.Add(save_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

            self.save_abap_button.Bind(wx.EVT_BUTTON, self.save_abap_input_file)
            self.middle_panel.SetSizer(middle_panel_sizer)
            self.middle_panel.Layout()

            self.next_button.SetLabelText("Finish")
            self.next_button.Bind(wx.EVT_BUTTON, self.finish_button_pressed)

            # Disable connection saving and loading inputs for ABAP
            self.load_previous_choice.Disable()
            self.save_connection.SetValue(False)
            self.save_connection.Disable()
            self.connection_name.SetValue("")
            self.connection_name.Disable()
            self.save_password.Disable()

        else:

            self.middle_panel.DestroyChildren()

            # Middle panel is made up of two columns of UserInputs
            middle_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)
            self.left_panel = wx.Panel(self.middle_panel)
            self.right_panel = wx.Panel(self.middle_panel)
            left_sizer = wx.BoxSizer(wx.VERTICAL)
            right_sizer = wx.BoxSizer(wx.VERTICAL)

            # Direct Connection
            if self.connection_type_choice.GetCurrentSelection() in [0]:

                # Left Panel
                attrs_labels = (
                    ('client', 'Client*'),
                    ('user', 'User*'),
                    ('password', 'Password*'),
                    ('language', 'Language*'),
                )
                for attr, label in attrs_labels:
                    self.add_user_input(self.left_panel, left_sizer, attr, label)

                self.left_panel.SetSizer(left_sizer)
                self.left_panel.Layout()
                left_sizer.Fit(self.left_panel)
                middle_panel_sizer.Add(self.left_panel, 1, wx.EXPAND | wx.ALL, 5)

                # Right Panel
                attrs_labels = (
                    ('ashost', 'App Server*'),
                    ('sysnr', 'System Number*'),
                )
                for attr, label in attrs_labels:
                    self.add_user_input(self.right_panel, right_sizer, attr, label)

                # Common
                self.right_panel.SetSizer(right_sizer)
                self.right_panel.Layout()
                right_sizer.Fit(self.right_panel)
                middle_panel_sizer.Add(self.right_panel, 1, wx.EXPAND | wx.ALL, 5)

                self.middle_panel.SetSizer(middle_panel_sizer)
                self.middle_panel.Layout()
                middle_panel_sizer.Fit(self.middle_panel)

                self.next_button.SetLabelText("Next")
                self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)

                # Enable connection saving and loading inputs for non-ABAP
                self.load_previous_choice.Enable()
                self.save_connection.SetValue(False)
                self.save_connection.Enable()
                self.connection_name.SetValue("")
                self.connection_name.Enable()
                self.save_password.Enable()

            # Load Balanced Connection
            elif self.connection_type_choice.GetCurrentSelection() in [1]:

                # Left Panel
                attrs_labels = (
                    ('client', 'Client*'),
                    ('user', 'User*'),
                    ('password', 'Password*'),
                    ('language', 'Language*'),
                )
                for attr, label in attrs_labels:
                    self.add_user_input(self.left_panel, left_sizer, attr, label)

                self.left_panel.SetSizer(left_sizer)
                self.left_panel.Layout()
                left_sizer.Fit(self.left_panel)
                middle_panel_sizer.Add(self.left_panel, 1, wx.EXPAND | wx.ALL, 5)

                # Right Panel
                attrs_labels = (
                    ('mshost', 'Message Server*'),
                    ('msserv', 'MS Service'),
                    ('sysid', 'System ID*'),
                    ('group', 'Group/Server*'),
                )
                for attr, label in attrs_labels:
                    self.add_user_input(self.right_panel, right_sizer, attr, label)

                # Common
                self.right_panel.SetSizer(right_sizer)
                self.right_panel.Layout()
                right_sizer.Fit(self.right_panel)
                middle_panel_sizer.Add(self.right_panel, 1, wx.EXPAND | wx.ALL, 5)

                self.middle_panel.SetSizer(middle_panel_sizer)
                self.middle_panel.Layout()
                middle_panel_sizer.Fit(self.middle_panel)

                self.next_button.SetLabelText("Next")
                self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)

                # Enable connection saving and loading inputs for non-ABAP
                self.load_previous_choice.Enable()
                self.save_connection.SetValue(False)
                self.save_connection.Enable()
                self.connection_name.SetValue("")
                self.connection_name.Enable()
                self.save_password.Enable()

            # Direct Connection w/SNC
            elif self.connection_type_choice.GetCurrentSelection() in [2]:

                attrs_labels = (
                    ('client', 'Client*'),
                    ('language', 'Language*'),
                    ('ashost', 'App Server*'),
                    ('sysnr', 'System Number*'),
                )
                for attr, label in attrs_labels:
                    self.add_user_input(self.left_panel, left_sizer, attr, label)

                self.left_panel.SetSizer(left_sizer)
                self.left_panel.Layout()
                left_sizer.Fit(self.left_panel)
                middle_panel_sizer.Add(self.left_panel, 1, wx.EXPAND | wx.ALL, 5)

                # Right Panel
                attrs_labels = (
                    ('snc_qop', 'SNC QoP*'),
                    ('snc_myname', 'SNC Name*'),
                    ('snc_partnername', 'SNC Partner Name*'),
                )
                for attr, label in attrs_labels:
                    self.add_user_input(self.right_panel, right_sizer, attr, label)

                # SNC LIB
                self.snc_lib_panel = wx.Panel(self.right_panel)
                snc_lib_sizer = wx.BoxSizer(wx.HORIZONTAL)

                snc_lib_label = wx.StaticText(self.snc_lib_panel,
                                              label="SNC Lib*:",
                                              size=wx.Size(100, -1))
                snc_lib_label.Wrap(-1)
                snc_lib_sizer.Add(snc_lib_label, 0, wx.ALL | wx.ALIGN_CENTER_VERTICAL, 5)

                self.controls['snc_lib'] = wx.FilePickerCtrl(self.snc_lib_panel,
                                                             message="Select a file:",
                                                             wildcard="*.*")
                self.controls['snc_lib'].SetBackgroundColour(wx.Colour(255, 255, 255))
                snc_lib_sizer.Add(self.controls['snc_lib'], 1, wx.ALL, 5)

                self.snc_lib_panel.SetSizer(snc_lib_sizer)
                self.snc_lib_panel.Layout()
                snc_lib_sizer.Fit(self.snc_lib_panel)
                right_sizer.Add(self.snc_lib_panel, 0, wx.EXPAND | wx.ALL, 5)

                # Common
                self.right_panel.SetSizer(right_sizer)
                self.right_panel.Layout()
                right_sizer.Fit(self.right_panel)
                middle_panel_sizer.Add(self.right_panel, 1, wx.EXPAND | wx.ALL, 5)

                self.middle_panel.SetSizer(middle_panel_sizer)
                self.middle_panel.Layout()
                middle_panel_sizer.Fit(self.middle_panel)

                self.next_button.SetLabelText("Next")
                self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)

                # Enable connection saving and loading inputs for non-ABAP
                self.load_previous_choice.Enable()
                self.save_connection.SetValue(False)
                self.save_connection.Enable()
                self.connection_name.SetValue("")
                self.connection_name.Enable()
                self.save_password.Enable()

            # Load Balanced w/SNC
            elif self.connection_type_choice.GetCurrentSelection() in [3]:

                attrs_labels = (
                    ('client', 'Client*'),
                    ('user', 'User*'),
                    ('password', 'Password*'),
                    ('language', 'Language*'),
                    ('mshost', 'Message Server*'),
                    ('msserv', 'MS Service'),
                    ('sysid', 'System ID*'),
                    ('group', 'Group/Server*'),
                )
                for attr, label in attrs_labels:
                    self.add_user_input(self.left_panel, left_sizer, attr, label)

                self.left_panel.SetSizer(left_sizer)
                self.left_panel.Layout()
                left_sizer.Fit(self.left_panel)
                middle_panel_sizer.Add(self.left_panel, 1, wx.EXPAND | wx.ALL, 5)

                # Right Panel
                attrs_labels = (
                    ('snc_qop', 'SNC QoP*'),
                    ('snc_myname', 'SNC Name*'),
                    ('snc_partnername', 'SNC Partner Name*'),
                )
                for attr, label in attrs_labels:
                    self.add_user_input(self.right_panel, right_sizer, attr, label)

                # SNC LIB
                self.snc_lib_panel = wx.Panel(self.right_panel)
                snc_lib_sizer = wx.BoxSizer(wx.HORIZONTAL)

                snc_lib_label = wx.StaticText(self.snc_lib_panel,
                                              label="SNC Lib*:",
                                              size=wx.Size(100, -1))
                snc_lib_label.Wrap(-1)
                snc_lib_sizer.Add(snc_lib_label, 0, wx.ALL | wx.ALIGN_CENTER_VERTICAL, 5)

                self.controls['snc_lib'] = wx.FilePickerCtrl(self.snc_lib_panel,
                                                             message="Select a file:",
                                                             wildcard="*.*")
                self.controls['snc_lib'].SetBackgroundColour(wx.Colour(255, 255, 255))
                snc_lib_sizer.Add(self.controls['snc_lib'], 1, wx.ALL, 5)

                self.snc_lib_panel.SetSizer(snc_lib_sizer)
                self.snc_lib_panel.Layout()
                snc_lib_sizer.Fit(self.snc_lib_panel)
                right_sizer.Add(self.snc_lib_panel, 0, wx.EXPAND | wx.ALL, 5)

                # Common
                self.right_panel.SetSizer(right_sizer)
                self.right_panel.Layout()
                right_sizer.Fit(self.right_panel)
                middle_panel_sizer.Add(self.right_panel, 1, wx.EXPAND | wx.ALL, 5)

                self.middle_panel.SetSizer(middle_panel_sizer)
                self.middle_panel.Layout()
                middle_panel_sizer.Fit(self.middle_panel)

                self.next_button.SetLabelText("Next")
                self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)

                # Enable connection saving and loading inputs for non-ABAP
                self.load_previous_choice.Enable()
                self.save_connection.SetValue(False)
                self.save_connection.Enable()
                self.connection_name.SetValue("")
                self.connection_name.Enable()
                self.save_password.Enable()


        self.Layout()
        self.reset_control_listeners()


class OracleConnectionDialog(BaseConnectionDialog):
    """Connection dialog window for connecting to ORACLE."""

    erp = 'ORACLE'
    required_submodules = ['connect.oracle']
    required_user_inputs = ['client', 'user', 'password', 'language']

    def __init__(self, *args, **kwargs):
        """Return a new instance of the dialog window."""
        super().__init__(*args, **kwargs)

        title = wx.StaticText(self, label="Oracle Connection")
        title.SetFont(FONT_TITLES)
        self.sizer.Add(title, 0, wx.EXPAND|wx.ALL, 10)

        line = wx.StaticLine(self)
        self.sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        content_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)
        main_panel_sizer = wx.BoxSizer(wx.VERTICAL)

        self.top_panel = wx.Panel(self.main_panel)
        top_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        # Connection Type
        conntype_panel = wx.Panel(self.top_panel)
        conntype_sizer = wx.BoxSizer(wx.HORIZONTAL)

        conntype_label = wx.StaticText(conntype_panel, label="Connection Type:")
        conntype_label.Wrap(-1)
        conntype_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        conntype_sizer.Add(conntype_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        choices = ["Oracle Server Authentication"]
        self.connection_type_choice = wx.Choice(conntype_panel,
                                                choices=choices)
        self.connection_type_choice.SetSelection(0)
        conntype_sizer.Add(self.connection_type_choice, 0, wx.ALL, 5)

        conntype_panel.SetSizer(conntype_sizer)
        conntype_panel.Layout()
        conntype_sizer.Fit(conntype_panel)
        top_panel_sizer.Add(conntype_panel, 1, wx.EXPAND|wx.ALL, 5)

        load_prev_panel = wx.Panel(self.top_panel)
        load_prev_sizer = wx.BoxSizer(wx.HORIZONTAL)

        load_label = wx.StaticText(load_prev_panel,
                                   label='Load Saved Connection:')
        load_label.Wrap(-1)
        load_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        load_prev_sizer.Add(load_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        # Load Previous
        saved_connections = [""]
        saved_connections += self.config_db.saved_credential_names(self.erp)
        self.load_previous_choice = wx.Choice(load_prev_panel,
                                              choices=saved_connections)
        self.load_previous_choice.SetSelection(0)
        load_prev_sizer.Add(self.load_previous_choice, 1, wx.ALL, 5)

        load_prev_panel.SetSizer(load_prev_sizer)
        load_prev_panel.Layout()
        load_prev_sizer.Fit(load_prev_panel)
        top_panel_sizer.Add(load_prev_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.top_panel.SetSizer(top_panel_sizer)
        self.top_panel.Layout()
        top_panel_sizer.Fit(self.top_panel)
        main_panel_sizer.Add(self.top_panel, 0, wx.EXPAND|wx.ALL, 5)

        line = wx.StaticLine(self.main_panel)
        main_panel_sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.middle_panel = wx.Panel(self.main_panel)
        main_panel_sizer.Add(self.middle_panel, 1, wx.EXPAND|wx.ALL, 5)

        line = wx.StaticLine(self.main_panel)
        main_panel_sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.build_bottom_panel()
        main_panel_sizer.Add(self.bottom_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.main_panel.SetSizer(main_panel_sizer)
        self.main_panel.Layout()
        content_panel_sizer.Add(self.main_panel, 1, wx.EXPAND|wx.RIGHT, 5)

        content = (
            'Please select an existing connection or enter the '
            'connection details.'
        )
        guidepanel = GuidePanel(parent=self.content_panel, content=content)
        content_panel_sizer.Add(guidepanel, 1, wx.EXPAND|wx.LEFT, 5)

        self.content_panel.SetSizer(content_panel_sizer)
        self.content_panel.Layout()
        content_panel_sizer.Fit(self.content_panel)
        self.sizer.Add(self.content_panel, 1, wx.EXPAND|wx.ALL, 5)

        nav_panel = wx.Panel(self)
        nav_sizer = wx.BoxSizer(wx.VERTICAL)

        button_panel = wx.Panel(nav_panel)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.cancel_button = wx.Button(button_panel, label="Cancel")
        button_sizer.Add(self.cancel_button, 0, wx.ALL, 5)

        self.previous_button = wx.Button(button_panel, label="Previous")
        button_sizer.Add(self.previous_button, 0, wx.ALL, 5)

        self.next_button = wx.Button(button_panel, label="Next")
        button_sizer.Add(self.next_button, 0, wx.ALL, 5)

        button_panel.SetSizer(button_sizer)
        button_panel.Layout()
        button_sizer.Fit(button_panel)
        nav_sizer.Add(button_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        nav_panel.SetSizer(nav_sizer)
        nav_panel.Layout()
        nav_sizer.Fit(nav_panel)
        self.sizer.Add(nav_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)

        # Initialize the middle panel for the first time
        self.init_middle_panel()

        # Connect Events
        self.connection_type_choice.Bind(wx.EVT_CHOICE, self.conn_type_changed)
        self.load_previous_choice.Bind(wx.EVT_CHOICE, self.load_previous_selected)
        self.cancel_button.Bind(wx.EVT_BUTTON, self.cancel_button_pressed)
        self.previous_button.Bind(wx.EVT_BUTTON, self.previous_button_pressed)
        self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)
        self.save_connection.Bind(wx.EVT_CHECKBOX, self.save_connection_changed)
        self.connection_name.Bind(wx.EVT_KEY_UP, self.validate_connection_name)
        self.Bind(wx.EVT_CLOSE, self.parent.confirm_close_extraction)

    def load_previous_selected(self, event: wx.Event):
        """Load saved connection data from user config DB into this panel."""
        connection_name = self.load_previous_choice.GetStringSelection()
        if connection_name == "":
            return  # No connection selected to reload

        saved_creds = self.config_db.get_credentials(connection_name, self.erp)

        self.init_middle_panel()

        if saved_creds["system_id"]:
            self.controls['orcl_instance_type'].SetStringSelection("System ID")
            self.controls['orcl_instance_value'].SetValue(saved_creds["system_id"])
        else:
            self.controls['orcl_instance_type'].SetStringSelection("Service Name")
            self.controls['orcl_instance_value'].SetValue(saved_creds["service_name"])

        self.connection_type_choice.SetStringSelection(saved_creds["type"])

        for key in ('host', 'port', 'user', 'password'):
            value = saved_creds.get(key) or ''
            _set_wx_control_value(self.controls[key], value)

        self.Layout()
        self.validate_required_controls()

    def next_button_pressed(self, event: wx.Event):
        """Save credentials if requested, then test the connection."""
        # Generate connection kwargs for Threaded test
        connection_args = {}
        for key in ('host', 'port', 'user', 'password'):
            connection_args[key] = _get_wx_control_value(self.controls[key])

        if self.controls['orcl_instance_type'].GetStringSelection() == "System ID":
            connection_args["system_id"] = self.controls['orcl_instance_value'].GetValue()
        else:
            connection_args["service_name"] = self.controls['orcl_instance_value'].GetValue()

        # Create messenger while busy message is shown to user
        self.busy_info = wx.BusyInfo("Testing connection...")
        self.Disable()

        connection_type = self.connection_type_choice.GetStringSelection()
        thread = GetConnectionThread(self, "Oracle RDBMS",
                                     connection_type, connection_args)
        thread.start()

    def init_middle_panel(self):
        """Instantiate components of the middle panel for this window."""
        self.middle_panel.DestroyChildren()

        # Middle panel is made up of two columns of UserInputs
        middle_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.left_panel = wx.Panel(self.middle_panel)
        self.right_panel = wx.Panel(self.middle_panel)
        left_sizer = wx.BoxSizer(wx.VERTICAL)
        right_sizer = wx.BoxSizer(wx.VERTICAL)

        # Add Host / Port inputs to left panel
        attrs_labels = (
            ('host', 'Host Name*'),
            ('port', 'Port Number*'),
        )
        for attr, label in attrs_labels:
            self.add_user_input(self.left_panel, left_sizer, attr, label)

        # Add Instance Type Choice Dropdown to left panel
        instance_panel = wx.Panel(self.left_panel)
        instance_sizer = wx.BoxSizer(wx.HORIZONTAL)

        instance_label = wx.StaticText(instance_panel, label="Instance*:",
                                       size=wx.Size(100, -1))
        instance_label.Wrap(-1)
        instance_sizer.Add(instance_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        choices = [u"System ID", u"Service Name"]
        self.controls['orcl_instance_type'] = wx.Choice(instance_panel, choices=choices)
        self.controls['orcl_instance_type'].SetSelection(1)
        instance_sizer.Add(self.controls['orcl_instance_type'], 1, wx.ALL, 5)

        instance_panel.SetSizer(instance_sizer)
        instance_panel.Layout()
        instance_sizer.Fit(instance_panel)
        left_sizer.Add(instance_panel, 0, wx.EXPAND|wx.ALL, 5)

        # Add Instance Type Value to left panel
        value_panel = UserInputPanel(self.left_panel, label_text='Value*',
                                     tooltip_text=' or '.join(choices))
        self.controls['orcl_instance_value'] = value_panel.control
        left_sizer.Add(value_panel, 0, wx.EXPAND|wx.ALL, 5)

        # Finish left-side panel items
        self.left_panel.SetSizer(left_sizer)
        self.left_panel.Layout()
        left_sizer.Fit(self.left_panel)
        middle_panel_sizer.Add(self.left_panel, 1, wx.EXPAND|wx.ALL, 5)

        # Add Username / Password inputs to the right-side panel
        attrs_labels = (
            ('user', 'User*'),
            ('password', 'Password*'),
        )
        for attr, label in attrs_labels:
            self.add_user_input(self.right_panel, right_sizer, attr, label)

        # Wrap-up right-side and entire middle panel layouts
        self.right_panel.SetSizer(right_sizer)
        self.right_panel.Layout()
        right_sizer.Fit(self.right_panel)
        middle_panel_sizer.Add(self.right_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.middle_panel.SetSizer(middle_panel_sizer)
        self.middle_panel.Layout()
        middle_panel_sizer.Fit(self.middle_panel)

        self.Layout()

        self.reset_control_listeners()


class MSSQLConnectionDialog(BaseConnectionDialog):
    """Dialog window where the user enters MSSQL connection information."""

    erp = 'MSSQL'
    required_submodules = ['connect.mssql']
    required_user_inputs = ['host', 'database', 'user', 'password']

    def __init__(self, *args, **kwargs):
        """Return a new dialog window."""
        super().__init__(*args, **kwargs)

        title = wx.StaticText(self, label='SQL Server Connection')
        title.SetFont(FONT_TITLES)
        self.sizer.Add(title, 0, wx.EXPAND|wx.ALL, 10)

        line = wx.StaticLine(self)
        self.sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        content_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)
        main_panel_sizer = wx.BoxSizer(wx.VERTICAL)

        self.top_panel = wx.Panel(self.main_panel)
        top_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        # Connection Type
        conntype_panel = wx.Panel(self.top_panel)
        conntype_sizer = wx.BoxSizer(wx.HORIZONTAL)

        conntype_label = wx.StaticText(conntype_panel, label="Connection Type:")
        conntype_label.Wrap(-1)
        conntype_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        conntype_sizer.Add(conntype_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        choices = ["Windows Authentication", "SQL Server Authentication"]
        self.connection_type_choice = wx.Choice(conntype_panel,
                                                choices=choices)
        self.connection_type_choice.SetSelection(0)
        conntype_sizer.Add(self.connection_type_choice, 0, wx.ALL, 5)

        conntype_panel.SetSizer(conntype_sizer)
        conntype_panel.Layout()
        conntype_sizer.Fit(conntype_panel)
        top_panel_sizer.Add(conntype_panel, 1, wx.EXPAND|wx.ALL, 5)

        load_prev_panel = wx.Panel(self.top_panel)
        load_prev_sizer = wx.BoxSizer(wx.HORIZONTAL)

        load_label = wx.StaticText(load_prev_panel,
                                   label='Load Saved Connection:')
        load_label.Wrap(-1)
        load_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        load_prev_sizer.Add(load_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        # Load Previous
        saved_connections = [""]
        saved_connections += self.config_db.saved_credential_names(self.erp)
        self.load_previous_choice = wx.Choice(load_prev_panel,
                                              choices=saved_connections)
        self.load_previous_choice.SetSelection(0)
        load_prev_sizer.Add(self.load_previous_choice, 1, wx.ALL, 5)

        load_prev_panel.SetSizer(load_prev_sizer)
        load_prev_panel.Layout()
        load_prev_sizer.Fit(load_prev_panel)
        top_panel_sizer.Add(load_prev_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.top_panel.SetSizer(top_panel_sizer)
        self.top_panel.Layout()
        top_panel_sizer.Fit(self.top_panel)
        main_panel_sizer.Add(self.top_panel, 0, wx.EXPAND|wx.ALL, 5)

        line = wx.StaticLine(self.main_panel)
        main_panel_sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.middle_panel = wx.Panel(self.main_panel)
        main_panel_sizer.Add(self.middle_panel, 1, wx.EXPAND|wx.ALL, 5)

        line = wx.StaticLine(self.main_panel)
        main_panel_sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.build_bottom_panel()
        main_panel_sizer.Add(self.bottom_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.main_panel.SetSizer(main_panel_sizer)
        self.main_panel.Layout()
        content_panel_sizer.Add(self.main_panel, 1, wx.EXPAND|wx.RIGHT, 5)

        content = (
            'Please select an existing connection or enter the connection '
            'details for the selected SQL Server instance.\n\n'
            'Select "Windows Authentication" to use the credentials '
            'of the user currently logged in.  Otherwise, enter user '
            'name and password using "SQL Server Authentication."'
        )
        guidepanel = GuidePanel(parent=self.content_panel, content=content)
        content_panel_sizer.Add(guidepanel, 1, wx.EXPAND|wx.LEFT, 5)

        self.content_panel.SetSizer(content_panel_sizer)
        self.content_panel.Layout()
        content_panel_sizer.Fit(self.content_panel)
        self.sizer.Add(self.content_panel, 1, wx.EXPAND|wx.ALL, 5)

        nav_panel = wx.Panel(self)
        nav_sizer = wx.BoxSizer(wx.VERTICAL)

        button_panel = wx.Panel(nav_panel)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.cancel_button = wx.Button(button_panel, label="Cancel")
        button_sizer.Add(self.cancel_button, 0, wx.ALL, 5)

        self.previous_button = wx.Button(button_panel, label="Previous")
        button_sizer.Add(self.previous_button, 0, wx.ALL, 5)

        self.next_button = wx.Button(button_panel, label="Next")
        button_sizer.Add(self.next_button, 0, wx.ALL, 5)

        button_panel.SetSizer(button_sizer)
        button_panel.Layout()
        button_sizer.Fit(button_panel)
        nav_sizer.Add(button_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        nav_panel.SetSizer(nav_sizer)
        nav_panel.Layout()
        nav_sizer.Fit(nav_panel)
        self.sizer.Add(nav_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)

        # Initialize the middle panel for the first time
        self.init_middle_panel()

        # Connect Events
        self.connection_type_choice.Bind(wx.EVT_CHOICE, self.conn_type_changed)
        self.load_previous_choice.Bind(wx.EVT_CHOICE, self.load_previous_selected)
        self.cancel_button.Bind(wx.EVT_BUTTON, self.cancel_button_pressed)
        self.previous_button.Bind(wx.EVT_BUTTON, self.previous_button_pressed)
        self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)
        self.save_connection.Bind(wx.EVT_CHECKBOX, self.save_connection_changed)
        self.connection_name.Bind(wx.EVT_KEY_UP, self.validate_connection_name)
        self.Bind(wx.EVT_CLOSE, self.parent.confirm_close_extraction)

    def load_previous_selected(self, event: wx.Event):
        """Load saved connection data from user config DB into this panel."""
        connection_name = self.load_previous_choice.GetStringSelection()
        if connection_name == "":
            return  # No connection selected to reload

        saved_creds = self.config_db.get_credentials(connection_name, self.erp)

        connection_type = saved_creds["type"]
        self.connection_type_choice.SetStringSelection(connection_type)

        self.init_middle_panel()

        controls_to_load = ['host', 'port', 'schema', 'database', 'driver']

        if connection_type == "SQL Server Authentication":
            controls_to_load += ['user', 'password']

        for key in controls_to_load:
            value = saved_creds.get(key) or ''
            _set_wx_control_value(self.controls[key], value)

        self.Layout()
        self.validate_required_controls()

    def next_button_pressed(self, event: wx.Event):
        """When user clicks 'Next' button on SQL server connection screen"""
        # Determine which args to use for connection test
        connection_type = self.connection_type_choice.GetStringSelection()
        controls_to_load = ['host', 'port', 'schema', 'database', 'driver']

        if connection_type == "SQL Server Authentication":
            controls_to_load += ['user', 'password']

        # Gather connection kwargs for Threaded test
        connection_args = {}
        for key in controls_to_load:
            connection_args[key] = _get_wx_control_value(self.controls[key])

        # Raise an error if the port value is non-numeric
        port = connection_args['port']
        if port and not port.isnumeric():
            message = 'Port must be a number (received "{}")'.format(port)
            wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
            return

        # Get MSSQL Driver
        if connection_args['driver'] == '':
            connection_args['driver'] = 'SQL Server'

        # Create messenger while busy message is shown to user
        self.busy_info = wx.BusyInfo("Testing connection...")
        self.Disable()
        thread = GetConnectionThread(self, "MSSQL RDBMS",
                                     connection_type, connection_args)
        thread.start()

    def init_middle_panel(self):
        """Destroy and rebuild the entire middle panel for this window."""
        self.middle_panel.DestroyChildren()

        # Middle panel is made up of two columns of UserInputs
        middle_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.left_panel = wx.Panel(self.middle_panel)
        self.right_panel = wx.Panel(self.middle_panel)
        left_sizer = wx.BoxSizer(wx.VERTICAL)
        right_sizer = wx.BoxSizer(wx.VERTICAL)

        # Add Host / Port / Schema / Database inputs to left panel
        attrs_labels = (
            ('host', 'Host*'),
            ('port', 'Port'),
            ('schema', 'Schema'),
            ('database', 'Database*'),
            ('driver', 'Driver')
        )
        for attr, label in attrs_labels:
            self.add_user_input(self.left_panel, left_sizer, attr, label)

        # Finish building left side of the panel
        self.left_panel.SetSizer(left_sizer)
        self.left_panel.Layout()
        left_sizer.Fit(self.left_panel)
        middle_panel_sizer.Add(self.left_panel, 1, wx.EXPAND|wx.ALL, 5)

        # If using 'Windows Auth', do nothing. If using 'SQL Server Auth',
        # Add an input for username and password on the right-side panel
        if self.connection_type_choice.GetCurrentSelection() == 1:
            attrs_labels = (
                ('user', 'User*'),
                ('password', 'Password*'),
            )
            for attr, label in attrs_labels:
                self.add_user_input(self.right_panel, right_sizer, attr, label)

        self.right_panel.SetSizer(right_sizer)
        self.right_panel.Layout()
        right_sizer.Fit(self.right_panel)
        middle_panel_sizer.Add(self.right_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.middle_panel.SetSizer(middle_panel_sizer)
        self.middle_panel.Layout()
        middle_panel_sizer.Fit(self.middle_panel)

        self.Layout()

        self.reset_control_listeners()


class DB2ConnectionDialog(BaseConnectionDialog):
    """Connection dialog window for connecting to DB2."""

    erp = 'DB2'
    required_submodules = ['connect.db2']
    required_user_inputs = ['host', 'port', 'database', 'user', 'password']

    def __init__(self, *args, **kwargs):
        """Return a new instance of the dialog window."""
        super().__init__(*args, **kwargs)

        title = wx.StaticText(self, label="IBM DB2 Connection")
        title.SetFont(FONT_TITLES)
        self.sizer.Add(title, 0, wx.EXPAND|wx.ALL, 10)

        line = wx.StaticLine(self)
        self.sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        content_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)
        main_panel_sizer = wx.BoxSizer(wx.VERTICAL)

        self.top_panel = wx.Panel(self.main_panel)
        top_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        # Connection Type
        conntype_panel = wx.Panel(self.top_panel)
        conntype_sizer = wx.BoxSizer(wx.HORIZONTAL)

        conntype_label = wx.StaticText(conntype_panel, label="Connection Type:")
        conntype_label.Wrap(-1)
        conntype_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        conntype_sizer.Add(conntype_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        choices = [u"DB2 Authentication"]
        self.connection_type_choice = wx.Choice(conntype_panel, choices=choices)
        self.connection_type_choice.SetSelection(0)
        conntype_sizer.Add(self.connection_type_choice, 0, wx.ALL, 5)

        conntype_panel.SetSizer(conntype_sizer)
        conntype_panel.Layout()
        conntype_sizer.Fit(conntype_panel)
        top_panel_sizer.Add(conntype_panel, 1, wx.EXPAND|wx.ALL, 5)

        load_prev_panel = wx.Panel(self.top_panel)
        load_prev_sizer = wx.BoxSizer(wx.HORIZONTAL)

        load_label = wx.StaticText(load_prev_panel,
                                   label='Load Saved Connection:')
        load_label.Wrap(-1)
        load_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        load_prev_sizer.Add(load_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        # Load Previous
        saved_connections = [""]
        saved_connections += self.config_db.saved_credential_names(self.erp)
        self.load_previous_choice = wx.Choice(load_prev_panel,
                                              choices=saved_connections)
        self.load_previous_choice.SetSelection(0)
        load_prev_sizer.Add(self.load_previous_choice, 1, wx.ALL, 5)

        load_prev_panel.SetSizer(load_prev_sizer)
        load_prev_panel.Layout()
        load_prev_sizer.Fit(load_prev_panel)
        top_panel_sizer.Add(load_prev_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.top_panel.SetSizer(top_panel_sizer)
        self.top_panel.Layout()
        top_panel_sizer.Fit(self.top_panel)
        main_panel_sizer.Add(self.top_panel, 0, wx.EXPAND|wx.ALL, 5)

        line = wx.StaticLine(self.main_panel)
        main_panel_sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.middle_panel = wx.Panel(self.main_panel)
        middle_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.left_panel = wx.Panel(self.middle_panel)
        left_sizer = wx.BoxSizer(wx.VERTICAL)

        self.left_panel.SetSizer(left_sizer)
        self.left_panel.Layout()
        left_sizer.Fit(self.left_panel)
        middle_panel_sizer.Add(self.left_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.right_panel = wx.Panel(self.middle_panel)
        right_sizer = wx.BoxSizer(wx.VERTICAL)

        self.right_panel.SetSizer(right_sizer)
        self.right_panel.Layout()
        right_sizer.Fit(self.right_panel)

        main_panel_sizer.Add(self.middle_panel, 1, wx.EXPAND|wx.ALL, 5)

        line = wx.StaticLine(self.main_panel)
        main_panel_sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.build_bottom_panel()
        main_panel_sizer.Add(self.bottom_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.main_panel.SetSizer(main_panel_sizer)
        self.main_panel.Layout()
        content_panel_sizer.Add(self.main_panel, 1, wx.EXPAND|wx.RIGHT, 5)

        content = (
            'Please select an existing connection or enter connection '
            'details for the selected DB2 instance.\n\n'
            'Host: Name of host/server\n\n'
            'Port: Numeric value\n\n'
            'Database: Database name on server\n\n'
            'User: User name (optionally with domain prefix)\n\n'
            'Password: Valid password for user\n\n'
        )

        guidepanel = GuidePanel(parent=self.content_panel, content=content)
        content_panel_sizer.Add(guidepanel, 1, wx.EXPAND|wx.LEFT, 5)

        self.content_panel.SetSizer(content_panel_sizer)
        self.content_panel.Layout()
        content_panel_sizer.Fit(self.content_panel)
        self.sizer.Add(self.content_panel, 1, wx.EXPAND|wx.ALL, 5)

        nav_panel = wx.Panel(self)
        nav_sizer = wx.BoxSizer(wx.VERTICAL)

        button_panel = wx.Panel(nav_panel)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.cancel_button = wx.Button(button_panel, label="Cancel")
        button_sizer.Add(self.cancel_button, 0, wx.ALL, 5)

        self.previous_button = wx.Button(button_panel, label="Previous")
        button_sizer.Add(self.previous_button, 0, wx.ALL, 5)

        self.next_button = wx.Button(button_panel, label="Next")
        button_sizer.Add(self.next_button, 0, wx.ALL, 5)

        button_panel.SetSizer(button_sizer)
        button_panel.Layout()
        button_sizer.Fit(button_panel)
        nav_sizer.Add(button_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        nav_panel.SetSizer(nav_sizer)
        nav_panel.Layout()
        nav_sizer.Fit(nav_panel)
        self.sizer.Add(nav_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)

        # Initialize the middle panel for the first time
        self.init_middle_panel()

        # Connect Events
        self.connection_type_choice.Bind(wx.EVT_CHOICE, self.conn_type_changed)
        self.load_previous_choice.Bind(wx.EVT_CHOICE, self.load_previous_selected)
        self.cancel_button.Bind(wx.EVT_BUTTON, self.cancel_button_pressed)
        self.previous_button.Bind(wx.EVT_BUTTON, self.previous_button_pressed)
        self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)
        self.save_connection.Bind(wx.EVT_CHECKBOX, self.save_connection_changed)
        self.connection_name.Bind(wx.EVT_KEY_UP, self.validate_connection_name)
        self.Bind(wx.EVT_CLOSE, self.parent.confirm_close_extraction)

    def load_previous_selected(self, event: wx.Event):
        """Load saved connection data from user config DB into this panel."""
        connection_name = self.load_previous_choice.GetStringSelection()
        if connection_name == "":
            return  # No connection selected to reload

        saved_creds = self.config_db.get_credentials(connection_name, self.erp)
        self.connection_type_choice.SetStringSelection(saved_creds["type"])

        self.init_middle_panel()

        for key in ('host', 'port', 'database', 'user', 'password'):
            value = saved_creds.get(key) or ''
            _set_wx_control_value(self.controls[key], value)

        self.Layout()
        self.validate_required_controls()

    def next_button_pressed(self, event: wx.Event):
        """Test the DB2 connection, then proceed to Content Preview screen."""
        # Determine which args to use for connection test
        connection_type = self.connection_type_choice.GetStringSelection()
        controls_to_load = ['host', 'port', 'database']

        if connection_type == "DB2 Authentication":
            controls_to_load += ['user', 'password']

        # Gather connection kwargs for Threaded test
        connection_args = {}
        for key in controls_to_load:
            connection_args[key] = _get_wx_control_value(self.controls[key])

        # Create messenger while busy message is shown to user
        self.busy_info = wx.BusyInfo("Testing connection...")
        self.Disable()
        thread = GetConnectionThread(self, "DB2 RDBMS",
                                     connection_type, connection_args)
        thread.start()

    def init_middle_panel(self):
        """Instantiate components of the middle panel for this window."""
        self.middle_panel.DestroyChildren()

        # Middle panel is made up of two columns of UserInputs
        middle_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.left_panel = wx.Panel(self.middle_panel)
        self.right_panel = wx.Panel(self.middle_panel)
        left_sizer = wx.BoxSizer(wx.VERTICAL)
        right_sizer = wx.BoxSizer(wx.VERTICAL)

        # Add Host / Port / Database inputs to left panel
        attrs_labels = (
            ('host', 'Host*'),
            ('port', 'Port*'),
            ('database', 'Database*'),
        )
        for attr, label in attrs_labels:
            self.add_user_input(self.left_panel, left_sizer, attr, label)

        self.left_panel.SetSizer(left_sizer)
        self.left_panel.Layout()
        left_sizer.Fit(self.left_panel)
        middle_panel_sizer.Add(self.left_panel, 1, wx.EXPAND|wx.ALL, 5)

        # If using "DB2 Auth", add Username / Password inputs to right panel
        if self.connection_type_choice.GetCurrentSelection() == 0:
            attrs_labels = (
                ('user', 'User*'),
                ('password', 'Password*'),
            )
            for attr, label in attrs_labels:
                self.add_user_input(self.right_panel, right_sizer, attr, label)

        self.right_panel.SetSizer(right_sizer)
        self.right_panel.Layout()
        right_sizer.Fit(self.right_panel)
        middle_panel_sizer.Add(self.right_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.middle_panel.SetSizer(middle_panel_sizer)
        self.middle_panel.Layout()
        middle_panel_sizer.Fit(self.middle_panel)

        self.Layout()

        self.reset_control_listeners()


class MySQLConnectionDialog(BaseConnectionDialog):
    """Connection dialog window for connecting to MySQL."""

    erp = 'MYSQL'
    required_submodules = ['connect.mysql']
    required_user_inputs = {
        "MySQL Authentication": ['host', 'port', 'database', 'user', 'password'],
        "Data Source Name (DSN)": ['dsn']
    }
    conntypes_to_controls = {
        'MySQL Authentication': (
            'host', 'port', 'database', 'driver', 'user', 'password'
        ),
        'Data Source Name (DSN)': (
            'dsn', 'database',
        ),
    }

    def __init__(self, *args, **kwargs):
        """Return a new instance of the dialog window."""
        super().__init__(*args, **kwargs)

        title = wx.StaticText(self, label="MySQL Connection")
        title.SetFont(FONT_TITLES)
        self.sizer.Add(title, 0, wx.EXPAND|wx.ALL, 10)

        line = wx.StaticLine(self)
        self.sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        content_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)
        main_panel_sizer = wx.BoxSizer(wx.VERTICAL)

        self.top_panel = wx.Panel(self.main_panel)
        top_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        # Connection Type
        conntype_panel = wx.Panel(self.top_panel)
        conntype_sizer = wx.BoxSizer(wx.HORIZONTAL)

        conntype_label = wx.StaticText(conntype_panel, label="Connection Type:")
        conntype_label.Wrap(-1)
        conntype_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        conntype_sizer.Add(conntype_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        choices = [u"MySQL Authentication", "Data Source Name (DSN)"]
        self.connection_type_choice = wx.Choice(conntype_panel, choices=choices)
        self.connection_type_choice.SetSelection(0)
        conntype_sizer.Add(self.connection_type_choice, 0, wx.ALL, 5)

        conntype_panel.SetSizer(conntype_sizer)
        conntype_panel.Layout()
        conntype_sizer.Fit(conntype_panel)
        top_panel_sizer.Add(conntype_panel, 1, wx.EXPAND|wx.ALL, 5)

        load_prev_panel = wx.Panel(self.top_panel)
        load_prev_sizer = wx.BoxSizer(wx.HORIZONTAL)

        load_label = wx.StaticText(load_prev_panel,
                                   label='Load Saved Connection:')
        load_label.Wrap(-1)
        load_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        load_prev_sizer.Add(load_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        # Load Previous
        saved_connections = [""]
        saved_connections += self.config_db.saved_credential_names(self.erp)
        self.load_previous_choice = wx.Choice(load_prev_panel,
                                              choices=saved_connections)
        self.load_previous_choice.SetSelection(0)
        load_prev_sizer.Add(self.load_previous_choice, 1, wx.ALL, 5)

        load_prev_panel.SetSizer(load_prev_sizer)
        load_prev_panel.Layout()
        load_prev_sizer.Fit(load_prev_panel)
        top_panel_sizer.Add(load_prev_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.top_panel.SetSizer(top_panel_sizer)
        self.top_panel.Layout()
        top_panel_sizer.Fit(self.top_panel)
        main_panel_sizer.Add(self.top_panel, 0, wx.EXPAND|wx.ALL, 5)

        line = wx.StaticLine(self.main_panel)
        main_panel_sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.middle_panel = wx.Panel(self.main_panel)
        middle_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.left_panel = wx.Panel(self.middle_panel)
        left_sizer = wx.BoxSizer(wx.VERTICAL)

        self.left_panel.SetSizer(left_sizer)
        self.left_panel.Layout()
        left_sizer.Fit(self.left_panel)
        middle_panel_sizer.Add(self.left_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.right_panel = wx.Panel(self.middle_panel)
        right_sizer = wx.BoxSizer(wx.VERTICAL)

        self.right_panel.SetSizer(right_sizer)
        self.right_panel.Layout()
        right_sizer.Fit(self.right_panel)

        main_panel_sizer.Add(self.middle_panel, 1, wx.EXPAND|wx.ALL, 5)

        line = wx.StaticLine(self.main_panel)
        main_panel_sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.build_bottom_panel()
        main_panel_sizer.Add(self.bottom_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.main_panel.SetSizer(main_panel_sizer)
        self.main_panel.Layout()
        content_panel_sizer.Add(self.main_panel, 1, wx.EXPAND|wx.RIGHT, 5)

        content = (
            'Please select an existing connection or enter connection '
            'details for the selected MySQL instance.\n\n'
            'Host: Name of host/server\n\n'
            'Port: Numeric value\n\n'
            'Database: Database name on server\n\n'
            'User: User name (optionally with domain prefix)\n\n'
            'Password: Valid password for user\n\n'
        )

        guidepanel = GuidePanel(parent=self.content_panel, content=content)
        content_panel_sizer.Add(guidepanel, 1, wx.EXPAND|wx.LEFT, 5)

        self.content_panel.SetSizer(content_panel_sizer)
        self.content_panel.Layout()
        content_panel_sizer.Fit(self.content_panel)
        self.sizer.Add(self.content_panel, 1, wx.EXPAND|wx.ALL, 5)

        nav_panel = wx.Panel(self)
        nav_sizer = wx.BoxSizer(wx.VERTICAL)

        button_panel = wx.Panel(nav_panel)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.cancel_button = wx.Button(button_panel, label="Cancel")
        button_sizer.Add(self.cancel_button, 0, wx.ALL, 5)

        self.previous_button = wx.Button(button_panel, label="Previous")
        button_sizer.Add(self.previous_button, 0, wx.ALL, 5)

        self.next_button = wx.Button(button_panel, label="Next")
        button_sizer.Add(self.next_button, 0, wx.ALL, 5)

        button_panel.SetSizer(button_sizer)
        button_panel.Layout()
        button_sizer.Fit(button_panel)
        nav_sizer.Add(button_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        nav_panel.SetSizer(nav_sizer)
        nav_panel.Layout()
        nav_sizer.Fit(nav_panel)
        self.sizer.Add(nav_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)

        # Initialize the middle panel for the first time
        self.init_middle_panel()

        # Connect Events
        self.connection_type_choice.Bind(wx.EVT_CHOICE, self.conn_type_changed)
        self.load_previous_choice.Bind(wx.EVT_CHOICE, self.load_previous_selected)
        self.cancel_button.Bind(wx.EVT_BUTTON, self.cancel_button_pressed)
        self.previous_button.Bind(wx.EVT_BUTTON, self.previous_button_pressed)
        self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)
        self.save_connection.Bind(wx.EVT_CHECKBOX, self.save_connection_changed)
        self.connection_name.Bind(wx.EVT_KEY_UP, self.validate_connection_name)
        self.Bind(wx.EVT_CLOSE, self.parent.confirm_close_extraction)

    def load_previous_selected(self, event: wx.Event):
        """Load saved connection data from user config DB into this panel."""
        connection_name = self.load_previous_choice.GetStringSelection()
        if connection_name == "":
            return  # No connection selected to reload

        saved_creds = self.config_db.get_credentials(connection_name, self.erp)
        self.connection_type_choice.SetStringSelection(saved_creds["type"])

        self.init_middle_panel()
        for key in self.conntypes_to_controls[saved_creds["type"]]:
            value = saved_creds.get(key) or ''
            _set_wx_control_value(self.controls[key], value)

        self.Layout()
        self.validate_required_controls()

    def next_button_pressed(self, event: wx.Event):
        """Test the DB2 connection, then proceed to Content Preview screen."""
        # Determine which args to use for connection test
        connection_type = self.connection_type_choice.GetStringSelection()
        controls_to_load = []

        if connection_type == "MySQL Authentication":
            controls_to_load += ['host', 'port', 'database', 'user', 'password', 'driver']
        elif connection_type == "Data Source Name (DSN)":
            controls_to_load += ['dsn', 'database']

        # Gather connection kwargs for Threaded test
        connection_args = {}
        for key in controls_to_load:
            connection_args[key] = _get_wx_control_value(self.controls[key])

        # Get MYSQL Driver
        if connection_args['driver'] == '':
            connection_args['driver'] = 'MySQL ODBC 5.3 Unicode Driver'

        # Create messenger while busy message is shown to user
        self.busy_info = wx.BusyInfo("Testing connection...")
        self.Disable()
        thread = GetConnectionThread(self, "MYSQL RDBMS",
                                     connection_type, connection_args)
        thread.start()

    def init_middle_panel(self):
        """Instantiate components of the middle panel for this window."""
        self.middle_panel.DestroyChildren()

        # Middle panel is made up of two columns of UserInputs
        middle_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)
        self.left_panel = wx.Panel(self.middle_panel)
        self.right_panel = wx.Panel(self.middle_panel)
        left_sizer = wx.BoxSizer(wx.VERTICAL)
        right_sizer = wx.BoxSizer(wx.VERTICAL)

        if self.connection_type_choice.GetCurrentSelection() == 0:

            # Add Host / Port / Database inputs to left panel
            attrs_labels = (
                ('host', 'Host*'),
                ('port', 'Port*'),
                ('database', 'Database*'),
                ('driver', 'Driver')
            )
            for attr, label in attrs_labels:
                self.add_user_input(self.left_panel, left_sizer, attr, label)

            self.left_panel.SetSizer(left_sizer)
            self.left_panel.Layout()
            left_sizer.Fit(self.left_panel)
            middle_panel_sizer.Add(self.left_panel, 1, wx.EXPAND|wx.ALL, 5)

            # Add User / Password inputs to right panel
            attrs_labels = (
                ('user', 'User*'),
                ('password', 'Password*'),
            )
            for attr, label in attrs_labels:
                self.add_user_input(self.right_panel, right_sizer, attr, label)

        elif self.connection_type_choice.GetCurrentSelection() == 1:

            # Add Host / Port / Database inputs to left panel
            attrs_labels = (
                ('dsn', 'DSN*'),
                ('database', 'Database*'),
            )
            for attr, label in attrs_labels:
                self.add_user_input(self.left_panel, left_sizer, attr, label)

            self.left_panel.SetSizer(left_sizer)
            self.left_panel.Layout()
            left_sizer.Fit(self.left_panel)
            middle_panel_sizer.Add(self.left_panel, 1, wx.EXPAND|wx.ALL, 5)

            # # Add User / Password inputs to right panel
            # attrs_labels = (
            #     ('user', 'User*'),
            #     ('password', 'Password*'),
            # )
            # for attr, label in attrs_labels:
            #     self.add_user_input(self.right_panel, right_sizer, attr, label)
        self.right_panel.SetSizer(right_sizer)
        self.right_panel.Layout()
        right_sizer.Fit(self.right_panel)
        middle_panel_sizer.Add(self.right_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.middle_panel.SetSizer(middle_panel_sizer)
        self.middle_panel.Layout()
        middle_panel_sizer.Fit(self.middle_panel)

        self.Layout()

        self.reset_control_listeners()


class ContentPreviewDialog(DefaultDialog):
    """Dialog window to preview queries and data before extraction."""

    def __init__(self, parent: wx.Frame):
        """Instantiate and return a new content dialog window."""
        super().__init__(parent)
        self.busy_info = None  # type: wx.BusyInfo

        self.sizer = wx.BoxSizer(wx.VERTICAL)

        title = wx.StaticText(self, label="Content Preview")
        title.SetFont(FONT_TITLES)
        self.sizer.Add(title, 0, wx.EXPAND|wx.ALL, 10)

        line = wx.StaticLine(self)
        self.sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.content_panel = wx.Panel(self)
        content_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.main_panel = MainPanel(self.content_panel)
        self.init_main_panel()

        content_panel_sizer.Add(self.main_panel, 1, wx.EXPAND|wx.RIGHT, 5)

        content = (
            "Double-click any row for more detail(s) on query.\n\n"
            "Right-click any cell to copy the text to your clipboard.\n\n"
        )
        guidepanel = GuidePanel(parent=self.content_panel, content=content)
        content_panel_sizer.Add(guidepanel, 1, wx.EXPAND|wx.LEFT, 5)

        self.content_panel.SetSizer(content_panel_sizer)
        self.content_panel.Layout()
        content_panel_sizer.Fit(self.content_panel)
        self.sizer.Add(self.content_panel, 1, wx.EXPAND|wx.ALL, 5)

        nav_panel = wx.Panel(self)
        nav_sizer = wx.BoxSizer(wx.VERTICAL)

        button_panel = wx.Panel(nav_panel)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.cancel_button = wx.Button(button_panel, label="Cancel")
        button_sizer.Add(self.cancel_button, 0, wx.ALL, 5)

        self.previous_button = wx.Button(button_panel, label="Previous")
        button_sizer.Add(self.previous_button, 0, wx.ALL, 5)

        self.next_button = wx.Button(button_panel, label="Next")
        button_sizer.Add(self.next_button, 0, wx.ALL, 5)

        button_panel.SetSizer(button_sizer)
        button_panel.Layout()
        button_sizer.Fit(button_panel)
        nav_sizer.Add(button_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        nav_panel.SetSizer(nav_sizer)
        nav_panel.Layout()
        nav_sizer.Fit(nav_panel)
        self.sizer.Add(nav_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)

        # Connect Events
        self.cancel_button.Bind(wx.EVT_BUTTON, self.cancel_button_pressed)
        self.previous_button.Bind(wx.EVT_BUTTON, self.previous_button_pressed)
        self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)
        self.Bind(EVT_FIRSTHUNDRED_DONE, self.first_hundred_gathered)
        self.Bind(wx.EVT_CLOSE, self.parent.confirm_close_extraction)

    def cell_double_clicked(self, event: wx.grid.GridEvent):
        """Start new thread to get sample data for a dataset."""
        self.next_button.Enable(True)
        if self.grid.GetCellValue(event.Row, 0) == 'No queries':
            message = ('This ECF contains no content for extraction. '
                       'Please submit feeedback to PwC to investigate')
            wx.MessageBox(message, 'Error', style=wx.ICON_EXCLAMATION)
            return

        if self.grid.GetCellValue(event.Row, 2) == '(KEY REPORT)':
            message = ('Previews not available for key reports.')
            wx.MessageBox(message, 'Warning', style=wx.ICON_EXCLAMATION)
            return

        elif self.grid.GetCellValue(event.Row, 2).startswith('ERROR'):
            message = ('Preview not available for query table due to an error. '
                       'Please see the content preview pane\'s '
                       '\'Fields/Queries\' column for error text.')
            wx.MessageBox(message, 'Warning', style=wx.ICON_EXCLAMATION)
            return

        # Get ECF data for the clicked row from the main Panel
        ecf_data = self.parent.ecf_meta_data[event.GetRow()]
        table_alias = self.grid.GetCellValue(event.Row, 1)

        if config.CONTENT_PREVIEW_ENABLED:
            # Fetch a set of sample data in a new thread, then display
            # it to the user in a new dialog screen
            self.busy_info = wx.BusyInfo("Pulling 50 rows of sample data...")
            kwargs = {
                "messenger": self.parent.dialogs['connection'].messenger,
                "ecf_data": ecf_data,
                "server": self.parent.data_server,
                "connector": self.parent.data_connector,
                "table_alias": table_alias,
            }
            thread = FirstHundredThread(self, **kwargs)
            thread.start()
            return

        # Otherwise, alert the user with the SQL query if available
        if isinstance(ecf_data.query_text, dict):
            # If SAP or not using SQL queries, show metadata from ECF
            columns = pformat(sorted(ecf_data.query_text['Columns']), compact=True)

            params = ecf_data.query_text.copy()
            for key in ('Columns', 'Name', 'NameAlias'):
                del params[key]

            text = (
                'Details for "{table}" Query\n\n'
                'Columns:\n\n{columns}\n\n'
                'Parameters:\n\n{params}'
                ).format(table=table_alias, columns=columns,
                         params=pformat(params))
        else:
            # Standard SQL query-based ECF. Just display the SQL query.
            text = (
                'SQL Query for "{table}"\n\n{query}'
                ).format(table=table_alias, query=ecf_data.query_text)

        CopyableMessageBox(self, 'Info', text)

    def cell_right_clicked(self, event: wx.grid.GridEvent):
        """Copy content to clipboard when a grid cell is right-clicked."""
        # Do not attempt to copy data if another process has a lock on it
        if wx.TheClipboard.IsOpened():
            return

        # Copy data into the TheClipboard
        if wx.TheClipboard.Open():
            value = self.grid.GetCellValue(event.Row, event.Col)
            textdata = wx.TextDataObject(text=value)
            wx.TheClipboard.SetData(textdata)
            wx.TheClipboard.Close()

    def first_hundred_gathered(self, event: wx.Event):
        """Pop open a dialog window to preview data from a query."""
        self.parent.dialogs['extraction'].clear_logbox()
        response = event.response

        self.busy_info = None
        if response['status'] != 'success':
            # Failed to run query, alert user of Error raised in Thread
            wx.MessageBox(response['status'], 'Error', style=wx.ICON_ERROR)
            return
        elif not response['data']:
            # No records were returned from the test query, alert user
            message = ('This query returned zero results for this connection. '
                       'Please submit feeedback to PwC to investigate')
            wx.MessageBox(message, 'Warning', style=wx.ICON_EXCLAMATION)
            return
        else:
            # Otherwise, show the content preview dialog
            title = "Content Preview ({})".format(response["table_alias"])
            dialog = FirstHundredRowsDialog(
                parent=self,
                data=response["data"],
                col_labels=response["col_labels"],
                title=title
            )
            dialog.ShowModal()

    def cancel_button_pressed(self, event: wx.Event):
        """Cancel extraction workflow and return to home screen."""
        self.EndModal(0)

    def previous_button_pressed(self, event: wx.Event):
        """Return to the database connection screen."""
        self.EndModal(-1)

    def next_button_pressed(self, event: wx.Event):
        """Prompt user to confirm request, moving to next dialog if they do."""
        if self.parent.dialogs['connection'].all_invalid:
            message = 'Cannot continue extraction as there are no valid queries in the ECF'
            wx.MessageBox(message, 'Error', style=wx.ICON_EXCLAMATION)
            return
        message = (
            'Do you agree with the data request shown on this screen? '
            'By clicking yes, you are consenting to making the data shown '
            'available to PwC and agreeing that PwC may extract this data.'
        )
        dialog = wx.MessageDialog(self, message, 'Info',
                                  style=wx.YES_NO|wx.ICON_INFORMATION)
        answer = dialog.ShowModal()
        if answer == wx.ID_YES:
            self.EndModal(1)

    def init_main_panel(self, queries: List[tuple] = None, erp: str = None):
        """Destroy and recreate this panel with updated data/layout.

        ARGS:
            queries: Table of query data to display in the grid.
            erp: The 'DataServer' value in the ECF that determines the
                label for last column of grid table (Query or Fields).
        """
        if erp == 'SAP Application Server':
            grid_columns = ('Table Name', 'Table Alias', 'Fields')
        else:
            grid_columns = ('Table Name', 'Table Alias', 'Query')

        self.main_panel.DestroyChildren()

        # Grid
        self.grid = wx.grid.Grid(self.main_panel)

        if queries:
            # Convert all queries to fit on a single line
            queries = [(name, alias, ' '.join(query.split()))
                       for name, alias, query in queries]
            self.grid.CreateGrid(len(queries), 3)
        else:
            self.grid.CreateGrid(1, 1)
        self.grid.EnableEditing(False)
        self.grid.EnableGridLines(True)
        self.grid.EnableDragGridSize(False)
        self.grid.SetMargins(0, 0)
        self.grid.SetSelectionMode(wx.grid.Grid.GridSelectRows)

        # Columns
        self.grid.EnableDragColMove(False)
        self.grid.EnableDragColSize(True)
        self.grid.SetColLabelSize(30)
        for index, label in enumerate(grid_columns):
            self.grid.SetColLabelValue(index, label)

        self.grid.SetColLabelAlignment(wx.ALIGN_LEFT, wx.ALIGN_BOTTOM)

        # Rows
        self.grid.EnableDragRowSize(True)
        self.grid.SetRowLabelSize(80)
        self.grid.SetRowLabelAlignment(wx.ALIGN_CENTRE, wx.ALIGN_CENTRE)

        # Label Appearance
        self.grid.SetRowLabelSize(50)

        # Cell Defaults
        self.grid.SetDefaultCellAlignment(wx.ALIGN_LEFT, wx.ALIGN_TOP)

        if queries:
            for row, each_query in enumerate(queries):
                for col, each in enumerate(each_query):
                    self.grid.SetCellValue(row, col, each)
        else:
            self.grid.SetCellValue(0, 0, "No queries")
        self.grid.AutoSizeColumns()

        main_panel_sizer = wx.BoxSizer(wx.VERTICAL)
        main_panel_sizer.Add(self.grid, 1, wx.EXPAND|wx.ALL, 5)

        self.main_panel.SetSizer(main_panel_sizer)
        self.main_panel.Layout()

        # Bind click events to the grid
        self.grid.Bind(wx.grid.EVT_GRID_CELL_LEFT_DCLICK,
                       self.cell_double_clicked)
        self.grid.Bind(wx.grid.EVT_GRID_CELL_RIGHT_CLICK,
                       self.cell_right_clicked)


class FirstHundredRowsDialog(wx.Dialog):
    """Pop-up window of data from a query that the user has previewed."""

    def __init__(self, parent, data, col_labels, title='Content Preview'):
        """Return a new instance of the dialog window."""
        super().__init__(parent, title=title, size=wx.Size(888, 528),
                         style=wx.DEFAULT_DIALOG_STYLE|wx.RESIZE_BORDER)

        self.SetSizeHints(wx.DefaultSize, wx.DefaultSize)

        self.sizer = wx.BoxSizer(wx.VERTICAL)

        scroll_panel = wx.ScrolledWindow(self)
        scroll_panel.SetScrollRate(5, 5)
        scroll_window_sizer = wx.BoxSizer(wx.VERTICAL)

        self.grid = wx.grid.Grid(scroll_panel)

        # Grid
        if data:
            self.grid.CreateGrid(len(data), len(data[0]))
        else:
            self.grid.CreateGrid(1, 1)

        self.grid.EnableEditing(False)
        self.grid.EnableGridLines(True)
        self.grid.EnableDragGridSize(False)
        self.grid.SetMargins(0, 0)

        # Columns
        for col, label in enumerate(col_labels):
            self.grid.SetColLabelValue(col, label)
        self.grid.EnableDragColMove(False)
        self.grid.EnableDragColSize(True)
        # self.grid.SetColLabelSize(30)
        self.grid.SetColLabelAlignment(wx.ALIGN_LEFT, wx.ALIGN_BOTTOM)

        # Rows
        self.grid.EnableDragRowSize(True)
        self.grid.SetRowLabelSize(80)
        self.grid.SetRowLabelAlignment(wx.ALIGN_CENTRE, wx.ALIGN_CENTRE)

        # Label Appearance

        # Data
        if data:
            for row, row_data in enumerate(data):
                for col, each in enumerate(row_data):
                    self.grid.SetCellValue(row, col, each)
        else:
            self.grid.SetCellValue(0, 0, "No queries")

        # Auto Size
        self.grid.AutoSizeColumns()

        # Cell Defaults
        self.grid.SetDefaultCellAlignment(wx.ALIGN_LEFT, wx.ALIGN_TOP)
        scroll_window_sizer.Add(self.grid, 0, wx.ALL, 5)

        scroll_panel.SetSizer(scroll_window_sizer)
        scroll_panel.Layout()
        scroll_window_sizer.Fit(scroll_panel)
        self.sizer.Add(scroll_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)


class ExtractionDialog(DefaultDialog):
    """Dialog window to show progress during an extraction."""

    def __init__(self, parent: wx.Frame):
        """Return a new instance of the dialog window."""
        super().__init__(parent)

        self.extract_kwargs = {}
        self.package_path = None
        self.is_paused = False
        self.logbox = None  # type: LogBox
        self.upload_thread = None  # type: threading.Thread
        self.busy_info = None  # type: wx.BusyInfo

        self.sizer = wx.BoxSizer(wx.VERTICAL)

        title = wx.StaticText(self, label="Extract")
        title.SetFont(FONT_TITLES)
        self.sizer.Add(title, 0, wx.EXPAND|wx.ALL, 10)

        line = wx.StaticLine(self)
        self.sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.content_panel = wx.Panel(self)
        content_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.main_panel = MainPanel(self.content_panel)
        main_panel_sizer = wx.BoxSizer(wx.VERTICAL)

        progress_label = wx.StaticText(self.main_panel, label="Status:")
        progress_label.Wrap(-1)
        progress_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        main_panel_sizer.Add(progress_label, 0, wx.ALL, 5)

        gauge_panel = wx.Panel(self.main_panel)
        gauge_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.progress_bar = wx.Gauge(gauge_panel, size=wx.Size(600, -1))
        self.progress_bar.SetValue(0)
        gauge_sizer.Add(self.progress_bar, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        self.pause_button = wx.Button(gauge_panel, label="Pause")
        self.pause_button.Enable(False)

        gauge_sizer.Add(self.pause_button, 0, wx.ALL, 5)

        self.start_button = wx.Button(gauge_panel, label="Resume")
        gauge_sizer.Add(self.start_button, 0, wx.ALL, 5)

        gauge_panel.SetSizer(gauge_sizer)
        gauge_panel.Layout()
        gauge_sizer.Fit(gauge_panel)
        main_panel_sizer.Add(gauge_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        self.m_staticline51 = wx.StaticLine(self.main_panel)
        main_panel_sizer.Add(self.m_staticline51, 0, wx.EXPAND|wx.ALL, 5)

        log_label = wx.StaticText(self.main_panel, label="Extraction Log:")
        log_label.Wrap(-1)
        log_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        main_panel_sizer.Add(log_label, 0, wx.ALL, 5)

        # Logbox
        logbox_window = wx.ScrolledWindow(self.main_panel)
        logbox_window.SetScrollRate(5, 5)
        logbox_sizer = wx.BoxSizer(wx.VERTICAL)

        self.logbox = LogBox(logbox_window, parent)
        logbox_sizer.Add(self.logbox, 1, wx.ALL|wx.EXPAND, 0)

        logbox_window.SetSizer(logbox_sizer)
        logbox_window.Layout()
        logbox_sizer.Fit(logbox_window)
        main_panel_sizer.Add(logbox_window, 1, wx.EXPAND|wx.ALL, 5)

        line = wx.StaticLine(self.main_panel)
        main_panel_sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        # Row with [Upload] button underneath logbox
        upload_panel = wx.Panel(self.main_panel)
        upload_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.upload_button = wx.Button(upload_panel, label="Upload")
        self.upload_button.Enable(False)

        upload_sizer.Add(self.upload_button, 0, wx.ALL, 5)

        upload_panel.SetSizer(upload_sizer)
        upload_panel.Layout()
        upload_sizer.Fit(upload_panel)
        main_panel_sizer.Add(upload_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        # Instructions panel on right-side side
        self.main_panel.SetSizer(main_panel_sizer)
        self.main_panel.Layout()
        content_panel_sizer.Add(self.main_panel, 1, wx.EXPAND|wx.RIGHT, 5)

        content = (
            "To pause the extraction and resume it at a later time, "
            "click \"Pause\".\n\n"
            "To resume the paused extraction now, click \"Resume\"  \n\n"
            "To resume the paused extraction later, click \"Finish\"  \n\n"
            "When the extraction is complete, you will have the "
            "ability to transmit the data back to PwC by clicking on "
            "the \"Upload\" button.  \n\n"
            "If you wish to defer upload or need to manually transfer "
            "data back to PwC, simply click on the \"Finish\" button."
        )
        guidepanel = GuidePanel(parent=self.content_panel, content=content)
        content_panel_sizer.Add(guidepanel, 1, wx.EXPAND|wx.LEFT, 5)

        self.content_panel.SetSizer(content_panel_sizer)
        self.content_panel.Layout()
        content_panel_sizer.Fit(self.content_panel)
        self.sizer.Add(self.content_panel, 1, wx.EXPAND|wx.ALL, 5)

        nav_panel = wx.Panel(self)
        nav_sizer = wx.BoxSizer(wx.VERTICAL)

        button_panel = wx.Panel(nav_panel)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.cancel_button = wx.Button(button_panel, label="Cancel")
        button_sizer.Add(self.cancel_button, 0, wx.ALL, 5)

        self.previous_button = wx.Button(button_panel, label="Previous")
        button_sizer.Add(self.previous_button, 0, wx.ALL, 5)

        self.finish_button = wx.Button(button_panel, label="Finish")
        self.finish_button.Enable(False)

        button_sizer.Add(self.finish_button, 0, wx.ALL, 5)

        button_panel.SetSizer(button_sizer)
        button_panel.Layout()
        button_sizer.Fit(button_panel)
        nav_sizer.Add(button_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        nav_panel.SetSizer(nav_sizer)
        nav_panel.Layout()
        nav_sizer.Fit(nav_panel)
        self.sizer.Add(nav_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)

        # Connect Events
        self.Bind(wx.EVT_SHOW, self.on_show)
        self.pause_button.Bind(wx.EVT_BUTTON, self.pause_button_pressed)
        self.start_button.Bind(wx.EVT_BUTTON, self.start_extraction)
        self.upload_button.Bind(wx.EVT_BUTTON, self.upload_button_pressed)
        self.cancel_button.Bind(wx.EVT_BUTTON, self.cancel_button_pressed)
        self.previous_button.Bind(wx.EVT_BUTTON, self.previous_button_pressed)
        self.finish_button.Bind(wx.EVT_BUTTON, self.finish_button_pressed)
        self.Bind(EVT_EXTRACTION_DONE, self.extraction_complete)
        self.Bind(EVT_UPLOAD_DONE, self.upload_feedback)
        self.Bind(wx.EVT_CLOSE, self.on_close)

    def pause_button_pressed(self, event: wx.Event):
        """Occurs when 'Pause' button is pressed mid-extraction."""
        self.pause_button.Disable()
        self.is_paused = True
        self.parent.stopevent.set()
        self.start_button.SetLabel("Resume")
        self.cancel_button.Disable()

    def start_extraction(self, event: wx.Event):
        """Occurs when 'Start' button is pressed to start/resume an Extraction."""
        resume_extract = self.is_paused
        self.is_paused = False
        self.start_button.Disable()
        self.previous_button.Disable()
        self.parent.stopevent.clear()
        self.pause_button.Enable()
        self.cancel_button.Enable()
        thread = ExtractThread(parent=self,
                               progress_bar=self.progress_bar,
                               resume_extract=resume_extract,
                               **self.extract_kwargs)

        thread.start()

    def on_show(self, event: wx.ShowEvent):
        """Start the extraction automatically when the dialog is shown."""
        if not event.IsShown():
            return  # Hiding the page, no action needed

        # Set the title to include ECF filename of current extraction
        if 'ecf_file' in self.extract_kwargs:
            filename = os.path.basename(self.extract_kwargs['ecf_file'])
            self.SetTitle('Extract ({})'.format(filename))
        else:
            self.SetTitle('Extract')

        self.start_extraction(event)

    def upload_button_pressed(self, event: wx.Event=None):
        """Occurs when the user uploads data to the LFU."""
        upload_method = self.parent.ecf_data["FileUploadMethod"]

        if upload_method == "DIF":
            message = ('Upload failed\n\n'
                       'DIF upload method is not supported at this time.')
            wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
            return

        if upload_method not in ['MFT_LFU', 'LFU']:
            message = (
                'Upload failed\n\n'
                'Unknown upload method in ECF:  {method}. Please '
                'contact the PyExtract support team for help.'
                ).format(method=upload_method)
            wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
            return

        # Collect kwargs needed to run the upload in another thread
        kwargs = {
            'parent': self,
            'upload_method': upload_method,
            'sftp_port': self.parent.configs['sftp_port'],
            'rename_wait': self.parent.configs['rename_wait']
        }

        # Determine location for upload. Use the Production server
        # based on 'Territory' value from the ECF if a production
        # build, or use user-selected Config value if QA build.
        if config.ALLOW_USER_UPLOAD_LOCATION:
            kwargs['sftp_location'] = self.parent.configs['sftp_location']
            kwargs['lfu_location'] = self.parent.configs['lfu_location']
        else:
            territory = self.parent.ecf_data["Territory"]
            if territory.upper() not in ('WEST', 'CENTRAL'):
                message = (
                    'Upload failed\n\n'
                    'Invalid Territory value in ECF ("{territory}"). '
                    'Only the "West" and "Central" territories are '
                    'supported for production use.'
                    ).format(territory=territory)
                wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
                return
            kwargs['sftp_location'] = 'PROD-' + territory.upper()
            kwargs['lfu_location'] = 'PROD-' + territory.upper()

        LOGGER.info('Beginning to upload package using %s method...',
                    upload_method)
        self.busy_info = wx.BusyInfo("Uploading Package...")
        self.upload_button.Disable()
        self.upload_thread = UploadPackageThread(**kwargs)
        self.upload_thread.start()

    def upload_feedback(self, event: wx.Event):
        """Will call the appropriate message if upload was a success/failure
            and will prompt user if they would like to delete the package
        """
        self.busy_info = None
        if event.response['status'] == 'success':
            alert_upload_success(filename=os.path.basename(self.package_path),
                                 host=event.response['host'],
                                 method=event.response['method'])
            self.prompt_to_delete_package()
        else:
            self.upload_button.Enable()
            alert_upload_failed(host=event.response['host'],
                                error=event.response['status'])

    def prompt_to_delete_package(self):
        """Prompt user to delete the data package after upload success."""
        package_name = os.path.basename(self.package_path)
        extract_id = common._extract_id_from_package(package_name)
        message = (
            'Would you like to delete your local copy of data package "{}"?'
            ).format(package_name)
        dialog = wx.MessageDialog(self, message, 'Info',
                                  style=wx.YES_NO|wx.ICON_INFORMATION)
        answer = dialog.ShowModal()
        if answer == wx.ID_YES:
            # Delete ECF from the GUI config DB and filesystem
            self.parent.config_db.delete_saved_extract(extract_id)
            if os.path.exists(self.package_path):
                os.remove(self.package_path)

    def cancel_button_pressed(self, event: wx.Event):
        """Occurs when 'Cancel' button is pressed during Extraction."""
        if self.parent.stopevent.is_set():
            # Extraction already paused, exit the modal
            self.prompt_to_delete_package()
            self.EndModal(0)
        else:
            # Pause the extraction, then have user choose next step
            self.cancel_button.Disable()
            self.is_paused = True
            self.parent.stopevent.set()
            LOGGER.info('Extraction interrupted by user.')
            LOGGER.info('No new rows will be read. '
                        'Finishing write of data still in memory.')

    def previous_button_pressed(self, event: wx.Event):
        """Close this window and take user to the previous window."""
        self.EndModal(-1)

    def finish_button_pressed(self, event: wx.Event):
        """End the extraction, or prompt user to take additional action."""
        # Do not close the window if an upload is in progress
        if self.upload_thread and self.upload_thread.is_alive():
            wx.MessageBox('Upload in progress. Please wait before starting a new extraction.',
                          'Info', style=wx.ICON_INFORMATION)
            return

        self.EndModal(2)

    def extraction_complete(self, event: wx.Event):
        """Modal and GUI updates that occur when an extraction completes."""
        if not self:
            return  # GUI has been closed/destroyed already

        self.package_path = event.package_path
        self.pause_button.Disable()

        # If paused, prompt user to take further action
        if self.is_paused:
            self.start_button.Enable()
            self.cancel_button.Enable()
            self.upload_button.Disable()
            self.finish_button.Disable()
            if self.IsShown():
                wx.MessageBox('Extraction paused\n\n'
                              'Click "Resume" to continue the extraction '
                              'or "Cancel" to return home.', 'Paused')
            return

        # If extract completed, and auto-uploading, begin upload process
        auto_upload = self.parent.config_db.get_config_value('auto_upload')
        if auto_upload.upper() == "YES":
            self.upload_button_pressed()
            self.cancel_button.Disable()
            self.finish_button.Enable()
            return

        # Otherwise, prompt user to take further action
        self.start_button.Disable()
        self.cancel_button.Disable()
        self.upload_button.Enable()
        self.finish_button.Enable()

        if event.errors:
            wx.MessageBox('Extraction complete with Errors\n\n'
                          'Review the log for warnings / errors, then "Upload" '
                          'data or "Finish" this extraction.', 'Complete',
                          wx.ICON_WARNING)
        if event.warnings:
            wx.MessageBox('Extraction complete with Warnings\n\n'
                          'Review the log for warnings / errors, then "Upload" '
                          'data or "Finish" this extraction.', 'Complete',
                          wx.ICON_WARNING)
        else:
            wx.MessageBox('Extraction complete\n\n'
                          'Review the log for warnings / errors, then "Upload" '
                          'data or "Finish" this extraction.', 'Complete')

    def clear_logbox(self):
        """Reset the log window on this panel before a new extraction."""
        self.logbox.SetValue("")

    def on_close(self, event: wx.CloseEvent):
        """Prompt user for permission, then safely close the Extraction."""

        # Do not close the window if an upload is in progress
        if self.upload_thread and self.upload_thread.is_alive():
            wx.MessageBox('Upload in progress. Please wait before closing application.',
                          'Info', style=wx.ICON_INFORMATION)
            event.Veto()
            return

        # Do not continue closing if user elects not to
        message = 'Are you sure you want to cancel this extraction?'
        dialog = wx.MessageDialog(self, message, 'Info',
                                  style=wx.YES_NO|wx.ICON_INFORMATION)
        answer = dialog.ShowModal()
        if answer == wx.ID_NO:
            event.Veto()
            return

        # Shutdown the extraction safely
        self.pause_button_pressed(event)

        # Close the window
        event.Skip()


class ContinueExtractionDialog(DefaultDialog):
    """A dialog window to view and continue partially completed extractions."""

    def __init__(self, parent: wx.Frame):
        """Return a new instance of the dialog window."""
        super().__init__(parent)

        self.config_db = ConfigDatabase()
        self.selected_row = None
        self.package_path = None

        # Dialog window objects
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.grid = None  # type: wx.grid.Grid

        # Begin building components of this window
        title = wx.StaticText(self, label="Continue Extraction")
        title.SetFont(FONT_TITLES)
        self.sizer.Add(title, 0, wx.EXPAND|wx.ALL, 10)

        line = wx.StaticLine(self)
        self.sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.content_panel = wx.Panel(self)
        content_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.main_panel = MainPanel(self.content_panel)
        main_panel_sizer = wx.BoxSizer(wx.VERTICAL)

        top_panel = wx.Panel(self.main_panel)
        top_panel_sizer = wx.BoxSizer(wx.VERTICAL)

        button_panel = wx.Panel(top_panel)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.delete_button = wx.Button(button_panel, label="Delete Selected")
        button_sizer.Add(self.delete_button, 0, wx.ALL, 5)

        self.reupload_button = wx.Button(button_panel, label="Re-Upload Selected")
        button_sizer.Add(self.reupload_button, 0, wx.ALL, 5)

        button_panel.SetSizer(button_sizer)
        button_panel.Layout()
        button_sizer.Fit(button_panel)
        top_panel_sizer.Add(button_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        top_panel.SetSizer(top_panel_sizer)
        top_panel.Layout()
        top_panel_sizer.Fit(top_panel)
        main_panel_sizer.Add(top_panel, 0, wx.ALL|wx.EXPAND, 5)

        self.grid_panel = wx.Panel(self.main_panel)

        main_panel_sizer.Add(self.grid_panel, 1, wx.EXPAND|wx.ALL, 5)

        self.main_panel.SetSizer(main_panel_sizer)
        self.main_panel.Layout()
        content_panel_sizer.Add(self.main_panel, 1, wx.EXPAND|wx.RIGHT, 5)

        content = "Double click a row to continue extraction from where it left off."
        guidepanel = GuidePanel(parent=self.content_panel, content=content)
        content_panel_sizer.Add(guidepanel, 1, wx.EXPAND|wx.LEFT, 5)

        self.content_panel.SetSizer(content_panel_sizer)
        self.content_panel.Layout()
        content_panel_sizer.Fit(self.content_panel)
        self.sizer.Add(self.content_panel, 1, wx.EXPAND|wx.ALL, 5)

        nav_panel = wx.Panel(self)
        nav_sizer = wx.BoxSizer(wx.VERTICAL)

        button_panel = wx.Panel(nav_panel)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.cancel_button = wx.Button(button_panel, label=u"Cancel")
        button_sizer.Add(self.cancel_button, 0, wx.ALL, 5)

        self.previous_button = wx.Button(button_panel, label=u"Previous")
        self.previous_button.Enable(False)
        button_sizer.Add(self.previous_button, 0, wx.ALL, 5)

        self.next_button = wx.Button(button_panel, label=u"Next")
        self.next_button.Enable(False)
        button_sizer.Add(self.next_button, 0, wx.ALL, 5)

        button_panel.SetSizer(button_sizer)
        button_panel.Layout()
        button_sizer.Fit(button_panel)
        nav_sizer.Add(button_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        nav_panel.SetSizer(nav_sizer)
        nav_panel.Layout()
        nav_sizer.Fit(nav_panel)
        self.sizer.Add(nav_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)

        # Build the grid panel once all other elements have been created
        self.init_grid_panel()

        # Connect Events
        self.delete_button.Bind(wx.EVT_BUTTON, self.delete_button_pressed)
        self.reupload_button.Bind(wx.EVT_BUTTON, self.reupload_button_pressed)
        self.cancel_button.Bind(wx.EVT_BUTTON, self.cancel_button_pressed)
        self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)
        self.Bind(EVT_UPLOAD_DONE, self.upload_feedback)

    def delete_button_pressed(self, event: wx.Event):
        """Action(s) to take when the 'Delete Selected' button is pressed."""
        selected_rows = self.grid.GetSelectedRows()
        if not selected_rows:
            wx.MessageBox('No row(s) selected for deletion.',
                          'Error', style=wx.ICON_ERROR)
            return
        elif self.grid.GetCellValue(0, 0) == 'No saved extractions':
            wx.MessageBox('No saved extractions available to delete.',
                          'Info', style=wx.ICON_EXCLAMATION)
            return

        if len(selected_rows) == 1:
            message = (
                'Are you sure you want to delete the selected Extraction?')
        elif len(selected_rows) > 1:
            message = (
                'Are you sure you want to delete all selected Extractions?')
        dialog = wx.MessageDialog(self, message, 'Info',
                                  style=wx.YES_NO|wx.ICON_INFORMATION)
        answer = dialog.ShowModal()
        if answer == wx.ID_YES:
            # Remove each row from local database, filepath, and this grid
            # Delete rows in reverse order to avoid IndexErrors.
            for row in reversed(selected_rows):
                extract_id = self.grid.GetCellValue(row, 0)
                request_id = self.grid.GetCellValue(row, 1)
                working_dir = self.parent.configs["working_directory"]
                package = os.path.join(working_dir, request_id,
                                       _package_name(extract_id))

                # Delete saved extraction from database and local machine
                self.config_db.delete_saved_extract(extract_id)
                if os.path.exists(package):
                    os.remove(package)

                # Delete RequestID folder for extraction if its now empty
                package_folder = os.path.join(working_dir, request_id)
                if os.path.exists(package_folder):
                    if not os.listdir(package_folder):
                        os.rmdir(package_folder)

        # De-select rows and rebuild the grid panel
        self.selected_row = None
        self.init_grid_panel()

    def reupload_button_pressed(self, event: wx.Event):

        upload_method = 'MFT_LFU'

        if upload_method == "DIF":
            message = ('Upload failed\n\n'
                       'DIF upload method is not supported at this time.')
            wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
            return

        if upload_method not in ['MFT_LFU', 'LFU']:
            message = (
                'Upload failed\n\n'
                'Unknown upload method in ECF:  {method}. Please '
                'contact the PyExtract support team for help.'
            ).format(method=upload_method)
            wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
            return

        # Collect kwargs needed to run the upload in another thread
        kwargs = {
            'parent': self,
            'upload_method': upload_method,
            'sftp_port': self.parent.configs['sftp_port'],
            'rename_wait': self.parent.configs['rename_wait']
        }

        # Determine location for upload. Use the Production server
        # based on 'Territory' value from the ECF if a production
        # build, or use user-selected Config value if QA build.
        if config.ALLOW_USER_UPLOAD_LOCATION:
            kwargs['sftp_location'] = self.parent.configs['sftp_location']
            kwargs['lfu_location'] = self.parent.configs['lfu_location']
        else:
            territory = self.parent.ecf_data["Territory"]
            if territory.upper() not in ('WEST', 'CENTRAL'):
                message = (
                    'Upload failed\n\n'
                    'Invalid Territory value in ECF ("{territory}"). '
                    'Only the "West" and "Central" territories are '
                    'supported for production use.'
                ).format(territory=territory)
                wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
                return
            kwargs['sftp_location'] = 'PROD-' + territory.upper()
            kwargs['lfu_location'] = 'PROD-' + territory.upper()

        selected_rows = self.grid.GetSelectedRows()
        if len(selected_rows) > 1:
            self.msg = wx.MessageBox("Please select a single row.")
            return
        LOGGER.info('Beginning to upload package using %s method...',
                    upload_method)
        for row in reversed(selected_rows):
            extract_id = self.grid.GetCellValue(row, 0)
            request_id = self.grid.GetCellValue(row, 1)
            working_dir = self.parent.configs["working_directory"]
            package = os.path.join(working_dir, request_id,
                                   _package_name(extract_id))

            kwargs["package_path"] = package
            self.package_path = package
            self.busy_info = wx.BusyInfo("Uploading Package...")
            self.upload_thread = UploadPackageThread(**kwargs)
            self.upload_thread.start()

    def upload_feedback(self, event: wx.Event):
        """Will call the appropriate message if upload was a success/failure
            and will prompt user if they would like to delete the package
        """
        self.busy_info = None

        if event.response['status'] == 'success':
            alert_upload_success(filename=os.path.basename(self.package_path),
                                 host=event.response['host'],
                                 method=event.response['method'])
        else:
            alert_upload_failed(host=event.response['host'],
                                error=event.response['status'])

    def cell_clicked(self, event: wx.grid.GridEvent):
        """Save selected row so the Next button can continue workflow."""
        extract_id = self.grid.GetCellValue(event.Row, 0)
        if extract_id != 'No saved extractions':
            self.next_button.Enable(True)
        self.selected_row = event.Row
        event.Skip()

    def cell_double_clicked(self, event: wx.grid.GridEvent):
        """Continue extract workflow for the row that was double-clicked."""
        extract_id = self.grid.GetCellValue(event.Row, 0)
        if extract_id != 'No saved extractions':
            self.next_button.Enable(True)
        request_id = self.grid.GetCellValue(event.Row, 1)
        connector = self.grid.GetCellValue(event.Row, 4)
        ecf_path = self.grid.GetCellValue(event.Row, 5)
        self._parse_grid_row_data(extract_id, request_id, connector, ecf_path)

    def _parse_grid_row_data(self, extract_id: str, request_id: str,
                             connector: str, ecf_path: str):
        """Attempt to continue an extraction from a row of data in Grid."""
        if extract_id == 'No saved extractions':
            return  # No action needed

        if not os.path.exists(ecf_path):
            # Original ECF deleted, prompt user to unzip ECF from package
            if not self.prompt_to_restore_ecf(ecf_path):
                return  # User elects not to restore from package

            # Locate the .zip package of data on local disk
            working_dir = self.parent.configs["working_directory"]
            request_dir = os.path.join(working_dir, request_id)
            package_path = os.path.join(request_dir, _package_name(extract_id))

            if not os.path.exists(package_path):
                # Package does not exist, user must restart
                message = (
                    'Could not find data package to continue at "{}". '
                    'Please restart this extraction.'
                    ).format(package_path)
                wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
                return

            # Unzip ECF from the data package and move it to old location
            common.unzip_package(package_path, request_dir, filetype='.ecf')
            saved_ecf = os.path.join(request_dir, os.path.basename(ecf_path))
            os.rename(saved_ecf, ecf_path)

        # If continuing an ABAP extraction, begin second part of that
        # workflow in a different dialog window.
        if "ABAP" in connector:
            self.parent.configs["ecf_file_path"] = ecf_path
            self.EndModal(0)
            self.parent.dialogs['ecf'].filepicker.SetPath(ecf_path)
            self.parent.dialogs['abap'].extract_id = extract_id
            self.parent.dialogs['abap'].ecf_request_id = request_id
            self.parent.dialogs['abap'].ecf_path = ecf_path
            self.parent.begin_extract_workflow(extract_id=extract_id,
                                               return_code=9)  # ABAP
            return

        # Begin parsing the ECF file
        self.parent.busy_info = wx.BusyInfo("Processing ECF...")
        thread = ParseECFThread(self.parent, ecf_path)
        thread.start()

        # Leave this modal and go to pre-filled 'ECF Selection' dialogs
        self.parent.continuing_extraction = True
        self.parent.configs["ecf_file_path"] = ecf_path
        self.EndModal(0)
        self.parent.dialogs['extraction'].is_paused = True
        self.parent.dialogs['ecf'].filepicker.SetPath(ecf_path)
        self.parent.begin_extract_workflow(extract_id=extract_id)

    def prompt_to_restore_ecf(self, ecf_path: str):
        """Prompt user to restore deleted ECF file from a saved data package."""
        message = (
            'Could not find original ECF at path "{}". '
            'Would you like to restore this file from saved data package?'
            ).format(ecf_path)
        dialog = wx.MessageDialog(self, message, 'Info',
                                  style=wx.YES_NO|wx.ICON_INFORMATION)
        answer = dialog.ShowModal()
        return bool(answer == wx.ID_YES)

    def next_button_pressed(self, event: wx.CommandEvent):
        """When the 'Next' button is pressed, start extracting from current row."""
        if self.selected_row is None:
            return
        extract_id = self.grid.GetCellValue(self.selected_row, 0)
        request_id = self.grid.GetCellValue(self.selected_row, 1)
        connector = self.grid.GetCellValue(self.selected_row, 4)
        ecf_path = self.grid.GetCellValue(self.selected_row, 5)
        self._parse_grid_row_data(extract_id, request_id, connector, ecf_path)

    def cancel_button_pressed(self, event: wx.Event):
        """End the dialog modal and return to the home page."""
        self.EndModal(0)

    def init_grid_panel(self):
        """Create the grid of ECFs/Extracts available to Continue from."""
        self.grid_panel.DestroyChildren()
        self.grid = wx.grid.Grid(self.grid_panel)

        # Create the grid to display saved extraction data
        saved_data = self.config_db.all_saved_extract_data()
        if saved_data:
            num_rows = len(saved_data)
        else:
            num_rows = 1
        num_columns = len(ConfigDatabase.ecf_cols) - 1
        self.grid.CreateGrid(num_rows, num_columns)

        self.grid.EnableEditing(False)
        self.grid.EnableGridLines(True)
        self.grid.SetGridLineColour(wx.SystemSettings.GetColour(wx.SYS_COLOUR_3DDKSHADOW))
        self.grid.EnableDragGridSize(False)
        self.grid.SetMargins(0, 0)
        self.grid.SetSelectionMode(wx.grid.Grid.GridSelectRows)

        # Columns
        self.grid.SetColSize(0, 280)
        self.grid.SetColSize(1, 280)
        self.grid.SetColSize(2, 180)
        self.grid.SetColSize(3, 200)
        self.grid.SetColSize(4, 200)
        self.grid.SetColSize(5, 500)
        self.grid.EnableDragColMove(False)
        self.grid.EnableDragColSize(True)
        self.grid.SetColLabelSize(25)
        self.grid.SetColLabelValue(0, 'Extraction ID')
        self.grid.SetColLabelValue(1, 'Request ID')
        self.grid.SetColLabelValue(2, 'Started On')
        self.grid.SetColLabelValue(3, 'Data Server')
        self.grid.SetColLabelValue(4, 'Data Connector')
        self.grid.SetColLabelValue(5, 'ECF File Path')
        self.grid.SetColLabelAlignment(wx.ALIGN_LEFT, wx.ALIGN_BOTTOM)

        # Rows
        self.grid.EnableDragRowSize(True)
        self.grid.SetRowLabelSize(50)
        self.grid.SetRowLabelAlignment(wx.ALIGN_CENTRE, wx.ALIGN_CENTRE)

        # Sizer to fit grid
        grid_sizer = wx.BoxSizer(wx.VERTICAL)

        # Cell Defaults
        self.grid.SetDefaultCellAlignment(wx.ALIGN_LEFT, wx.ALIGN_TOP)
        grid_sizer.Add(self.grid, 1, wx.ALL|wx.EXPAND, 5)

        # Cell Data
        if saved_data:
            self.delete_button.Enable()
            for row, each_query in enumerate(saved_data):
                for col, value in enumerate(each_query):
                    if col == 2:  # Started On Date
                        # Convert saved ISO datetime into human readable
                        dtval = datetime.strptime(value.split('.')[0], "%Y-%m-%dT%H:%M:%S")
                        value = datetime.strftime(dtval, '%Y-%m-%d @ %I:%M%p').lower()
                    self.grid.SetCellValue(row, col, value)
        else:
            self.delete_button.Disable()
            self.grid.SetCellValue(0, 0, 'No saved extractions')

        self.grid_panel.SetSizer(grid_sizer)
        self.grid_panel.Layout()
        grid_sizer.Fit(self.grid_panel)

        self.grid.Bind(wx.grid.EVT_GRID_CELL_LEFT_CLICK, self.cell_clicked)
        self.grid.Bind(wx.grid.EVT_GRID_CELL_LEFT_DCLICK,
                       self.cell_double_clicked)

        if self.grid.GetCellValue(0, 0) == 'No saved extractions':
            self.next_button.Disable()


class ABAPOutputSelectionDialog(DefaultDialog):
    """A dialog for selecting output for second phase of ABAP extraction."""

    def __init__(self, parent):
        """Return a new instance of this dialog window."""
        super().__init__(parent)

        self.extract_id = None
        self.ecf_request_id = None
        self.ecf_path = None
        self.busy_info = None  # type: wx.BusyInfo
        self.messenger = None

        self.sizer = wx.BoxSizer(wx.VERTICAL)

        title = wx.StaticText(self, label="Select PwC ABAP Output")
        title.SetFont(FONT_TITLES)
        self.sizer.Add(title, 0, wx.EXPAND|wx.ALL, 10)

        line = wx.StaticLine(self)
        self.sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        self.content_panel = wx.Panel(self)
        content_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.main_panel = MainPanel(self.content_panel)
        main_panel_sizer = wx.BoxSizer(wx.VERTICAL)

        content = (
            "The PwC-XTRACT program is an Advanced Business Application "
            "Programming (\"ABAP\") report, which extracts "
            "SAP-System-resident (not ar-chived) SAP tables in output "
            "files on the SAP application server.\n\n"
            "The PwC-XTRACT program is complete.  The output files will "
            "need to be transferred using FTP from the SAP application server onto the "
            "local system where PwC Extract is installed. \n\n"
            "Browse to the folder where the PwC-XTRACT output files are "
            "located and click \"Next.\""
        )
        description = wx.StaticText(self.main_panel, label=content,
                                    size=wx.Size(-1, 125))
        description.Wrap(-1)
        description.SetMinSize(wx.Size(-1, 125))
        description.SetMaxSize(wx.Size(-1, 125))

        main_panel_sizer.Add(description, 0, wx.ALL, 5)

        folder_panel = wx.Panel(self.main_panel)
        folder_sizer = wx.BoxSizer(wx.HORIZONTAL)

        folder_label = wx.StaticText(folder_panel, label="Select Folder:")
        folder_label.Wrap(-1)
        folder_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 93, 92))

        folder_sizer.Add(folder_label, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        self.abap_output_dirpicker = wx.DirPickerCtrl(folder_panel,
                                                      message="Select a folder")
        self.abap_output_dirpicker.SetBackgroundColour(wx.Colour(255, 255, 255))

        folder_sizer.Add(self.abap_output_dirpicker, 1, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        folder_panel.SetSizer(folder_sizer)
        folder_panel.Layout()
        folder_sizer.Fit(folder_panel)
        main_panel_sizer.Add(folder_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.main_panel.SetSizer(main_panel_sizer)
        self.main_panel.Layout()
        content_panel_sizer.Add(self.main_panel, 1, wx.EXPAND|wx.RIGHT, 5)

        guide_panel = wx.Panel(self.content_panel, size=wx.Size(235, 625),
                               style=wx.RAISED_BORDER|wx.TAB_TRAVERSAL)
        guide_panel.SetMinSize(wx.Size(235, 625))
        guide_panel.SetMaxSize(wx.Size(235, 625))

        guide_sizer = wx.BoxSizer(wx.VERTICAL)

        guide_label = wx.StaticText(guide_panel, label="Guide")
        guide_label.Wrap(-1)
        guide_label.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 90, 92))

        guide_sizer.Add(guide_label, 0, wx.ALL|wx.ALIGN_CENTER_HORIZONTAL, 5)

        line = wx.StaticLine(guide_panel)
        guide_sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        content = (
            "Click \"Start\" button to begin extraction.  "
            "During extraction, you can temporarily stop the transaction "
            "by clicking the \"Pause\" button.  \n\n"
            "When the extraction is complete, you will have the ability "
            "to transmit the data back to PwC by clicking on the "
            "\"Upload\" button.  \n\n"
            "If you wish to defer upload or need to manually transfer "
            "data back to PwC, simply click on the \"Finish\" button."
        )
        guide_text = wx.StaticText(guide_panel, label=content)
        guide_text.Wrap(-1)
        guide_sizer.Add(guide_text, 1, wx.ALL, 5)

        guide_panel.SetSizer(guide_sizer)
        guide_panel.Layout()
        content_panel_sizer.Add(guide_panel, 1, wx.EXPAND|wx.LEFT, 5)

        self.content_panel.SetSizer(content_panel_sizer)
        self.content_panel.Layout()
        content_panel_sizer.Fit(self.content_panel)
        self.sizer.Add(self.content_panel, 1, wx.EXPAND|wx.ALL, 5)

        nav_panel = wx.Panel(self)
        nav_sizer = wx.BoxSizer(wx.VERTICAL)

        button_panel = wx.Panel(nav_panel)
        button_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.cancel_button = wx.Button(button_panel, wx.ID_ANY, u"Cancel")
        button_sizer.Add(self.cancel_button, 0, wx.ALL, 5)

        self.previous_button = wx.Button(button_panel, wx.ID_ANY, u"Previous")

        button_sizer.Add(self.previous_button, 0, wx.ALL, 5)

        self.next_button = wx.Button(button_panel, wx.ID_ANY, u"Next")
        self.next_button.Enable(False)

        button_sizer.Add(self.next_button, 0, wx.ALL, 5)

        button_panel.SetSizer(button_sizer)
        button_panel.Layout()
        button_sizer.Fit(button_panel)
        nav_sizer.Add(button_panel, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        nav_panel.SetSizer(nav_sizer)
        nav_panel.Layout()
        nav_sizer.Fit(nav_panel)
        self.sizer.Add(nav_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)

        # Connect Events
        self.abap_output_dirpicker.Bind(wx.EVT_DIRPICKER_CHANGED, self.abap_output_changed)
        self.cancel_button.Bind(wx.EVT_BUTTON, self.cancel_button_pressed)
        self.previous_button.Bind(wx.EVT_BUTTON, self.previous_button_pressed)
        self.next_button.Bind(wx.EVT_BUTTON, self.next_button_pressed)
        self.Bind(EVT_ABAPVALID_DONE, self.abap_validation_done)
        self.Bind(wx.EVT_CLOSE, self.parent.confirm_close_extraction)

    def abap_output_changed(self, event):
        """Enable the Next button when text is entered for ABAP output."""
        if self.abap_output_dirpicker.GetPath != "":
            self.next_button.Enable()
        else:
            self.next_button.Disable()

    def cancel_button_pressed(self, event):
        """Return to the Extract home page."""
        self.EndModal(0)

    def previous_button_pressed(self, event):
        """Return to the ECF Selection page."""
        self.EndModal(-1)

    def next_button_pressed(self, event):
        """Validate the ABAP folder selected in a new thread."""
        self.busy_info = wx.BusyInfo("Validating folder selection...")

        if self.abap_output_dirpicker.GetPath() != "":
            thread = ValidateABAPFolderThread(
                self,
                self.abap_output_dirpicker.GetPath(),
                self.extract_id,
                self.ecf_request_id,
                self.ecf_path
            )
            thread.start()

    def abap_validation_done(self, event: wx.Event):
        """Event that occurs after validating an ABAP folder for phase 2."""
        self.busy_info = None
        results = event.response

        if results["status"] == "success":
            self.messenger = results["messenger"]
            self.extract_id = results["extract_id"]
            self.ecf_request_id = results["ecf_request_id"]
            self.ecf_path = results["ecf_path"]
            self.parent.ecf_data = pyextract.read_encrypted_json(results["ecf_path"])
            self.parent.dialogs['connection'] = self
            wx.MessageBox('Please note that ABAP extractions cannot be paused. '
                          'Any paused or canceled extraction will be restarted '
                          'completely when it is resumed.',
                          'Info', style=wx.ICON_INFORMATION)
            self.EndModal(1)
        else:
            self.messenger = results["messenger"]
            # self.extract_id = results["extract_id"]
            # self.ecf_request_id = results["ecf_request_id"]
            # self.ecf_path = results["ecf_path"]
            # self.parent.ecf_data = pyextract.read_encrypted_json(results["ecf_path"])
            self.parent.dialogs['connection'] = self
            wx.MessageBox(results['status'], 'Error', style=wx.ICON_ERROR)
            self.EndModal(2)


ABAPValidEvent, EVT_ABAPVALID_DONE = NewEvent()
class ValidateABAPFolderThread(threading.Thread):
    """Validate a folder of ABAP *.fil files in a new thread."""

    def __init__(self, parent: wx.Frame, folder_path: str,
                 extract_id: str, ecf_request_id: str, ecf_path: str):
        """Return a new instance of the dialog window."""
        super().__init__()
        self.parent = parent
        self.folder_path = folder_path
        self.extract_id = extract_id
        self.ecf_request_id = ecf_request_id
        self.ecf_path = ecf_path

    def run(self):
        """Validate an ABAP folder by instantiating a messenger."""
        messenger = ABAPMessenger(
            folder=self.folder_path,
            ecf_requestid=self.ecf_request_id
        )
        try:

            messenger.validate_input()
            response = {
                "status": "success",
                "messenger": messenger,
                "extract_id": self.extract_id,
                "ecf_request_id": self.ecf_request_id,
                "ecf_path": self.ecf_path,
            }
        except Exception as error:
            response = {
                "messenger": messenger,
                "status": str(error),
            }

        event = ABAPValidEvent()
        event.response = response
        wx.PostEvent(self.parent, event)


GetConnectionDoneEvent, EVT_GETCONNECTION_DONE = NewEvent()
class GetConnectionThread(threading.Thread):
    """Establish connection to a database in a new thread."""

    def __init__(self, parent, data_server, connection_type, connection_args):
        """Return a new instance of the dialog window."""
        super().__init__()
        self.parent = parent
        self.data_server = data_server
        self.connection_type = connection_type
        self.connection_args = connection_args

    def run(self):
        """Try to instantiate a Messenger object, posting status afterward."""

        # Does ECF use Data Services?
        uses_ds = False
        for query in self.parent.parent.ecf_data["Queries"]:
            if "FunctionModule" not in query:
                uses_ds = True
                break

        # Does ECF use BBP?
        uses_bbp = False
        for query in self.parent.parent.ecf_data["Queries"]:
            if "FunctionModule" in query and query["FunctionModule"] == "BBP_RFC_READ_TABLE":
                uses_bbp = True
                break

        try:
            if self.data_server == "SAP Application Server":

                # Confirm required function modules exist and access is gravy.  Return the
                # function module.

                fms = []
                if uses_ds:
                    fms += ['/SAPDS/RFC_READ_TABLE2', '/BODS/RFC_READ_TABLE2']
                if uses_bbp:
                    fms += ['BBP_RFC_READ_TABLE']

                function_module, exc_msg = pyextract.connect.sap.check_func_mod_auths(
                        self.connection_type,
                        self.connection_args,
                        fms
                    )
                messenger = pyextract.connect.sap.SAPMessenger(
                    connection_type=self.connection_type,
                    logon_details=self.connection_args,
                    function_module=function_module
                )

                # There are no usable function modules.
                if not function_module:
                    status = "Error"
                    messenger = None

                elif exc_msg:
                    status = "Warning"
                    messenger = pyextract.connect.sap.SAPMessenger(
                        connection_type=self.connection_type,
                        logon_details=self.connection_args,
                        function_module=function_module
                    )
                else:
                    status = "success"

                response = {
                    "status": status,
                    "message": exc_msg,
                    "connection_type": self.connection_type,
                    "messenger": messenger,
                    "connection_args": self.connection_args
                }
            elif self.data_server == "Oracle RDBMS":
                messenger = pyextract.connect.oracle.OracleMessenger(
                    **self.connection_args
                )
                response = {
                    "status": "success",
                    "message": None,
                    "connection_type": self.connection_type,
                    "messenger": messenger,
                    "connection_args": self.connection_args
                }
            elif self.data_server == "MSSQL RDBMS":
                messenger = pyextract.connect.mssql.MSSQLMessenger(
                    **self.connection_args
                )
                messenger.validate_schema()
                response = {
                    "status": "success",
                    "message": None,
                    "connection_type": self.connection_type,
                    "messenger": messenger,
                    "connection_args": self.connection_args
                }
            elif self.data_server == "DB2 RDBMS":
                messenger = pyextract.connect.db2.DB2Messenger(
                    **self.connection_args
                )
                response = {
                    "status": "success",
                    "message": None,
                    "connection_type": self.connection_type,
                    "messenger": messenger,
                    "connection_args": self.connection_args
                }
            elif self.data_server == "MYSQL RDBMS":
                messenger = pyextract.connect.mysql.MySQLMessenger(
                    **self.connection_args
                )
                response = {
                    "status": "success",
                    "message": None,
                    "connection_type": self.connection_type,
                    "messenger": messenger,
                    "connection_args": self.connection_args
                }
        except Exception as error:
            response = {
                "status": "Error",
                "message": clean_connection_error(traceback.format_exc()),
            }

        event = GetConnectionDoneEvent()
        event.response = response
        wx.PostEvent(self.parent, event)


FirstHundredDoneEvent, EVT_FIRSTHUNDRED_DONE = NewEvent()
class FirstHundredThread(threading.Thread):
    """Thread to pull a small sample of data for Content Preview."""

    def __init__(self, parent: wx.Dialog, messenger: ABCMessenger,
                 ecf_data: pyextract.ecfreader.ExtractData,
                 server: str, connector: str, table_alias: str):
        """Return a new thread to preview data."""
        super().__init__()
        self.parent = parent
        self.messenger = messenger
        self.ecf_data = ecf_data
        self.server = server
        self.connector = connector
        self.table_alias = table_alias

    def run(self):
        """Extract data using a messenger and post response event when done."""
        try:
            # Convert ExtractData query text into DataDefinition object
            metadata = pyextract.DataDefinition(
                parameters=self.ecf_data.query_text,
                source=self.messenger,
                ecf_data=self.ecf_data
            )

            if self.server in ("Oracle RDBMS", "SQL RDBMS", "DB2 RDBMS", "MYSQL RDBMS"):
                self.messenger.begin_extraction(metadata, chunk_size=50)
                orig_data = self.messenger.continue_extraction(50)
                self.messenger.finish_extraction()
                columns = self.messenger.get_metadata_from_query(metadata.parameters)
                col_labels = [each["sourceFieldName"] for each in columns]
            elif self.server == "SAP Application Server" and self.connector == "RFC":
                table = metadata.parameters["Name"]
                col_labels = metadata.parameters["Columns"]
                orig_data = self.messenger.single_readtable(
                    table=table, columns=col_labels, package_size=50,
                )
            else:
                raise ValueError(
                    'Content preview not available for server "{}" '
                    'and connector "{}"'
                    .format(self.server, self.connector)
                )
        except Exception as error:
            response = {
                "status": (
                    'There was an issue obtaining data for a preview. '
                    'Please check your network connection. '
                    'If the problem persists please contact PwC. '
                    ' Error: {}'
                    ).format(error),
            }
        else:
            # Convert Everything to str and combine into response JSON
            data = []
            for row in orig_data:
                row_data = []
                for col in row:
                    if col is None:
                        col = 'Null'
                    row_data.append(str(col))
                data.append(row_data)

            response = {
                "status": "success",
                "data": data,
                "col_labels": col_labels,
                "table_alias": self.table_alias,
            }

        # Post event with success or error data back to the main thread
        event = FirstHundredDoneEvent()
        event.response = response
        wx.PostEvent(self.parent, event)


ExtractionDoneEvent, EVT_EXTRACTION_DONE = NewEvent()
class ExtractThread(threading.Thread):
    """A thread that runs the primary data extraction routine."""

    def __init__(self, parent: wx.Dialog, progress_bar: wx.Gauge,
                 resume_extract: bool, source, callback_error,
                 chunk_results, chunk_size, ecf_file, output_folder,
                 package_name, queue_size, row_limit, sqlite_password,
                 stopevent, worker_timeout, sap_batch_size, max_readers):
        """Return a new thread ready to run an extraction."""
        super().__init__()
        self.parent = parent
        self.progress_bar = progress_bar
        self.resume_extract = resume_extract
        self.source = source
        self.callback_error = callback_error
        self.chunk_results = chunk_results
        self.chunk_size = chunk_size
        self.ecf_file = ecf_file
        self.output_folder = output_folder
        self.package_name = package_name
        self.queue_size = queue_size
        self.row_limit = row_limit
        self.sqlite_password = sqlite_password
        self.stopevent = stopevent
        self.max_readers = max_readers
        self.worker_timeout = worker_timeout
        self.sap_batch_size = sap_batch_size

    def run(self):
        """Run the main extraction function, then post a 'Done' event."""
        delete_old_extract_databases(self.output_folder)

        package_path = os.path.join(self.output_folder, self.package_name)
        if self.resume_extract:
            if not os.path.exists(package_path):
                LOGGER.warning('Data package to continue was not found at "%s".',
                               package_path)
                LOGGER.warning('Extraction will be restarted completely.')
            else:
                LOGGER.info('Restoring data and logs from saved extraction.')
                common.unzip_package(package_path, self.output_folder, filetype='.dat')
                common.unzip_package(package_path, self.output_folder, filetype='.log')

        # Translate the unique GUI password into a SQLite messenger
        output = common._messenger_from_password(self.sqlite_password,
                                          self.output_folder)

        # Create a stream object to manage data flow from the source Messenger

        if 'SAPMessenger' in str(type(self.source)):
            stream = pyextract.streams.sapstream.SAPStream(
                messenger=self.source,
                batch_size=self.sap_batch_size,
                chunk_size=self.chunk_size,
                queue_size=self.queue_size,
                max_readers=self.max_readers,
                row_limit=self.row_limit,
                stopevent=self.stopevent,
                chunk_results=self.chunk_results,
                output=output,
            )
        else:
            ecf_ed = pyextract.ecfreader.get_ecf_meta_data(self.ecf_file)
            if len(ecf_ed) > 0:
                if isinstance(ecf_ed[0].query_text, dict):

                    stream = pyextract.ODBCStream(
                        messenger=self.source,
                        batch_size=self.sap_batch_size,
                        chunk_size=self.chunk_size,
                        queue_size=self.queue_size,
                        max_readers=self.max_readers,
                        row_limit=self.row_limit,
                        stopevent=self.stopevent,
                        chunk_results=self.chunk_results,
                        output=output,
                    )
                else:
                    stream = pyextract.DataStream(
                        messenger=self.source,
                        chunk_size=self.chunk_size,
                        queue_size=self.queue_size,
                        row_limit=self.row_limit,
                        stopevent=self.stopevent,
                    )
            else:
                stream = None

        LOGGER.info("PX Version: v{}".format(version.EXTRACT_VERSION))
        LOGGER.info("Active Configuration: {}".format(self.parent.parent.configs))

        # Extract data (using previous paused data if provided)
        errors, warnings = _run_extraction(stream=stream,
                        output=output,
                        resume_extract=self.resume_extract,
                        gauge=self.progress_bar,
                        output_folder=self.output_folder,
                        package_name=self.package_name,
                        worker_timeout=self.worker_timeout,
                        ecf_file=self.ecf_file,
                        callback_error=self.callback_error,
                        chunk_results=self.chunk_results,
                        sqlite_password=self.sqlite_password)

        # Post an event after extraction is complete
        event = ExtractionDoneEvent()
        event.package_path = package_path
        event.errors = errors
        event.warnings = warnings
        wx.PostEvent(self.parent, event)


ECFParseDoneEvent, EVT_ECFPARSE_DONE = NewEvent()
class ParseECFThread(threading.Thread):
    """A separate thread in which to parse an encrypted ECF file."""

    def __init__(self, parent: wx.Frame, filepath: str):
        """Return a thread that will parse the ECF at filepath."""
        super().__init__()
        self.parent = parent
        self.filepath = filepath

    def run(self):
        """To to parse an ECF, raising an error if a failure occurs."""
        try:
            orig_ecf = pyextract.read_encrypted_json(self.filepath)
            ecf_meta_data = pyextract.get_ecf_meta_data(self.filepath)
        except Exception as error:
            response = {
                "status": (str(error) + ' Please select a valid ECF file '
                           'to navigate to the next step'),
            }
        else:
            response = {
                "status": "success",
                "orig_ecf": orig_ecf,
                "ecf_meta_data": ecf_meta_data
            }

        event = ECFParseDoneEvent()
        event.response = response
        wx.PostEvent(self.parent, event)


QueryValidateDoneEvent, EVT_QUERY_VALIDATE = NewEvent()
class ValidateQueriesThread(threading.Thread):
    """A separate thread to validate queries from an ECF file."""

    def __init__(self, parent: wx.Frame, ecf_meta_data: list,
                 data_server: str, data_connector: str,
                 messenger: ABCMessenger):
        """Return a thread that will parse the ECF at filepath."""
        super().__init__()
        self.parent = parent
        self.ecf_meta_data = ecf_meta_data
        self.data_server = data_server
        self.data_connector = data_connector
        self.messenger = messenger

    def run(self):
        """Check all queries in this ECF for errors with sample data pulls."""
        try:
            queries, errors = get_content_preview(
                self.ecf_meta_data,
                self.data_server,
                self.data_connector,
                self.messenger
            )
        except Exception as error:
            response = {
                "status": str(error),
            }
        else:
            response = {
                "status": "success",
                "queries": queries,
                "errors": errors,
                "data_server": self.data_server,
            }

        event = QueryValidateDoneEvent()
        event.response = response
        wx.PostEvent(self.parent, event)


UploadDoneEvent, EVT_UPLOAD_DONE = NewEvent()
class UploadPackageThread(threading.Thread):
    """A separate thread to upload a completed extraction package"""

    def __init__(self, parent: wx.Frame, upload_method: str,
                 sftp_location: str, lfu_location: str, sftp_port: str, rename_wait: str, package_path=None):
        super().__init__()
        self.parent = parent
        self.upload_method = upload_method
        self.sftp_location = sftp_location
        self.lfu_location = lfu_location
        self.sftp_port = int(sftp_port)
        self.rename_wait = int(rename_wait)
        if package_path:
            self.package_path = package_path
        else:
            self.package_path = self.parent.package_path

    def run(self):
        """Try uploading data package to SFTP (MFT) and/or LFU locations."""

        # Default response if neither MFT_LFU or LFU methods are selected
        response = {'status': 'Invalid upload method selected'}

        if self.upload_method == 'MFT_LFU':
            response = self.try_upload_sftp()

        if response['status'] != 'success':
            LOGGER.info("Upload via SFTP failed due to {}".format(response['status']))

        mft_upload_failed = (self.upload_method == 'MFT_LFU' and
                             response['status'] != 'success')

        # Save friendly name of upload path that succeeded for response
        ultimate_method = 'SFTP'

        if self.upload_method == 'LFU' or mft_upload_failed:
            response = self.try_upload_lfu()
            ultimate_method = 'LFU'
            if response['status'] != 'success':
                LOGGER.info("Upload via HTTPS failed due to {}".format(response['status']))

        response['method'] = ultimate_method

        event = UploadDoneEvent()
        event.response = response
        wx.PostEvent(self.parent, event)

    def try_upload_sftp(self) -> dict:
        """Upload to PwC using the SFTP method"""
        creds = config.SFTP_UPLOAD_LOCATIONS[self.sftp_location]
        LOGGER.info('Attempting upload to host "%s", folder "%s" using SFTP',
                    creds['HOSTADDRESS'], creds['CURRDIRECTORY'])
        try:
            client = pyextract.SFTPClient(creds=creds, port=self.sftp_port, rename_wait=self.rename_wait)
            client.send(self.package_path)
        except Exception as error:
            LOGGER.error(error)
            response = {
                "status": error,
                "host": creds['HOSTADDRESS'],
            }
        else:
            response = {
                "status": "success",
                "host": creds['HOSTADDRESS'],
            }
        return response

    def try_upload_lfu(self) -> dict:
        """Upload to PwC using the LFU REST API service"""
        kwargs = config.LFU_UPLOAD_LOCATIONS[self.lfu_location]
        LOGGER.info('Attempting LFU upload to host "%s"', kwargs['host'])
        try:
            client = pyextract.LFUClient(**kwargs)
            client.send(self.package_path, chunk_size=1000000)
        except Exception as error:
            response = {
                "status": error,
                "host": kwargs['host'],
            }
        else:
            response = {
                "status": "success",
                "host": kwargs['host'],
            }
        return response


class PyExtract(wx.Frame):
    """Top-level parent object that controls the entire GUI."""

    def __init__(self, parent=None):
        """Return a new instance of the GUI with all components built."""
        super().__init__(parent, style=wx.DEFAULT_FRAME_STYLE|wx.TAB_TRAVERSAL)
        self.SetIcon(_extract_icon())
        self.SetSizeHints(wx.DefaultSize, wx.DefaultSize)
        self.SetBackgroundColour(wx.Colour(255, 255, 255))
        self.SetTitle('Extract')
        self.SetSize(826, 629)

        # Primary attributes
        self.stopevent = threading.Event()
        self.config_db = ConfigDatabase()
        self.configs = {}  # type: Dict[str, str]
        self.dialogs = {}  # type: Dict[str, wx.Dialog]
        self.continuing_extraction = False
        self.extract_id = None
        self.busy_info = None  # type: wx.BusyInfo
        self.data_connector = None  # type: str
        self.data_server = None  # type: str
        self.ecf_data = None
        self.ecf_meta_data = None

        # GUI objects
        self.sizer = wx.BoxSizer(wx.VERTICAL)
        self.main_panel = wx.Panel(self)
        main_panel_sizer = wx.BoxSizer(wx.VERTICAL)

        # Logo
        bitmap = wx.Image(_resource_path('assets/pwc-logo.png')).ConvertToBitmap()
        logo = wx.StaticBitmap(self.main_panel, label=bitmap,
                               size=wx.Size(-1, 225))
        main_panel_sizer.Add(logo, 1, wx.ALL|wx.ALIGN_CENTER_HORIZONTAL, 15)

        # Title
        title = wx.StaticText(self.main_panel, label="Extract")
        title.SetFont(wx.Font(
            wx.FontInfo(26).FaceName('Georgia').Bold().Italic()
        ))
        main_panel_sizer.Add(title, 0, wx.ALL|wx.ALIGN_CENTER_HORIZONTAL, 5)

        menu_btn_panel = wx.Panel(self.main_panel)
        menu_btn_panel_sizer = wx.BoxSizer(wx.HORIZONTAL)

        button_size = wx.Size(125, -1)
        self.open_ecf_button = wx.Button(menu_btn_panel, label='New Extraction',
                                         size=button_size)
        menu_btn_panel_sizer.Add(self.open_ecf_button, 0, wx.ALL, 5)

        self.cont_extraction_button = wx.Button(menu_btn_panel,
                                                label='Continue Extraction',
                                                size=button_size)
        menu_btn_panel_sizer.Add(self.cont_extraction_button, 0, wx.ALL, 5)

        menu_btn_panel.SetSizer(menu_btn_panel_sizer)
        menu_btn_panel.Layout()
        menu_btn_panel_sizer.Fit(menu_btn_panel)
        main_panel_sizer.Add(menu_btn_panel, 0, wx.ALL|wx.ALIGN_CENTER_HORIZONTAL, 5)

        self.main_panel.SetSizer(main_panel_sizer)
        self.main_panel.Layout()
        main_panel_sizer.Fit(self.main_panel)
        self.sizer.Add(self.main_panel, 1, wx.EXPAND|wx.ALL, 5)

        # Panel at bottom with [Configs] button and Version #
        bottom_panel = wx.Panel(self)
        bottom_sizer = wx.BoxSizer(wx.HORIZONTAL)

        # Container to align [Config] button on bottom-left of screen
        config_btn_aligner = wx.Panel(bottom_panel)
        config_btn_align_sizer = wx.BoxSizer(wx.VERTICAL)

        # Container for the [Config] button
        configs_panel = wx.Panel(config_btn_aligner)
        configs_sizer = wx.BoxSizer(wx.HORIZONTAL)

        self.configs_button = wx.Button(configs_panel, label="Configs")
        configs_sizer.Add(self.configs_button, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        configs_panel.SetSizer(configs_sizer)
        configs_panel.Layout()
        configs_sizer.Fit(configs_panel)
        config_btn_align_sizer.Add(configs_panel, 1, wx.EXPAND|wx.ALL, 5)

        config_btn_aligner.SetSizer(config_btn_align_sizer)
        config_btn_aligner.Layout()
        config_btn_align_sizer.Fit(config_btn_aligner)
        bottom_sizer.Add(config_btn_aligner, 0, wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        # Panel to align Version # in bottom-right of screen
        version_align_panel = wx.Panel(bottom_panel)
        version_align_sizer = wx.BoxSizer(wx.VERTICAL)

        # Version box to vertically align text within (invisible)
        version_box = wx.Panel(version_align_panel)
        version_box_sizer = wx.BoxSizer(wx.VERTICAL)

        # Version Text
        version_panel = wx.Panel(version_box)
        version_sizer = wx.BoxSizer(wx.VERTICAL)

        version_text = "Extract v{}".format(version.EXTRACT_VERSION)

        version_label = wx.StaticText(version_panel, label=version_text)
        version_label.Wrap(-1)
        version_label.SetFont(FONT_BOLD)

        version_sizer.Add(version_label, 0, wx.ALL|wx.ALIGN_RIGHT, 5)

        version_panel.SetSizer(version_sizer)
        version_panel.Layout()
        version_sizer.Fit(version_panel)
        version_box_sizer.Add(version_panel, 1,
                              wx.EXPAND|wx.ALL|wx.ALIGN_RIGHT, 5)

        version_box.SetSizer(version_box_sizer)
        version_box.Layout()
        version_box_sizer.Fit(version_box)
        version_align_sizer.Add(version_box, 0,
                                wx.ALL|wx.ALIGN_RIGHT|wx.ALIGN_CENTER_VERTICAL, 5)

        version_align_panel.SetSizer(version_align_sizer)
        version_align_panel.Layout()
        version_align_sizer.Fit(version_align_panel)
        bottom_sizer.Add(version_align_panel, 1,
                         wx.EXPAND|wx.ALL|wx.ALIGN_CENTER_VERTICAL, 5)

        bottom_panel.SetSizer(bottom_sizer)
        bottom_panel.Layout()
        bottom_sizer.Fit(bottom_panel)
        self.sizer.Add(bottom_panel, 0, wx.ALL|wx.ALIGN_RIGHT|wx.EXPAND, 5)

        # End of bottom panel, Beginning of legal text
        line = wx.StaticLine(self)
        self.sizer.Add(line, 0, wx.EXPAND|wx.ALL, 5)

        legal_panel = wx.Panel(self)
        legal_sizer = wx.BoxSizer(wx.VERTICAL)

        header_text = '- Property of PricewaterhouseCoopers LLP -'
        header = wx.StaticText(legal_panel, wx.ID_ANY, header_text,
                               wx.DefaultPosition, wx.DefaultSize, wx.ALIGN_CENTRE)
        header.Wrap(-1)
        header.SetFont(wx.Font(wx.NORMAL_FONT.GetPointSize(), 70, 94, 92))
        legal_sizer.Add(header, 0, wx.ALL|wx.EXPAND, 5)

        disclaimer_text = (''.join([
                "Copyright {0} 2017 PricewaterhouseCoopers LLP. All rights reserved. ".format(chr(194)+chr(169)),
                "PricewaterhouseCoopers LLP refers to the US member firm or ",
                "one of its subsidiaries or affiliates, and may sometimes refer ",
                "to the PwC network. Each member firm is a separate legal ",
                "entity. Please see http://www.pwc.com/structure for details."
            ])
        )
        disclaimer = wx.StaticText(legal_panel, label=disclaimer_text,
                                   style=wx.ALIGN_CENTRE)
        disclaimer.SetMinSize(wx.Size(820, 50))
        disclaimer.SetMaxSize(wx.Size(820, 50))
        legal_sizer.Add(disclaimer, 0, wx.ALL|wx.EXPAND|wx.CENTER, 5)

        legal_panel.SetSizer(legal_sizer)
        legal_panel.Layout()
        legal_sizer.Fit(legal_panel)
        self.sizer.Add(legal_panel, 0, wx.EXPAND|wx.ALL, 5)

        self.SetSizer(self.sizer)
        self.Layout()
        self.Centre(wx.BOTH)

        # Init Pages
        self.init_dialogs()

        # Connect Events
        self.open_ecf_button.Bind(wx.EVT_BUTTON, self.prompt_for_ecf_file)
        self.cont_extraction_button.Bind(wx.EVT_BUTTON, self.cont_extraction_button_pressed)
        self.configs_button.Bind(wx.EVT_BUTTON, self.show_configs_dialog)
        self.Bind(EVT_ECFPARSE_DONE, self.ecf_parse_done)

        # Show the app and load user configs from the SQLite database
        self.Show()
        self.load_user_configs()
        self.load_user_dependencies()
        error = validate_folder_access(self.configs['working_directory'],
                                       read_only=False)
        if error:
            message = 'Failed to access user-configured Working Directory. ' + error
            wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
            self.dialogs['config'].ShowModal()

    def load_user_configs(self):
        """Load user config (or defaults), create Working Directory on disk."""
        self.config_db.set_default_user_configs()
        self.configs.update(**self.config_db.get_config_dict())
        update_logging(self.configs['log_level'])

    def load_user_dependencies(self):
        """Update the PATH env variable to include all user-provided deps."""
        sap_folder = self.configs.get('sap_sdk_folder')
        if sap_folder:
            pyextract.utils.update_path(sap_folder)

        oracle_folder = self.configs.get('oracle_client_folder')
        if oracle_folder:
            pyextract.utils.update_path(oracle_folder)

        ibm_folder = self.configs.get('ibm_dll_folder')
        if ibm_folder:
            os.environ['PWC_IBM_DLL'] = ibm_folder
            pyextract.utils.update_path(os.path.join(ibm_folder, 'clidriver', 'bin'))

    def init_dialogs(self):
        """Instantiate dialog boxes that will be shown during the app."""
        self.dialogs = {
            'abap': ABAPOutputSelectionDialog(self),
            'config': ConfigsDialog(self),
            'connection': None,  # type: BaseConnectionDialog
            'content_preview': ContentPreviewDialog(self),
            'continue_extract': ContinueExtractionDialog(self),
            'db2': DB2ConnectionDialog(self),
            'mysql': MySQLConnectionDialog(self),
            'ecf': ECFSelectionDialog(self),
            'extraction': ExtractionDialog(self),
            'manage_conn': ManageSavedConnDialog(self),
            'mssql': MSSQLConnectionDialog(self),
            'oracle': OracleConnectionDialog(self),
            'sap': SAPConnectionDialog(self),
        }

    def validate_ecf(self, filepath: str):
        """Begin parsing an ECF when selected from the ECFSelectionDialog."""
        self.busy_info = wx.BusyInfo("Processing ECF...")
        self.configs["ecf_file_path"] = filepath
        thread = ParseECFThread(self, self.configs["ecf_file_path"])
        thread.start()

    def ecf_parse_done(self, event: wx.Event):
        """Event that fires after a thread has parsed an ECF file."""
        # If ECF parse failed, alert the user and take no further action
        if event.response["status"] != "success":
            self.busy_info = None
            wx.MessageBox(event.response["status"],
                          'Error', style=wx.ICON_ERROR)
            self.dialogs['ecf'].next_button.Disable()
            return

        # Get ECF Data from the event sent by ECF Parsing thread
        self.ecf_data = event.response["orig_ecf"]
        self.ecf_meta_data = event.response["ecf_meta_data"]

        if not self.continuing_extraction:
            # If the RequestID for this ECF has already been started during
            # a previous extraction, prompt the user to use that data
            request_id = self.ecf_data["RequestId"]
            if self.config_db.saved_request_exists(request_id):
                self.busy_info = None
                message = (
                    'Some data for request "{}" has already been extracted. '
                    'Would you like to continue extraction using that data? '
                    'Select "Yes" to resume from previous data, or "No" to '
                    'start a new extraction.'
                    ).format(request_id)
                dialog = wx.MessageDialog(self, message, 'Info',
                                          style=wx.YES_NO|wx.ICON_INFORMATION)
                answer = dialog.ShowModal()
                if answer == wx.ID_YES:
                    # Exit ECF selection modal and go to 'Continue Extract' page
                    self.dialogs['ecf'].EndModal(3)
                    return

        # Populate ECF Preview
        ecf_info = self.ecf_meta_data[0].ecfjson
        ecf_preview_data = (
            ("Client Name:", ecf_info["ClientName"]),
            ("Extraction Method", ecf_info["ExtractionMethod"]),
            ("Instance Name:", ecf_info["ErpInstance"]),
            ("Application:", ecf_info["DataSource"]["Application"]),
            ("Version:", str(ecf_info["DataSource"]["Version"])),
            ("Data Server:", ecf_info["DataSource"]["DataServer"]),
            ("Data Connector:", ecf_info["DataSource"]["DataConnector"]),
            ("Request Key:", ecf_info["RequestId"]),
            ("ECF Version:", str(ecf_info["EcfVersion"])),
            ("Expiration Date:", ecf_info["ExpiryDate"]),
        )

        self.dialogs['ecf'].refresh_dynamic_panel(content=ecf_preview_data)

        # Determine which Connection Page to Display Next
        data_server = self.ecf_data["DataSource"]["DataServer"]
        data_connector = self.ecf_data["DataSource"]["DataConnector"]
        self.data_server = data_server
        self.data_connector = data_connector

        if data_server == "SAP Application Server":
            erp = 'sap'
            # Set default dialog information for SAP connections
            self.dialogs['sap'].controls['client'].SetValue("")
            self.dialogs['sap'].controls['user'].SetValue("")
            self.dialogs['sap'].controls['password'].SetValue("")
            self.dialogs['sap'].controls['language'].SetValue("EN")
            self.dialogs['sap'].controls['ashost'].SetValue("")
            self.dialogs['sap'].controls['sysnr'].SetValue("")
            self.dialogs['sap'].connection_type_choice.SetStringSelection(data_connector)
            self.dialogs['sap'].Layout()
            self.dialogs['connection'] = self.dialogs['sap']
        elif data_server == "Oracle RDBMS":
            erp = 'oracle'
            # Set default dialog information for Oracle connections
            self.dialogs['oracle'].controls['host'].SetValue("")
            self.dialogs['oracle'].controls['port'].SetValue("")
            self.dialogs['oracle'].controls['orcl_instance_type'].SetSelection(0)
            self.dialogs['oracle'].controls['orcl_instance_value'].SetValue("")
            self.dialogs['oracle'].controls['user'].SetValue("")
            self.dialogs['oracle'].controls['password'].SetValue("")
            self.dialogs['oracle'].connection_type_choice.SetStringSelection(data_connector)
            self.dialogs['oracle'].Layout()
            self.dialogs['connection'] = self.dialogs['oracle']
        elif data_server == "SQL RDBMS":
            erp = 'mssql'
            # Set default dialog information connections
            self.dialogs['mssql'].controls['host'].SetValue("")
            self.dialogs['mssql'].controls['database'].SetValue("")
            self.dialogs['mssql'].connection_type_choice.SetStringSelection(data_connector)
            self.dialogs['mssql'].Layout()
            self.dialogs['connection'] = self.dialogs['mssql']
        elif data_server == "DB2 RDBMS":
            erp = 'db2'
            # Set default dialog information for DB2 connections
            self.dialogs['db2'].controls['host'].SetValue("")
            self.dialogs['db2'].controls['port'].SetValue("")
            self.dialogs['db2'].controls['database'].SetValue("")
            self.dialogs['db2'].controls['user'].SetValue("")
            self.dialogs['db2'].controls['password'].SetValue("")
            self.dialogs['db2'].connection_type_choice.SetStringSelection(data_connector)
            self.dialogs['db2'].Layout()
            self.dialogs['connection'] = self.dialogs['db2']
        elif data_server == "MYSQL RDBMS":
            erp = 'mysql'
            # Set default dialog information for DB2 connections
            self.dialogs['mysql'].controls['host'].SetValue("")
            self.dialogs['mysql'].controls['port'].SetValue("")
            self.dialogs['mysql'].controls['database'].SetValue("")
            self.dialogs['mysql'].controls['user'].SetValue("")
            self.dialogs['mysql'].controls['password'].SetValue("")
            self.dialogs['mysql'].connection_type_choice.SetStringSelection(data_connector)
            self.dialogs['mysql'].Layout()
            self.dialogs['connection'] = self.dialogs['mysql']
        else:
            self.busy_info = None
            msg = (
                'DataServer value must be one of: "SAP Application Server", '
                '"Oracle RDBMS", "SQL RDBMS", "DB2 RDBMS", "MYSQL RDBMS".'
            )
            wx.MessageBox(msg, 'Error', style=wx.ICON_ERROR)
            return

        # Try to load submodules for this connection, return if failure
        try:
            self.load_submodules(erp)
        except DependencyError as error:
            # Alert user of failure and don't save configs
            self.busy_info = None
            wx.MessageBox(error.text, 'Error', style=wx.ICON_ERROR)
            if not self.dialogs['config'].IsShown():
                self.dialogs['config'].ShowModal()
            return
        self.busy_info = None
        self.dialogs['ecf'].next_button.Enable()
        self.dialogs['ecf'].next_button.SetFocus()

    def begin_extract_workflow(self, event: wx.Event=None,
                               extract_id: str = None, return_code=99):
        """Loop through dialog windows to set up and run an extraction.

        ARGS:
            extract_id: If provided, begin this workflow from a previously
                started extraction. Each final package (extraction) gets a
                unique ID separate from the RequestID in the ECF, so that
                multiple extractions can be run from the same ECF.
            return_code: Tracks the exit status of the last window to decide
                which screen the user should be shown next. Will be refactored
                when we get the chance so program operation is less opaque.
        """
        self.Show(False)
        current = "home"  # Track name of current dialog window

        while True:

            # Get the next dialog window to show
            current = next_extract_workflow_dialog(return_code, current)
            if not current:
                # No pages left to show, exit the workflow
                self.dialogs['ecf'].next_button.Disable()
                break
            # If the Extract dialog window is next, create the kwargs
            # needed to run an ExtractThread, and save them to it
            if current == 'extraction':
                # Create a unique ID for this extraction if not provided
                extract_id = extract_id or str(uuid.uuid4())
                self.dialogs[current].extract_kwargs = \
                    self._create_extract_kwargs(extract_id)

            if not self.dialogs[current].IsModal():
                return_code = self.dialogs[current].ShowModal()

        self.init_dialogs()
        self.Show(True)

    def prompt_for_ecf_file(self, event: wx.CommandEvent):
        """Prompt user to select ECF, beginning extract workflow if they do."""
        dialog = wx.FileDialog(self, message="Select the ECF file",
                               wildcard="ECF files (*.ecf)|*.ecf")
        result = dialog.ShowModal()
        if result != wx.ID_OK:
            return  # Use closed dialog, take no action

        filepath = dialog.GetPath()
        if not filepath:
            return  # User did not select a filepath, take no action

        # Set the filepath on the ECF Preview Screen and begin workflow
        self.dialogs['ecf'].filepicker.SetPath(filepath)
        self.begin_extract_workflow()

    def _create_extract_kwargs(self, extract_id: str) -> dict:
        """Return dict of keyword arguments used to run an extraction."""
        # Use the ECF RequestId as a subfolder for output
        ecf_path = self.configs["ecf_file_path"]
        parsed_ecf_data = pyextract.get_ecf_meta_data(ecf_path)
        ecfjson = parsed_ecf_data[0].ecfjson
        request_id = parsed_ecf_data[0].request_id
        package_name = _package_name(extract_id)
        output_folder = os.path.join(self.configs["working_directory"],
                                     request_id)
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        log_fp = os.path.join(output_folder, package_name.lower().replace('package', 'extract').replace(".zip", ".log"))
        pyextract.utils.setup_file_logger(log_fp)
        # output_folder = os.path.join(self.configs["working_directory"],
        #                              request_id, package_name.replace('.zip', ''))



        # Translate encryption selection into a password for core.extract()
        assert self.configs["encryption"] in config.ENCRYPTION_OPTIONS
        if self.configs["encryption"] == 'AES-128':
            password = common._unique_sqlite_password(16)
        elif self.configs["encryption"] == 'AES-256':
            password = common._unique_sqlite_password(32)
        else:
            password = None

        if self.config_db.saved_extract_exists(extract_id):
            password = self.config_db.get_extraction_password(extract_id)[0][0]

        # Set Extraction Dialog's extract_kwargs.
        # NOTE -- 'output' messenger will be created in Thread right before
        # extraction and not saved in the kwargs, because the connection
        # on that messenger will be closed after each .extract() call

        if ecfjson["DataSource"]["DataConnector"] == "RFC":
            chunk_size = int(self.configs["sap_chunk_size"])
        else:
            chunk_size = int(self.configs["chunk_size"])

        extract_kwargs = {
            "source": self.dialogs['connection'].messenger,
            "output_folder": output_folder,
            "package_name": package_name,
            "sap_batch_size": int(self.configs["sap_batch_size"]),
            "chunk_size": chunk_size,
            "queue_size": int(self.configs["queue_size"]),
            "max_readers": int(self.configs["max_readers"]),
            "row_limit": 0,
            "ecf_file": self.configs["ecf_file_path"],
            "callback_error": alert_error,
            "stopevent": self.stopevent,
            "chunk_results": "db_per_table",
            "sqlite_password": password,
        }

        if config.ALLOW_WORKER_TIMEOUT_SETTING:
            # Convert worker_timeout from a user config string to a kwarg
            # Treat a timeout of 0 as None, otherwise workers timeout instantly
            worker_timeout = self.configs['worker_timeout']
            if worker_timeout == '0' or not worker_timeout:
                extract_kwargs['worker_timeout'] = None
            else:
                extract_kwargs['worker_timeout'] = int(worker_timeout)
        else:
            extract_kwargs['worker_timeout'] = None

        # Save this extract request to the local DB so it can be
        # restarted later in case of a pause or failure

        if not self.config_db.saved_extract_exists(extract_id):
            self.config_db.save_extract_request(
                extract_id=extract_id,
                request_id=request_id,
                data_server=ecfjson["DataSource"]["DataServer"],
                data_connector=ecfjson["DataSource"]["DataConnector"],
                file_path=self.configs["ecf_file_path"],
                password=password
            )

        return extract_kwargs

    def cont_extraction_button_pressed(self, event: wx.Event):
        """Display the ContinueExtractionDialog for user to select from."""
        self.dialogs['continue_extract'].ShowModal()

    def show_configs_dialog(self, event: wx.Event):
        """Open dialog where user can view and edit program configuration."""
        self.dialogs['config'].ShowModal()

    def confirm_close_extraction(self, event: wx.CloseEvent):
        """When user tries to close the app, prompt them to confirm."""
        message = 'Are you sure you want to cancel this extraction?'
        dialog = wx.MessageDialog(self, message, 'Info',
                                  style=wx.YES_NO|wx.ICON_INFORMATION)
        answer = dialog.ShowModal()
        if answer == wx.ID_YES:
            event.Skip()  # Continue closing the application
        else:
            event.Veto()

    def load_submodules(self, erp: str) -> bool:
        """Attempt to load submodules from the `pyextract` package.
        Raise a DependencyError if any of the submodules fail to import.
        """
        dependency_names = {
            'sap': 'SAP Netweaver SDK',
            'oracle': 'Oracle Client',
            'db2': 'IBM DB2 Driver',
            # 'mysql': 'MySQL Driver',
            # 'mssql': 'MSSQL ODBC Driver',
        }
        for module in self.dialogs[erp].required_submodules:
            try:
                importlib.import_module('pyextract.' + module)
            except ImportError:
                dependency = dependency_names.get(erp)
                if not dependency:
                    dependency = 'dependencies for "{}"'.format(module)
                message = (
                    'Failed to locate the {dependency} on this machine. '
                    'Please set the location of this folder on the '
                    'User Config screen and ensure all components '
                    'are included in that folder.'
                    ).format(dependency=dependency)
                raise DependencyError(message)


def _extract_icon() -> wx.Icon:
    """Return an Icon object of the PyExtract logo."""
    logo = wx.Image(_resource_path('assets/extract-logo.png'))
    icon = wx.Icon()
    icon.CopyFromBitmap(logo.ConvertToBitmap())
    return icon


def _resource_path(relative_path: str) -> str:
    """Get absolute path to resource, works for dev and for PyInstaller.
    (from http://stackoverflow.com/a/31966932).
    """
    return os.path.join(ROOT, relative_path)


def get_content_preview(ecf_meta_data: list, data_server: str,
                        data_connector: str,
                        messenger: ABCMessenger):
    """Return content preview format (table, alias, queries) of ExtractData.

    ABAP currently has no content preview, SAP is shown as a
    comma-separated list of columns, and all other sources are
    SQL queries shown verbatim.

    RETURNS:
        rows (list): list of tuples (Name, Alias, Query) to display
        as a table in the content preview, or None if in the second
        round of an ABAP extraction (folder of *.fil files) that does
        not rely on the ECF for this section.

        bool: Will return true if any queries will result in an error
    """
    if data_connector == "ABAP":
        return None  # ABAP does not support content preview

    rows = []

    #Calls to pyextract to do a prelim metadata check to ID errors for previews
    tables, errors = pyextract.gui_ecf_validation(ecf_meta_data, messenger)

    for record in ecf_meta_data:
        queries = record.query_text
        if record.table_alias in tables:
            error_message = "ERROR: {}".format(str(errors[tables.index(record.table_alias)]))
            rows += [(record.table_name, record.table_alias, error_message)]
        else:
            if isinstance(queries, dict):
                queries = ",".join(queries["Columns"])
                rows += [(record.table_name, record.table_alias, queries)]
            else:
                rows += [(record.table_name, record.table_alias, record.query_text)]
    #Return a bool here to indicate if a message box should pop up notifying
    #user of errors in ECF

    return rows, tables



def check_disk_space(path: str = None):
    """Alert user if recommended disk space free is not available."""

    if path:
        drive, _ = os.path.splitdrive(path)
    else:
        drive = os.path.abspath(os.sep)

    current_space = round(shutil.disk_usage(drive)[2] / 1024**3, 2)

    if current_space < config.RECOMMENDED_SPACE:
        message = (
            'Low Disk Space\n\n'
            'You are running low on disk space on drive ({drive}). '
            'System currently has {current} GB available. '
            'Please make {space} GB or more available for extractions.'
            ).format(drive=drive, current=current_space,
                     space=config.RECOMMENDED_SPACE)
        wx.MessageBox(message, 'Warning', style=wx.ICON_EXCLAMATION)





def _package_name(extract_id: str) -> str:
    """Return standard Extract Package name from unique Extract ID."""
    return "Package_{}.zip".format(extract_id)





def _run_extraction(stream: pyextract.DataStream,
                    output: ABCMessenger,
                    output_folder: str,
                    package_name: str,
                    worker_timeout: int,
                    ecf_file: str,
                    callback_error: Callable[[str], None] = None,
                    chunk_results: str = 'db_per_table',
                    resume_extract: bool = False,
                    gauge: wx.Gauge=None,
                    sqlite_password: str = None):
    """Top-level API/function to run all extractions from external interfaces."""

    pyextract.utils.setup_multiproc_logger()

    assert os.path.exists(ecf_file), \
        'ECF file does not exist at "{}"'.format(ecf_file)

    starttime = time.time()

    # Build logfile filename from Extract ID
    extract_id = common._extract_id_from_package(package_name)
    logname = package_name.lower().replace("package", "extract").replace(".zip", ".log")
    logfile = os.path.join(output_folder, logname)

    # If an error occurs during extraction, this will still be None,
    # and we can determine which cleanup actions to take based on that
    extraction = None
    errors = False
    warnings = False
    try:
        extraction, errors, warnings = pyextract.extract_from_ecf(
            ecf=ecf_file, source=stream, output=output, logfile=logfile,
            worker_timeout=worker_timeout,
            chunk_results=chunk_results, resume_extract=resume_extract,
            extract_id=extract_id, gauge=gauge,
        )
    except (AssertionError, NetworkDisconnectError) as error:
        # A known error is occuring, log error and alert user
        LOGGER.error(error)
        # pyextract.utils.flush_log_to_file(LOGGER, logfile)
        if callback_error:
            callback_error(str(error))
    except:  # pylint: disable=broad-except
        message = ('An unhandled error occured during extraction. '
                   'See the log file for details.')
        LOGGER.error(message)
        # Remove the GUI handler if it's enabled before writing traceback
        LOGGER.handlers = [handler for handler in LOGGER.handlers
                           if not isinstance(handler, WxTextCtrlHandler)]
        # Write the traceback to the shell and text log
        LOGGER.exception('An unhandled error occured during extraction')
        # pyextract.utils.flush_log_to_file(LOGGER, logfile)
        # Call the callback function to alert via GUI if provided
        if callback_error:
            callback_error(message)

    LOGGER.info("Package Name: {}".format(package_name))
    LOGGER.info("Package Path: {}".format(os.path.join(output_folder, package_name)))

    # Report total time and # records extracted to the user
    if extraction:
        if errors:
            LOGGER.warning("Extractions completed WITH ERRORS in {:.2f} seconds."
                           .format(time.time() - starttime))
        elif warnings:
            LOGGER.warning("Extractions completed WITH WARNINGS in {:.2f} seconds."
                           .format(time.time() - starttime))
        else:
            LOGGER.info("Extractions completed in {:.2f} seconds."
                        .format(time.time() - starttime))
        common.log_final_record_count(extraction, resume_extract)
    else:
        LOGGER.warning("Extractions completed with errors in {:.2f} seconds."
                       .format(time.time() - starttime))

    # Remove File Handler so we can pack up the .log
    for i, hndlr in enumerate(LOGGER.handlers):
        if isinstance(hndlr, logging.FileHandler):
            hndlr.close()
            LOGGER.removeHandler(hndlr)

    LOGGER.info('Packaging data into a single .zip file...')
    package = common._create_data_package(ecf_file, output_folder)
    zip_path = os.path.join(output_folder, package_name)
    package.create(zip_path, sqlite_password)
    LOGGER.info('Data packaging complete, data is ready for upload')


    # Delete tracking table and close any open DB connections
    # output.drop_table_if_exists("temp_tracker")
    output._conn.close()

    # Delete items that are now packaged
    for item in os.listdir(output_folder):
        if not item.endswith('.zip') and not item.endswith('.dat-journal'):
            for _ in range(10):
                try:
                    fp = os.path.join(output_folder, item)
                    if os.path.exists(fp):
                        os.remove(fp)
                    break
                except PermissionError as err:
                    # print("Permission error.  Waiting on {}...".format(item))
                    time.sleep(3)

    # If using a gauge and extraction is not paused,
    # set its progress to finished (100%) position
    if gauge and not stream.is_stopped():
        wx.CallAfter(gauge.SetValue, gauge.GetRange())

    return errors, warnings


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


def _get_wx_control_value(ctrl: wx.Control) -> str:
    """Get the value of a wx.Control object."""
    if isinstance(ctrl, wx.Choice):
        return ctrl.GetStringSelection()
    elif (isinstance(ctrl, wx.DirPickerCtrl)
            or isinstance(ctrl, wx.FilePickerCtrl)):

        return ctrl.GetPath()
    elif isinstance(ctrl, wx.TextCtrl):
        return ctrl.GetValue()
    raise TypeError('Unknown wx.Control type "%s"', type(ctrl))


def valid_user_config_value(field: str, value: str) -> bool:
    """Validates user inputs for predefined configs"""
    user_config_value_limits = {
        'worker_timeout': (120, 5000),
        'chunk_size': (1000, 500000),  ## Oracle
        'sap_chunk_size': (1000, 5000000),
        'queue_size': (5, 50),
        'max_readers': (1, 32),
        'sap_batch_size': (1, 1000),
        'rename_wait': (1, 10)
    }

    # Only validate the three numeric fields
    if field not in user_config_value_limits:
        return True

    # Allow worker_timeout to be null or 0 (no timeout at all)
    if field == 'worker_timeout' and (value == '0' or not value):
        return True

    low, high = user_config_value_limits[field]
    if not value.isnumeric() or not low <= int(value) <= high:
        message = (
            'Value for "{}" must be between {:,} and {:,}.'
            ).format(USER_CONFIG_NAMES[field], low, high)
        wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
        return False

    return True


def _set_wx_control_value(ctrl: wx.Control, value: str):
    """Set the value of a wx.Control object."""
    if isinstance(ctrl, wx.Choice):
        ctrl.SetStringSelection(value)
    elif (isinstance(ctrl, wx.DirPickerCtrl)
            or isinstance(ctrl, wx.FilePickerCtrl)):
        ctrl.SetPath(value)
    elif isinstance(ctrl, wx.TextCtrl):
        ctrl.SetValue(value)
    else:
        raise TypeError('Unknown wx.Control type "%s"', type(ctrl))

def validate_folder_access(filepath: str, read_only=True) -> str:
    """Validate a user can access local folder; return reason if invalid."""
    # Only accept fully-qualified filepaths
    if filepath.rstrip() != os.path.abspath(filepath):
        error = (
            'Invalid relative filepath "{}". '
            'Filepath must be absolute and fully-qualified.'
            ).format(filepath)
        return error

    # If not allowed to create this folder, ensure it already exists
    if read_only and not os.path.exists(filepath):
        error = (
            'Filepath "{}" does not exist on this machine.'
            ).format(filepath)
        return error

    # Otherwise, assure the folder provided can be created, and that
    # the user can actually write a file to this directory too
    try:
        os.makedirs(filepath, exist_ok=True)
        testfile = os.path.join(filepath, 'test.txt')
        open(testfile, 'w').close()
    except FileNotFoundError:
        error = (
            'Filepath "{}" is invalid on local machine.'
            ).format(filepath)
        return error
    except PermissionError:
        error = (
            'User does not have permission to access "{}".'
            ).format(filepath)
        return error
    except OSError:
        # Cannot create files in folder, but can read them
        if not read_only:
            error = (
                'User does not have permission to create files in "{}".'
                ).format(filepath)
            return error
    else:
        # Permission to folder is valid, delete testfile
        os.remove(testfile)


def delete_old_extract_databases(folder: str):
    """Will remove any floating .dat files within the request ID subdirectory
        due to the implementation of encrypted databases, if there is a floating
        database there was an issue with a previous extraction and the password
        to decrypt isn't available, as such it will be removed for a clean
        extraction
    """
    if not os.path.exists(folder):
        return

    databases_to_be_dropped = []
    for filename in os.listdir(folder):
        if filename.endswith('.dat'):
            filepath = os.path.join(folder, filename)
            databases_to_be_dropped.append(filepath)

    if databases_to_be_dropped:
        LOGGER.warning('Outdated content found in working directory %s. '
                       'Deleting content and starting a new extraction.',
                       folder)

    for filepath in databases_to_be_dropped:
        os.remove(filepath)


def validate_folder_contents(folder: str, required: List[str], name: str):
    """Validate contents of a folder against a list of required files / folders.
    Raise a user-friendly DependencyError if any files are invalid / not found.
    """
    missing_files = []
    for filename in required:
        if not os.path.exists(os.path.join(folder, filename)):
            missing_files += [filename]

    if not missing_files:
        return

    if len(missing_files) == len(required):
        message = (
            'No {name} files were found in folder "{folder}"'
            ).format(name=name, folder=folder)
    else:
        message = (
            '{name} is missing the files / folders:  {files}. '
            'Please reinstall the {name} ({folder}) from a known working source.'
            ).format(name=name, files=missing_files, folder=folder)

    raise DependencyError(message)


def validate_sap_sdk(filepath: str):
    """Validate all files in an SAP dependency folder.
    Raises a DependencyError if any files are invalid / not found.
    """
    required = (
        'icudt34.dll', 'icuin34.dll', 'icuuc34.dll',
        'libicudecnumber.dll', 'libsapucum.dll', 'libsapucum.lib',
        'sapdecfICUlib.lib', 'sapnwrfc.dll', 'sapnwrfc.lib'
    )
    validate_folder_contents(filepath, required, 'SAP Netweaver SDK')


def validate_oracle_client(filepath: str):
    """Validate all files in an Oracle dependency folder.
    Raises a DependencyError if any files are invalid / not found.
    """
    required = (
        'adrci.exe', 'adrci.sym', 'genezi.exe', 'genezi.sym',
        'oci.dll', 'oci.sym', 'ociw32.dll', 'ociw32.sym',
        'uidrvci.exe', 'uidrvci.sym', 'xstreams.jar'
    )
    validate_folder_contents(filepath, required, 'Oracle Client')


def validate_db2_driver(filepath: str):
    """Validate all files in an IBM DB2 dependency folder.
    Raises a DependencyError if any files are invalid / not found.

    NOTE: IBM DB2 Driver contains over 100 files. We just look for
    a sample of the biggest files in each subfolder at the moment,
    but can expand in the future if needed.
    """
    required = (
        os.path.join('clidriver', 'bin', 'amd64.VC11.CRT', 'msvcp110.dll'),
        os.path.join('clidriver', 'bin', 'amd64.VC11.CRT', 'msvcr110.dll'),
        os.path.join('clidriver', 'bin', 'icc64', 'gsk8cms_64.dll'),
        os.path.join('clidriver', 'bin', 'db2app64.dll'),
        os.path.join('clidriver', 'bin', 'db2osse64.dll'),
        os.path.join('ibm_db_dlls', 'ibm_db.dll'),
    )
    validate_folder_contents(filepath, required, 'IBM DB2 Driver')


def update_logging(log_level: str):
    """Update multiprocessing loggers to log at user-requested level."""
    if not log_level in ('INFO', 'DEBUG'):
        message = 'Invalid logging level "{}"'.format(log_level)
        wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)
        return

    if log_level == 'DEBUG':
        LOGGER.setLevel(logging.DEBUG)
    else:
        LOGGER.setLevel(logging.INFO)


def alert_upload_success(filename: str, host: str, method: str):
    """Display alert to user that upload of the data package succeeded."""
    message = (
        'Upload succeeded\n\n'
        'File "{filename}" was uploaded using the '
        '"{method}" method.'
        ).format(filename=filename, method=method)
    LOGGER.info(' '.join(message.split()))
    wx.MessageBox(message, 'Info')


def alert_upload_failed(host: str, error: Exception):
    """Display alert to user that upload of the data package failed."""
    message = (
        'Upload failed\n\n'
        'Attempted upload to the PwC Large File Upload service '
        'at "{host}" failed with error:  {error}'
        ).format(host=host, error=error)
    LOGGER.error(' '.join(message.split()))
    wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)


def clean_connection_error(error: Exception) -> str:
    """Return a string error message, with cleaner syntax if available."""
    if 'message=Incomplete logon data.' in str(error):
        return 'Connection failed\n\nIncomplete logon data.'
    elif 'message=Name or password is incorrect' in str(error):
        return 'Connection failed\n\nUsername or password is incorrect.'
    return 'Connection failed\n\n{}'.format(error)


def next_extract_workflow_dialog(return_code: int, current_page: str) -> str:
    """Return name of the next Dialog window to show to user.

    ARGS:
        return_code: integer value which corresponds to the type of
            action last taken by the user (i.e. which button they clicked.)

    NOTE:
        Probably needs to be refactored long term, as the integer mapping
        gets messy and makes program operation less obvious.
    """
    assert return_code in (None, -1, 0, 1, 2, 3, 9, 99, 5101)

    # 'Cancel' or 'Finish' return codes
    # (None and 5101 are the top-right 'X' button codes)
    if return_code in (None, 0, 2, 5101):
        return None
    # 'Home' return code
    elif return_code == 99:
        return 'ecf'
    # 'Continue ABAP Workflow' return code
    elif return_code == 9:
        return 'abap'
    # 'Continue Extraction' return code
    elif return_code == 3:
        return 'continue_extract'
    # 'Next' return code
    elif return_code == 1:
        next_page_mapping = {
            'ecf': 'connection',
            'connection': 'content_preview',
            'abap': 'extraction',
            'content_preview': 'extraction',
        }
        try:
            return next_page_mapping[current_page]
        except KeyError:
            raise ValueError('return_code not paired with valid page')

    # 'Previous' return code
    elif return_code == -1:
        previous_page_mapping = {
            'connection': 'ecf',
            'content_preview': 'connection',
            'extraction': 'content_preview',
            'abap': 'continue_extract'
        }
        try:
            return previous_page_mapping[current_page]
        except KeyError:
            if current_page != 'continue_extract':
                raise ValueError('return_code not paired with valid page')


def alert_error(message: str):
    """Alert an error message to the user (used for Threaded extraction)."""
    wx.MessageBox(message, 'Error', style=wx.ICON_ERROR)





if __name__ == '__main__':

    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        ROOT = sys._MEIPASS
    except AttributeError:
        ROOT = os.path.abspath('.')

    multiprocessing.freeze_support()
    APP = wx.App()
    check_disk_space()
    PyExtract()
    APP.MainLoop()
