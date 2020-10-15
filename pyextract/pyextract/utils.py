"""Library of standalone, generic utility functions."""

from datetime import datetime
import logging
from logging.handlers import MemoryHandler
import multiprocessing
import os
import socket
import sys

from .queries import ParsedQuery


class DataDefinition(object):
    """Metadata about an entire dataset to be extracted."""

    def __init__(self, parameters: str = None, source: str = None,
                 ecf_data: dict = None, output_table: str = None):
        """Create a new metadata definition for a data extraction.

        NOTE: Do not make a cursor object an attribute of this class, it is
            pickled later in the application and open connections cannot be pickled

        ARGS:
            parameters (str/dict): Information to be used to curate query
                nested extraction from ECF
            source (ABCMessenger): The Messenger to parse query metadata with.
            ecf_data (ecfreader.ECFData): Named tuple with data from an ECF
                file. If not provided, will attempt to parse information
                from the SQL query.
            output_table: Specific table name to use for the output. If
                provided, this value takes priority over all other methods of
                finding the table name (ecf/query parsing).
        """
        self.parameters = parameters

        if isinstance(parameters, dict) and 'SAPMessenger' not in str(source.__class__):
            self.columns = source.get_metadata_from_query2(parameters)
        else:
            self.columns = source.get_metadata_from_query(parameters)

        self.ecf_data = ecf_data
        if isinstance(parameters, dict):
            self.function_module = parameters.get('FunctionModule')
        else:
            self.function_module = None

        # Parse the table name and alias from ECF or SQL query
        if ecf_data:
            source_table = ecf_data.table_name
            source_alias = ecf_data.table_alias
            set_id = ecf_data.set_id
        else:
            try:
                # Regular SQL extraction from a query
                parsed = ParsedQuery(parameters)
                source_table = parsed.table
            except ValueError:
                # ABAP extraction -- just a table name
                source_table = parameters
            source_alias = None
            set_id = None

        # Determine what to call the output table based on ECF / query
        if output_table:
            self.target_table = output_table
        elif ecf_data:
            self.target_table = ecf_data.table_alias
            #Check for ECF 2.0s to collect the type (table, BAPI, etc)
            try:
                self.query_type = ecf_data.query_text['Type']
            except (AttributeError, TypeError):
                pass
        else:
            self.target_table = source_table

        # Update the remaining column-level metadata
        for config in self.columns:
            config.update({
                'datetime': datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                'sourceTableName': source_table,
                'targetTableName': self.target_table,
                'source_table_alias': source_alias,
                'SetId': set_id,
                'targetType': 'TEXT',
            })
            if 'mssql_datatype' not in config:
                config['mssql_datatype'] = 'NVARCHAR(MAX)'

        # Make sure duplicate columns don't exist in the metadata
        self._assert_no_duplicate_columns()

    def __repr__(self):
        """Display metadata as a query with only single space whitespace."""
        return cleanstr("<DataDefinition query='{}'>".format(self.parameters))

    def as_table(self):
        """Return metadata as a sorted, tabular 2D list."""
        data = [
            [column.get(key) for key in self.metadata_columns]
            for column in self.columns
        ]
        return data

    @property
    def metadata_columns(self):
        """Return tuple of metadata column names based on ECF DataConnector."""

        column_names = [
            'datetime', 'sourceSystem', 'source_schema', 'sourceTableName',
            'source_table_alias', 'sourceFieldName', 'sourceType',
            'sourceFieldLength', 'sourceFieldNumericPrecision',
            'sourceFieldNumericScale', 'sourceFieldNumericDisplaySize',
            'source_field_nullable', 'targetTableName', 'targetFieldName',
            'targetType', 'shard_number', 'isKey', 'SetId',
        ]

        if (self.ecf_data and self.ecf_data.ecfjson['DataSource']['Application'] == 'SAP'
                and self.ecf_data.ecfjson['DataSource']['DataConnector'] == 'RFC'):
            column_names.extend(('SIGN', 'longDataType'))

        return tuple(column_names)

    def _assert_no_duplicate_columns(self):
        """Raise an error if duplicate columns exist in the ECF metadata."""
        names = sorted(column["sourceFieldName"] for column in self.columns)
        last = None
        for name in names:
            if name == last:
                raise ValueError('Duplicate column "{}" found for table "{}"'
                                 .format(name, self.target_table))
            last = name


class DependencyError(Exception):
    """Error raised when user has not set all dependencies in config file."""
    def __init__(self, text: str, *args, **kwargs):
        super().__init__(text, *args, **kwargs)
        self.text = text


class NetworkDisconnectError(Exception):
    """Error raised when the extraction suffers a prolonged network disconnect."""
    def __init__(self, text: str, *args, **kwargs):
        super().__init__(text, *args, **kwargs)
        self.text = text


def parse_query_from_filepath(filepath: str) -> str:
    """Return a query string with standardized whitespace from a filepath."""
    with open(filepath, 'r') as script:
        lines = [line.strip('\n').strip('\r') for line in script]
    query = ' '.join(lines)
    return query


def cleanstr(string: str) -> str:
    """Return a string with all whitespace replaced with single spaces."""
    return ' '.join(string.split())


def _format_sap_connection_props(client: str, user: str, password: str,
                                 language='EN', system: str = None,
                                 ashost: str = None, mshost: str = None,
                                 group: str = None) -> dict:
    """Returns the dictionary used to connect to SAP via RFC.

    ARGS:
        client: Client number of the SAP database.
        user: Username to authenticate with.
        password: Password to authenticate with.
        language: 2-digit language code to connect with.
        system: System number to connect to. If direct connection, is
            the 'system number'. If load balancing connection, 'system id'.
        ashost: Network location of the SAP server. If provided, will use
            a direct connection to SAP.
        mshost: A Message Server Host that will provide a load balancing
            connection to SAP.
        group: The group to use with the load-balancing messaging server.
    """
    # pylint: disable=too-many-arguments

    connection_options = {
        'client': client,
        'user': user,
        'passwd': password,
        'lang': language,
    }

    if system and ashost:
        connection_options.update({
            'sysnr': system,
            'ashost': ashost,
        })

    if system and mshost and group:
        connection_options.update({
            'sysid': system,
            'mshost': mshost,
            'group': group,
        })

    return connection_options


def validate_test_environment():
    """Validate environment variables, and load dependent modules."""
    # pylint: disable=unused-variable

    if 'SAP_NETWEAVER_SDK' in os.environ:
        update_path(os.environ['SAP_NETWEAVER_SDK'])

    if 'ORACLE_HOME' in os.environ:
        update_path(os.environ['ORACLE_HOME'])

    try:
        import pyrfc
    except ImportError as error:
        if 'DLL load failed:' in str(error):
            raise DependencyError(
                'Could not locate the SAP Netweaver SDK on this machine. '
                'Please locate or install it, and set the '
                'SAP_NETWEAVER_SDK environment variable to that value.'
            )
        else:
            raise error

    try:
        import cx_Oracle
    except ImportError as error:
        if 'DLL load failed:' in str(error):
            raise DependencyError(
                'Could not locate a valid Oracle client on this machine. '
                'Please locate or install it, and set the '
                'ORACLE_HOME environment variable to that value.'
            )
        else:
            raise error


def update_path(filepath: str):
    """Update the PATH environment variable to include a filepath."""
    os.environ['PATH'] = filepath + os.pathsep + os.environ['PATH']


def setup_logging():
    """Setup logging for basic config on package import."""
    logging.basicConfig(
        level=logging.INFO,
        format='\t'.join(('%(asctime)s', '%(processName)s', '%(levelname)s',
                          '%(message)s', socket.gethostname()))
    )


def setup_file_logger(fn):

    """Setup a logger that spans processes for multiprocessing."""
    logger = multiprocessing.get_logger()

    filehandler_exists = any(True for hndlr in logger.handlers
                            if isinstance(hndlr, logging.FileHandler))

    if not filehandler_exists:
        file_handler = logging.FileHandler(fn)
        formatter = logging.getLogger().handlers[0].formatter
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)


def setup_multiproc_logger():
    """Setup a logger that spans processes for multiprocessing."""
    logger = multiprocessing.get_logger()

    streamhandler_exists = any(True for hndlr in logger.handlers
                               if isinstance(hndlr, logging.StreamHandler))

    if not streamhandler_exists:
        logger.addHandler(logging.StreamHandler(sys.stdout))


def flush_log_to_file(logger: logging.Logger, filepath: str):
    """Write in-memory logger data to a text file."""
    handlers = [lgr for lgr in logger.handlers
                if isinstance(lgr, MemoryHandler)]
    assert handlers, 'Logger object does not have a MemoryHandler'
    memhandler = handlers[0]
    if not memhandler.buffer:
        logger.warning('No log data exists in this MemoryHandler')
        return

    # Write the main processes data to the log
    file_handler = logging.FileHandler(filepath)
    formatter = logging.getLogger().handlers[0].formatter
    file_handler.setFormatter(formatter)
    memhandler.setTarget(file_handler)
    memhandler.flush()
    file_handler.close()
    memhandler.close()


def local_appdata_path(filename: str) -> str:
    """Return the default path for the PyExtract SQLite config database."""
    local = os.getenv('LOCALAPPDATA')
    return os.path.join(local, 'PwC', 'Extract', filename)
