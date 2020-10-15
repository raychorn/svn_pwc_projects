"""Query interface built on an open MySQL / MemSQL connection."""
#pylint: disable=no-member

import getpass
import multiprocessing
import sys

import ceODBC

from .. import utils
from .base import ABCMessenger
from .. import config
from .. import vm_lookup

LOGGER = multiprocessing.get_logger()

class MySQLMessenger(ABCMessenger):
    """Interfaces with a single MySQL database."""

    def __init__(self, connection_string: str = None, **kwargs):
        """Instantiate a new instance of a connector."""
        super().__init__()
        if not connection_string:
            try:
                connection_string = mysql_conn_string(**kwargs)
            except ConnectionError as error:
                LOGGER.error("Failed to connect to MySQL with error:  %s", error)
                raise error
        self.connection_string = connection_string

        try:
            self._conn = ceODBC.connect(connection_string)
        except ceODBC.DatabaseError as error:
            LOGGER.error("Failed to connect to MySQL with error:  %s", error)
            raise error

        self._extract_cursor = None

    def fetch_data(self, query):
        """Return data from a SQL query."""
        cursor = self._conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        return data

    def execute_statement(self, statement):
        """Executes a SQL statement."""
        cursor = self._conn.cursor()
        try:
            cursor.execute(statement)
        except ceODBC.Error as error:
            LOGGER.error('ceODBC error "%s" caused by statement "%s"',
                         error, statement)
            raise error
        else:
            self._conn.commit()
        finally:
            cursor.close()

    def get_metadata_from_query(self, query: str) -> list:
        """Test the SQL query on the source database and return its metadata.

        ARGS:
            query (str): SQL query that defines the data to be extracted.

        RETURNS:
            metadata (list[dict[str]]): List of metadata dictionaries
                about the columns being read from source DB.
        """

        LOGGER.debug("Reading metadata from MySQL database cursor.")
        cursor = self._conn.cursor()
        try:
            cursor.execute(query)
        except ceODBC.DatabaseError:
            LOGGER.error('Error caused by query: %s', utils.cleanstr(query))
            raise

        metadata = []  # type: List[Dict[str, str]]
        for column in cursor.description:

            datatype = column[1].__name__
            if datatype in ('DecimalVar', 'IntegerVar'):
                sqlite_datatype = 'REAL'
            else:
                sqlite_datatype = 'TEXT'

            column_metadata = {
                'sourceSystem': 'MySQL',
                'sourceFieldName': column[0].strip(),
                'sourceType': column[1].__name__,
                'sourceFieldLength': column[3],
                'sourceFieldNumericPrecision': column[4],
                'source_field_nullable': column[6],
                'targetFieldName': column[0],
                'sqlite_datatype': sqlite_datatype,
            }
            metadata += [column_metadata]

        _validate_metadata_types(metadata, query)

        cursor.close()
        return metadata

    def list_all_tables(self):
        """Return a list of tables that exist in this database."""
        query = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_TYPE != 'SYSTEM VIEW'
            AND TABLE_SCHEMA = DATABASE()
            ORDER BY TABLE_NAME
            """
        cursor = self._conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        return [row[0] for row in data]

    def queries_for_all_data(self):
        """Return list of queries that covers all tables/data available."""
        tables = self.list_all_tables()
        queries = ["SELECT * FROM {}".format(table) for table in tables]
        return queries

    def create_table(self, table, columns, datatypes=None):
        """Create a new table for storing extraction results.

        ARGS:
            table (str): The table name to create in the database.
        """

        if self.table_exists(table):
            LOGGER.warning('Table %s already exists', table)
            return

        LOGGER.info("Creating table '%s' in MySQL database", table)
        statement = create_table(table, columns, datatypes)
        cursor = self._conn.cursor()

        try:
            cursor.execute(statement)
        except ceODBC.DatabaseError as error:
            LOGGER.warning('Table %s already exists. Error: %s', table, error)
            raise
        else:
            self._conn.commit()
            LOGGER.info("Created MySQL table with statement: %s.",
                        utils.cleanstr(statement))
        finally:
            cursor.close()

    def table_exists(self, table):
        """Return True if a table already exists in this database."""
        query = """
            SELECT 1
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME = '{table}'
            """.format(table=table)
        cursor = self._conn.cursor()
        cursor.execute(query)
        row = cursor.fetchall()
        cursor.close()
        return bool(row)

    def rows_exist(self, table, where_clause):
        """Return true if a table has rows that meet the where clause"""
        query = rows_exist(table, where_clause)
        cursor = self._conn.cursor()
        cursor.execute(query)
        row = cursor.fetchall()
        cursor.close()

        return bool(row)

    def drop_table_if_exists(self, table):
        """Drop a table from the database."""
        query = drop_table_if_exists(table)
        cursor = self._conn.cursor()
        cursor.execute(query)
        self._conn.commit()
        cursor.close()

    def insert_into(self, table, data, metadata=None):
        """Bulk insert data into a table on this database.

        APSW doesn't support cursor row count, so we cannot use that to
        get the number of rows committed back.

        ARGS:
            table (str): Name of the table in MySQL to insert into.
            data (tuple[tuple[str]]): String data held in a 2D tuple.
            metadata (list[dict[str]]): List of metadata dicts
                about the columns being written to the source DB.
        """

        cursor = self._conn.cursor()
        statement = insert_into(table=table, number_columns=len(data[0]))

        LOGGER.debug("Executing SQL statement with %d rows of data: %s",
                     len(data), utils.cleanstr(statement))

        if metadata:
            # Specify datatypes to insert into MySQL using metadata
            datatypes = []
            for column in metadata:
                string_dtype = column['mssql_datatype']
                if string_dtype == 'FLOAT':
                    datatype = ceODBC.DoubleVar
                elif string_dtype == 'DATETIME':
                    datatype = ceODBC.DateVar
                elif string_dtype == 'TIMESTAMP':
                    datatype = ceODBC.TimestampVar
                elif string_dtype == 'BIGINT':
                    datatype = ceODBC.BigIntegerVar
                elif sys.version_info >= (3, 0):
                    datatype = ceODBC.StringVar
                else:
                    datatype = ceODBC.UnicodeVar
                datatypes += [datatype]

            LOGGER.info("Setting datatypes for write to SQL as: %s", datatypes)
            cursor.setinputsizes(*datatypes)

        try:
            data = _ceodbc_safe_data(data)
            for row in data:
                cursor.execute(statement, row)
            self._conn.commit()
        except ceODBC.Error as error:
            LOGGER.error("Data failed to insert into MySQL: %s", error)
            raise RuntimeError
        else:
            LOGGER.debug("Data successfully inserted into MySQL")
        finally:
            cursor.close()

    def update_records(self, table, columns, updated_values, where_condition=None):
        """method to update records in the database"""
        cursor = self._conn.cursor()
        statement = update_rows(table=table, columns=columns,
                                updated_values=updated_values,
                                condition=where_condition)

        LOGGER.debug("Executing SQL statement updating %i columns with %s",
                     len(columns), where_condition)

        try:
            cursor.execute(statement)
            self._conn.commit()
        except ceODBC.Error as error:
            LOGGER.error("Data failed to update rows in MSSQL: %s", error)
            raise error
        else:
            LOGGER.debug("Data successfully updated in MSSQL")
        finally:
            cursor.close()

    def create_metadata_table(self, metadata: utils.DataDefinition, table: str):
        """Writes extraction metadata to a MySQL table."""

        if self.table_exists(table):
            LOGGER.warning('Table %s already exists', table)
        else:
            statement = create_table(table=table, columns=metadata.metadata_columns)
            LOGGER.debug("Creating metadata table in MySQL with query: %s",
                         utils.cleanstr(statement))
            cursor = self._conn.cursor()
            cursor.execute(statement)
            cursor.close()

        LOGGER.info("Inserting metadata into MySQL table.")
        self.insert_into(table, metadata.as_table())
        LOGGER.debug("Finished writing metadata to MySQL table.")

    def begin_extraction(self, metadata, chunk_size=None):
        """Begin pulling data from a query."""
        self._extract_cursor = self._conn.cursor()
        self._extract_cursor.execute(metadata.parameters)

    def continue_extraction(self, chunk_size=None):
        """Continue pulling data from a query."""
        if self._extract_cursor:
            data = self._extract_cursor.fetchmany(chunk_size)
        return data

    def finish_extraction(self):
        """Clean up after a long-term extraction is finished"""
        self._extract_cursor.close()
        self._extract_cursor = None


def _ceodbc_safe_data(data):
    """Return 2D list of data with strs coerced to unicode for ceODBC."""
    #pylint: disable=undefined-variable

    if sys.version_info >= (3, 0):
        return list(data)

    return [
        tuple(unicode(item) if isinstance(item, str) else item
              for item in row)
        for row in data
    ]


def mysql_conn_string(host: str = None, database: str = None, user: str = None,
                      port: str = 3306, password: str = None, adapt_id: str = None,
                      dsn: str = None, driver: str = None) -> str:
    """Return a connection string for a ceODBC MySQL connection.

    ARGS:
        server: Server the database is on.
        database: Name of the database to connect to.
        user: User ID to authenticate against.
        port: Port to connect to.
        adapt_id: Adapt ID used to lookup VM's IP Address in DAW.
    """

    # If An Adapt ID is provided, need to get the IP Address.
    if dsn:
        connection_string = (
            'DSN={dsn};'
            'DATABASE={database};'
        ).format(dsn=dsn, database=database)
        return connection_string

    elif adapt_id:

        status, ip = vm_lookup.lookup(adapt_id, LOGGER)

        if status is None:
            raise ConnectionError("Failed to identify host associated with Adapt ID {}.  (1) Ensure the Adapt ID is correct and (2) Ensure the VM is running")

        if status != 'Ready':
            raise ConnectionError("Failed to connect to host.  Status = '{}'  Ensure VM (and MySQL) is running and try again.".format(status))

        connection_string = (
            'DRIVER={driver};'
            'SERVER={server};'
            'DATABASE={database};'
            'UID=dawremote;'
            'PWD=Daw@remote17!' ## Remove Creds!!
        ).format(driver=driver, server=ip, database=database)
        return connection_string

    # Else...
    if user is None:
        user = getpass.getuser()

    connection_string = (
        'DRIVER={driver};'
        'SERVER={server};'
        'DATABASE={database};'
        'UID={user};'
        ).format(driver=driver, server=host, user=user, database=database)

    if port:
        connection_string += 'PORT={};'.format(port)

    if password:
        connection_string += 'PWD={};'.format(password)
    return connection_string


def connect_to_mssql(*args, **kwargs):
    """Return an open connection to the target MySQL database.

    ARGS:
        **kwargs: See 'mysql_conn_string'.

    RETURNS:
        (ceODBC.Connection): Open connection to target MySQL database.
    """

    connection_string = mysql_conn_string(*args, **kwargs)
    connection = ceODBC.connect(connection_string)
    return connection


def create_table(table, columns, datatypes=None):
    """Return a CREATE TABLE statement for MySQL."""

    if datatypes is None:
        datatypes = ['TEXT' for _ in columns]

    if len(columns) != len(datatypes):
        raise AttributeError('len(datatypes) must equal len(columns)')

    columns_datatypes = zip(columns, datatypes)
    columns_datatypes = ['{} {} NULL'.format(c, d) for c, d in columns_datatypes]
    joined = ', '.join(columns_datatypes)

    statement = """
        CREATE TABLE {table} ({columns_dtypes});
        """.format(table=table, columns_dtypes=joined)

    return statement


def drop_table_if_exists(table: str) -> str:
    """Return a MySQL statement that will drop a table if it exists."""
    return "DROP TABLE IF EXISTS {table}".format(table=table)

def rows_exist(table: str, where_clause: str = None):
    """A query that checks if a table exists in a MySQL database."""
    statement = """
        SELECT * FROM {table}
        WHERE {where_clause}
        """.format(table=table, where_clause=where_clause)
    return statement


def insert_into(table, columns=None, number_columns=None):
    """Return an INSERT statement for MySQL with placeholder tokens."""

    if not columns and not number_columns:
        raise ValueError("must provide 'columns' list or 'number_columns'")

    target = "{}".format(table)

    if columns is None:
        column_string = ''
        placeholders = ', '.join(['?'] * number_columns)
    else:
        column_string = '({})'.format(', '.join(columns))
        placeholders = ', '.join(['?'] * len(columns))

    statement = """
        INSERT INTO {} {} VALUES ({})
        """.format(target, column_string, placeholders)

    return statement

def update_rows(table, columns: list = None,
                updated_values: list = None, condition: str = None):
    """Return a UPDATE statement for MSSSQL for the provided values"""
    if not columns and not updated_values and len(columns) != len(updated_values):
        raise ValueError("Must provide columns and values of equal length for a valid update")

    set_condition = ', '.join(
        "{} = '{}'".format(col, val)
        for col, val in zip(columns, updated_values)
    )

    statement = """
       UPDATE {}
       SET {}
       WHERE {}
       """.format(table, set_condition, condition)

    return statement


def _validate_metadata_types(metadata: dict, query: str):
    """Validates no records in the query are of a data type which is disallowed"""

    erroneous_columns = []

    for column in metadata:
        if column['sourceType'] in config.BANNED_SQL_DTYPES:
            LOGGER.error("WARNING: column %s is not of a supported data type. "
                         "Removing table from extraction due to unsupported data type %s.",
                         column['sourceFieldName'], column['sourceType'])
            erroneous_columns.append(column['sourceFieldName'])

    assert not erroneous_columns, \
        "Query: {} will be skipped due to disallowed datatypes".format(query)
