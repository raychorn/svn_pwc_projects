"""Query interface built on an open SQLite connection."""
# pylint: disable=no-member

from contextlib import contextmanager
import multiprocessing
import os
import shutil
from typing import Iterable
import uuid

import apsw  # custom compiled with ZipVFS (AES256 compatible)
import keyring

from .. import utils
from .base import ABCMessenger
from .. import config


LOGGER = multiprocessing.get_logger()


class SQLiteMessenger(ABCMessenger):
    """Interfaces with a single SQLite database."""

    def __init__(self, filepath: str, is_zipped=False,
                 password: str = None, aes256=False) -> None:
        """Instantiate a new instance of a SQLite database messenger.

        ARGS:
            filepath: Location of this database on the local filesystem.
            is_zipped: If True, connects to a zipped, encrypted database.
            password: If is_zipped, password the db is encrypted with.
            aes256: If True, use AES-256 encryption instead of AES-128.
        """
        super().__init__()
        self.filepath = filepath  # Read-only
        self.is_zipped = is_zipped
        self.password = password
        self.aes256 = aes256
        self._conn = connect_to_sqlite(filepath, password, is_zipped, aes256)
        self._extract_cursor = None

    def __repr__(self):
        return "<SQLiteMessenger filepath='{}'>".format(self.filepath)

    def create_table(self, table, columns, datatypes=None):
        """Create a new table for storing extraction results."""

        LOGGER.debug("Creating table '%s' in SQLite database", table)

        statement = sqlite_create_table(table, columns, datatypes)

        cursor = self._conn.cursor()
        try:
            cursor.execute(statement)
        except apsw.SQLError as error:
            LOGGER.error("Error creating table with statement:  %s",
                         utils.cleanstr(statement))
            LOGGER.error(error)
            raise error
        cursor.close()

        LOGGER.debug("Created SQLite table with statement: %s.",
                     utils.cleanstr(statement))

    def table_exists(self, table):
        """Return True if a table already exists in this database."""

        query = sqlite_table_exists()
        cursor = self._conn.cursor()
        cursor.execute(query, (table, ))
        row = cursor.fetchall()
        cursor.close()

        return bool(row)

    def rows_exist(self, table, where_clause):
        """Return true if a table has rows that meet the where clause"""

        query = sqlite_rows_exist(table, where_clause)
        cursor = self._conn.cursor()
        cursor.execute(query)
        row = cursor.fetchall()
        cursor.close()

        return bool(row)

    def drop_table_if_exists(self, table):
        """Drop a table from the database."""

        if not self.table_exists(table):
            return

        cursor = self._conn.cursor()
        cursor.execute("DROP TABLE [{0}]".format(table))
        cursor.close()

    def insert_into(self, table, data, metadata=None):
        """Bulk insert data into a table on this database.

        APSW doesn't support cursor row count, so we cannot use that to
        get the number of rows committed back.

        ARGS:
            table (str): Name of the table in SQLite to insert into.
            data (tuple[tuple[str]]): String data held in a 2D tuple.
            metadata...
        """
        cursor = self._conn.cursor()
        cursor.setrowtrace(None)
        cursor.setexectrace(None)

        statement = sqlite_insert_into(table, len(data[0]))
        LOGGER.debug("Executing SQL statement with %d rows of data: %s",
                     len(data), utils.cleanstr(statement))


        # Convert all non-NULL values to strings before insertion
        data = stringify_data(data)

        try:
            cursor.execute("begin")
            cursor.executemany(statement, data)
            cursor.execute("commit;")
        except apsw.Error as error:
            cursor.close()
            LOGGER.error("Data failed to insert into SQLite: %s", error)
            raise RuntimeError
        else:
            cursor.close()
            LOGGER.debug("Data successfully inserted into SQLite")

    def update_records(self, table: str, columns: list,
                       updated_values: list, where_condition: str = None):
        """method to update records in the database"""
        cursor = self._conn.cursor()

        statement = sqlite_update_rows(table=table, columns=columns,
                                       updated_values=updated_values,
                                       condition=where_condition)

        LOGGER.debug("Executing SQL statement updating %i columns with %s",
                     len(columns), where_condition)

        try:
            cursor.execute("begin")
            cursor.execute(statement)
            cursor.execute("commit;")
        except apsw.Error as error:
            cursor.close()
            LOGGER.error("Data failed to update rows in SQLite: %s", error)
            raise RuntimeError
        else:
            cursor.close()
            LOGGER.debug("Data successfully updated in SQLite")

    def delete_duplicates(self, table: str, fields: str):
        """Removes duplicates from a given table for the provided fields"""
        if not self.table_exists(table):
            return

        cursor = self._conn.cursor()
        statement = """
            DELETE FROM {table}
            WHERE rowid NOT IN (
                SELECT MIN(rowid)
                FROM {table}
                GROUP BY {fields}
            );
            """.format(table=table, fields=fields)
        cursor.execute(statement)

    def fetch_data(self, query):
        """Return data from a SQL query."""
        cursor = self._conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()
        return data

    def create_metadata_table(self, metadata: utils.DataDefinition, table: str):
        """Writes extraction metadata to a SQLite table."""

        statement = sqlite_create_table(table=table,
                                        columns=metadata.metadata_columns)

        LOGGER.debug("Creating metadata table in SQLite with query: %s",
                     utils.cleanstr(statement))

        cursor = self._conn.cursor()
        cursor.execute(statement)
        cursor.close()

        LOGGER.info("Inserting metadata into new SQLite table.")
        try:
            data = metadata.as_table()
            self.insert_into(table, data)
        except TypeError:
            pass
        LOGGER.debug("Finished writing metadata to new SQLite table.")

    def begin_extraction(self, metadata: utils.DataDefinition, chunk_size: int = None):
        """Begin pulling data from a SQL query."""
        self._extract_cursor = self._conn.cursor()
        self._extract_cursor.execute(metadata.parameters)

    def continue_extraction(self, chunk_size: int = None) -> list:
        """Continue pulling data from a query."""
        if not self._extract_cursor:
            return None
        # Filter out any blank rows from returned data
        data = [self._extract_cursor.fetchone() for _ in range(chunk_size)]
        data = [item for item in data if item is not None]
        return data or None

    def finish_extraction(self):
        """Clean up after a long-term extraction is finished"""
        self._extract_cursor.close()
        self._extract_cursor = None

    def get_metadata_from_query(self, query):
        """Return metadata about a query from the database."""
        LOGGER.debug("Reading metadata from SQLite database cursor.")
        cursor = self._conn.cursor()
        cursor.execute(query)
        description = cursor.getdescription()
        cursor.close()

        metadata = []
        for name, datatype in description:
            col_metadata = {
                'sourceSystem': 'SQLite',
                'sourceFieldName': name,
                'sourceType': datatype,
                'sourceFieldLength': None,
                'sourceFieldNumericPrecision': None,
                'source_field_nullable': None,
                'targetFieldName': name,
                'sqlite_datatype': datatype,
                'mssql_datatype': _mssql_dtype(datatype),
            }
            metadata += [col_metadata]

        return metadata

    def update_filepath(self, newpath):
        """Change the filepath of this SQLite database."""
        self._conn.close()
        os.makedirs(os.path.dirname(newpath), exist_ok=True)
        shutil.move(self.filepath, newpath)
        self.filepath = newpath
        self._conn = connect_to_sqlite(newpath, self.password, self.is_zipped)

    def list_all_tables(self):
        """Return a list of tables that exist in this Messengers database."""

        query = """
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            """.format()

        cursor = self._conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()

        return [row[0] for row in data]

    def queries_for_all_data(self):
        """Return list of queries that covers all tables/data available."""
        tables = self.list_all_tables()
        queries = ['SELECT * FROM "{}"'.format(table) for table in tables]
        return queries


    def get_rows_read_so_far(self, table_alias):

        query = "SELECT SourceRecordCount FROM TableExtractions WHERE table_alias = '{}'".format(table_alias)
        cursor = self._conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()

        if data:
            count = [row[0] for row in data][0]
            if count:
                return int(count)

        return 0

def _mssql_dtype(sqlite_dtype):
    """Return a string MSSQL datatype from a string SQLite datatype."""
    sqlite_dtype = sqlite_dtype.upper()
    if sqlite_dtype in config.DATETIME_DTYPES:
        datatype = 'DATETIME'
    elif sqlite_dtype in config.INT_DTYPES:
        datatype = 'BIGINT'
    elif sqlite_dtype in config.FLOAT_DTYPES:
        datatype = 'FLOAT'
    else:
        datatype = 'NVARCHAR(MAX)'
    return datatype


def connect_to_sqlite(filepath: str, password: str = None,
                      is_zipped=False, aes256=False) -> apsw.Connection:
    """Connect to or create a new SQLite database at the target location.

    ARGS:
        filepath: The filepath of the database on the local machine.
        password: The password to encrypt contents with if zipped.
        is_zipped: If True, connects to a zipped, encrypted database.
        aes256: If True, use AES-256 encryption instead of AES-128.
    """
    if is_zipped:
        assert password, (
            'password must have at least one character '
            'when zipping and encrypting a database.'
        )
    # Make a directory for the database if it does not exist
    folder = os.path.dirname(filepath)
    os.makedirs(folder, exist_ok=True)

    if is_zipped:
        # Determine key strength (AES-128 or AES-256)
        if aes256:
            passwordtype = 'password256'
        else:
            passwordtype = 'password'

        # Connect to the SQLite database with custom APSW build
        connection_string = (
            "file:{filepath}?zv=zlib&level=9&vfs=zipvfs&"
            "{passwordtype}={password}"
            ).format(filepath=filepath,
                     passwordtype=passwordtype,
                     password=password)

        flags = (apsw.SQLITE_OPEN_READWRITE |
                 apsw.SQLITE_OPEN_CREATE |
                 apsw.SQLITE_OPEN_URI)

        connection = apsw.Connection(connection_string, flags=flags)
    else:
        # Connect to a normal SQLite file without custom APSW build
        connection = apsw.Connection(filepath)

    # Addresses database lockout issue encountered during testing
    connection.setbusytimeout(100)

    cursor = connection.cursor()

    if os.path.exists(filepath):
        # Test the connection, raising an error if it fails
        try:
            cursor.execute("SELECT * FROM sqlite_master LIMIT 1")
        except apsw.IOError:
            LOGGER.error('Failed to connect to SQLite database: %s', filepath)
            raise
        else:
            set_database_pragmas(cursor)
        finally:
            cursor.close()
    return connection


def sqlite_table_exists(placeholder='?'):
    """A query that checks if a table exists in a SQLite database."""
    statement = """
        SELECT name FROM sqlite_master
        WHERE type = 'table' AND name = {placeholder}
        """.format(placeholder=placeholder)
    return statement


def sqlite_rows_exist(table, where_clause):
    """A query that checks if a table exists in a SQLite database."""
    statement = """
        SELECT * FROM {table}
        WHERE {where_clause}
        """.format(table=table, where_clause=where_clause)
    return statement


def sqlite_insert_into(table, number_columns):
    """Return an INSERT statement for SQLite with placeholder tokens."""
    placeholders = ', '.join(['?'] * number_columns)
    statement = """
        INSERT INTO "{table}" VALUES ({placeholders})
        """.format(table=table, placeholders=placeholders)
    return statement


def sqlite_update_rows(table, columns: list = None,
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


def sqlite_create_table(table, columns, datatypes=None):
    """Return a CREATE TABLE statement for SQLite."""

    if datatypes is None:
        datatypes = ['TEXT' for _ in columns]

    if len(columns) != len(datatypes):
        raise AttributeError('len(datatypes) must equal len(columns)')

    columns_datatypes = zip(columns, datatypes)
    columns_datatypes = ['"{}" {}'.format(c, d) for c, d in columns_datatypes]
    joined = ', '.join(columns_datatypes)

    statement = """
        CREATE TABLE IF NOT EXISTS "{table}" ({columns_dtypes});
        """.format(table=table, columns_dtypes=joined)

    return statement


def stringify_data(data: Iterable[Iterable]) -> Iterable[Iterable]:
    """Convert all non-NULL data into strings for SQLite insertion."""
    return tuple(
        (str(item) if item is not None else item for item in row)
        for row in data
    )


def set_database_pragmas(cursor):
    """Set the standard database pragmas for encrypted SQLite cursors.
    NOTE -- https://sqlite.org/pragma.html
    """
    cursor.execute("PRAGMA main.journal_mode=MEMORY;")
    cursor.execute("PRAGMA main.synchronous=OFF;")
    cursor.execute("PRAGMA main.page_size=4096;")
    cursor.execute("PRAGMA main.cache_size=10000;")


def get_set_keyring_password() -> str:
    """Will check if a password currently exists in the vault and if so, return
        the stored password to unlock the config.db, or will create and set
        set the password in the vault
    """
    if keyring.get_password(config.KEYRING_SYSTEM, config.KEYRING_USER):
        return keyring.get_password(config.KEYRING_SYSTEM, config.KEYRING_USER)

    password = str(uuid.uuid4())
    keyring.set_password(config.KEYRING_SYSTEM, config.KEYRING_USER, password)
    return password


@contextmanager
def sqlite_connection(filename: str):
    """Context manager for safe, fresh SQLite connections every time."""
    #Connect to the SQLite database with custom APSW build
    connection_string = (
        "file:{file}?zv=zlib&level=9&vfs=zipvfs&"
        "password256={password}"
        ).format(file=filename,
                 password=get_set_keyring_password())

    flags = (apsw.SQLITE_OPEN_READWRITE |
             apsw.SQLITE_OPEN_CREATE |
             apsw.SQLITE_OPEN_URI)

    connection = apsw.Connection(connection_string, flags=flags)
    cursor = connection.cursor()
    set_database_pragmas(cursor)
    yield cursor
    cursor.close()
    connection.close()
