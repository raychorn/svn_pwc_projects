"""Query interface built on an open MSSQL connection."""
#pylint: disable=no-member

import getpass
import multiprocessing
import sys

import ceODBC

from .. import utils
from .base import ABCMessenger
from .. import config

LOGGER = multiprocessing.get_logger()


class MSSQLMessenger(ABCMessenger):
    """Interfaces with a single MSSQL database."""

    def __init__(self, schema=None, connection_string=None, *args, **kwargs):
        """Instantiate a new instance of a connector.

        ARGS:
            schema (str): Name of the schema to write results to.
            connection_string (str): Connection string for MSSQL.
        """
        super().__init__()

        if connection_string is None:
            connection_string = mssql_connection_string(*args, **kwargs)
        self.connection_string = connection_string

        try:
            self._conn = ceODBC.connect(connection_string)
        except ceODBC.DatabaseError as error:
            LOGGER.error("Failed to connect to ceODBC with error: %s", error)
            raise error

        self.schema = schema
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

        LOGGER.debug("Reading metadata from MSSQL database cursor.")
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

            col_metadata = {
                'sourceSystem': 'MSSQL',
                'sourceFieldName': column[0].strip(),
                'sourceType': config.CEODBC_TO_MSSQL_DTYPES[datatype],
                'sourceFieldLength': column[3],
                'sourceFieldNumericPrecision': column[4],
                'source_field_nullable': column[6],
                'targetFieldName': column[0],
                'sqlite_datatype': sqlite_datatype,
            }
            metadata += [col_metadata]

        _validate_metadata_types(metadata, query)

        cursor.close()
        return metadata

    def list_all_tables(self):
        """Return a list of tables that exist in this Messengers schema."""

        query = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
            ORDER BY TABLE_NAME
            """.format(schema=self.schema)

        cursor = self._conn.cursor()
        cursor.execute(query)
        data = cursor.fetchall()
        cursor.close()

        return [row[0] for row in data]

    def queries_for_all_data(self):
        """Return list of queries that covers all tables/data available."""
        tables = self.list_all_tables()
        queries = ["SELECT * FROM [{}].[{}]".format(self.schema, table)
                   for table in tables]
        return queries

    def create_table(self, table, columns, datatypes=None):
        """Create a new table for storing extraction results.

        ARGS:
            table (str): The table name to create in the database.
        """

        if self.table_exists(table):
            LOGGER.warning('Table %s already exists in schema %s.',
                           table, self.schema)
            return

        LOGGER.info("Creating table '%s' in MSSQL database", table)
        self.create_schema_if_not_exists()

        statement = mssql_create_table(table, columns, datatypes,
                                       schema=self.schema)

        cursor = self._conn.cursor()
        try:
            cursor.execute(statement)
        except ceODBC.DatabaseError as error:
            LOGGER.warning('Table %s already exists in schema %s. Error: %s',
                           table, self.schema, error)
            raise
        else:
            self._conn.commit()
            LOGGER.info("Created MSSQL table with statement: %s.",
                        utils.cleanstr(statement))
        finally:
            cursor.close()

    def table_exists(self, table):
        """Return True if a table already exists in this database."""

        query = """
            SELECT * FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
            AND TABLE_NAME = '{table}'
            """.format(schema=self.schema, table=table)

        cursor = self._conn.cursor()
        cursor.execute(query)
        row = cursor.fetchall()
        cursor.close()

        return bool(row)

    def drop_table_if_exists(self, table):
        """Drop a table from the database."""

        query = mssql_drop_table_if_exists(table, self.schema)
        cursor = self._conn.cursor()
        cursor.execute(query)
        self._conn.commit()
        cursor.close()

    def insert_into(self, table, data, metadata=None):
        """Bulk insert data into a table on this database.

        APSW doesn't support cursor row count, so we cannot use that to
        get the number of rows committed back.

        ARGS:
            table (str): Name of the table in MSSQL to insert into.
            data (tuple[tuple[str]]): String data held in a 2D tuple.
            metadata (list[dict[str]]): List of metadata dicts
                about the columns being written to the source DB.
        """
        cursor = self._conn.cursor()
        statement = mssql_insert_into(table=table, schema=self.schema,
                                      number_columns=len(data[0]))

        LOGGER.debug("Executing SQL statement with %d rows of data: %s",
                     len(data), utils.cleanstr(statement))

        if metadata:
            # Specify datatypes to insert into MSSQL using metadata
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
            cursor.executemany(statement, _ceodbc_safe_data(data))
            self._conn.commit()
        except ceODBC.Error as error:
            LOGGER.error("Data failed to insert into MSSQL: %s", error)
            raise error
        else:
            LOGGER.debug("Data successfully inserted into MSSQL")
        finally:
            cursor.close()

    def rows_exist(self, table, where_clause):
        """Return true if a table has rows that meet the where clause"""
        query = mssql_rows_exist(table=table, schema=self.schema,
                                 where_clause=where_clause)
        cursor = self._conn.cursor()
        cursor.execute(query)
        row = cursor.fetchall()
        cursor.close()

        return bool(row)

    def update_records(self, table, columns, updated_values, where_condition=None):
        """method to update records in the database"""
        cursor = self._conn.cursor()
        statement = mssql_update_rows(table=table, schema=self.schema,
                                      columns=columns, updated_values=updated_values,
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

    def schema_exists_in_database(self):
        """Return True if this Connector's schema exists in this database."""
        query = mssql_schema_exists(self.schema)
        cursor = self._conn.cursor()
        cursor.execute(query)
        row = cursor.fetchall()
        cursor.close()
        return bool(row)

    def drop_schema_from_database(self):
        """Drop this Connector's schema from the database."""

        query = mssql_drop_schema(self.schema)
        cursor = self._conn.cursor()
        try:
            cursor.execute(query)
        except ceODBC.DatabaseError as error:
            LOGGER.warning('Failed to drop schema with error: %s', error)
        else:
            self._conn.commit()
        finally:
            cursor.close()

    def create_schema_if_not_exists(self):
        """Create a new schema if one does not yet exist."""

        if self.schema_exists_in_database():
            return

        query = mssql_create_schema(self.schema)
        cursor = self._conn.cursor()
        cursor.execute(query)
        self._conn.commit()
        cursor.close()

    def create_metadata_table(self, metadata: utils.DataDefinition, table: str):
        """Writes extraction metadata to a MSSQL table."""

        if self.table_exists(table):
            LOGGER.warning('Table %s already exists in schema %s.',
                           table, self.schema)
        else:
            self.create_schema_if_not_exists()
            statement = mssql_create_table(table=table,
                                           columns=metadata.metadata_columns,
                                           schema=self.schema)

            LOGGER.debug("Creating metadata table in MSSQL with query: %s",
                         utils.cleanstr(statement))
            cursor = self._conn.cursor()
            cursor.execute(statement)
            cursor.close()

        LOGGER.info("Inserting metadata into MSSQL table.")
        self.insert_into(table, metadata.as_table())
        LOGGER.debug("Finished writing metadata to MSSQL table.")

    def validate_schema(self):
        """Method to validate the schema being used is a valid within the
            specified database
        """
        if self.schema:
            cursor = self._conn.cursor()
            cursor.execute("""SELECT SCHEMA_NAME
                              FROM INFORMATION_SCHEMA.SCHEMATA
                              WHERE SCHEMA_NAME = '{}'""".format(self.schema))
            if cursor.fetchall():
                return
            else:
                raise Exception('Invalid Schema: Does not exist in database')

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


def mssql_connection_string(host: str, database: str, user: str = None,
                            port: str = None, app: str = None,
                            password: str = None, driver: str = None, *args, **kwargs):

    """Return a connection string for a ceODBC MSSQL connection.

    ARGS:
        server: Server the database is on.
        database: Name of the database to connect to.
        user: User ID to authenticate against.
        port: Port to connect to.
        app: Application name to connect to.
        password: Password to authenticate with. If None,
            connection will attempt to use native authentication.

    RETURNS:
        (str): ceODBC connection string to MSSQL database.
    """

    # pylint: disable=too-many-arguments
    assert host, 'Cannot connect to MSSQL without a "host" value'
    assert database, 'Cannot connect to MSSQL without a "database" value'

    if user is None:
        user = getpass.getuser()

    if port:
        assert ',' not in host, \
            'MSSQL host may not contain a comma when providing a port'
        host += ',' + port

    connection_string = (
        'DRIVER={{{driver}}};'
        'SERVER={host};'
        'DATABASE={database};'
        'UID={user};'
    ).format(driver=driver, host=host, user=user, database=database)

    if app:
        connection_string += 'APP={};'.format(app)

    if password:
        connection_string += 'PWD={};'.format(password)
        connection_string += 'autocommit=False;'
    else:
        connection_string += 'Trusted_Connection=Yes;'
    print("DEBUG: {}".format(connection_string))
    return connection_string


def connect_to_mssql(*args, **kwargs):
    """Return an open connection to the target MSSQL database.

    ARGS:
        **kwargs: See 'mssql_connection_string'.

    RETURNS:
        (ceODBC.Connection): Open connection to target MSSQL database.
    """

    connection_string = mssql_connection_string(*args, **kwargs)
    connection = ceODBC.connect(connection_string)
    return connection


def mssql_create_schema(schema):
    """Return a MSSQL query that creates a schema."""
    query = "CREATE SCHEMA [{schema}]".format(schema=schema)
    return query


def mssql_create_table(table, columns, datatypes=None, schema=None):
    """Return a CREATE TABLE statement for MSSQL."""

    if datatypes is None:
        datatypes = ['NVARCHAR(MAX)' for _ in columns]

    if len(columns) != len(datatypes):
        raise AttributeError('len(datatypes) must equal len(columns)')

    if schema is None:
        schema = 'dbo'

    columns_datatypes = zip(columns, datatypes)
    columns_datatypes = ['[{}] {} NULL'.format(c, d) for c, d in columns_datatypes]
    joined = ', '.join(columns_datatypes)

    statement = """
        CREATE TABLE [{schema}].[{table}] ({columns_dtypes});
        """.format(schema=schema, table=table, columns_dtypes=joined)

    return statement


def mssql_drop_schema(schema):
    """Return a MSSQL query that drops a schema."""
    query = "DROP SCHEMA [{schema}]".format(schema=schema)
    return query


def mssql_drop_table_if_exists(table, schema=None):
    """Return a MSSQL statement that will drop a table if it exists."""

    if schema:
        statement = """
            IF OBJECT_ID('{schema}.{table}', 'U') IS NOT NULL
                DROP TABLE [{schema}].[{table}]
            """.format(schema=schema, table=table)
    else:
        statement = """
            IF OBJECT_ID('{table}', 'U') IS NOT NULL
                DROP TABLE [{table}]
            """.format(table=table)

    return statement


def mssql_insert_into(table: str, schema: str = None, columns: list = None,
                      number_columns: int = None) -> str:
    """Return an INSERT statement for MSSQL with placeholder tokens."""

    if not columns and not number_columns:
        raise ValueError("must provide 'columns' list or 'number_columns'")

    if schema is None:
        target = "[{}]".format(table)
    else:
        target = "[{}].[{}]".format(schema, table)

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


def mssql_rows_exist(table: str, schema: str = None,
                     where_clause: str = None) -> str:
    """A query that checks if a table exists in a MSSQL database."""

    if schema is None:
        target = "[{}]".format(table)
    else:
        target = "[{}].[{}]".format(schema, table)

    statement = """
       SELECT *
       FROM {}
       WHERE {}
       """.format(target, where_clause)

    return statement


def mssql_update_rows(table, schema: str = None, columns: list = None,
                      updated_values: list = None, condition: str = None):
    """Return a UPDATE statement for MSSQL for the provided values"""
    if not columns and not updated_values and len(columns) != len(updated_values):
        raise ValueError("Must provide columns and values of equal length for a valid update")

    if schema is None:
        target = "[{}]".format(table)
    else:
        target = "[{}].[{}]".format(schema, table)

    set_condition = ', '.join(
        "{} = '{}'".format(col, val)
        for col, val in zip(columns, updated_values)
    )

    statement = """
       UPDATE {}
       SET {}
       WHERE {}
       """.format(target, set_condition, condition)

    return statement


def mssql_schema_exists(schema):
    """Return a MSSQL query that checks if a schema exists."""
    query = """
        SELECT * FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME = '{schema}'
        """.format(schema=schema)
    return query


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
