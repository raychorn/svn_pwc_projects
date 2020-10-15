"""Query interface built on an open DB2 connection."""

import multiprocessing
import sys

import ibm_db_dbi as ibm

from .base import ABCMessenger
from .. import config
from .. import utils


LOGGER = multiprocessing.get_logger()


class DB2Messenger(ABCMessenger):
    """Interfaces with a single DB2 database."""

    def __init__(self, host: str, database: str, port: str = None,
                 protocol='TCPIP', schema: str = None, user: str = None,
                 password: str = None):
        """Instantiate a new instance of a connector."""
        super().__init__()
        self.schema = schema
        self._port = None
        self.port = port
        self._conn = connect_to_db2(host, database,
			            port, protocol,
                                    user, password)
        self._extract_cursor = self._conn.cursor()

        #using attributes below for the write workers
        self.host = host
        self.database = database
        self.protocol = protocol
        self.schema = schema
        self.user = user
        self.password = password

    @property
    def port(self):
        """Return the validated port value for this messenger."""
        return self._port

    @port.setter
    def port(self, port_val):
        assert port_val.isdigit(), 'The Port entered is invalid. Please enter a numeric value'
        self._port = port_val

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
        except ibm.Error as error:
            LOGGER.error('IBM DB2 error "%s" caused by statement:  %s',
                         error, utils.cleanstr(statement))
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

        LOGGER.debug("Reading metadata from DB2 database cursor.")
        cursor = self._conn.cursor()
        try:
            cursor.execute(query)
        except ibm.Error as error:
            LOGGER.error('IBM DB2 error "%s" caused by query:  %s',
                         error, utils.cleanstr(query))
            cursor.close()
            raise error

        metadata = []
        for column in cursor.description:
            # Unpack the data from the description tuple
            name = column[0].strip()
            datatype = column[1].col_types[1]
            # display_size = column[2]
            length = column[3]
            precision = column[4]
            # scale = column[5]
            nullable = column[6]

            # Determine MSSQL datatype from the DB2 datatype
            if datatype == 'NUMBER' and precision in (15, 38):
                db2_datatype = 'BIGINT'
            elif datatype == 'NUMBER'and name.endswith('_ID'):
                db2_datatype = 'BIGINT'
            elif datatype == 'NUMBER':
                db2_datatype = 'FLOAT'
            elif datatype in ('DATETIME', 'TIMESTAMP'):
                db2_datatype = 'DATETIME'
            elif datatype == 'STRING':
                db2_datatype = 'NVARCHAR({})'.format(length)
            else:
                db2_datatype = 'NVARCHAR(MAX)'

            col_metadata = {
                'sourceSystem': 'DB2',
                'sourceFieldName': name,
                'sourceType': datatype,
                'sourceFieldLength': length,
                'sourceFieldNumericPrecision': precision,
                'source_field_nullable': nullable,
                'targetFieldName': name,
                'sqlite_datatype': 'TEXT',
                'db2_datatype': db2_datatype,
            }
            metadata += [col_metadata]

        _validate_metadata_types(metadata, query)

        cursor.close()
        return metadata

    def list_all_tables(self):
        """Return a list of tables that exist in this Messengers schema."""

        query = """
            SELECT tabname
            FROM syscat.tables
            WHERE tabschema = '{schema}'
            ORDER BY tabname
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

        if table[0] == '_':
            table = table[1:]

        if self.table_exists(table):
            LOGGER.warning('Table %s already exists in schema %s.',
                           table, self.schema)
            return

        LOGGER.info("Creating table '%s' in DB2 database", table)
        self.create_schema_if_not_exists()

        statement = db2_create_table(table, columns, datatypes,
                                     schema=self.schema)

        cursor = self._conn.cursor()

        try:
            cursor.execute(statement)
        except ibm.Error as error:
            LOGGER.warning('Table %s already exists in schema %s. Error: %s',
                           table, self.schema, error)
            raise
        else:
            self._conn.commit()
            LOGGER.info("Created db2 table with statement: %s.",
                        utils.cleanstr(statement))
        finally:
            cursor.close()

    def table_exists(self, table):
        """Return True if a table already exists in this database."""

        if table[0] == '_':
            table = table[1:]

        query = """SELECT tabname
                   FROM syscat.tables
                   WHERE tabschema = '{schema}'
                   AND tabname = '{table}'
                """.format(schema=self.schema, table=table.upper())
        cursor = self._conn.cursor()
        cursor.execute(query)
        row = cursor.fetchall()

        cursor.close()

        return bool(row)

    def drop_table_if_exists(self, table):
        """Drop a table from the database."""

        if table[0] == '_':
            table = table[1:]

        if self.table_exists(table):
            query = db2_drop_table_if_exists(table, self.schema)
            cursor = self._conn.cursor()
            cursor.execute(query)
            self._conn.commit()
            cursor.close()

    def insert_into(self, table, data, metadata=None):
        """Bulk insert data into a table on this database.

        APSW doesn't support cursor row count, so we cannot use that to
        get the number of rows committed back.

        ARGS:
            table (str): Name of the table in db2 to insert into.
            data (tuple[tuple[str]]): String data held in a 2D tuple.
            metadata (list[dict[str]]): List of metadata dicts
                about the columns being written to the source DB.
        """

        if table[0] == '_':
            table = table[1:]

        cursor = self._conn.cursor()
        statement = db2_insert_into(table=table, schema=self.schema,
                                    number_columns=len(data[0]))

        LOGGER.debug("Executing SQL statement with %d rows of data: %s",
                     len(data), utils.cleanstr(statement))

        try:
            cursor.executemany(statement, _ibm_safe_data(data))
            self._conn.commit()
        except ibm.Error as error:
            LOGGER.error("Data failed to insert into DB2: %s", error)
            raise RuntimeError
        else:
            LOGGER.debug("Data successfully inserted into DB2")
        finally:
            cursor.close()

    def schema_exists_in_database(self):
        """Return True if this Connector's schema exists in this database."""
        query = db2_schema_exists(self.schema)
        cursor = self._conn.cursor()
        cursor.execute(query)
        row = cursor.fetchall()
        cursor.close()
        return bool(row)

    def drop_schema_from_database(self):
        """Drop this Connector's schema from the database."""

        query = db2_drop_schema(self.schema)
        cursor = self._conn.cursor()
        try:
            cursor.execute(query)
        except ibm.Error as error:
            LOGGER.warning('Failed to drop schema with error: %s', error)
        else:
            self._conn.commit()
        finally:
            cursor.close()

    def create_schema_if_not_exists(self):
        """Create a new schema if one does not yet exist."""

        if self.schema_exists_in_database():
            return

        query = db2_create_schema(self.schema)
        cursor = self._conn.cursor()
        cursor.execute(query)
        self._conn.commit()
        cursor.close()

    def create_metadata_table(self, metadata: utils.DataDefinition, table: str):
        """Writes extraction metadata to a DB2 table."""

        if table[0] == '_':
            table = table[1:]

        if self.table_exists(table):
            LOGGER.warning('Table %s already exists in schema %s.',
                           table, self.schema)
        else:
            self.create_schema_if_not_exists()
            statement = db2_create_table(table=table,
                                         columns=metadata.metadata_columns,
                                         schema=self.schema)

            LOGGER.debug("Creating metadata table in DB2 with query: %s",
                         utils.cleanstr(statement))
            cursor = self._conn.cursor()
            cursor.execute(statement)
            self._conn.commit()
            cursor.close()

        metadata_info = metadata.as_table()

        for row_num, record in enumerate(metadata_info):
            for col_num, col in enumerate(record):
                if str(col) == 'False':
                    metadata_info[row_num][col_num] = True

        LOGGER.info("Inserting metadata into db2 table.")
        self.insert_into(table, metadata_info)
        LOGGER.debug("Finished writing metadata to db2 table.")

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


def connect_to_db2(host: str, database: str, port: str, protocol: str,
                   user: str, password: str) -> ibm.Connection:
    """Return an open connection to a DB2 database.

    ARGS:
        host: Host server the db is on.
        database: Name of the database the user is to connect to
        port: Port to connect to.
        protocol: Protocol to be used for the connection (e.g. TCIPIP)
        user: User ID to authenticate against.
        password: Password to authenticate with.
    """
    connstr = """
        HOSTNAME={0}; DATABASE={1}; PORT={2}; PROTOCOL={3}; UID={4}; PWD={5};
        """.format(host, database, port, protocol, user, password)
    connstr = utils.cleanstr(connstr)

    return ibm.connect(connstr)


def _ibm_safe_data(data):
    """Return 2D list of data with strs coerced to unicode for ibm."""
    #pylint: disable=undefined-variable

    if sys.version_info >= (3, 0):
        return list(data)

    return [
        tuple(unicode(item) if isinstance(item, str) else item
              for item in row)
        for row in data]


def db2_create_schema(schema):
    """Return a DB2 query that creates a schema."""
    query = "CREATE SCHEMA {schema}".format(schema=schema)
    return query


def db2_create_table(table, columns, datatypes=None, schema=None):
    """Return a CREATE TABLE statement for DB2."""

    datatypes = ['VARGRAPHIC(16300)' for _ in columns]

    if len(columns) != len(datatypes):
        raise AttributeError('len(datatypes) must equal len(columns)')

    if schema is None:
        schema = 'dbo'

    columns_datatypes = zip(columns, datatypes)
    columns_datatypes = ['{} {} NULL'.format(c, d) for c, d in columns_datatypes]
    joined = ', '.join(columns_datatypes)

    if table[0] == '_':
        table = table[1:]

    statement = "CREATE TABLE {schema}.{table} ({columns_dtypes})".format(schema=schema,
                                                                          table=table,
                                                                          columns_dtypes=joined)
    return statement


def db2_drop_schema(schema):
    """Return a DB2 query that drops a schema."""
    query = "DROP SCHEMA [{schema}]".format(schema=schema)
    return query


def db2_drop_table_if_exists(table, schema=None):
    """Return a DB2 statement that will drop a table if it exists."""
    if table[0] == '_':
        table = table[1:]

    if schema:
        statement = 'DROP TABLE {schema}.{table}'.format(schema=schema, table=table)
    else:
        statement = 'DROP TABLE {table}'.format(table=table)

    return statement


def db2_insert_into(table, schema=None, columns=None, number_columns=None):
    """Return an INSERT statement for DB2 with placeholder tokens."""

    if table[0] == '_':
        table = table[1:]

    if not columns and not number_columns:
        raise ValueError("must provide 'columns' list or 'number_columns'")

    if schema is None:
        target = "{}".format(table)
    else:
        target = "{}.{}".format(schema, table)

    if columns is None:
        column_string = ''
        placeholders = ', '.join(['?'] * number_columns)
    else:
        column_string = '({})'.format(', '.join(columns))
        placeholders = ', '.join(['?'] * len(columns))

    statement = """INSERT INTO {} {} VALUES ({})""".format(target, column_string, placeholders)

    return statement


def db2_schema_exists(schema):
    """Return a DB2 query that checks if a schema exists."""
    query = """
        SELECT schemaname
        FROM syscat.schemata
        WHERE schemaname = '{schema}'
        """.format(schema=schema)
    return query


def _validate_metadata_types(metadata: dict, query: str):
    """Validates no records in the query are of a data type which is disallowed"""

    erroneous_columns = []

    for column in metadata:
        if column['sourceType'] in config.BANNED_DB2_DTYPES:
            LOGGER.error("WARNING: column %s is not of a supported data type. "
                         "Removing table from extraction due to unsupported data type %s.",
                         column['sourceFieldName'], column['sourceType'])
            erroneous_columns.append(column['sourceFieldName'])

    assert not erroneous_columns, \
        "Query: {} will be skipped due to disallowed datatypes".format(query)
