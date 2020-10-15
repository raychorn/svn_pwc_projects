"""Query interface built on an open Oracle connection."""
#pylint: disable=no-member

import multiprocessing
import decimal
import cx_Oracle
import os

from .. import utils
from .base import ABCMessenger
from .. import config


LOGGER = multiprocessing.get_logger()
try:
    LOGGER.info("cx_Oracle.__version__ = {}".format(cx_Oracle.__version__))
except:
    LOGGER.info("cx_Oracle.__version__ = not imported")


def NumbersAsDecimal(cursor, name, defaultType, size, precision,
        scale):
    if defaultType == cx_Oracle.NUMBER:
        return cursor.var(str, 100, cursor.arraysize,
                outconverter = decimal.Decimal)



class OracleMessenger(ABCMessenger):
    """Interfaces with a single Oracle database."""

    def __init__(self, host: str, user: str, port: str = None,
                 password: str = None, system_id: str = None,
                 service_name: str = None, tnsname: str = None, *args, **kwargs):
        """Instantiate a new instance of a connector."""
        super().__init__()
        self._port = None
        self.port = port
        self._conn = connect_to_oracle(host, user, port, password,
                                       system_id, service_name, tnsname,
                                       *args, **kwargs)
        self._conn.outputtypehandler = NumbersAsDecimal
        self._extract_cursor = None
        self._extract_cursor = self._conn.cursor()
        self._extract_cursor.execute(
            "ALTER SESSION SET NLS_DATE_FORMAT = 'DD-Mon-YY'"
        )
        self._extract_cursor.execute(
            "ALTER SESSION SET NLS_TIMESTAMP_FORMAT = "
            "'MM-DD-YYYY HH.MI.SS.FF2 AM'"
        )

    @property
    def port(self):
        """Return validated port value for this connection."""
        return self._port

    @port.setter
    def port(self, value: int):
        assert value.isdigit(), 'Database port must be a numeric value'
        self._port = value

    def get_metadata_from_query(self, query: str) -> list:
        """Test the SQL query on the source database and return its metadata.

        ARGS:
            query: SQL query that defines the data to be extracted.

        RETURNS:
            metadata (list[dict[str]]): List of metadata dictionaries
                about the columns being read from source DB.
        """
        LOGGER.debug("Reading metadata from Oracle query: %s",
                     utils.cleanstr(query))
        cursor = self._conn.cursor()
        try:
            cursor.execute(query)
        except cx_Oracle.DatabaseError as error:
            LOGGER.error("Error caused by SQL query: %s",
                         utils.cleanstr(query))
            raise error

        metadata = []
        for column in cursor.description:
            # Unpack the data from the description tuple
            name = column[0].strip()
            datatype = column[1].__name__
            display_size = column[2]
            length = column[3]
            precision = column[4]
            scale = column[5]
            nullable = column[6]

            # Fix when Upgrade made to 6.0.2
            if datatype == 'NUMBER' and length is None:
                length = 22
            elif datatype == 'DATETIME' and length is None:
                length = 7
            elif length is None:
                length = 0

            if precision is None:
                precision = 0

            if scale is None:
                scale = 0

            # Determine MSSQL datatype from the Oracle datatype
            if datatype == 'NUMBER' and precision in (15, 38):
                mssql_datatype = 'BIGINT'
            elif datatype == 'NUMBER'and name.endswith('_ID'):
                mssql_datatype = 'BIGINT'
            elif datatype == 'NUMBER':
                mssql_datatype = 'FLOAT'
            elif datatype in ('DATETIME', 'TIMESTAMP'):
                mssql_datatype = 'DATETIME'
            elif datatype == 'STRING':
                mssql_datatype = 'NVARCHAR({})'.format(length)
            else:
                mssql_datatype = 'NVARCHAR(MAX)'

            col_metadata = {
                'sourceSystem': 'Oracle',
                'sourceFieldName': name,
                'sourceType': datatype,
                'sourceFieldLength': length,
                'sourceFieldNumericPrecision': precision,
                'sourceFieldNumericScale': scale,
                'sourceFieldNumericDisplaySize': display_size,
                'source_field_nullable': nullable,
                'targetFieldName': name,
                'sqlite_datatype': 'TEXT',
                'mssql_datatype': mssql_datatype,
            }
            metadata += [col_metadata]

        _validate_metadata_types(metadata, query)

        cursor.close()
        return metadata

    def get_metadata_from_query2(self, query: str) -> list:

        fields_str = ", ".join(query["Columns"])

        if query["Schema"] != "":
            table_str = ".".join([query["Schema"], query["Name"]])
        else:
            table_str = query["Name"]

        # if query["Parameters"]:
        #     where_str = ""
        #     for each in query["Parameters"]:
        #         if each["Operation"] == "IN":
        #             tmp_where = "{} IN ({})".format(each["Name"], ",".join(["'{}'".format(each) for each in each["Values"]]))
        #         elif each["Operation"] == "BETWEEN":
        #             tmp_where = "{} IN ({})".format(each["Name"],
        #                                             ",".join(["'{}'".format(each) for each in each["Values"]]))
        # else:
        #     where_str = None

        q = "SELECT {} FROM {} WHERE ROWNUM <= 1".format(fields_str, table_str)
        # if where:
        #     select_from += " WHERE {}".format(where_str)
        print(q)
        metadata = self.get_metadata_from_query(q)

        return metadata

    def drop_table_if_exists(self, table):
        """Drop a table from the database."""

        query = oracle_drop_table(table)
        cursor = self._conn.cursor()
        cursor.execute(query)
        self._conn.commit()
        cursor.close()

    def create_table(self, table, columns):
        """Create a new table on the database."""
        statement = oracle_create_table(table, columns)
        cursor = self._conn.cursor()
        cursor.execute(statement)

    def insert_into(self, table, data, columns):
        """Insert data into a table in the database."""
        statement = oracle_insert_into(table, columns)
        cursor = self._conn.cursor()
        for row in data:
            cursor.execute(statement, row)
        self._conn.commit()
        cursor.close()

    def begin_extraction(self, metadata, chunk_size=None):
        """Begin pulling data from a SQL query."""
        import six
        self._extract_cursor = self._conn.cursor()
        if chunk_size:
            self._extract_cursor.arraysize = chunk_size / 2
        self._extract_cursor.execute(metadata if (isinstance(metadata, six.string_types)) else metadata.parameters)

    def continue_extraction(self, chunk_size=None):
        """Continue pulling data from a query."""

        if self._extract_cursor:
            data = self._extract_cursor.fetchmany(chunk_size)
        return data

    def finish_extraction(self):
        """Clean up after a long-term extraction is finished"""
        self._extract_cursor.close()
        self._extract_cursor = None


def connect_to_oracle(host: str, user: str, port: str = None, password: str = None,
                      system_id: str = None, service_name: str = None,
                      tnsname: str = None, *args, **kwargs) -> cx_Oracle.Connection:
    """Return an open connection to an Oracle database.

    ARGS:
        server: Server the database is on.
        user: User ID to authenticate against.
        port: Port to connect to.
        password: Password to authenticate with.
        service_id: If supplied, service id to connect with.
        service_name: If supplied, service name to connect with.
        tnsname: If supplied, alias from TNSNAMES.ora to connect with.
    """
    # pylint: disable=too-many-arguments

    assert sum(bool(arg) for arg in (tnsname, system_id, service_name)), \
        'Must supply ONE of tnsname, service_id or a service_name'

    os.environ["NLS_LANG"] = "AMERICAN_AMERICA.AL32UTF8"
    if tnsname:
        return cx_Oracle.connect(user, password, tnsname)

    elif system_id:
        dsn = cx_Oracle.makedsn(host=host, port=port, sid=system_id)
        return cx_Oracle.connect(user, password, dsn, threaded=True)

    elif service_name:
        dsn = cx_Oracle.makedsn(host=host, port=port, service_name=service_name)
        return cx_Oracle.connect(user, password, dsn)


def oracle_create_table(table, columns, datatype='smallint'):
    """Return a CREATE TABLE statement for Oracle."""
    columns_dtypes = ', '.join([
        '{} {}'.format(col, datatype) for col in columns
    ])
    statement = """
        CREATE TABLE {table} ({columns_dtypes})
        """.format(table=table, columns_dtypes=columns_dtypes)
    return statement


def oracle_drop_table(table):
    """Return an Oracle statement that will drop a table if it exists."""
    statement = """
        BEGIN
           EXECUTE IMMEDIATE 'DROP TABLE {table}';
        EXCEPTION
           WHEN OTHERS THEN
              IF SQLCODE != -942 THEN
                 RAISE;
              END IF;
        END;
        """.format(table=table)
    return statement


def oracle_insert_into(table, columns):
    """Return an INSERT statement for Oracle with placeholder tokens."""
    numbers = range(1, len(columns) + 1)
    placeholders = (':{}'.format(i) for i in numbers)
    placeholders = ', '.join(placeholders)
    statement = """
        INSERT INTO {table} ({columns}) VALUES ({placeholders})
        """.format(table=table,
                   columns=', '.join(columns),
                   placeholders=placeholders)
    return statement


def _validate_metadata_types(metadata: dict, query: str):
    """Validates no records in the query are of a data type which is disallowed"""

    erroneous_columns = []

    for column in metadata:
        if column['sourceType'] in config.BANNED_ORCL_DTYPES:
            LOGGER.error("WARNING: column %s is not of a supported data type. "
                         "Removing table from extraction due to unsupported data type %s.",
                         column['sourceFieldName'], column['sourceType'])
            erroneous_columns.append(column['sourceFieldName'])

    assert not erroneous_columns, \
        "Query: {} will be skipped due to disallowed datatypes".format(query)
