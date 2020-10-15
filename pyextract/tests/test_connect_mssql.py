"""Tests for the pyextract.connect.mssql module."""
# pylint: disable=no-member

import unittest

import ceODBC

from pyextract.connect import mssql

from tests import CREDENTIALS

CONNECTION = mssql.MSSQLMessenger(schema='_test_pyextract', **CREDENTIALS['MSSQL'])


class TestBasicConnection(unittest.TestCase):
    """Can open a basic connection to MSSQL with testing credentials."""

    def test_init_mssql_connector(self):
        """Can instantiate a basic MSSQL connection class."""
        # pylint: disable=protected-access
        self.assertIsInstance(CONNECTION, mssql.MSSQLMessenger)
        self.assertIsInstance(CONNECTION._conn, ceODBC.Connection)
        cursor = CONNECTION._conn.cursor()
        self.assertIsInstance(cursor, ceODBC.Cursor)
        cursor.close()

    def test_fetch_data(self):
        """Can get results from a query sent to MSSQL."""
        query = "SELECT TOP 1 * FROM INFORMATION_SCHEMA.TABLES"
        result = CONNECTION.fetch_data(query)
        self.assertNotEqual(len(result), 0)


class TestMSSQLMessenger(unittest.TestCase):
    """Can interact with MSSQL through a Connector object."""

    tablename = '_test_pyextract_table'
    rows = 10
    cols = 10

    @classmethod
    def setUpClass(cls):
        """Create the schema if it does not exist."""
        CONNECTION.create_schema_if_not_exists()

    @classmethod
    def tearDownClass(cls):
        """Delete the schema from the database."""
        CONNECTION.drop_schema_from_database()

    def setUp(self):
        """Write a table with test data to MSSQL."""
        CONNECTION.drop_table_if_exists(self.tablename)

        # Create a new 10-column table for test data
        column_names = ['column{}'.format(i) for i in range(self.cols)]
        column_values = ', '.join('{} tinyint'.format(c) for c in column_names)
        statement = """
            CREATE TABLE [{schema}].[{table}] ({column_values});
            """.format(schema=CONNECTION.schema,
                       table=self.tablename,
                       column_values=column_values)
        CONNECTION.execute_statement(statement)

        # Create the SQL statement that will insert test data into SQL
        placeholders = ', '.join(['?'] * len(column_names))
        statement = """
            INSERT INTO [{schema}].[{table}] ({columns}) VALUES ({placeholders})
            """.format(schema=CONNECTION.schema,
                       table=self.tablename,
                       columns=', '.join(column_names),
                       placeholders=placeholders)

        # Create the tuple array of zeroes for test data
        testdata = tuple(
            tuple(0 for _ in range(self.cols)) for _ in range(self.rows)
        )
        CONNECTION.insert_into(self.tablename, testdata)

    def tearDown(self):
        """Drop test table from SQL, close connect, delete output."""
        CONNECTION.drop_table_if_exists(self.tablename)

    def test_drop_table(self):
        """Can drop a table from a database."""
        self.assertTrue(CONNECTION.table_exists(self.tablename))
        CONNECTION.drop_table_if_exists(self.tablename)
        self.assertFalse(CONNECTION.table_exists(self.tablename))

    def test_create_table(self):
        """Can create a table on a database."""
        CONNECTION.drop_table_if_exists(self.tablename)
        self.assertFalse(CONNECTION.table_exists(self.tablename))

        columns = ['column{}'.format(i) for i in range(self.cols)]
        dtypes = ['tinyint'] * self.cols
        CONNECTION.create_table(self.tablename, columns, dtypes)

        self.assertTrue(CONNECTION.table_exists(self.tablename))

    def test_insert_into(self):
        """Can insert data into a table on a database."""

        select_all_query = """
            SELECT * FROM [{schema}].[{table}]
            """.format(schema=CONNECTION.schema,
                       table=self.tablename)

        # Assert that the current table is (rows x columns) large
        rows = CONNECTION.fetch_data(select_all_query)
        self.assertEqual(len(rows), self.rows)
        self.assertEqual(len(rows[0]), self.cols)

        # Insert new data into the table
        data = tuple(
            tuple(0 for _ in range(self.cols)) for _ in range(self.rows)
        )
        CONNECTION.insert_into(self.tablename, data)

        # Assert that the table now has twice as many rows
        rows = CONNECTION.fetch_data(select_all_query)
        self.assertEqual(len(rows), self.rows * 2)
        self.assertEqual(len(rows[0]), self.cols)


class TestMSSQLQueries(unittest.TestCase):
    """Can create valid queries for an MSSQL database."""

    def test_mssql_create_schema(self):
        """Can build a statement that creates a new schema."""

        result = mssql.mssql_create_schema('TestSchema')
        expected = "CREATE SCHEMA [TestSchema]"
        self.assertEqual(result.split(), expected.split())

    def test_mssql_create_table(self):
        """Can build a statement that creates a new table."""

        result = mssql.mssql_create_table(table='TestTable',
                                          schema='TestSchema',
                                          columns=('col1', 'col2', 'col3'),
                                          datatypes=('TEXT', 'REAL', 'INT'))
        expected = """
            CREATE TABLE [TestSchema].[TestTable]
                ([col1] TEXT NULL, [col2] REAL NULL, [col3] INT NULL);
            """
        self.assertEqual(result.split(), expected.split())

    def test_mssql_drop_schema(self):
        """Can build a statement that drops an existing schema."""

        result = mssql.mssql_drop_schema('TestSchema')
        expected = "DROP SCHEMA [TestSchema]"
        self.assertEqual(result.split(), expected.split())

    def test_mssql_drop_table_if_exists(self):
        """Can build a statement that drops a table if it exists."""

        result = mssql.mssql_drop_table_if_exists(table='TestTable')
        expected = """
            IF OBJECT_ID('TestTable', 'U') IS NOT NULL
                DROP TABLE [TestTable]
            """
        self.assertEqual(result.split(), expected.split())

        result = mssql.mssql_drop_table_if_exists(table='TestTable',
                                                  schema='TestSchema')
        expected = """
            IF OBJECT_ID('TestSchema.TestTable', 'U') IS NOT NULL
                DROP TABLE [TestSchema].[TestTable]
            """
        self.assertEqual(result.split(), expected.split())

    def test_mssql_insert_into(self):
        """Can build a statement that inserts data into a table."""

        result = mssql.mssql_insert_into(table='TestTable',
                                         schema='TestSchema',
                                         columns=('col1', 'col2', 'col3'))
        expected = """
            INSERT INTO [TestSchema].[TestTable]
                (col1, col2, col3) VALUES (?, ?, ?)
            """
        self.assertEqual(result.split(), expected.split())

        result = mssql.mssql_insert_into(table='TestTable', number_columns=5)
        expected = """
            INSERT INTO [TestTable] VALUES (?, ?, ?, ?, ?)
            """
        self.assertEqual(result.split(), expected.split())

    def test_mssql_schema_exists(self):
        """Can build a statement that tests whether a schema exists."""

        result = mssql.mssql_schema_exists('TestSchema')
        expected = """
            SELECT * FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME = 'TestSchema'
            """
        self.assertEqual(result.split(), expected.split())
