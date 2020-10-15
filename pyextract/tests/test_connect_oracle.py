"""Tests for the pyextract.connect.oracle module."""

import unittest

from pyextract.connect import oracle

from tests import CREDENTIALS


class TestOracleConnection(unittest.TestCase):
    """Can connect to and query Oracle."""

    def test_direct_query(self):
        """Can connect to Oracle with test credentials and directory query it."""

        connection = oracle.connect_to_oracle(**CREDENTIALS['EBSR12'])
        cursor = connection.cursor()

        query = 'SELECT owner, table_name FROM dba_tables'
        cursor.execute(query)
        result = cursor.fetchone()
        expected = ('SYS', 'ASSEMBLY$')

        self.assertEqual(result, expected)

        cursor.close()
        connection.close()


class TestOracleQueries(unittest.TestCase):
    """Can create valid oracle for an Oracle database."""

    def test_oracle_create_table(self):
        """Can build a statement that creates a new table."""

        result = oracle.oracle_create_table('TestTable',
                                            columns=('col1', 'col2', 'col3'),
                                            datatype='int')
        expected = """
            CREATE TABLE TestTable (col1 int, col2 int, col3 int)
            """
        self.assertEqual(result.split(), expected.split())

    def test_oracle_drop_table(self):
        """Can build a statement that drops a table."""

        result = oracle.oracle_drop_table('TestTable')
        expected = """
            BEGIN
               EXECUTE IMMEDIATE 'DROP TABLE TestTable';
            EXCEPTION
               WHEN OTHERS THEN
                  IF SQLCODE != -942 THEN
                     RAISE;
                  END IF;
            END;
            """
        self.assertEqual(result.split(), expected.split())

    def test_oracle_insert_into(self):
        """Can build a statement that inserts data into a table."""

        result = oracle.oracle_insert_into('TestTable',
                                           columns=('col1', 'col2', 'col3'))
        expected = """
            INSERT INTO TestTable (col1, col2, col3) VALUES (:1, :2, :3)
            """
        self.assertEqual(result.split(), expected.split())
