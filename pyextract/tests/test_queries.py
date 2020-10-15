"""Tests for the pyextract.queries module."""

import unittest

from pyextract import queries


class TestParsedQuery(unittest.TestCase):
    """Can parse a generic SQL query into components."""

    def test_init(self):
        """Can create a ParsedQuery object with a basic query."""

        query = "SELECT * FROM TestTable"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.query, query)

    def test_table(self):
        """Can parse the table name from a query."""

        query = "SELECT * FROM TestTable"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.table, 'TestTable')

        query = "SELECT * FROM [TestTable]"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.table, 'TestTable')

    def test_columns(self):
        """Can parse specific columns to select from a query."""

        query = "SELECT * FROM TestTable"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.columns, None)

        query = "SELECT col1, col2, col3 FROM TestTable"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.columns, ['col1', 'col2', 'col3'])

    def test_column_aliases(self):
        """Can parse aliased columns to select from a query."""

        query = """
            SELECT TestTable.col1, TestTable.col2, TestTable.col3
            FROM TestTable
            """
        result = queries.ParsedQuery(query)
        self.assertEqual(result.columns, ['col1', 'col2', 'col3'])

    def test_table_alias(self):
        """Can parse aliased columns with aliased table from a query."""

        query = """
            SELECT A.col1, A.col2, A.col3
            FROM TestTable AS A
            """
        result = queries.ParsedQuery(query)
        self.assertEqual(result.table, 'TestTable')
        self.assertEqual(result.columns, ['col1', 'col2', 'col3'])

    def test_row_limit(self):
        """Can parse a row limit from a query."""

        query = "SELECT * FROM TestTable"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.limit, None)

        query = "SELECT TOP 1000 * FROM TestTable"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.limit, 1000)

        query = "SELECT TOP 2000 col1, col2, col3 FROM TestTable"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.limit, 2000)

    def test_schema(self):
        """Can parse a schema from a query."""

        query = "SELECT * FROM TestTable"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.schema, None)

        query = "SELECT * FROM TestSchema.TestTable"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.schema, 'TestSchema')

        query = "SELECT * FROM [TestSchema].[TestTable]"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.schema, 'TestSchema')

    def test_where_simple(self):
        """Can parse a simple where statement from a query."""

        query = "SELECT * FROM TestTable"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.where, None)

        query = "SELECT * FROM TestTable WHERE Field1 = 'foo'"
        result = queries.ParsedQuery(query)
        self.assertEqual(result.where, ["Field1 = 'foo'"])

    def test_where_multi_word(self):
        """Can parse a multi-word where clause from a query."""

        query = """
            SELECT * FROM TestTable
            WHERE Field1 = 'multi word string'
            """
        result = queries.ParsedQuery(query)
        self.assertEqual(result.where, ["Field1 = 'multi word string'"])

    def test_where_two_statements(self):
        """Can parse multiple where filters from a query."""

        query = """
            SELECT * FROM TestTable
            WHERE Field1 = 'foo' AND Field2 = 'bar'
        """
        result = queries.ParsedQuery(query)
        self.assertEqual(result.where, ["Field1 = 'foo'", "Field2 = 'bar'"])

    def test_where_aliases(self):
        """Can parse multiple where filters from a query."""

        query = """
            SELECT *
            FROM TestTable
            WHERE TestTable.Field1 = 'foo'
            AND TestTable.Field2 = 'bar'
        """
        result = queries.ParsedQuery(query)
        self.assertEqual(result.where, ["Field1 = 'foo'", "Field2 = 'bar'"])

        query = """
            SELECT *
            FROM TestTable AS A
            WHERE A.Field1 = 'foo'
            AND A.Field2 = 'bar'
        """
        result = queries.ParsedQuery(query)
        self.assertEqual(result.where, ["Field1 = 'foo'", "Field2 = 'bar'"])

    def test_where_all_operators(self):
        """Can parse all approved operators into distinct WHERE clauses."""

        query = """
            SELECT * FROM TestTable
            WHERE Field1 = 1000
            AND Field2 < 1000
            AND Field3 <= 1000
            AND Field4 > 1000
            AND Field5 >= 1000
            AND Field6 <> 1000
            AND Field7 IN (1000)
            AND Field8 IN (1000, 2000, 3000)
        """
        expected = [
            "Field1 = 1000",
            "Field2 < 1000",
            "Field3 <= 1000",
            "Field4 > 1000",
            "Field5 >= 1000",
            "Field6 <> 1000",
            "Field7 IN (1000)",
            "Field8 IN (1000, 2000, 3000)",
        ]
        result = queries.ParsedQuery(query)
        self.assertEqual(result.where, expected)
