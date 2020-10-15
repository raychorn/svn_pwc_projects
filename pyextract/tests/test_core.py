"""Tests for the core API of the pyextract module."""

import os
import shutil
import sys
import tempfile
import unittest
from zipfile import ZipFile

import pyextract
from pyextract import DataStream, Extraction, SQLiteMessenger
from pyextract.connect.abap import ABAPMessenger, ABAPInputGenerate
from pyextract.connect.db2 import DB2Messenger
from pyextract.connect.mssql import MSSQLMessenger
from pyextract.connect.mysql import MySQLMessenger
from pyextract.connect.oracle import OracleMessenger
from pyextract.connect.sap import SAPMessenger
from pyextract.streams.sapstream import SAPStream

from tests import CREDENTIALS

ROW_LIMIT = 10  # Used so all test extracts are relatively quick
MSSQL_OUTPUT = MSSQLMessenger(schema='_test_pyextract',
                              **CREDENTIALS['MSSQL'])


class TestFromMSSQL(unittest.TestCase):
    """Test ability to extract testing data from MSSQL."""

    table = 'test_pyextract_table'
    query = 'SELECT * FROM test_pyextract_table'
    rows = 10
    columns = 10

    @classmethod
    def setUpClass(cls):
        """Create a table with test data in MSSQL for extraction."""
        cls.msgr = MSSQLMessenger(schema='dbo', **CREDENTIALS['MSSQL'])
        cls.msgr.drop_table_if_exists(cls.table)
        columns = ['column{}'.format(i) for i in range(cls.columns)]
        cls.msgr.create_table(cls.table, columns)
        testdata = create_zeroes_testing_table(cls.rows, cls.columns)
        cls.msgr.insert_into(cls.table, testdata)

    @classmethod
    def tearDownClass(cls):
        """Drop the test data table from MSSQL."""
        cls.msgr.drop_table_if_exists(cls.table)

    def setUp(self):
        """Create output folder for extraction."""
        self.output_folder = tempfile.mkdtemp()

    def tearDown(self):
        """Drop extraction output folder."""
        shutil.rmtree(self.output_folder, ignore_errors=True)

    def test_to_sqlite(self):
        """Can extract test data to SQLite."""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = DataStream(self.msgr, row_limit=ROW_LIMIT)
        extraction = Extraction(source=source, output=output)
        extraction.extract_from_query(self.query)

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                self.table: self.rows,
                'query_level_status': 1,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': self.columns,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 1,
                '_pyextract_full_log': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

        # Assert that source datatypes in the metadata are accurate
        filepath = os.path.join(self.output_folder, 'Encrypted_Content_TableMetaData.dat')
        assert_sqlite_metadata_dtype(filepath, 'NVARCHAR(MAX)')

    def test_to_mssql(self):
        """Can extract test data to MSSQL."""
        output_tables = ('test_pyextract_table', 'TableMetaData',
                         '_pyextract_chunk_log', '_pyextract_full_log',
                         'query_level_status', 'temp_tracker')

        # Drop previous output tables from the test schema and ensure they're gone
        for table in output_tables:
            MSSQL_OUTPUT.drop_table_if_exists(table)
            self.assertFalse(MSSQL_OUTPUT.table_exists(table))

        # Perform the data extraction
        source = DataStream(self.msgr, row_limit=ROW_LIMIT)
        extraction = Extraction(source=source, output=MSSQL_OUTPUT)
        extraction.extract_from_query(self.query)

        # Test that the extract tables were created in MSSQL, then drop them
        for table in output_tables:
            self.assertTrue(MSSQL_OUTPUT.table_exists(table),
                            '{} does not exist in database'.format(table))
            MSSQL_OUTPUT.drop_table_if_exists(table)

        # Drop the schema if no tables exist in it
        MSSQL_OUTPUT.drop_schema_from_database()

    def test_to_sqlite_from_ecf(self):
        """Test a MSSQL to SQLite extraction using an ECF file."""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = DataStream(self.msgr, row_limit=ROW_LIMIT)
        ecf_file = "./ecfs/sql-server-2-0-info-schema-tables.ecf"
        pyextract.extract_from_ecf(ecf_file, source=source, output=output)

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                'INFORMATION_SCHEMA.TABLES': 10,
                'query_level_status': 1,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableExtractions.dat': {
                'TableExtractions': 1,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': 4,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 1,
                '_pyextract_full_log': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

        # Make sure that the ECF table was populated as expected
        filepath = os.path.join(self.output_folder, 'Encrypted_Content_TableExtractions.dat')
        messenger = SQLiteMessenger(filepath)
        query = """
            SELECT RequestID, TableName
            FROM TableExtractions
            """
        ecfdata = messenger.fetch_data(query)[0]
        expected = (
            '38f93ed1-9448-4c56-ab6b-d3a6f6b890ad',
            'INFORMATION_SCHEMA.TABLES',
        )
        self.assertEqual(ecfdata, expected)

    @unittest.skip('MemSQL testing database is offline')
    def test_to_memsql(self):
        """Can extract an MSSQL database into MemSQL."""
        tables_to_expected_rows = {
            self.table: self.rows,
            'TableMetaData': self.columns,
            '_pyextract_chunk_log': 1,
            '_pyextract_full_log': 0,
        }
        source = DataStream(self.msgr, row_limit=ROW_LIMIT)
        output = MySQLMessenger(server='10.1.198.226', port=3306,
                                database='test_db', user='root')

        # Drop previous output tables from the test schema and ensure they're gone
        for table in tables_to_expected_rows:
            output.drop_table_if_exists(table)
            self.assertFalse(output.table_exists(table))

        # Perform the data extraction
        extraction = Extraction(source=source, output=output)
        extraction.extract_from_query(self.query)

        # Test that the extract tables were created with right number of rows
        for table, expected in tables_to_expected_rows.items():
            query = 'SELECT COUNT(*) FROM {}'.format(table)
            actual = output.fetch_data(query)[0][0]
            self.assertEqual(expected, actual, '{} has {} rows'.format(table, actual))
            output.drop_table_if_exists(table)


class TestFromOracle(unittest.TestCase):
    """Test ability to extract testing data from Oracle."""

    table = 'test_pyextract_table'
    query = 'SELECT * FROM test_pyextract_table'
    rows = 10
    columns = 10

    @classmethod
    def setUpClass(cls):
        """Create a table with test data in Oracle for extraction."""
        cls.msgr = OracleMessenger(**CREDENTIALS['EBSR12'])
        cls.msgr.drop_table_if_exists(cls.table)
        columns = ['column{}'.format(i) for i in range(cls.columns)]
        cls.msgr.create_table(cls.table, columns)
        testdata = create_zeroes_testing_table(cls.rows, cls.columns)
        cls.msgr.insert_into(cls.table, testdata, columns)

    @classmethod
    def tearDownClass(cls):
        """Drop the table with test data from Oracle."""
        cls.msgr.drop_table_if_exists(cls.table)

    def setUp(self):
        """Create test data in Oracle for extraction."""
        self.output_folder = tempfile.mkdtemp()

    def tearDown(self):
        """Drop test table from SQL, close connections, delete output."""
        shutil.rmtree(self.output_folder, ignore_errors=True)

    def test_to_sqlite(self):
        """Can extract test data that was just inserted."""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = DataStream(self.msgr, row_limit=ROW_LIMIT)
        extraction = Extraction(source=source, output=output)
        extraction.extract_from_query(self.query)

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                self.table: self.rows,
                'query_level_status': 1,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': self.columns,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 1,
                '_pyextract_full_log': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

        # Assert that source datatypes in the metadata are accurate
        filepath = os.path.join(self.output_folder, 'Encrypted_Content_TableMetaData.dat')
        assert_sqlite_metadata_dtype(filepath, 'NUMBER')

    def test_to_mssql(self):
        """Can extract test data to MSSQL."""
        output_tables = ('test_pyextract_table', 'TableMetaData',
                         '_pyextract_chunk_log', '_pyextract_full_log',
                         'query_level_status',)

        # Drop previous output tables from the test schema and ensure they're gone
        for table in output_tables:
            MSSQL_OUTPUT.drop_table_if_exists(table)
            self.assertFalse(MSSQL_OUTPUT.table_exists(table))

        # Perform the data extraction
        source = DataStream(self.msgr, row_limit=ROW_LIMIT)
        extraction = Extraction(source=source, output=MSSQL_OUTPUT)
        extraction.extract_from_query(self.query)

        # Test that the extract tables were created in MSSQL, then drop them
        for table in output_tables:
            self.assertTrue(MSSQL_OUTPUT.table_exists(table),
                            '{} does not exist in database'.format(table))
            MSSQL_OUTPUT.drop_table_if_exists(table)

        # Drop the schema if no tables exist in it
        MSSQL_OUTPUT.drop_schema_from_database()

    def test_ecf_file_v1_6_single_db(self):
        """Test extraction from a version 1.6 ECF file for Oracle."""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = DataStream(self.msgr, row_limit=ROW_LIMIT)
        ecf_file = "./ecfs/oracle-r12-2-0-gl-headers.ecf"
        pyextract.extract_from_ecf(ecf_file, source=source, output=output)

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                'GL_JE_BATCHES': ROW_LIMIT,
                'GL_JE_HEADERS': ROW_LIMIT,
                'GL_JE_LINES': ROW_LIMIT,
                'query_level_status': 3,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': 40,
            },
            'Encrypted_Content_TableExtractions.dat': {
                'TableExtractions': 3,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 3,
                '_pyextract_full_log': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

        # Assert that None/NULL values from Oracle are saved properly
        samplerow = output.fetch_data('SELECT * FROM GL_JE_LINES LIMIT 1')
        self.assertEqual(samplerow[0][1], None)

    def test_ecf_file_v1_6_db_per_table(self):
        """Test extraction from a version 1.6 ECF file for Oracle."""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = DataStream(self.msgr, row_limit=ROW_LIMIT)
        ecf_file = "./ecfs/oracle-r12-2-0-gl-headers.ecf"
        pyextract.extract_from_ecf(ecf_file, source=source, output=output,
                                   chunk_results='db_per_table')

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                'query_level_status': 3,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': 40
            },
            'Encrypted_Content_TableExtractions.dat': {
                'TableExtractions':3
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 3,
                '_pyextract_full_log': 0,
            },
            'Encrypted_Content_GL_JE_BATCHES.dat': {
                'GL_JE_BATCHES': ROW_LIMIT,
            },
            'Encrypted_Content_GL_JE_HEADERS.dat': {
                'GL_JE_HEADERS': ROW_LIMIT,
            },
            'Encrypted_Content_GL_JE_LINES.dat': {
                'GL_JE_LINES': ROW_LIMIT,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))


class TestFromSAP(unittest.TestCase):
    """Test ability to extract testing data from SAP."""

    ecf = "./ecfs/sap-2-0-bkpf-bseg.ecf"
    rep_ecf = "./ecfs/sap-2-0-mb5b-key-report.ecf"

    @classmethod
    def setUpClass(cls):
        """Open connection to SAP using a Messenger object."""
        cls.msgr = SAPMessenger(logon_details=CREDENTIALS['SAP'])

    def setUp(self):
        """Create test data in Oracle for extraction."""
        self.output_folder = tempfile.mkdtemp()

    def tearDown(self):
        """Drop test table from SQL, close connections, delete output."""
        shutil.rmtree(self.output_folder, ignore_errors=True)

    def test_to_sqlite(self):
        """Can extract to a single SQLite database."""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = SAPStream(self.msgr, output=output)
        pyextract.extract_from_ecf(self.ecf, source=source, output=output)

        # Make sure the data package was created as expected
        expected_package = {
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': 105
            },
            'Encrypted_Content_TableExtractions.dat': {
                'TableExtractions': 2
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 32,
                '_pyextract_full_log': 0,
            },
            'outputtest.dat': {
                'BKPF': 591,
                'BSEG': 3112,
                'query_level_status': 2,
                'temp_tracker': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

    def test_to_sqlite_one_table_per(self):
        """Can extract from SAP using a 1.6 ECF from SAP."""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = SAPStream(self.msgr, row_limit=None,
                           output=output, chunk_results='db_per_table')
        pyextract.extract_from_ecf(self.ecf, source=source, output=output,
                                   chunk_results='db_per_table')

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                'query_level_status': 2,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': 105,
            },
            'Encrypted_Content_TableExtractions.dat': {
                'TableExtractions': 2,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 32,
                '_pyextract_full_log': 0,
            },
            'Encrypted_Content_BKPF.dat': {
                'BKPF': 591,
            },
            'Encrypted_Content_BSEG.dat': {
                'BSEG': 3112,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

    def test_with_data_package(self):
        """Can extract nested SAP tables into a zipped data package."""
        sqlite_path = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath=sqlite_path)
        source = SAPStream(self.msgr, output=output)
        pyextract.extract_from_ecf(self.ecf, source=source, output=output)

        # Package resulting SQLite database (single) into a Zip package
        package = pyextract.DataPackage(self.ecf)
        package.add_sqlite_file(sqlite_path)
        zip_path = os.path.join(self.output_folder, 'testpackage.zip')
        package.create(zip_path)

        with ZipFile(zip_path, 'r') as source:
            # Assert the ZIP file was created with only the expected files
            self.assertEqual(sorted(source.namelist()), [
                'Encrypted_RequestInfo.EPF',
                'Metadata.txt',
                'outputtest.dat',
                'sap-2-0-bkpf-bseg.ecf',
            ])
            zipepf = source.read('Encrypted_RequestInfo.EPF')
            assert source.read('outputtest.dat')

        # Assert that source datatypes in the metadata are accurate
        filepath = os.path.join(self.output_folder, 'Encrypted_Content_TableMetaData.dat')
        assert_sqlite_metadata_dtype(filepath, 'C')

    def test_to_mssql_row_limit(self):
        """Can extract test data to MSSQL (with a row limit)."""
        tables_to_expected_rows = {
            'BKPF': ROW_LIMIT,
            'BSEG': ROW_LIMIT,
            '_pyextract_full_log': 0,
            '_pyextract_chunk_log': None,  # Always between 2-4; don't verify
            'TableExtractions': 2,
            'TableMetaData': 105,
            'query_level_status': 2,
            'temp_tracker': 0,
        }
        # This table randomly shows up and fails tests from time to time
        MSSQL_OUTPUT.drop_table_if_exists('test_pyextract_table')

        # Drop previous output tables from the test schema and ensure they're gone
        for table in tables_to_expected_rows:
            MSSQL_OUTPUT.drop_table_if_exists(table)
            self.assertFalse(MSSQL_OUTPUT.table_exists(table))

        # Perform the data extraction
        source = SAPStream(self.msgr, row_limit=ROW_LIMIT, output=MSSQL_OUTPUT)
        pyextract.extract_from_ecf(self.ecf, source=source, output=MSSQL_OUTPUT)

        # Make sure the data package was created as expected
        for table, expected in tables_to_expected_rows.items():
            # The chunk log contains between 2-4 rows, the exact number which
            # changes every run for some reason, so don't test that table
            # because we really don't care how many chunks are created for
            # writes to MSSQL
            if table == '_pyextract_chunk_log':
                continue

            query = """
                SELECT COUNT(*) FROM [{}].[{}]
                """.format(MSSQL_OUTPUT.schema, table)
            actual = MSSQL_OUTPUT.fetch_data(query)[0][0]
            self.assertEqual(expected, actual, '{} has {} rows'.format(table, actual))
            MSSQL_OUTPUT.drop_table_if_exists(table)

        # Assert that no extra tables were created
        for table in MSSQL_OUTPUT.list_all_tables():
            self.assertIn(table, tables_to_expected_rows)

        # Drop the schema if no tables exist in it
        MSSQL_OUTPUT.drop_schema_from_database()

    @unittest.skip('Need 2.0 ECF with SAP Invalid Columns')
    def test_with_ecf_errors(self):
        """Can extract to a SQLite database despite errors in the ECF."""
        ecf = "./ecfs/SAP-v1.6-Revenue-Match-Invalid-Column.ecf"

        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = SAPStream(self.msgr, output=output)
        pyextract.extract_from_ecf(ecf, source=source, output=output)

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                'KONV': 86,
                'LIKP': 34,
                #  'LIPS': None,  -- Not created -- bad 'VVVVV' col in ECF
                'VBAK': 38,
                'VBAP': 86,
                'VBRK': 39,
                'VBRP': 86,
                'query_level_status':7,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': 106,
            },
            'Encrypted_Content_TableExtractions.dat': {
                'TableExtractions': 7,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 36,
                '_pyextract_full_log': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

        # Assert that the TableExtractions table shows that an error
        # occured for the LIPS table
        filepath = os.path.join(self.output_folder, 'Encrypted_Content_TableExtractions.dat')
        messenger = SQLiteMessenger(filepath)
        query = """
            SELECT TableName, SourceRecordCount, error_encountered
            FROM TableExtractions
            """
        data = messenger.fetch_data(query)
        expected = [
            ('KONV', '86', None),
            ('LIKP', '34', None),
            ('LIPS', None, 'True'),
            ('VBAK', '38', None),
            ('VBAP', '86', None),
            ('VBRK', '39', None),
            ('VBRP', '86', None),
        ]
        self.assertEqual(sorted(data), sorted(expected))

    @unittest.skip('testing data was deleted from test database')
    def test_key_report_to_sqlite(self):
        """Can extract a key report, MB5B, from SAP into a SQLite file"""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = SAPStream(self.msgr, row_limit=None,
                           output=output, chunk_results='db_per_table')
        pyextract.extract_from_ecf(self.rep_ecf, source=source, output=output,
                                   chunk_results='db_per_table', encrypted=False)

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                'query_level_status': 1,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': 14,
            },
            'Encrypted_Content_TableExtractions.dat': {
                'TableExtractions': 1,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_full_log': 0,
                '_pyextract_chunk_log': 4
            },
            'Encrypted_Content_RM07MLBD_PWC.dat': {
                'RM07MLBD_PWC': 2269,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))


class TestFromSQLite(unittest.TestCase):
    """Test ability to extract testing data from SQLite."""

    table = 'test_pyextract_table'
    query = 'SELECT * FROM test_pyextract_table'
    rows = 10
    columns = 10

    @classmethod
    def setUpClass(cls):
        """Create a table with test data in SQLite for extraction."""
        cls.input_folder = tempfile.mkdtemp()
        filepath = os.path.join(cls.input_folder, 'test.dat')
        cls.source = SQLiteMessenger(filepath=filepath)
        columns = ['column{}'.format(i) for i in range(cls.columns)]
        datatypes = ['tinyint'] * cls.columns
        cls.source.create_table(cls.table, columns, datatypes)
        testdata = create_zeroes_testing_table(cls.rows, cls.columns)
        cls.source.insert_into(cls.table, testdata, columns)

    @classmethod
    def tearDownClass(cls):
        """Drop the table with test data from SQLite."""
        shutil.rmtree(cls.input_folder, ignore_errors=True)

    def setUp(self):
        """Create temporary output folder for each test."""
        self.output_folder = tempfile.mkdtemp()

    def tearDown(self):
        """Drop test table from SQL, close connections, delete output."""
        shutil.rmtree(self.output_folder, ignore_errors=True)

    def test_to_sqlite_unzipped(self):
        """Can extract test data that was just inserted."""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = DataStream(self.source, row_limit=ROW_LIMIT)
        extraction = Extraction(source=source, output=output)
        extraction.extract_from_query(self.query)

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                self.table: self.rows,
                'query_level_status': 1,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': self.columns,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 1,
                '_pyextract_full_log': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

        # Assert that source datatypes in the metadata are accurate
        filepath = os.path.join(self.output_folder, 'Encrypted_Content_TableMetaData.dat')
        assert_sqlite_metadata_dtype(filepath, 'tinyint')

    def test_to_sqlite_chunked(self):
        """Can extract test data into one table per chunk."""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = DataStream(self.source, row_limit=ROW_LIMIT)
        extraction = Extraction(source=source, output=output,
                                chunk_results='db_per_chunk')
        extraction.extract_from_query(self.query)

        # Make sure the data package was created as expected
        expected_package = {
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': self.columns,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 1,
                '_pyextract_full_log': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Find the location of the chunked database
        logdb_path = os.path.join(self.output_folder, 'Encrypted_Content_ExtractionLogs.dat')
        logdb = SQLiteMessenger(logdb_path)
        query = 'SELECT * FROM _pyextract_chunk_log'
        chunkdb_path = logdb.fetch_data(query)[0][1]
        chunkdb = SQLiteMessenger(chunkdb_path)

        # Assert chunk database contains correct amount of rows
        query = 'SELECT COUNT(*) FROM {}'.format(self.table)
        self.assertEqual(self.rows, chunkdb.fetch_data(query)[0][0])

    def test_to_sqlite_zipped(self):
        """Can extract test data that was just inserted."""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath=filepath, is_zipped=True,
                                 password='testpassword')
        source = DataStream(self.source, row_limit=ROW_LIMIT)
        extraction = Extraction(source=source, output=output)
        extraction.extract_from_query(self.query)

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                self.table: self.rows,
                'query_level_status': 1,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': self.columns,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 1,
                '_pyextract_full_log': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows,
                                     password='testpassword')

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

    def test_different_output_table(self):
        """Can perform an extraction into a table with a different name."""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = DataStream(self.source, row_limit=ROW_LIMIT)
        extraction = Extraction(source=source, output=output)
        metadata = pyextract.DataDefinition(self.query, self.source,
                                            output_table='different_table')

        pyextract.core._create_new_temp_status_tracker(output)
        pyextract.core._create_pause_resume_table(output)
        extraction.extract(metadata=metadata)

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                'different_table': self.rows,
                'query_level_status': 1,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': self.columns,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 1,
                '_pyextract_full_log': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

    def test_to_mssql(self):
        """Can extract test data to MSSQL."""
        output_tables = ('test_pyextract_table', 'TableMetaData',
                         '_pyextract_chunk_log', '_pyextract_full_log',
                         'query_level_status',)

        # Drop previous output tables from the test schema and ensure they're gone
        for table in output_tables:
            MSSQL_OUTPUT.drop_table_if_exists(table)
            self.assertFalse(MSSQL_OUTPUT.table_exists(table))

        # Perform the data extraction
        source = DataStream(self.source, row_limit=ROW_LIMIT)
        extraction = Extraction(source=source, output=MSSQL_OUTPUT)
        extraction.extract_from_query(self.query)

        # Test that the extract tables were created in MSSQL, then drop them
        for table in output_tables:
            self.assertTrue(MSSQL_OUTPUT.table_exists(table),
                            '{} does not exist in database'.format(table))
            MSSQL_OUTPUT.drop_table_if_exists(table)

        # Drop the schema if no tables exist in it
        MSSQL_OUTPUT.drop_schema_from_database()


class TestFromDB2(unittest.TestCase):
    """Test ability to extract testing data from DB2."""

    schema = 'JDEDATA910'
    table = 'test_pyextract_table'
    query = 'SELECT * FROM JDEDATA910.test_pyextract_table'
    rows = 10
    columns = 10

    @classmethod
    def setUpClass(cls):
        """Create a table with test data in DB2 for extraction."""
        cls.msgr = DB2Messenger(schema=cls.schema, **CREDENTIALS['DB2'])
        cls.msgr.drop_table_if_exists(cls.table)
        columns = ['column{}'.format(i) for i in range(cls.columns)]
        cls.msgr.create_table(cls.table, columns)
        testdata = create_zeroes_testing_table(cls.rows, cls.columns)
        cls.msgr.insert_into(cls.table, testdata)

    @classmethod
    def tearDownClass(cls):
        """Drop the test data table from MSSQL."""
        cls.msgr.drop_table_if_exists(cls.table)

    def setUp(self):
        """Create output folder for extraction."""
        self.output_folder = tempfile.mkdtemp()

    def tearDown(self):
        """Drop extraction output folder."""
        shutil.rmtree(self.output_folder, ignore_errors=True)

    def test_to_sqlite(self):
        """Can extract test data to SQLite."""
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath)
        source = DataStream(self.msgr, row_limit=ROW_LIMIT)
        extraction = Extraction(source=source, output=output)
        extraction.extract_from_query(self.query)

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                self.table: self.rows,
                'query_level_status': 1,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': self.columns,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 1,
                '_pyextract_full_log': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

        # Assert that source datatypes in the metadata are accurate
        filepath = os.path.join(self.output_folder, 'Encrypted_Content_TableMetaData.dat')
        assert_sqlite_metadata_dtype(filepath, 'CHAR')


@unittest.skip('Need 2.0 ABAP Fil Files ECF')
class TestFromABAP(unittest.TestCase):
    """Test ability to extract testing data from ABAP."""

    abap_data_folder = './tests/assets/abap_9D58ADEA14'
    ecf = "./ecfs/ABAP-v1.6-local-fil-files.ecf"

    # ecf_requestid will be passed from GUI in non-test scenario
    ecf_requestid='38033dbc-e750-45d9-9af5-f48b0c'

    def setUp(self):
        """Create temporary output folder for each test."""
        self.output_folder = tempfile.mkdtemp()

    def tearDown(self):
        """Drop test table from SQL, close connections, delete output."""
        shutil.rmtree(self.output_folder, ignore_errors=True)

    def test_into_sqlite(self):
        """Can extract from ABAP .fil files into SQLite."""
        messenger = ABAPMessenger(folder=self.abap_data_folder,
                                  ecf_requestid=self.ecf_requestid)

        source = DataStream(messenger, row_limit=ROW_LIMIT)
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath=filepath)
        pyextract.extract_from_ecf(self.ecf, source=source, output=output)

        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                'ANLA': ROW_LIMIT,
                'BKPF': ROW_LIMIT,
                'BSEG': ROW_LIMIT,
                'SKA1': ROW_LIMIT,
                'SKAT': ROW_LIMIT,
                'query_level_status': 5,
                'temp_tracker': 0,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': 148,
            },
            'Encrypted_Content_TableExtractions.dat': {
                'TableExtractions': 5,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 5,
                '_pyextract_full_log': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

        # Assert that source datatypes in the metadata are accurate
        filepath = os.path.join(self.output_folder,
                                'Encrypted_Content_TableMetaData.dat')
        assert_sqlite_metadata_dtype(filepath, 'C')


class TestABAPInputGenerate(unittest.TestCase):
    """Test the ability to create parameter csv file."""

    ecf = './ecfs/abap-2-0-sample.ecf'

    def setUp(self):
        """Create temporary output folder for each test."""
        self.output_folder = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.output_folder, ignore_errors=True)

    @unittest.skip('skipping for encrpytion arg getting passed')
    def test_generated_file(self):
        """Test the generation of parameter file."""
        abap_filepath = os.path.join(self.output_folder, 'test_parameter.csv')
        generator = ABAPInputGenerate(self.ecf, abap_filepath)
        generator.create_parameter_file()

        #Test if the output parameter file exists
        output_exists = os.path.exists(abap_filepath)

        self.assertEqual(output_exists, True)

        #Test the contents of the output parameter file

        #expected file contents
        expected_header_line = 'Tablename;Fieldname;Value/From;To\n'
        expected_line_count = 152

        expected_tables = ['ANLA', 'BKPF', 'BSEG', 'SKA1', 'SKAT']

        # read the contents of parameter csv file
        with open(abap_filepath, 'r') as op_file:
            op_content = op_file.readlines()

            #validate the file contains a header
            self.assertEqual(op_content[0], expected_header_line)

            #validate the line count of the file
            self.assertEqual(len(op_content), expected_line_count)

            #validate the file contains the expected tables
            tables_found = []
            for op_line in op_content:
                if ('RequestID' not in op_line and
                    'stripeKey' not in op_line and
                    'Schema' not in op_line and
                    'Tablename' not in op_line):

                    op_position = op_line.find(";")
                    #validate the file doesn't contain any unexpected table
                    self.assertIn(op_line[0:op_position],expected_tables)
                    tables_found.append(op_line[0:op_position])
                assert '"' not in op_line, 'double quotes not allowed in parameter file'

            #validate the file contains all the expected tables
            tables_found_unq = list(set(tables_found))
            self.assertEqual(sorted(tables_found_unq), sorted(expected_tables))


@unittest.skip('Need 2.0 ABAP Fil Files ECF')
class TestJournalsOutput(unittest.TestCase):
    """Test ability to extract testing data from ABAP."""

    abap_data_folder = './ecfs/abap_138DA2DF4F'
    ecf = "./ecfs/ABAP-v1.6-local-fil-files.ecf"
    ecf_requestid='38033dbc-e750-45d9-9af5-f48b0c'

    def setUp(self):
        """Create temporary output folder for each test."""
        self.output_folder = tempfile.mkdtemp()

    def tearDown(self):
        """Drop test table from SQL, close connections, delete output."""
        shutil.rmtree(self.output_folder, ignore_errors=True)

    def test_into_sqlite(self):
        """Can extract from ABAP .fil files into SQLite."""
        messenger = ABAPMessenger(folder=self.abap_data_folder,
                                  ecf_requestid=self.ecf_requestid)

        source = DataStream(messenger, row_limit=ROW_LIMIT)
        filepath = os.path.join(self.output_folder, 'outputtest.dat')
        output = SQLiteMessenger(filepath=filepath)
        pyextract.extract_from_ecf(self.ecf, source=source, output=output,
                                   chunk_results='db_per_table')
        # Make sure the data package was created as expected
        expected_package = {
            'outputtest.dat': {
                'query_level_status': 3,
                'temp_tracker': 0,
            },
            'Encrypted_Content_ADRP.dat': {
                'ADRP': ROW_LIMIT,
            },
            'Encrypted_Content_TBSLT.dat': {
                'TBSLT': ROW_LIMIT,
            },
            'Encrypted_Content_TSTCT.dat': {
                'TSTCT': ROW_LIMIT,
            },
            'Encrypted_Content_TableExtractions.dat': {
                'TableExtractions': 3,
            },
            'Encrypted_Content_TableMetaData.dat': {
                'TableMetaData': 14,
            },
            'Encrypted_Content_TableExtractions.dat': {
                'TableExtractions': 3,
            },
            'Encrypted_Content_ExtractionLogs.dat': {
                '_pyextract_chunk_log': 3,
                '_pyextract_full_log': 0,
            },
        }
        for database, tables_to_rows in expected_package.items():
            filepath = os.path.join(self.output_folder, database)
            assert_sqlite_row_counts(filepath, tables_to_rows)

        # Assert no extra files were created in the output folder
        self.assertEqual(sorted(os.listdir(self.output_folder)),
                         sorted(expected_package))

        # Assert that source datatypes in the metadata are accurate
        filepath = os.path.join(self.output_folder,
                                'Encrypted_Content_TableMetaData.dat')
        assert_sqlite_metadata_dtype(filepath, 'C')


def setUpModule():
    """Prevent unittest multiprocessing from exploding on Windows.

    Fixes the Windows forking system by pointing __main__ to the module being
    executed in the forked process.
    """
    # pylint: disable=invalid-name
    sys.modules["__main__"] = sys.modules['pyextract']
    sys.modules["__main__"].__file__ = sys.modules['pyextract'].__file__


def create_zeroes_testing_table(rows: int, cols: int) -> tuple:
    """Return a table filled with zeroes for testing extractions."""
    return tuple(tuple(0 for _ in range(cols)) for _ in range(rows))


def assert_sqlite_metadata_dtype(filepath: str, dtype: str):
    """Assert that the metadata dtype of a filepath is as expected."""
    messenger = SQLiteMessenger(filepath)
    query = 'SELECT sourceType FROM TableMetaData LIMIT 1'
    actual = messenger.fetch_data(query)[0][0]
    assert actual == dtype, \
        'Expected datatype "{}", got "{}"'.format(dtype, actual)


def assert_sqlite_row_counts(filepath: str, tables_to_rows: dict,
                             password: str = None):
    """Assert that the table row counts of a SQLite database are correct.

    ARGS:
        filepath: Local filepath of the SQLite database to test.
        tables_to_rows: Dictionary with table names as keys and the
            number of rows that should be in each table as values.
        password: If provided, connect to an AES-128 encrypted database.
    """
    if password:
        messenger = SQLiteMessenger(filepath, is_zipped=True,
                                    password=password)
    else:
        messenger = SQLiteMessenger(filepath)
    # Assert no unexpected tables were created in the database
    for table in messenger.list_all_tables():
        assert table in tables_to_rows, \
            'Extra table "{}" found'.format(table)
    # Assert the row count is as expected for each table
    for table, expected in tables_to_rows.items():
        query = 'SELECT COUNT(*) FROM "{}"'.format(table)
        actual = messenger.fetch_data(query)[0][0]
        assert actual == expected, \
            '{} has {} rows, expected {}'.format(table, actual, expected)
