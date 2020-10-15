"""Tests for the pyextract.connect.sqp module."""

from datetime import datetime
import unittest

import pyrfc

from pyextract.connect import sap

from tests import CREDENTIALS


CONNECTION = sap.SAPMessenger(logon_details=CREDENTIALS['SAP'])


class TestConnection(unittest.TestCase):
    """Can get data from direct API calls to SAP via pyrfc."""
    # pylint: disable=protected-access

    def test_init_connector(self):
        """Can connect to SAP through a custom class."""
        self.assertIsInstance(CONNECTION, sap.SAPMessenger)
        self.assertIsInstance(CONNECTION._conn, pyrfc.Connection)

    def test_call_get_attributes(self):
        """Can call RFC that returns basic connection information."""
        response = CONNECTION._conn.call('RFC_GET_ATTRIBUTES')
        self.assertEqual(response['CALLER_CODEPAGE'], '4103')
        self.assertEqual(response['CALLER_PROGRAM'], 'python')
        self.assertEqual(response['CALLER_RFC_TYPE'], 'E')

    def test_call_stfc_connection(self):
        """Can get a simple ECHO response from the SAP connection."""
        sample = 'Hello SAP!'
        response = CONNECTION._conn.call('STFC_CONNECTION', REQUTEXT=sample)
        self.assertEqual(str(response['ECHOTEXT']), sample)

    def test_call_ar_open_items(self):
        """Can read data about accounts receievable open items."""
        options = {
            'COMPANYCODE': '1000',
            'CUSTOMER': '0000001032',
            'KEYDATE': datetime.strptime('2012-12-31', '%Y-%M-%d'),
        }
        response = CONNECTION._conn.call('BAPI_AR_ACC_GETOPENITEMS', **options)
        number_rows = len(response['LINEITEMS'])
        number_cols = len(response['LINEITEMS'][0])
        self.assertEqual(number_rows, 117)
        self.assertEqual(number_cols, 111)

    def test_call_read_table_t001(self):
        """Can read data directly from a SAP table."""
        options = {
            'QUERY_TABLE': 'T001',
            'FIELDS': '',
            'ROWCOUNT': 1,
            'DELIMITER': '|',
        }
        response = CONNECTION._conn.call('RFC_READ_TABLE', **options)
        self.assertEqual(len(response), 3)


class TestSAPMessenger(unittest.TestCase):
    """Test methods of the SAPMessenger class."""

    def test_read_table_data(self):
        """Can read data directly from a specific SAP table."""
        fields = ('MANDT', 'GJAHR', 'BUDAT')
        response = CONNECTION.single_readtable(table='BKPF',
                                               columns=fields,
                                               package_size=1)
        expected_data = [["100","1995","19950606"]]
        self.assertEqual(response, expected_data)

    def test_abap_function_enabled(self):
        """Correctly identifies if an ABAP function is enabled."""
        result = CONNECTION.abap_function_enabled('RFC_GET_ATTRIBUTES')
        self.assertTrue(result)
        result = CONNECTION.abap_function_enabled('FakeFunction')
        self.assertFalse(result)

    def test_list_abap_functions(self):
        """Test if the Messenger can list available ABAP functions."""

        result = CONNECTION.list_abap_functions()
        self.assertIsInstance(result, list)
        self.assertGreater(len(result), 47000)
        self.assertLess(len(result), 48000)
        try:
            self.assertIsInstance(result[0], unicode)  # Python 2
        except NameError:
            self.assertIsInstance(result[0], list)  # Python 3

        result = CONNECTION.list_abap_functions(name_like='RFC_%',
                                                names_only=False)
        self.assertIsInstance(result, list)
        self.assertIsInstance(result, list)
        self.assertEqual(len(result), 272)

    def test_get_table_columns(self):
        """Test if the Messenger can get a list of columns for a table."""
        result = CONNECTION.get_table_columns('TFTIT')
        self.assertEqual(result, ['FUNCNAME', 'SPRAS', 'STEXT'])

    def test_table_exists_in_db(self):
        """Correctly identifies when a table exists in a database."""
        result = CONNECTION.table_exists('BKPF')
        self.assertTrue(result)
        result = CONNECTION.table_exists('FakeTableName')
        self.assertFalse(result)

    def test_get_metadata_from_query(self):
        """Can get metadata about a query from SAP."""

        query = {'Name':'BKPF', 'Columns': ['BUKRS', 'GJAHR', 'KURSF'],
                 'Type': 'TABLE'}

        result = CONNECTION.get_metadata_from_query(query)

        expected = [{
            'sourceSystem': 'SAP',
            'sourceFieldName': 'BUKRS',
            'sourceType': 'C',
            'sourceFieldLength': '000004',
            'sourceFieldNumericPrecision': '000000',
            'source_field_nullable': False,
            'sqlite_datatype': 'TEXT',
            'targetFieldName': 'BUKRS',
            'isKey': True,
            'mssql_datatype' : 'NVARCHAR(MAX)',
            'SIGN': '',
            'longDataType': 'CHAR'
        }, {
            'sourceSystem': 'SAP',
            'sourceFieldName': 'GJAHR',
            'sourceType': 'N',
            'sourceFieldLength': '000004',
            'sourceFieldNumericPrecision': '000000',
            'source_field_nullable': False,
            'sqlite_datatype': 'TEXT',
            'targetFieldName': 'GJAHR',
            'isKey': True,
            'mssql_datatype' : 'NVARCHAR(MAX)',
            'SIGN': '',
            'longDataType': 'NUMC'
        }, {
            'sourceSystem': 'SAP',
            'sourceFieldName': 'KURSF',
            'sourceType': 'P',
            'sourceFieldLength': '000012',
            'sourceFieldNumericPrecision': '000005',
            'source_field_nullable': True,
            'sqlite_datatype': 'REAL',
            'targetFieldName': 'KURSF',
            'isKey': False,
            'mssql_datatype' : 'NVARCHAR(MAX)',
            'SIGN': '',
            'longDataType': 'DEC'
        }]

        self.assertEqual(result, expected)


class TestRFCWhereClause(unittest.TestCase):
    """SAP RFC WHERE clauses can be properly split to fit SAP format."""

    def test_empty_list(self):
        original = []
        result = sap.where_clause_rfc_format(original)
        self.assertEqual(result, [])

    def test_single_blank_value(self):
        original = ['']
        result = sap.where_clause_rfc_format(original)
        self.assertEqual(result, [])

    def test_single_where(self):
        original = ["BUKRS = '100'"]
        result = sap.where_clause_rfc_format(original)
        self.assertEqual(result, original)

    def test_two_wheres(self):
        original = ["BUKRS = '100'", "GJAHR = '2000'"]
        expected = ["BUKRS = '100' AND ",
                    "GJAHR = '2000'"]
        result = sap.where_clause_rfc_format(original)
        self.assertEqual(result, expected)

    def test_long_where(self):
        original = ["BUKRS = '100'", "GJAHR = '2000'",
                    "BELNR IN ('100000000001', '100000000002', '100000000003', '100000000004', '100000000005', '100000000006', '100000000007', '100000000008', '100000000009')"]
        expected = ["BUKRS = '100' AND ",
                    "GJAHR = '2000' AND ",
                    "BELNR IN (",
                    "'100000000001',",
                    "'100000000002',",
                    "'100000000003',",
                    "'100000000004',",
                    "'100000000005',",
                    "'100000000006',",
                    "'100000000007',",
                    "'100000000008',",
                    "'100000000009')"]
        result = sap.where_clause_rfc_format(original)
        self.assertEqual(result, expected)
