"""Test the ecfreader module."""
# pylint: disable=line-too-long

import sys
import unittest

from pyextract import ecfreader

MSSQL_ECF_CONTENT = {
    'ErpInstance': 'test',
    'FileUploadMethod': 'MFT_LFU',
    'ClientId': '123',
    'DatabaseName': None,
    'DataSourceId': '00000000-0000-0000-0000-000000000000',
    'ExpiryDate': None,
    'CreatedDate': '2017-02-24T00:00:00-05:00',
    'RequestId': '38f93ed1-9448-4c56-ab6b-d3a6f6b890ad',
    'IsDatabaseNameRequired': False,
    'EcfFileName': None,
    'PublicKeyType': None,
    'DatabasePort': None,
    'SaveToRepository': False,
    'DataDestination': 'HaloforERP',
    'PublicKey': 'MIIGdDCCBVygAwIBAgIKVpyTQgADABkWrjANBgkqhkiG9w0BAQUFADBUMRMwEQYKCZImiZPyLGQBGRYDY29tMRMwEQYKCZImiZPyLGQBGRYDcHdjMSgwJgYDVQQDEx9QcmljZXdhdGVyaG91c2VDb29wZXJzIElzc3VpbmcxMB4XDTE0MTAyMTIzNDY0NVoXDTE3MDQyMTIzNTY0NVowcDELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkZMMQ4wDAYDVQQHEwVUYW1wYTEMMAoGA1UEChMDUFdDMQwwCgYDVQQLEwNJRlMxKDAmBgNVBAMTH2F1cmFhdHN0c3R3aGQwMS5wd2NpbnRlcm5hbC5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDdJHQ53Mdjh+JdxA8z1syTylio6qV5gDNhB0FirOqHBa65hZyGjEmIifFg0POeJj84r1//Y0QqnXBpfMkhdSjfogN3qggdjXMX7/CSNlwNFzzprOC4oVEE0reblxodu0n/932OZA99Cn9szEt8ZiYCaxRWEhC/1NGCe85Ev649sWK4/az5IK/0c35gk1FETXh4cQThWTGNMFgMD3LwBZSArAUpTpg5PKSi9TSEw5czyoZNzl2OOkMMLXIEmrrbZmHkNDwm8nyPru9ubeLRHKyxgp9WHWMd4r/soTLTWOObRUVWWcjbv2O1EZaMUs5qvga0VMyosBs2vsPzAM8NEIa5AgMBAAGjggMqMIIDJjAOBgNVHQ8BAf8EBAMCBPAwEwYDVR0lBAwwCgYIKwYBBQUHAwEweAYJKoZIhvcNAQkPBGswaTAOBggqhkiG9w0DAgICAIAwDgYIKoZIhvcNAwQCAgCAMAsGCWCGSAFlAwQBKjALBglghkgBZQMEAS0wCwYJYIZIAWUDBAECMAsGCWCGSAFlAwQBBTAHBgUrDgMCBzAKBggqhkiG9w0DBzAdBgNVHQ4EFgQUQGq4zr5KvzGHHrCjChD24JL8fIMwHwYDVR0jBBgwFoAUQqa135u6wchnnl+neti+GnU/dO0wggELBgNVHR8EggECMIH/MIH8oIH5oIH2hlBodHRwOi8vY2VydGRhdGExLnB3Y2ludGVybmFsLmNvbS9DZXJ0RGF0YTEvUHJpY2V3YXRlcmhvdXNlQ29vcGVycyUyMElzc3VpbmcxLmNybIZQaHR0cDovL2NlcnRkYXRhMi5wd2NpbnRlcm5hbC5jb20vQ2VydERhdGEyL1ByaWNld2F0ZXJob3VzZUNvb3BlcnMlMjBJc3N1aW5nMS5jcmyGUGh0dHA6Ly9jZXJ0ZGF0YTMucHdjaW50ZXJuYWwuY29tL0NlcnREYXRhMy9QcmljZXdhdGVyaG91c2VDb29wZXJzJTIwSXNzdWluZzEuY3JsMIIBJgYIKwYBBQUHAQEEggEYMIIBFDCBhwYIKwYBBQUHMAKGe2h0dHA6Ly9jZXJ0ZGF0YTEucHdjaW50ZXJuYWwuY29tL0NlcnREYXRhMS9VU1RQQTNHVFNDQTAzLnVzLm5hbS5hZC5wd2NpbnRlcm5hbC5jb21fUHJpY2V3YXRlcmhvdXNlQ29vcGVycyUyMElzc3VpbmcxKDMpLmNydDCBhwYIKwYBBQUHMAKGe2h0dHA6Ly9jZXJ0ZGF0YTIucHdjaW50ZXJuYWwuY29tL0NlcnREYXRhMi9VU1RQQTNHVFNDQTAzLnVzLm5hbS5hZC5wd2NpbnRlcm5hbC5jb21fUHJpY2V3YXRlcmhvdXNlQ29vcGVycyUyMElzc3VpbmcxKDMpLmNydDAMBgNVHRMBAf8EAjAAMA0GCSqGSIb3DQEBBQUAA4IBAQB1vWzkxtKSqpR2ndRJLLCaRXDD9sATk/c5ziSvi1mNqDANXyDazbeJHk7d6XHmpQdrrEp/Z7JLJZreN392ZTPf9ltzzXKBfWaL7c434suROj74fmYqK5d3MVAnIe97V/KvJdJJ+n/ac+c0A8VkJ0REI8tXuqjF2D/M5AFAIBUQzdMaLSEbERpEq7xiDJTQStzcVUV7nkgOBZoQKBx/P8e94/QIjOPwm+RaGEQo1CHQz7UbJ8GppE86M0lWlkkB8tQT6RJXTy+1DVp9Q+p+EYvnWWWZSyOCArH7EIwt5heDCP2iy5V3nIcE2yN7EPjbUJAXOFx6BokCIk70M+Y+ULQJ',
    'ChannelLos': 'Channel_1',
    'Queries': [{
        'Name': 'INFORMATION_SCHEMA.TABLES',
        'NameAlias': 'INFORMATION_SCHEMA.TABLES',
        'Script': 'SELECT * FROM INFORMATION_SCHEMA.TABLES',
        'QueryId': 'cf79e0f0-b750-47a0-a4d9-1ac316d038be',
        'Meta': {
            'FieldParamMappings': None,
            'BatchSize': 1000
        },
        'Parameters': None,
        'SubQueries': None
    }],
    'Description': 'test',
    'DataSource': {
        'Version': 'Microsoft SQL Server',
        'DataServer': 'SQL RDBMS',
        'Application': 'Generic MS SQL Server Database',
        'DataConnector': 'Data Provider for SQL Server'
    },
    'Username': 'NAM\\aroche009',
    'ExtractionMethod': 'Direct',
    'EcfVersion': '1.6',
    'AdditionalTags': None,
    'DatabaseServerName': None,
    'SchemaName': None,
    'ClientName': 'test',
    'CurrentApplicationId': 'b8f46751-3484-4c0d-93ac-8798eea9988e',
    'Territory': 'East',
    'SetId': None,
    'OriginalApplicationId': 'b8f46751-3484-4c0d-93ac-8798eea9988e',
}

ORACLE_ECF_CONTENT = [
    'AdditionalTags', 'ChannelLos', 'ClientId', 'ClientName', 'CreatedDate',
    'CurrentApplicationId', 'DataDestination', 'DataSource', 'DataSourceId',
    'DatabaseName', 'DatabasePort', 'DatabaseServerName', 'Description',
    'EcfFileName', 'EcfVersion', 'ErpInstance', 'ExpiryDate', 'ExtractionMethod',
    'FileUploadMethod', 'IsDatabaseNameRequired', 'OriginalApplicationId',
    'PublicKey', 'PublicKeyType', 'Queries', 'RequestId', 'SaveToRepository',
    'SchemaName', 'SetId', 'Territory', 'Username'
]


@unittest.skip('ECFs updated to 2.0, these tests need to be updated')
class TestReadDataFromECF(unittest.TestCase):
    """Can read data from encrypted ECF files."""

    def test_mssql_v1_6(self):
        """Can read in data from an MSSQL v1.6 ECF file."""
        filepath = 'tests/assets/MSSQL-v1.6-info-schema-table.ecf'
        data = ecfreader.read_encrypted_json(filepath)
        self.assertEqual(data, MSSQL_ECF_CONTENT)

    def test_sap_v1_6(self):
        """Can read in data from an SAP v1.6 ECF file."""
        filepath = ".\\tests\\assets\\SAP-v1.6-bkpf-bseg.ecf"
        data = ecfreader.read_encrypted_json(filepath)
        self.assertTrue(data)

    def test_oracle_v1_6(self):
        """Can read in data from a ECF file for v1.6 GATT Extract."""
        filepath = 'tests/assets/Oracle-v1.6-R12-gl-headers.ecf'
        data = ecfreader.read_encrypted_json(filepath)
        self.assertEqual(data['EcfVersion'], '1.6')
        self.assertEqual(sorted(data), ORACLE_ECF_CONTENT)
        self.assertEqual(sys.getsizeof(data), 1632)


@unittest.skip('ECFs updated to 2.0, these tests need to be updated')
class TestECFParsing(unittest.TestCase):
    """Can read data from encrypted ECF files."""

    def test_oracle_v1_6(self):
        """Can parse an Oracle ECF using the GATT v1.6 format."""
        filepath = 'tests/assets/Oracle-v1.6-R12-gl-headers.ecf'
        metadata = ecfreader.get_ecf_meta_data(filepath=filepath, encrypted=True)
        self.assertEqual(len(metadata), 3)
        self.assertEqual(metadata[0].table_name, 'GL.GL_JE_BATCHES')
        self.assertEqual(metadata[0].table_alias, 'GL_JE_BATCHES')
        self.assertEqual(metadata[0].query_text, "SELECT  GL_JE_BATCHES.JE_BATCH_ID,GL_JE_BATCHES.NAME,GL_JE_BATCHES.STATUS,GL_JE_BATCHES.POSTED_DATE,GL_JE_BATCHES.CHART_OF_ACCOUNTS_ID,GL_JE_BATCHES.DEFAULT_EFFECTIVE_DATE FROM GL.GL_JE_BATCHES INNER JOIN (SELECT DISTINCT CHART_OF_ACCOUNTS_ID FROM GL.GL_LEDGERS WHERE GL_LEDGERS.LEDGER_ID IN('1','42','62','102','103','104','122','123','124','125','126','142','143','162','163','165','186','187','205','207','208','268','288','289','290','332','393','454','477','609','650','670','874','875','876','914','916','956'))GL_LEDGERS ON GL_JE_BATCHES.CHART_OF_ACCOUNTS_ID = GL_LEDGERS.CHART_OF_ACCOUNTS_ID WHERE (GL_JE_BATCHES.DEFAULT_EFFECTIVE_DATE BETWEEN '01-JAN-00' AND '31-DEC-16')")


@unittest.skip('ECFs updated to 2.0, these tests need to be updated')
class TestParseInvalidECFs(unittest.TestCase):
    """Appropriate errors are raised when ECF is invalid."""

    def test_duplicate_table_aliases(self):
        """Error raised when ECF has duplicate TableAliases."""
        filepath = 'tests/assets/Oracle-v1.6-R12-duplicate-table-aliases.ecf'
        with self.assertRaises(AssertionError):
            ecfreader.get_ecf_meta_data(filepath=filepath, encrypted=True)
