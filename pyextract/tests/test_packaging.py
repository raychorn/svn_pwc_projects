"""Tests for the packaging module of the pyextract program."""

import json
import os
import shutil
import tempfile
import unittest
import uuid
from zipfile import ZipFile

from pyextract import SQLiteMessenger
from pyextract.ecfreader import read_encrypted_json
from pyextract.packaging import DataPackage, LFUClient, SFTPClient


INVALID_ECF = 'ecfs/oracle-r12-2-0-gl-headers-unencrypted.ecf'
VALID_ECF = 'ecfs/oracle-r12-2-0-gl-headers.ecf'
TEST_ZIP_ARCHIVE = 'tests/assets/test.zip'


class TestPackageContentErrors(unittest.TestCase):
    """Tests to ensure invalid content cannot be added to the data package."""

    def test_bad_ecf_file(self):
        """Cannot add an invalid ECF file to the data package."""
        with self.assertRaises(Exception):
            DataPackage(INVALID_ECF)


class TestEmptyDataPackage(unittest.TestCase):
    """Tests with an empty data package."""

    def setUp(self):
        """Create a temp directory for testing."""
        self.testfolder = tempfile.mkdtemp()
        self.filepath = os.path.join(self.testfolder, 'test.zip')

    def tearDown(self):
        """Delete temporary testing directory."""
        shutil.rmtree(self.testfolder)

    def test_output(self):
        """Can create an empty package with no contents."""
        package = DataPackage()
        # Assert the ZIP file does not exist, then create it
        self.assertFalse(os.path.exists(self.filepath))
        package.create(self.filepath)
        # Assert the ZIP file was created with no contents
        with ZipFile(self.filepath, 'r') as zipper:
            self.assertEqual(len(zipper.namelist()), 0)


class TestECFOnlyDataPackage(unittest.TestCase):
    """Tests with a data package with only ECF / EPF / MetaData files."""

    def setUp(self):
        """Create a temp directory for testing with a DataPackage in it."""
        self.testfolder = tempfile.mkdtemp()
        self.filepath = os.path.join(self.testfolder, 'test.zip')
        package = DataPackage(VALID_ECF)
        package.create(self.filepath)

    def tearDown(self):
        """Delete temporary testing directory."""
        shutil.rmtree(self.testfolder)

    def test_output(self):
        """Output is as expected in the zipped folder."""
        with ZipFile(self.filepath, 'r') as zipper:
            # Assert the ZIP file was created with only the expected files
            self.assertEqual(sorted(zipper.namelist()), [
                'Encrypted_RequestInfo.EPF',
                'Metadata.txt',
                'oracle-r12-2-0-gl-headers.ecf',
            ])
            zipmetadata = zipper.read('Metadata.txt')
            zipecf = zipper.read('oracle-r12-2-0-gl-headers.ecf')
            zipepf = zipper.read('Encrypted_RequestInfo.EPF')

        # Assert the Metadata.txt file has the correct content
        metadata_content = json.loads(zipmetadata.decode())
        self.assertEqual(sorted(metadata_content), [
            'PackagePath', 'PublicKey', 'RequestId', 'Topic'
        ])

        # Assert the ECF (as encrypted bytes) is identical to the original
        with open(VALID_ECF, 'rb') as stream:
            self.assertEqual(zipecf, stream.read())

        # Unzip EPF file to temp directory for inspection
        unzippedepf = os.path.join(self.testfolder, 'unzipped.EPF')
        with open(unzippedepf, 'wb') as stream:
            stream.write(zipepf)


class TestFullDataPackage(unittest.TestCase):
    """Tests with a package with ECF, EPF, MetaData, and SQLite files."""

    def setUp(self):
        """Create temp testing directory with a full data package in it."""
        self.testfolder = tempfile.mkdtemp()
        self.filepath = os.path.join(self.testfolder, 'test.zip')
        package = DataPackage(VALID_ECF)
        # Add the SQLite specific files to the data package
        filepath = os.path.join(self.testfolder, 'test.dat')
        sqlite = SQLiteMessenger(filepath=filepath)
        # Include a 10x10 table of zeroes in the SQLite database
        columns = ['column{}'.format(i) for i in range(10)]
        datatypes = ['tinyint'] * 10
        sqlite.create_table('TestTable', columns, datatypes)
        testdata = [[0 for _ in range(10)] for _ in range(10)]
        sqlite.insert_into('TestTable', testdata, columns)
        package.add_sqlite_file(filepath)
        # Create the ZIP package from ECF and SQLite files
        package.create(self.filepath)

    def tearDown(self):
        """Delete temporary testing directory."""
        shutil.rmtree(self.testfolder)

    def test_output(self):
        """Output is as expected in the zipped folder."""
        with ZipFile(self.filepath, 'r') as zipper:
            # Assert the ZIP file was created with only the expected files
            self.assertEqual(sorted(zipper.namelist()), [
                'Encrypted_RequestInfo.EPF',
                'Metadata.txt',
                'oracle-r12-2-0-gl-headers.ecf',
                'test.dat',
            ])
            zipepf = zipper.read('Encrypted_RequestInfo.EPF')
            assert zipper.read('test.dat')


class TestSFTPClient(unittest.TestCase):
    """Tests for upload of data packages to a remote SFTP folder."""
    # pylint: disable=line-too-long

    creds = {
        "HOSTADDRESS": "mft4app-west-stage.pwcinternal.com",
        "PASSWORD": "Of9zsDDX59",
        "USERNAME": "haloerp_udx_writer_qa",
        "KEY": "MIIEoAIBAAKCAQEAwXKCDYS4A+rJjlK5+AFH4HF6wJDB/Qg4z44RaSmGWRYCapQSg+EaHYXZxB8+Mrgko3iOgeNWghTj+sq7un9gac3/jdEA/kBklxAkZHxtrARjE6tn4mXXeo6UOC9Y/mjRlG29W6ko3k8P6Gs0nPEJ02SrLjiZ4AcoAGoZV5VuzAVkn4yJECWlVMCBxCcOc3lOePGKKzU3e353hVPy7YaJ8/ZlvyphGDZKyBxacomFsnOjj3Ot42Rad56WNBR7sKzywgsfzi3BQSX+PEqtAZFBniyt4KJmifp3gEAo45wS1UoF8Av2Vhh7WsaR1HMat5kd4T2ByGxUE+UEBnnM3iP/9wIBEQKCAQAREaIQQGqW7xHIyxBotNGi3NYgDMXSj8jHBQGJR228kQA2lJg40BjkfMBkIN/XTHu0EiqxG5axH/X/ilvbv/HrN9LIv540jTYNVD9yRzbaeNuQy1v9Y1bHDJSbi7UHY5oFkTZa6Ude6NvBrxs7BjWSpwALm5UbS+zw+k2HulyZh9oTs3HkuUcSWkocY7GHmuX4gGif/fRfe5V5yTTggRkoMEw8ZPAjwacFQ3/oEXxpvP/ofQELmVjQz8mVx37vnD+LeDsQJ0x+A5Sg5W67gMQ/lh7bRTEUCGu4t55/gQ3JyUeiP+Zk/sKuTnaDVHqRKQ0LBrfylPQEsjyiCCuHwzUNAoGBAOAhTLtvN+u5/P5GEq53xulhCZXVDo4ExwblstMADHIWeV8CwqpeMIuvtzaSS9Ql4d2tSd4+lPkiaC/ff+9b8TGDeivm2ouPfNtfk/I3b9HSXjN7WGsgvOyCl4Ij/MUwCaCudt/rGwFnatajfFFsG8Y+YqZkTsX+MqdpPUb+ET6lAoGBANz0ThgrZJQgGZDm/9lQLDbCOKfrky08lCfmDh39GFAWBFFlRRUkyFoyEs+aIi+I3nGXS8PMBXLBWM8IakMOe5u8k6va6ygdgI5n49PSbhT7KRywOc8w3ht63PfFPga2VzloFjidC4Y74nwLOVwzrBkl5Av8MBXgeQLGNUMyw71rAoGATxrPya7IcVCzpQmsPZOvnanHJdK518Vza9iZd6WqCiYMuB8Xh2yJmrZ85hWELMITe2pWTnBw0GZ/H/SHgaf6xi5nWsnyx9hKL07o7BOQ4KSZt9EuQ+1v2wDqLe6VcsWpC4jeqWIJiAZh8WbglTU2+qybSckMvlm3hmFg6+EzQ0kCgYAz/T+NN2L1rTMxJ0tCMPtYLbL6VYwKpNeQ6tYlLH4w9h8iNfIjF7arz4v0nLy/4/gaui/x1abt0yP0enNbEndv8CK/BlVy2cPlRZ8EqfvIs5Ez7TrHVsrZShXf8iy2SQV261CFyplMwsvg85UGor8U+dtOHTh9njqXPbIt7dO0GQKBgH6gpr04QNhcacPblsA4TZr7RN/9p2sVJ1kEuPATLAUvEHViczlGRFW696ZTUJF/tTg2wDyFfuz9hVEboU2yXAdYLunl6z4td0qjYQXVSFKPaNa8ablkT1n5nZsobuYiNM9ZjeRuMzv3vcuYewwr0SkxPVxkd1xeZkjILSIDUTi3",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "ec:80:8e:3e:70:79:6a:44:cb:7c:df:4c:0e:89:c0:63",
        "CURRDIRECTORY": "PwCIT_HALOERP-QA/UDX"
    }

    @classmethod
    def setUpClass(cls):
        """Set up a SFTP client to use during testing."""
        cls.client = SFTPClient(creds=cls.creds, territory="WEST")

    def test_write_new_file(self):
        """Can write a new file to the SFTP site."""
        uniquename = '{}.zip'.format(uuid.uuid4())
        self.client.send(TEST_ZIP_ARCHIVE, newname=uniquename)

    def test_overwrite_file(self):
        """Cannot overwrite files that already exist on the SFTP site."""
        with self.assertRaises(AssertionError):
            self.client.send(TEST_ZIP_ARCHIVE, overwrite=False)


class TestLFUClient(unittest.TestCase):
    """Tests for upload of data packages using the LFU service."""

    host = "https://lfuapi-west-qa1.pwcinternal.com"
    token = "d15281a5-afbd-4538-b99c-fa6b1ca02798"

    @classmethod
    def setUpClass(cls):
        """Set up a SFTP client to use during testing."""
        cls.client = LFUClient(host=cls.host, token=cls.token)

    def test_upload(self):
        """Can upload a local file to the LFU service."""
        self.client.send(filepath=TEST_ZIP_ARCHIVE, chunk_size=1000)

    def test_upload_no_network(self):
        """Receive a useful error message when network is disconnected."""
        disable_local_network()
        with self.assertRaises(Exception):
            self.client.send(filepath=TEST_ZIP_ARCHIVE, chunk_size=1000)


def disable_local_network():
    """Monkey patch socket to block network connections"""
    import socket
    def guard(*args, **kwargs):
        raise Exception("I told you not to use the Internet!")
    socket.socket = guard
