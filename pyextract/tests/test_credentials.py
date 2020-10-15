"""Test the credentials module."""
# pylint: disable=no-member

import os
import shutil
import tempfile
import unittest

import apsw

from pyextract import credentials


class TestCredentials(unittest.TestCase):
    """Can save and read back a set of credentials to the database."""

    def setUp(self):
        self.folder = tempfile.mkdtemp()
        self.database = os.path.join(self.folder, 'credentials.dat')
        self.password = 'testpassword'

    def tearDown(self):
        shutil.rmtree(self.folder)

    def test_save_and_load(self):
        """Save data to the DB, read it back, and assert its the same."""

        data_to_save = {
            'mssql': {
                'server': 'TESTSERVER',
                'database': 'TESTDATABASE',
            },
            'ebsr12': {
                'server': 'STRL069063.MSO.NET',
                'port': '1521',
                'service_id': 'VIS01',
                'user': 'TESTUSER',
                'password': 'TESTPASS',
            },
        }

        for key, creds in data_to_save.items():
            credentials.save(filepath=self.database,
                             password=self.password,
                             key=key, json=creds)

        # Ensure database was created and is password protected
        self.assertTrue(os.path.exists(self.database))
        with self.assertRaises(apsw.IOError):
            credentials.load(filepath=self.database,
                             password='badpassword', key='mssql')

        for key, creds in data_to_save.items():
            saved = credentials.load(filepath=self.database,
                                     password=self.password, key=key)
            self.assertEqual(creds, saved)
