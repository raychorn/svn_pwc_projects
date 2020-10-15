"""Tests for the pyextract.connect.sqlite module."""
# pylint: disable=no-member

import os
import shutil
import tempfile
import unittest

import apsw

from pyextract.connect import sqlite


class TestConnectZipped(unittest.TestCase):
    """Can connect to Zipped / Encrypted SQLite files."""

    password = 'testpassword'
    table = 'testtable'

    def setUp(self):
        """Create testing folder."""
        self.folder = tempfile.mkdtemp()

    def tearDown(self):
        """Remove testing folder."""
        shutil.rmtree(self.folder, ignore_errors=True)

    def test_aes128(self):
        """Test that a zipped, encrypted database can be created and secured."""
        # Designate location for test DB, and assert it does not exist yet
        filepath = os.path.join(self.folder, 'zipped.dat')
        self.assertFalse(os.path.exists(filepath))

        # Create new zipped DB, with a new table to invoke zip/encrypt process
        zipper = sqlite.SQLiteMessenger(filepath=filepath, is_zipped=True,
                                        password=self.password)
        zipper.create_table(self.table, ('a', 'b'), ('tinyint', 'tinyint'))
        self.assertTrue(os.path.exists(filepath))

        # Cannot connect to database with an unzipped connection
        with self.assertRaises(apsw.IOError):
            sqlite.SQLiteMessenger(filepath=filepath, is_zipped=False)

        # Cannot connect to database with a bad password
        with self.assertRaises(apsw.IOError):
            sqlite.SQLiteMessenger(filepath=filepath, is_zipped=True,
                                   password='badpassword')

        # Cannot connect to database with an AES-256 connection
        with self.assertRaises(apsw.IOError):
            sqlite.SQLiteMessenger(filepath=filepath, is_zipped=True,
                                   password=self.password, aes256=True)

        # Can connect to database with a new messenger with a good password
        msgr = sqlite.SQLiteMessenger(filepath=filepath, is_zipped=True,
                                      password=self.password)
        self.assertEqual(msgr.list_all_tables(), [self.table])

    def test_aes256(self):
        """Can be encrypted with AES-256 instead of the default."""
        # Designate location for test DB, and assert it does not exist yet
        filepath = os.path.join(self.folder, 'zipped.dat')
        self.assertFalse(os.path.exists(filepath))

        # Create new zipped DB, with a new table to invoke zip/encrypt process
        zipper = sqlite.SQLiteMessenger(filepath=filepath, is_zipped=True,
                                        password=self.password, aes256=True)
        zipper.create_table(self.table, ('a', 'b'), ('tinyint', 'tinyint'))
        self.assertTrue(os.path.exists(filepath))

        # Cannot connect to database with an unzipped connection
        with self.assertRaises(apsw.IOError):
            sqlite.SQLiteMessenger(filepath=filepath, is_zipped=False)

        # Cannot connect to database with a bad password
        with self.assertRaises(apsw.IOError):
            sqlite.SQLiteMessenger(filepath=filepath, is_zipped=True,
                                   password='badpassword', aes256=True)

        # Cannot connect to database with an AES-128 connection
        with self.assertRaises(apsw.IOError):
            sqlite.SQLiteMessenger(filepath=filepath, is_zipped=True,
                                   password=self.password, aes256=False)

        # Can connect to database with a new messenger with a good password
        msgr = sqlite.SQLiteMessenger(filepath=filepath, is_zipped=True,
                                      password=self.password, aes256=True)
        self.assertEqual(msgr.list_all_tables(), [self.table])



class TestSQLiteQueries(unittest.TestCase):
    """Can create valid queries for a SQLite database."""

    def test_sqlite_table_exists(self):
        """Can build a query to check if a table exists."""

        result = sqlite.sqlite_table_exists()
        expected = """
            SELECT name FROM sqlite_master
            WHERE type = 'table' AND name = ?
            """
        self.assertEqual(result.split(), expected.split())

    def test_sqlite_insert_into(self):
        """Can build a statement to insert data into a table."""

        result = sqlite.sqlite_insert_into('TestTable', number_columns=5)
        expected = """
            INSERT INTO "TestTable" VALUES (?, ?, ?, ?, ?)
            """
        self.assertEqual(result.split(), expected.split())

    def test_sqlite_create_table(self):
        """Can build a statement to create a table."""

        result = sqlite.sqlite_create_table('TestTable',
                                            columns=('col1', 'col2', 'col3'))
        expected = """
            CREATE TABLE IF NOT EXISTS
                "TestTable" ("col1" TEXT, "col2" TEXT, "col3" TEXT);
            """
        self.assertEqual(result.split(), expected.split())

    def test_sqlite_create_table_dtypes(self):
        """Can build a statement to create a table with specific datatypes."""

        result = sqlite.sqlite_create_table('TestTable',
                                            columns=('col1', 'col2', 'col3'),
                                            datatypes=('TEXT', 'REAL', 'INT'))
        expected = """
            CREATE TABLE IF NOT EXISTS
                "TestTable" ("col1" TEXT, "col2" REAL, "col3" INT);
            """
        self.assertEqual(result.split(), expected.split())


class TestUpdateFilepath(unittest.TestCase):
    """Can update the filepath of an open SQLite database."""

    @classmethod
    def setUpClass(cls):
        """Create testing folder."""
        cls.folder = tempfile.mkdtemp()

    @classmethod
    def tearDownClass(cls):
        """Remove testing folder."""
        shutil.rmtree(cls.folder)

    def test(self):
        """Create a SQLite database, move it, and assure that it moved."""
        filepath = os.path.join(self.folder, 'test.dat')
        table = 'testtable'

        msgr = sqlite.SQLiteMessenger(filepath=filepath)
        msgr.create_table(table, ('a', 'b'), ('tinyint', 'tinyint'))
        self.assertTrue(os.path.exists(filepath))

        newpath = os.path.join(self.folder, 'nested', 'newname.dat')
        self.assertFalse(os.path.exists(newpath))
        msgr.update_filepath(newpath)
        self.assertTrue(os.path.exists(newpath))
        self.assertEqual(msgr.list_all_tables(), [table])
