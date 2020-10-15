"""Tests for the pyextract.utils module."""

import os
import shutil
import tempfile
import unittest

from pyextract import utils


class TestParseQueryFromFilepath(unittest.TestCase):
    """Can read a SQL query from a SQL script filepath."""

    def setUp(self):
        """Create a temporary directory"""
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        """Remove the directory after the test"""
        shutil.rmtree(self.test_dir)

    def test_parse_query(self):
        """Can read SQL query from filepath."""

        test_query = 'SELECT * FROM proc_BKPF_BSEG'

        # Create a test file in the temporary directory
        testfile = os.path.join(self.test_dir, 'test.sql')
        with open(testfile, 'w') as output:
            output.write(test_query)

        result_query = utils.parse_query_from_filepath(testfile)
        self.assertEqual(result_query, test_query)
