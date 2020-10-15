"""Query interface built on a connection to a CSV file."""
#pylint: skip-file

# WARNING -- This Messenger is not currently tested or supported
# We will remove this file, or delete this comment once a decision
# has been made on if we will include it with the production PyExtract.

import csv
import multiprocessing
import os

from .. import utils
from ..queries import ParsedQuery
from .base import ABCMessenger


LOGGER = multiprocessing.get_logger()


class CSVMessenger(ABCMessenger):
    """Interfaces with a folder of CSV files."""

    def __init__(self, folder, *args, **kwargs):
        """Instantiate a new instance of a connector."""

        super(CSVMessenger, self).__init__()
        self._conn = None
        self.folder = folder
        self._extract_filepath = None

    def get_metadata_from_query(self, query):
       """Get metadata about columns from a SQL query."""
       table = ParsedQuery(query).table
       filepath = os.path.join(self.folder, table + '.csv')
       assert os.path.exists(filepath), \
           'Filepath does not exist: {}'.format(filepath)
       with open(filepath) as csv_file:
           reader = csv.DictReader(csv_file)
           row = next(reader)
           fieldname = reader.fieldnames

           metadata = []
           for name in fieldname:
               if name == '':
                   name = 'BLANK'
               config = {
                   'sourceSystem': 'CSV',
                   'sourceFieldName': name,
                   'sourceType': 'TEXT',
                   'sourceFieldLength': None,
                   'sourceFieldNumericPrecision': None,
                   'source_field_nullable': None,
                   'targetFieldName': name,
                   'sqlite_datatype': 'TEXT'
               }
               metadata += [config]

       return metadata

    def drop_table_if_exists(self, table):
        """Drop a table from the database."""
        filepath = os.path.join(self.folder, table + '.csv')
        assert os.path.exists(filepath), \
            'Filepath does not exist: {}'.format(filepath)
        os.remove(filepath)

    def create_table(self, table, columns):
        """Create a new table on the database."""
        filepath = os.path.join(self.folder, table + '.csv')
        assert not os.path.exists(filepath), \
            'Filepath already exists: {}'.format(filepath)
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(columns)

    def insert_into(self, table, data, columns):
        """Insert data into a table in the database."""
        filepath = os.path.join(self.folder, table + '.csv')
        assert os.path.exists(filepath), \
            'Filepath does not exist: {}'.format(filepath)
        with open(filepath, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            for row in data:
                writer.writerow(row)

    def begin_extraction(self, metadata, chunk_size=None):
        """Begin pulling data from a query."""
        query = metadata.parameters
        table = ParsedQuery(query).table
        filepath = os.path.join(self.folder, table + '.csv')
        assert os.path.exists(filepath), \
            'Filepath does not exist: {}'.format(filepath)
        self._extract_filepath = filepath
        self.index = 1

    def continue_extraction(self, chunk_size=None):
        """Continue pulling data from a query."""
        with open(self._extract_filepath, 'r', newline='') as csvfile:
            reader = csv.reader(csvfile)
            # Skip rows that were already read
            for _ in range(self.index):
                next(reader)
            # Read next chunk of rows from the reader into memory
            data = []
            for _ in range(chunk_size):
                try:
                    data += [next(reader)]
                    self.index += 1
                except StopIteration:
                    break
        return data

    def finish_extraction(self):
        """Clean up after a long-term extraction is finished"""
        self._extract_filepath = None
