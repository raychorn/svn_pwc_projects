"""A sample script that uses PyExtract for a data extraction."""

import os
import pyextract
from pyextract.connect.mssql import MSSQLMessenger

EXTRACT_DIR = os.path.join(os.path.expanduser('~'), 'Desktop', 'test_extraction')
LOGFILE = os.path.join(EXTRACT_DIR, 'test.log')

SOURCE = MSSQLMessenger(
    host=r'MATLKHALOSQP008',
    database=r'c_0045_Innophos',
    schema=r'r_FY16_GL',
)

OUTPUT = pyextract.SQLiteMessenger(
    filepath=os.path.join(EXTRACT_DIR, 'NewExtract.dat'),
)

def main():
    """Core logic must remain in a function for multiprocessing."""
    stream = pyextract.DataStream(SOURCE, chunk_size=50000,
                                  row_limit=None, queue_size=20)
    extraction = pyextract.Extraction(source=stream, output=OUTPUT,
                                      logfile=LOGFILE)
    query = 'SELECT TOP 100 "GLREG#" FROM r_FY16_GL.F0911'
    extraction.extract_from_query(query)


if __name__ == '__main__':
    main()
