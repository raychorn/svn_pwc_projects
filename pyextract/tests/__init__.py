"""Testing code for the pyextract module."""

import logging
import multiprocessing
import os

from pyextract import credentials
from pyextract import utils

utils.setup_logging()
logging.getLogger().setLevel(logging.CRITICAL)
multiprocessing.get_logger().setLevel(logging.CRITICAL)

utils.validate_test_environment()

# Load master password for PyExtract from environment variables
__PASSWORD = os.environ.get("PYEXTRACT_PASSWORD", None)
if not __PASSWORD:
    raise RuntimeError(
        "Environment variable PYEXTRACT_PASSWORD not set. "
        "You can get this password from the PyExtract dev team."
    )

# Load testing credentials from encrypted SQLite database
CRED_DB = os.path.join(os.path.dirname(__file__), 'credentials.dat')
CREDENTIALS = {
    'MSSQL': credentials.load(CRED_DB, password=__PASSWORD, key='mssql'),
    'EBSR12': credentials.load(CRED_DB, password=__PASSWORD, key='ebsr12'),
    'PSFT92': credentials.load(CRED_DB, password=__PASSWORD, key='psft92'),
    'SAP': credentials.load(CRED_DB, password=__PASSWORD, key='sap'),
    'DB2': credentials.load(CRED_DB, password=__PASSWORD, key='db2'),
}
