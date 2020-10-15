"""Configuration for QA and Dev builds used to test the PyExtract program."""
# pylint: disable=wildcard-import,unused-wildcard-import

from defaultconfig import *

# Allow use of weaker or no encryption
ENCRYPTION_OPTIONS = ('AES-256', 'AES-128', 'None')

# Allow user to choose SFTP and LFU upload locations
ALLOW_USER_UPLOAD_LOCATION = True

# Use a hardcoded public key to encrypt EPF files
# !!!!! DO NOT CHANGE THIS SETTING TO
USE_PWC_ECF_PUBLIC_KEY = True

# Allow user to set the writer_timeout for testing impact of 'None'
ALLOW_WORKER_TIMEOUT_SETTING = True
