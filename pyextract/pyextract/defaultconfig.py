"""Default configuration values (Production) for the PyExtract program."""
# pylint: disable=line-too-long

# Names for tables used to log pause resume/status in SQLite databases
LOG_TABLE = 'ExtractionLogs'
METADATA_TABLE = 'TableMetaData'
STATUS_TABLE = 'TableExtractions'

# System and user name for keyring password stores
KEYRING_SYSTEM = 'pyextract'
KEYRING_USER = 'extraction_user'

# Thresholds for SAP query timeouts and time before retrying the query
QUERY_TIMEOUT = 1500
QUERY_RETRY_WAIT_TIME = 2

# Disk space free (in GBs) recommended for user to run an extraction
RECOMMENDED_SPACE = 2

# Submodules / ERPs that the user can use for data extraction
CONNECT_SUBMODULES = ('connect.sap', 'streams.sapstream', 'connect.oracle',
                      'connect.db2', 'connect.mssql', 'connect.mysql')

# Encryption options for the source data held in SQLite files
ENCRYPTION_OPTIONS = ('AES-256', )

# Values that must be provided by user / saved for each ERP system
ERPS_TO_CREDENTIALS = {
    'SAP': ('name', 'type', 'client', 'user', 'language',
            'ashost', 'sysnr', 'mshost', 'msserv', 'sysid', 'group',
            'snc_qop', 'snc_myname', 'snc_partnername', 'snc_lib', 'password'),
    'ORACLE': ('name', 'type', 'host', 'port',
               'system_id', 'service_name', 'user', 'password'),
    'MSSQL': ('name', 'type', 'host', 'port', 'schema', 'database', 'user', 'password', 'driver'),
    'MYSQL': ('name', 'type', 'host', 'port', 'dsn', 'database', 'user', 'password', 'driver'),
    'DB2': ('name', 'type', 'host', 'port', 'database', 'user', 'password'),
}

# Datatypes from source systems that are not supported by the Extract program
BANNED_DB2_DTYPES = ("BLOB", "VARBIN", "ROWID", "XML", "LONGVARBIN")
BANNED_SQL_DTYPES = ("BINARY", "VARBINARY", "ROWVERSION", "XML", "FILESTREAM",
                     "SQL_VARIANT", "IMAGE", "VARBINARY(MAX)")
BANNED_SAP_DTYPES = ("D16D", "D34D", "STRG", "D16R", "INT2", "D34R", "SSTR",
                     "D16S", "D34S", "RSTR")
BANNED_ORCL_DTYPES = ("LONG", "LONG_RAW", "XML Type", "RAW", "LONG_STRING",
                      "BLOB", "CLOB", "LONG_BINARY", "BINARY", "OBJECTVAR",
                      "NCLOB")

# Datatypes available in SQLite
# https://www.techonthenet.com/sqlite/datatypes.php
DATETIME_DTYPES = ('DATE', 'DATETIME', 'TIMESTAMP', 'TIME')
INT_DTYPES = ('TINYINT', 'SMALLINT', 'MEDIUMINT', 'INT', 'INTEGER', 'BIGINT',
              'INT2', 'INT4', 'INT8')
FLOAT_DTYPES = ('NUMERIC', 'DECIMAL', 'REAL', 'DOUBLE', 'DOUBLE PRECISION',
                'FLOAT', 'BOOLEAN')

# Mapping of ceODBC Python datatypes to SQL Server datatypes
CEODBC_TO_MSSQL_DTYPES = {
    'BigIntegerVar': 'BIGINT',
    'BinaryVar': 'VARBINARY',
    'BitVar': 'BIT',
    'DateVar': 'DATETIME',
    'DecimalVar': 'DECIMAL',
    'DoubleVar': 'FLOAT',
    'IntegerVar': 'INT',
    'LongBinaryVar': 'VARBINARY(MAX)',
    'LongStringVar': 'VARCHAR(MAX)',
    'LongUnicodeVar': 'NVARCHAR(MAX)',
    'StringVar': 'VARCHAR',
    'TimeVar': 'TIME',
    'TimestampVar': 'TIMESTAMP',
    'UnicodeVar': 'NVARCHAR',
}

# If True, allow the user to choose LFU and SFTP locations to upload to
# If False, force uploads to production environments based on the
# 'UploadMethod' and 'Territory' values in the ECF
ALLOW_USER_UPLOAD_LOCATION = False

# User-facing names for LFU upload locations to their connection details
LFU_UPLOAD_LOCATIONS = {
    "DEV-EAST": {
        "host": "https://lfuapi-east-dev1.pwcinternal.com",
        "token": "93ec6e44-4a59-402b-8476-1ffe43403ed9",
    },
    "DEV-WEST": {
        "host": "https://lfuapi-west-dev1.pwcinternal.com",
        "token": "fcc73052-0623-44be-a0be-0cc030915b01",
    },
    "DEV-CENTRAL": {
        "host": "https://lfuapi-central-dev1.pwcinternal.com",
        "token": "b7d300bf-40d6-41c9-b541-fccddff7907c",
    },
    "QA-EAST": {
        "host": "https://lfuapi-east-qa1.pwcinternal.com",
        "token": "74570d3f-1acf-45eb-9fb7-bbc282c09541",
    },
    "QA-WEST": {
        "host": "https://lfuapi-west-qa1.pwcinternal.com",
        "token": "d15281a5-afbd-4538-b99c-fa6b1ca02798",
    },
    "QA-CENTRAL": {
        "host": "https://lfuapi-central-qa1.pwcinternal.com",
        "token": "dc105e47-110d-430a-b68a-03cefc27fe3c",
    },
    "QA2-EAST": {
        "host": "https://lfuapi-east-qa2.pwcinternal.com",
        "token": "345d3f49-104a-47b0-9dce-798f652cb83a",
    },
    "QA2-WEST": {
        "host": "https://lfuapi-west-qa2.pwcinternal.com",
        "token": "6b45f036-1d28-4fc6-9e13-38a2d449784d",
    },
    "QA2-CENTRAL": {
        "host": "https://lfuapi-central-qa2.pwcinternal.com",
        "token": "a095dc5c-d517-4843-bbfd-a128dbcdcb0e",
    },
    "QA3-EAST": {
        "host": "https://lfuapi-east-qa3.pwcinternal.com",
        "token": "d04544cb-68d0-46f5-89fb-1e1031ba5d67",
    },
    "QA3-WEST": {
        "host": "https://lfuapi-west-qa3.pwcinternal.com",
        "token": "007807d9-4872-4b30-885b-27e38a63578c",
    },
    "QA3-CENTRAL": {
        "host": "https://lfuapi-central-qa3.pwcinternal.com",
        "token": "17324753-7162-44ca-803d-97b0d558da5e",
    },
    "INT-EAST": {
        "host": "https://lfuapi-east-int.pwcinternal.com",
        "token": "fb23a7a3-b444-4447-98dc-fc2c61830112",
    },
    "INT-WEST": {
        "host": "https://lfuapi-west-int.pwcinternal.com",
        "token": "d5cdbf26-4d05-45f4-91af-1119b86f6510",
    },
    "INT-CENTRAL": {
        "host": "https://lfuapi-central-int.pwcinternal.com",
        "token": "84195f9e-07b8-4d42-af87-071d4bc8444b",
    },
    "STAGE-EAST": {
        "host": "https://lfuapi-east-stg.pwc.com",
        "token": "08efa92e-7006-4087-8712-a150c1b5e56d",
    },
    "STAGE-WEST": {
        "host": "https://lfuapi-west-stg.pwc.com",
        "token": "2a0eb2dd-f07f-4fd1-b0a4-ed2e534c446a",
    },
    "STAGE-CENTRAL": {
        "host": "https://lfuapi-central-stg.pwc.com",
        "token": "4b62a790-91e4-4db6-95c8-1cf16de5830d",
    },
    "PROD-EAST": {
        "host": "https://lfuapi-east.pwc.com",
        "token": "f29d091c-5d97-4e4a-bed9-8b8af0edef7a",
    },
    "PROD-WEST": {
        "host": "https://lfuapi-west.pwc.com",
        "token": "e2f2eb83-3348-4f73-b6a7-303e48a3fc45",
    },
    "PROD-CENTRAL": {
        "host": "https://lfuapi-central.pwc.com",
        "token": "387c392f-d832-47af-be72-01a71fc299b5",
    },
}

# User-facing names for SFTP upload locations to their connection details
SFTP_UPLOAD_LOCATIONS = {
    'DEV': {
        "CURRDIRECTORY": "PwCIT_HALOERP-DEV/UDX",
        "PASSWORD": "1Jh6fQWq2F5egw",
        "USERNAME": "haloerp_udx_writer_dev",
        "HOSTADDRESS": "mft4app-west-stage.pwc.com",
        "KEY": "MIIEogIBAAKCAQEApbxw/gdnSzKDr8VTByn+fA8ocDWfUZRqf/DS4gDfquF1q1X0BrMnZdyt5P6J99yy22VH6AaXAD56bWIaRqJUnOfPbKHxx/fTN5pIHRDzs/hxsw5epgZ+87xZiD0e5OMt7sAUfwlNuX5vWfWxU7rG/9sKrqc+gIbbGqlHWwrJYRxLPQIOW2ZwIqM9oPjyWMWLrRObfiIdM5jz5LJpuwHoNllB4tOXYURXbxmcYXD0V83AX02v0sYT2C2IqkgNLXQgOT7d1yv6ieI8T4ws1LJadGHB79n4yBI/jgDM0ISQBHkbu+/37QFfefL/WFeH23hZhBVtzcvHsKzYBifBsSpHWQIBEQKCAQAML70Dnql6PC9UdCf8wxZ6ECximofzLMuYd1rM2mrMkJPyPspp54apX0kFiy/Kshw1x3J2tTDABJgPkoHumv6xLymaz6whhu2l+IVNbmxFtCZ6V6UTvLaC3Oh3Mauu8pYzcAGBz74vhYgwBsWCZFJlou32QQDUvp8rX0U/KjR0UQVF5obWK9ht8STSD5AEVdY+saN2LvGsRIs3uhx37RBVM4SN1NR0UCbcBBZJ0mlNK90lvkmQHt7zaKeRnDErVyn2W1l/T8RPV0bkBZQQdBnNiNzdQMmudID5I8l0sX1LWw8c0JyiOp/1J3VfYfd3n3uowIyLjVprMsFmBscuHa8FAoGBAN4KjttnkBJ/zTMydQ2kwG2R3NGCuATHs9dYn6MaMETBTUL9QlputxLnA1YgCf5igAa3qaisZVjrxoQJkWp7kJgdD5MTAgbYx1vIgvY8N/t/YqT2MPqeJcE2tNg5fD1v8NhrklV3ICJH2EmN82cguAHxoul+pB4/h6V3syrZ9LvFAoGBAL8VZzxf6HfLoBWsFqR5D1X5xue0HttFDbcvyUN1Bl7uT1qP3Fn35M2kq32In8RCLGBzIbhLI0kxPh2xNzkxQho47omaU4Xgd+k2vVjr6QAJucefBK2yInEa/tpkAN3G5Q9iFVA99yQsAhMfli5f4PkB0XNLpUIlph+JS5uXO3KFAoGBAND64M5/lqf/0DAvfToEeN+YV1vGUtdSiyUIHcavPHzyKplm1QnRnT73iqtpctFNpav4JzVW9flWYHxFPZFlPMtmpT8g8tlEf2VxbDMLf/vDL6pRHwn+QabKE55UOLJLPQfsxfYVw+QHYiccMGEPvD4QmVQ69NEsu+cHPzdjuSkxAoGBAJ1c65shzoDFzyDoEqWQ33P64Aob3S0LsPE2aYLY2BHxbobQ05VivG0eMt/Z7Pv6QqnIOeMQs6WwFQlkxBD7Y50f04Bg+X1Pcc8eBVhJzvD481kZi2Hd/j8HLDthaiANNRu6L6t+Uw661Ig4IVNeBJDUUiK2xFSXeb+ePkPl9LipAoGAQAhGMHhAyRWg8gE5d3NSTosNQp6C9jFpJwVOxld36heLQl9Yxk6A1IMzxIHWcXhjozeKa1W9ziRzoYaFxh7P82hYvwiAYl8Lof3JejviF11MD/vuOWhAs94fGcHAL/1RQljALHXGizr6bdkJGJGevyiU0HHFWGkScMkgQ8u4lQU=",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "ec:80:8e:3e:70:79:6a:44:cb:7c:df:4c:0e:89:c0:63",
    },
    'DEVSET-WEST': {
        "CURRDIRECTORY": "PwCIT_HALOERP-DEVSET/UDX",
        "PASSWORD": "#M;NgYy/S}S~#r*f06:5",
        "USERNAME": "extract_udx_writer_devset1_west",
        "HOSTADDRESS": "mft4app-west-stage.pwc.com",
        "KEY": "MIIEogIBAAKCAQEApbxw/gdnSzKDr8VTByn+fA8ocDWfUZRqf/DS4gDfquF1q1X0BrMnZdyt5P6J99yy22VH6AaXAD56bWIaRqJUnOfPbKHxx/fTN5pIHRDzs/hxsw5epgZ+87xZiD0e5OMt7sAUfwlNuX5vWfWxU7rG/9sKrqc+gIbbGqlHWwrJYRxLPQIOW2ZwIqM9oPjyWMWLrRObfiIdM5jz5LJpuwHoNllB4tOXYURXbxmcYXD0V83AX02v0sYT2C2IqkgNLXQgOT7d1yv6ieI8T4ws1LJadGHB79n4yBI/jgDM0ISQBHkbu+/37QFfefL/WFeH23hZhBVtzcvHsKzYBifBsSpHWQIBEQKCAQAML70Dnql6PC9UdCf8wxZ6ECximofzLMuYd1rM2mrMkJPyPspp54apX0kFiy/Kshw1x3J2tTDABJgPkoHumv6xLymaz6whhu2l+IVNbmxFtCZ6V6UTvLaC3Oh3Mauu8pYzcAGBz74vhYgwBsWCZFJlou32QQDUvp8rX0U/KjR0UQVF5obWK9ht8STSD5AEVdY+saN2LvGsRIs3uhx37RBVM4SN1NR0UCbcBBZJ0mlNK90lvkmQHt7zaKeRnDErVyn2W1l/T8RPV0bkBZQQdBnNiNzdQMmudID5I8l0sX1LWw8c0JyiOp/1J3VfYfd3n3uowIyLjVprMsFmBscuHa8FAoGBAN4KjttnkBJ/zTMydQ2kwG2R3NGCuATHs9dYn6MaMETBTUL9QlputxLnA1YgCf5igAa3qaisZVjrxoQJkWp7kJgdD5MTAgbYx1vIgvY8N/t/YqT2MPqeJcE2tNg5fD1v8NhrklV3ICJH2EmN82cguAHxoul+pB4/h6V3syrZ9LvFAoGBAL8VZzxf6HfLoBWsFqR5D1X5xue0HttFDbcvyUN1Bl7uT1qP3Fn35M2kq32In8RCLGBzIbhLI0kxPh2xNzkxQho47omaU4Xgd+k2vVjr6QAJucefBK2yInEa/tpkAN3G5Q9iFVA99yQsAhMfli5f4PkB0XNLpUIlph+JS5uXO3KFAoGBAND64M5/lqf/0DAvfToEeN+YV1vGUtdSiyUIHcavPHzyKplm1QnRnT73iqtpctFNpav4JzVW9flWYHxFPZFlPMtmpT8g8tlEf2VxbDMLf/vDL6pRHwn+QabKE55UOLJLPQfsxfYVw+QHYiccMGEPvD4QmVQ69NEsu+cHPzdjuSkxAoGBAJ1c65shzoDFzyDoEqWQ33P64Aob3S0LsPE2aYLY2BHxbobQ05VivG0eMt/Z7Pv6QqnIOeMQs6WwFQlkxBD7Y50f04Bg+X1Pcc8eBVhJzvD481kZi2Hd/j8HLDthaiANNRu6L6t+Uw661Ig4IVNeBJDUUiK2xFSXeb+ePkPl9LipAoGAQAhGMHhAyRWg8gE5d3NSTosNQp6C9jFpJwVOxld36heLQl9Yxk6A1IMzxIHWcXhjozeKa1W9ziRzoYaFxh7P82hYvwiAYl8Lof3JejviF11MD/vuOWhAs94fGcHAL/1RQljALHXGizr6bdkJGJGevyiU0HHFWGkScMkgQ8u4lQU=",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "ec:80:8e:3e:70:79:6a:44:cb:7c:df:4c:0e:89:c0:63",
    },
    'QA': {
        "CURRDIRECTORY": "PwCIT_HALOERP-QA/UDX",
        "PASSWORD": "Of9zsDDX59",
        "USERNAME": "haloerp_udx_writer_qa",
        "HOSTADDRESS": "mft4app-west-stage.pwc.com",
        "KEY": "MIIEoAIBAAKCAQEAwXKCDYS4A+rJjlK5+AFH4HF6wJDB/Qg4z44RaSmGWRYCapQSg+EaHYXZxB8+Mrgko3iOgeNWghTj+sq7un9gac3/jdEA/kBklxAkZHxtrARjE6tn4mXXeo6UOC9Y/mjRlG29W6ko3k8P6Gs0nPEJ02SrLjiZ4AcoAGoZV5VuzAVkn4yJECWlVMCBxCcOc3lOePGKKzU3e353hVPy7YaJ8/ZlvyphGDZKyBxacomFsnOjj3Ot42Rad56WNBR7sKzywgsfzi3BQSX+PEqtAZFBniyt4KJmifp3gEAo45wS1UoF8Av2Vhh7WsaR1HMat5kd4T2ByGxUE+UEBnnM3iP/9wIBEQKCAQAREaIQQGqW7xHIyxBotNGi3NYgDMXSj8jHBQGJR228kQA2lJg40BjkfMBkIN/XTHu0EiqxG5axH/X/ilvbv/HrN9LIv540jTYNVD9yRzbaeNuQy1v9Y1bHDJSbi7UHY5oFkTZa6Ude6NvBrxs7BjWSpwALm5UbS+zw+k2HulyZh9oTs3HkuUcSWkocY7GHmuX4gGif/fRfe5V5yTTggRkoMEw8ZPAjwacFQ3/oEXxpvP/ofQELmVjQz8mVx37vnD+LeDsQJ0x+A5Sg5W67gMQ/lh7bRTEUCGu4t55/gQ3JyUeiP+Zk/sKuTnaDVHqRKQ0LBrfylPQEsjyiCCuHwzUNAoGBAOAhTLtvN+u5/P5GEq53xulhCZXVDo4ExwblstMADHIWeV8CwqpeMIuvtzaSS9Ql4d2tSd4+lPkiaC/ff+9b8TGDeivm2ouPfNtfk/I3b9HSXjN7WGsgvOyCl4Ij/MUwCaCudt/rGwFnatajfFFsG8Y+YqZkTsX+MqdpPUb+ET6lAoGBANz0ThgrZJQgGZDm/9lQLDbCOKfrky08lCfmDh39GFAWBFFlRRUkyFoyEs+aIi+I3nGXS8PMBXLBWM8IakMOe5u8k6va6ygdgI5n49PSbhT7KRywOc8w3ht63PfFPga2VzloFjidC4Y74nwLOVwzrBkl5Av8MBXgeQLGNUMyw71rAoGATxrPya7IcVCzpQmsPZOvnanHJdK518Vza9iZd6WqCiYMuB8Xh2yJmrZ85hWELMITe2pWTnBw0GZ/H/SHgaf6xi5nWsnyx9hKL07o7BOQ4KSZt9EuQ+1v2wDqLe6VcsWpC4jeqWIJiAZh8WbglTU2+qybSckMvlm3hmFg6+EzQ0kCgYAz/T+NN2L1rTMxJ0tCMPtYLbL6VYwKpNeQ6tYlLH4w9h8iNfIjF7arz4v0nLy/4/gaui/x1abt0yP0enNbEndv8CK/BlVy2cPlRZ8EqfvIs5Ez7TrHVsrZShXf8iy2SQV261CFyplMwsvg85UGor8U+dtOHTh9njqXPbIt7dO0GQKBgH6gpr04QNhcacPblsA4TZr7RN/9p2sVJ1kEuPATLAUvEHViczlGRFW696ZTUJF/tTg2wDyFfuz9hVEboU2yXAdYLunl6z4td0qjYQXVSFKPaNa8ablkT1n5nZsobuYiNM9ZjeRuMzv3vcuYewwr0SkxPVxkd1xeZkjILSIDUTi3",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "ec:80:8e:3e:70:79:6a:44:cb:7c:df:4c:0e:89:c0:63",
    },
    'QA2': {
        "CURRDIRECTORY": "PwCIT_HALOERP-QA2/UDX",
        "PASSWORD": "DgUGBFayK0P",
        "USERNAME": "haloerp_udx_writer_qa2",
        "HOSTADDRESS": "mft4app-west-stage.pwc.com",
        "KEY": "MIIEoQIBAAKCAQEAt1OiafkHbWXm/TSrnqO/XYpXnQVQG9XanSh9MA/afwFnnPwW0ML5G4hIA5LQjlwEXYfm0RHkRYpoAYEaSLM3mou17TF6bk3WNjHOyeqKSbnx/5YWW3Wfk2qFuoGjEPDsEKJu2+cAxB5ISoae/CW/3mPBau2LwZXgj/UxjAw/PUgd/4+12yQQs382L8suea72k+d6+C8NHGLc3LUxI8YerXX/kJ9nMKZ6GlBl6cJL3nA18qXl0FNPjWmJDqMM0dJMKed+j7UzostsRjJ3V5DcFNa7dP19K5E2GPuy/a8vvAtS7OsEkq59XwQ1AFzhpLuhSKuzkxsCQbWhYr3YKcBqhwIBEQKCAQBWRXmbSAN+xorRgjKlAcN3UCk61VLf7CqkT0n4f+5Z4oscst2Pas+UfF4fzJ5hHD5KIdYICGtsBOWmWt8xRUdXyUaNvO5SBoLsU6ybQTIEk70s+1XQr9KfuahXxIj46diATHBne8QgDkAjEiyy5JaGxYgyUast7C1w69sUungc1diMgNUIehDCeJmXHsHI8XJeQR9NmDHtWdbUlcljXV3+NAXKDASqpjdlccZscb0O41k/3P2+hPMpXRx7ddmjCrexBfUWMnpZ+JlSQklk67neBgZLQ3/lk9RUlBmf3y5f1ZNmqCroH2tI2NshBDJrhLYHzdIk+l40ZuMb4RGYsvBxAoGBAOPphnfBc4hCQZVlNS8FPKTPGMlABy+7Xv68xHTnFk5ls/G1v+eek8xh3YzSh8Px42iniSiBxLz/E1nCsJAZIqMY2Om7Dj3hFnJz/Ffvb1qyhEF6IHhcWDHZ94kivm753BpTvSPor3Kqr7tJo/P1faHF2YbOcn522GUPqm9PoE3JAoGBAM3rd3lnrSTT/VppdKOpcT68hE8zRHNIpPWcMnBJiWiLk4GNhfWncXgA4RNwyNYs6a+jZyIZtA3WcFLFmA4+OOK44ewFe7HChLQDaUNxrJZqhaya7ZUHXuELSXNnNn42iV9fCIV8vEa5h6gQkAKKw/yyPM42FHubgpm31SN1o93PAoGBANaBb3/FIXEvTMjXuZWqk23wF1QABsODLDsMBDHKb1jYMON94crvminFhTk+neWYXY+svVNM9WaVt9wCiA8mtzAXYr3dOpSXnKf0sUO0LJGY9PJU00QarVwYcIERpCw2dM17/U75HZkZHd1yfDDnDNR9+ehJ1Su7JgTDczuWPIVxAoGAYOdHSDDJ8zaVV7kn1Iv5DnbU2foCGCIvgqPbgCKbBAVyl1Goc5oXR4fxco9tkfcEjuN708/cQr9D6rdWnUp1H2YP9pkrJnmn3D3XLswU+31sFP2c+tZKxEGL+hJz/yi5HdJeXO9nqM/HXiXpapunhfmGBrAJo5R5sb/r1HOYaGECgYAYObBEXyQkLWWDXku61JH4TyyVlyLlI9XVbZneO7svNa8/nFO6no4yuePaf1h5FgMaLHHjhJ7Xe6F+bOpt/le51IHgg594NZn4OVj6l/XOr+gDg4femFxuFtzMrKIa5B+HSJGsIb/rqPGIsYmJh2qpL6hq+S4FB70vjMTxsyXMnQ==",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "ec:80:8e:3e:70:79:6a:44:cb:7c:df:4c:0e:89:c0:63",
    },
    'QA3-AU': {
        "CURRDIRECTORY": "PwCIT_HALOERP-QA3/UDX",
        "PASSWORD": "KbiiHGn0buaFbsW",
        "USERNAME": "haloerp_udx_writer_qa3",
        "HOSTADDRESS": "mft4app-west-stage.pwc.com",
        "KEY": "MIIEoQIBAAKCAQEAsxb+LjDMPt0CQi+yHlRxt7sqiHMV0rj6x6m/a9HyJCWugmMDAwXK/k/EUNS93BYDWbkcd047vRVGEnrmrnsrMZPXubt+7tBp8g9edI/q9fXLaXCw1XKhLZBxFYj/mthLGqqMJjLcdmHmGZgxrb8udJKoAOVs/wHsSCUpPgYwQgo87x+OQLjjL2jHli6H56amvDwjYJv38VgrzEjvoOikJpC+PCAZ8MI0KswmRZf0vAK3lzSaGPMO5ln6XSsLUyR0qt9+tJP2VKNozjgodXmPIGU85dGtyjzHZFpTmlgsfzSObh/pd/50L8CY62Dk2Nl4oHNRzjZ0jpA92j6SyWUfGQIBEQKCAQBURw4z2rp374iXnfl3r0SSsm5eVEaBR/2LIrRu+WLj1X9MatQ9qF+GvCAmCcLB7D3P3qP76JSVNzAIsk5wOfY1cr/e7tJSQ/WfFkqRNKrOGVCqFunN25ckgDU3T4dX7VCFBPaokGe/PSD8/FOdDqxzF9aXAo2lLhTWqBNoe2IA9PifhNM/5XseRRi0vB6jrSVo0c3fBPAaIBUL/78UbH8L4/rmb09DsjLyrIm6acD1lOTDkGH38P2+Jt5AHHB1b8KIDdNtGKLWa5KeH4QV6i5iTBMjDt73bMIZIaNrhd7XJ2BpxuIMr+cTqVRcMpg09pZul7duO67j6Z5jLtlFwjARAoGBAN86eqWQ26nOPRW4wuWT/TlAB9EW79QRRDZpnHbzrSzeivDPiyQgpGg8VPz4mbp3aoFBF0YNXT2Ta9u+vrT8Jictg0+Y1G+qLTxuZ7XSFfHYWrllOer2AeHdHgGeYnH3wkuxjTtKF0ZsgxUcqzNfZ7p0kjjNYiQkDWYq+qtnnE1bAoGBAM1hqqfIFZPA2N1dW+E32X39dnzPwaWoj8ipLQKh1OlOwVhC6G1gQx+qyISg3YA6u9A2ioPctbYe24ZWL+ddcNAcPs3T6yyi0ZTPzXn00fq3KNrWGCkGc8kR1Na3eUD+PpXbzA3S8WK5dbCKyi4I5b6A3rM2lXzGMmccgFmtjCubAoGBAJ2SsO1XMaUKDQBGLziksr8ABYSIqUpmioDDBQisAcVRy3zOvJH47IXQO/3cqL/b0rWXW7j6X+864rk7WXCx/NBcXLCoHXv/p3XzlIBYD32JqXPO+7Tp4zYFfpe7GFByp0R9VKJSashMmMObxCRDWEdhWCgYgYLsRbGHwACFX0WpAoGAPGgFBCvKHGXlbkiijYjlnYbIfw/tqTGTs30NPQJrzCY43blTa3aqVJukYz5fQ9UKEBAKn0DqJoGL3Dd3gEiota30Wqe9o7dq0WpLfjjyduqTi6hhdXpeSjJrxq5u9P970bkd5fK/d2O5M+yV0U3pOAfJB4iGUd/wtOpD3iP8DNMCgYAOvZ4L19yDGHGyYq8aZhfgY8iCMCZhCyCU9Ve8Od6HPEdK4ODN4/28TtYKQzeT0FlgRm+ft6hLsc2O9+y3FuLFxo6mzSYEPaofsvcAddb7SmMdW9rE9H9AOouPspKNCKCv4VLehBntqe3FDLLLhP2Y0LYNKrotACYhbktu2oCLhw==",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "ec:80:8e:3e:70:79:6a:44:cb:7c:df:4c:0e:89:c0:63",
    },
    'QA3-INT': {
        "CURRDIRECTORY": "PwCIT_HALOERP-INT/UDX",
        "PASSWORD": "8cLuzBEttFV3ne7",
        "USERNAME": "haloerp_udx_writer_int",
        "HOSTADDRESS": "mft4app-west-stage.pwc.com",
        "KEY": "MIIEoQIBAAKCAQEAjqISVySOxMjsddGb7NiBjJKUH4okSkWaYrT+VBtHmtIE4ZQjAvAfwLl7DFwDWvKEFI5guaw8AaINZoM9UC1c6B94yE/hTWafC/sXIBRa7L+jzwW1YFIBH1wy7Q2Zqn/IMjwblHpojdv1uu0xSUYHujRncPT3TP4w4h9H1D3sMDSog3H5ubomlSzuhQHIkgjqs83sl6L/3P0bB7ff0B7dSbkkouOZ4DHZCS8gkQX4XgCns2niJGB4rebQRiYP35UlEY3sjwoHNAU0/BqA2S4JzBC1dHr4c1M6APi5QuVuDarOOyHHseF7vtDJCRHwZGBNeTuZh+z9s/DjsHd0xaFZUwIBEQKCAQAZK6jiM6C5UKIy6MEpy9qgVhojrvdYZqLGH/ClbjnQBvHNdH6mKl/01XARPWoA/Z7Wc3p7HmTxOrcSFyjwCAFWI6vnHSfCXWdcd5qrTuLefCv3W1w+LJbJTIF1IISHf/Ym+4xlfwNkU/4wC71nSJfzruUT7v52/65GBYUldFbbVFK3B+GbymJlA2NClGfeo6HJ5gvWp7+TO6Zr2D+Wli2Dhv8uBLWpe58LWKyStzQ+HEod/Y7yfHTGsu4LVhnCAxIGOc7TWx7VOIo11rxFeJJV4kA1/Vzb2jzXSUhaMrtnMCP91tpr+NrO6nGX/YWEw7WLfZmIMMv0Bjm9RKc1iZ35AoGBAMaphgudx8w6+bdtJqGd9HKQkW9XWxVKX7k5L6Tod5gFrLkpmM3e/loKz3EIxMuswzxg4RhO2D7tEcWnNrPkptwF0SkkearZkwWUW0ss3X7FYXz0SiTcnk0wW7Vm1A2bHunorikQV3zDTQCCct+M6kV/ibXxowl3WHUTngLOfUl3AoGBALfMv0T+d4Idyq9JPNpbyo4AejoqkVeloLLAvtKSVENauhrJ2saWIaRovohI3a9RR8dTQUp3caQFicW0B3Svh42TH9oSNq2iMcrhU7V3+qMgAndOP8Vjiulh/b8yRzp/JcMJXMjoXScRqJopV8bnIQM4lNLn4BsrH9IW/xPHwuYFAoGBAJfrDCcAL1/w3RPL8F14yf1BYCf3geMawavCUZw5TGUxhBUfzzQE4J81cXSOPCNH4JeVYNZaaSEPpC28GsXM+BGqGGrBijdbFhNTVN8iTwaW8DJgdO8DD6RwRiFOoiiFrjpmo0yUBqqzdx6B31+n/nFhh20EE0N5YcLw4j5hqxoPAoGANg8LFErX2vm0FW/kuLGV7YereoT9kj/F2jiwmEkn15MnrYapwe/rxvGhc2C5rAjY4EWpypuZ5PKR7tqYx/dkGpSvA+c9QiClO6uvNV+F874e18u4ZztlF3csg4dCIEODk7d1pICT3lB84gwo4CXciHoNp3FgB/2f8n879sJIYcUCgYB4fLEpMsznOb1f+BD5Igk688LTMmX9pMEufIUFE4J+V7/Rd4Vpc9iBsC8OZGBj6+1IQ91Nqex4vGWkUnHDaDJ2JwqBI6exOHQQt2LWS9cbSv5QJhs5v8FTNLBlZAhedPTB9LKB2t6Dze0/BMqbRwUfmCd5uLdqrolPchjWOGnfEQ==",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "ec:80:8e:3e:70:79:6a:44:cb:7c:df:4c:0e:89:c0:63",
    },
    'QA3-INT-WEST': {
        "CURRDIRECTORY": "PwCIT_HALOERP-INT/UDX",
        "PASSWORD": "X!yl7ARL,]U*DGxvEj/H",
        "USERNAME": "extract_udx_writer_int1_west",
        "HOSTADDRESS": "mft4app-west-stage.pwc.com",
        "KEY": "MIIEoQIBAAKCAQEAjqISVySOxMjsddGb7NiBjJKUH4okSkWaYrT+VBtHmtIE4ZQjAvAfwLl7DFwDWvKEFI5guaw8AaINZoM9UC1c6B94yE/hTWafC/sXIBRa7L+jzwW1YFIBH1wy7Q2Zqn/IMjwblHpojdv1uu0xSUYHujRncPT3TP4w4h9H1D3sMDSog3H5ubomlSzuhQHIkgjqs83sl6L/3P0bB7ff0B7dSbkkouOZ4DHZCS8gkQX4XgCns2niJGB4rebQRiYP35UlEY3sjwoHNAU0/BqA2S4JzBC1dHr4c1M6APi5QuVuDarOOyHHseF7vtDJCRHwZGBNeTuZh+z9s/DjsHd0xaFZUwIBEQKCAQAZK6jiM6C5UKIy6MEpy9qgVhojrvdYZqLGH/ClbjnQBvHNdH6mKl/01XARPWoA/Z7Wc3p7HmTxOrcSFyjwCAFWI6vnHSfCXWdcd5qrTuLefCv3W1w+LJbJTIF1IISHf/Ym+4xlfwNkU/4wC71nSJfzruUT7v52/65GBYUldFbbVFK3B+GbymJlA2NClGfeo6HJ5gvWp7+TO6Zr2D+Wli2Dhv8uBLWpe58LWKyStzQ+HEod/Y7yfHTGsu4LVhnCAxIGOc7TWx7VOIo11rxFeJJV4kA1/Vzb2jzXSUhaMrtnMCP91tpr+NrO6nGX/YWEw7WLfZmIMMv0Bjm9RKc1iZ35AoGBAMaphgudx8w6+bdtJqGd9HKQkW9XWxVKX7k5L6Tod5gFrLkpmM3e/loKz3EIxMuswzxg4RhO2D7tEcWnNrPkptwF0SkkearZkwWUW0ss3X7FYXz0SiTcnk0wW7Vm1A2bHunorikQV3zDTQCCct+M6kV/ibXxowl3WHUTngLOfUl3AoGBALfMv0T+d4Idyq9JPNpbyo4AejoqkVeloLLAvtKSVENauhrJ2saWIaRovohI3a9RR8dTQUp3caQFicW0B3Svh42TH9oSNq2iMcrhU7V3+qMgAndOP8Vjiulh/b8yRzp/JcMJXMjoXScRqJopV8bnIQM4lNLn4BsrH9IW/xPHwuYFAoGBAJfrDCcAL1/w3RPL8F14yf1BYCf3geMawavCUZw5TGUxhBUfzzQE4J81cXSOPCNH4JeVYNZaaSEPpC28GsXM+BGqGGrBijdbFhNTVN8iTwaW8DJgdO8DD6RwRiFOoiiFrjpmo0yUBqqzdx6B31+n/nFhh20EE0N5YcLw4j5hqxoPAoGANg8LFErX2vm0FW/kuLGV7YereoT9kj/F2jiwmEkn15MnrYapwe/rxvGhc2C5rAjY4EWpypuZ5PKR7tqYx/dkGpSvA+c9QiClO6uvNV+F874e18u4ZztlF3csg4dCIEODk7d1pICT3lB84gwo4CXciHoNp3FgB/2f8n879sJIYcUCgYB4fLEpMsznOb1f+BD5Igk688LTMmX9pMEufIUFE4J+V7/Rd4Vpc9iBsC8OZGBj6+1IQ91Nqex4vGWkUnHDaDJ2JwqBI6exOHQQt2LWS9cbSv5QJhs5v8FTNLBlZAhedPTB9LKB2t6Dze0/BMqbRwUfmCd5uLdqrolPchjWOGnfEQ==",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "ec:80:8e:3e:70:79:6a:44:cb:7c:df:4c:0e:89:c0:63",
    },
    'STAGE': {
        "CURRDIRECTORY": "PwCIT_HALOERP-STG/UDX",
        "PASSWORD": "jfkdCHQGPn1rA",
        "USERNAME": "haloerp_udx_writer_stg",
        "HOSTADDRESS": "mft4app-west-stage.pwc.com",
        "KEY": "MIIEoAIBAAKCAQEAriktthsj6K4Q2FZfPgZtVm6r/DxVtav8qeVHdyB1iNvtaP+OGJZfMlT+fOu8zWHAkChKwXcYlaBHtrofFH1kKNsI9HecMNPIf4f4/v+7bG3avZXbPOQtI3xVi+QUwbt1RL4Pq0XccrlD0RE2FyS8OCB7ZORrb/T3831H7gRY7PepQUTm+lFq1HrfIShcAXKC1THDBZ4aLVsTS0mIdBBxUTzka2evvsAAawvJ65xPJqSsQvSWHOR//HhZP+hEor14mKlBtuApAP/46NWaCohj90hrVfBgUOXtu4qRt3hLOiT5k/mU4EEP3S1KkyfVZkeYW4sSG8Y1iyfrpHlvc9MKVwIBEQKCAQANqOByrMuZx2CxmFfIoSGsbRKCNu2f0UD+RC3DEZrJeqk1aWB6ZiWVhCgJzDb3AqWw9BnxDlxH+H4YXulW8LyPwN2QqgI2BpI8MtJGMizRSd71xXqbXTC3fTPi0KJBZAkykWqkBXqzqiNvxR1XJgS5G6VJKwNoJ0qpr3kXsAb5fLTiiX6JbUlskzauiW3sEOI65zrIoiCljK5Je6XMtyyCghzLj2S6CrTI+2WmMcmvY25QhwML9VWOBA/5vtUGnwsxvIwT4PFGUChPguuo4m3GrVwyCVwQNCH9LHJrsDGnkLcJ1/j9vzf0wYQYvflNbhjXD72Q9fCIWdi8BIm1oyLRAoGBAN9eswy86VQIZSFuKgtycAPUGf/yf5oxwfmUAcGMpybSW4LJXkssD/Qo23yOya6hrznAhoaLbD0DxYZnPZ8rL9oUe2+ufkirju7lcQ1ALOoZl8/rf4ULK8zRzQYivbcFUDhjWSSJVIHjw4MT7GeAdqGpvslnSCfbNveZViY2T04LAoGBAMeaOMzlNq6jwMSBphcNKzqSE5WVC1+812uOX0T1SdL+5nJ+pSBOp4s/qv1V19UIwbP/tKi5AEAl/wcoaYljIc/KCdgK7dwU8wgNn1AhFII4jkPseYV3IW+Nxti3tBscX6NzHsmzpoSIxnNDxUNKlTn7w5e8PlLld2it6W1xo8BlAoGAQbJw5aD5VPNpCdUbbMdOHz5h//wHaZYp/hx4/LDl3j3erf79naOMOMC5BoRZb5j3TTigCXRNAuL93Dx7iSrR1rq65Jy7uwVIKCVdfF4rU+lo4szLJyFnPD20xZG/U/KBAYahv3OvcX85gOe+AFL1txPOs7T3GsgBG6WRzwDqJgMCgYAXe442ORV99SW8w/VsH6q7mLcCigFWjq/uiThidzXclnV24bkS3BO2B32HN0aRiI9CWlF9QvD4fPDTqmbE3nxyzHmg8joZ5FjTxV4JbU2095hEOfAt0ceyxWKwFZy35ThtlRKuUV7ianGzNSZEJuRhLK2ZYXC+dVlXm/1YK3ytOQKBgG1HyfbHl0mMx1RmVt5l47ROxVuxZADiTyXQp/R5BzT2xxtLi0sGwbkwMEDDsp1iO4G8Bs3ndsM1LYgtEGyvHb3+SF7DtnZLdgAbpHTzLtZC+wNrgSj6HK8vZL5d+Q6SVdZssGgRen/P+5BFc8IGeBIRFFw0tLur0Duhlo9DuA9R",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "ec:80:8e:3e:70:79:6a:44:cb:7c:df:4c:0e:89:c0:63",
    },
    'STAGE-CENTRAL': {
        "CURRDIRECTORY": "PwCIT_EXTRACT-STG-CENTRAL/UDX",
        "PASSWORD": "D}5a/or<_TSs",
        "USERNAME": "extract_udx_writer_stg_central",
        "HOSTADDRESS": "mft4app-s.pwcinternal.com",
        "KEY": "MIIEogIBAAKCAQEA8lmYoXyiUEXwz8wR4y3Oyooiva+soy0Tg2duPaaVoKbtDcwJGExgxzpCr8Ds31/9l0MpRgPPbmXnJbQQAl5MvYS2ZKuwXPu7DzoRW3/r1nYR4TL1ecnVwUCH0RUxq8S8BGYDPvCC8wOhdEp8Mlz/H3BL1yRqImPzDUClu5OvhAY9lFCJxpm187PfU/ozl2AHdtv0NdXMIywQVAiAbMU5EjoFkBffHTl9lLNHdfYi0WqVK63gV1av/ArBQsxNHIVdaBM0CuutlL6YuVkeGzMLVJj2lCGpEEoeoJyugMcubyFrPGigvZHTm7DUwbG0l1UB0oh7oh3U4hDy67A3GL7JtwIBEQKCAQBjyookYH8R/q5zrmG35a+Akzt7SFYlEo+QdeIZYrYU+XCrVAO+tgm7clexx+kQrw4RG6eVTNzxOQTTSiS1rlvVkP/PGYTa/j33NgclrSTf1kOY9t2MgEj1OLBlJtg3q1x6SB90ROqgTMoCtUIy2v+jiJey8OBog4IyoiYgD6KfxYh2iNcPHGfednGyw1LNKQNxDO/C810yARYEzH8TvpAyt/I72uPzP0MnJNeoY5pyui258VrDVY+7tc0F/RACRb+jZRPIIVsE1lAKmaZ82Ti0vd35dT6ohAqY2mKBGBjJqcPg0yhHX1HAGKOeLtqMNTlpLB5S2gjCFrvtUf5qZQFJAoGBAPpSNWxknulNx9sh+8FUQCV3IIQl7ifxv9lhQ9VHcB7uDtnKKXQeuNB4apiZXCd67r6CsXNCwOFu9CC6aSDJ87HEEdMM5GBctSHm4kjiUDa74LvKe9luZsufcvsy2xTVeUygavKlZgVg8XpZwJGnU4EP9a0TsOWuvPJmTwgAZc1lAoGBAPfZF+6GbKvSzF3I/4SaBWvtLk5bBlQlsrhU1AjQz3LNkQQP/2X45woGXOxDqBaJANVTH/5exgPFOIoMISoD6EfynjDzbScHt9kB3RxS00rhxo1r0jd2WYJpYHrv1iaLVPnDIXDYjXVoOl9Vf1lSW6ww3oTpySMS06XXUOnM0WbrAoGBAL9sCrxM8f2z8y8Z/MEEMQ2XVRnCpw98ocRZcBuQ+2LyR5d8eg2A5656M2Wic6W4XDdU4gzYsZ1U2M27uc3HnDyk/pJVJxyDIRnsyyitECna9yYxbcRje8jjV+1FAeLBXMIgUcicmU9oQDBiwG9h1nHA6Qvh4aCUrps/LWB4xlHFAoGAV3nMNhFTh/AL5N2HW/ogB/lboy8vaP4/E+G0P1jBsAxRTLpaI/1+uD57JjX/FwMteHew8F2vTJ/11l6iSxBwGWSwTYMXdy/miNN7NyxKkuZGE8uzmxq2LgcS/jaH0V5aOgiiZBAx7TPYXeHwth0RS9UDPfgo3zPSOog6rN7gfq0CgYEAwTNs+pSzmOF17tMn6Pfu8SmF/0DKSHD+hp6dehAdgyBYfBka/dgSSq7eyKnZi6gm/yKymBzhvP5WQsqucsirU6KD5bGMFBlh2nhCtB3SP0HfdP4l8UKHJdcSrN9hQjFwnP52zJRDHkfAE6vMIetUBg4zc9sKnRfKXxNtlr4Pk2A=",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "43:6b:18:1b:a0:76:42:98:56:c3:e8:2b:11:96:a9:73",
    },
    'PROD-WEST': {
        "CURRDIRECTORY": "PwCIT_HALOERP-PRD/UDX",
        "PASSWORD": "6pf6iSPnCUnX",
        "USERNAME": "haloerp_udx_writer_prd",
        "HOSTADDRESS": "mft4app-west.pwc.com",
        "KEY": "MIIEogIBAAKCAQEA1DsL6emlkcPiwKUMg4gsHUVcMaMNp6TPLp7ZrwLgeJPPNYd+n7Mn/xOk0aieVIsWuBZIdCmo/nOodPSggH/LjhctPL5j99Bv5ryZ9DctPHFFh6QRmRgv9xqnCML5lNkdmzAHkTc2gC+4nOVqwadi/ce+qL3ZNCZnT91VSI2qLYoAegQF/cSTo5sRRTR9jrBh0lydMc/jk8E9HQJKeAENjeFQZXTri3B7Wb4vGBoU5KmeYDUiyf98JaR0jnMqJgXEUDwanyJh2AQFyCZrVtAGfACVd0kwtHAve7F6LAydVyaMINhIkTZuaqIbRY4Plry8zCnjQ3TK7213G9Q4fspuLQIBEQKCAQASuep2hY6b66MQ/4EaqiIClS3IJPms6OUas6nLrW4owb91qhKzvPv/6yUhjuDLOXLyIBVzqVKtCjSCykplsOxQTVbQp2MsdEYb43b+9c9JGQ2qFgGNgiJZkWkXXH9vBBkzVw+5/Veh5hfRm8WnrOMWZHK0iTjP5UVZ3tLRsiWamxMaCX5BHa/+HmHrrXiikr30lQZI47LLT1GUN9aywWKocjI1+oiHWEuuLFGFC7y/YHonI8Id5rTw3HXJDZY1yzKrpG7/yaZlTOIEUV6fYVI3vw4keqhsiSMqeBgEr1dA/XhLiGSby/EYpA4pYg6YjLCTzzc263WuJPSEnQe0bVWBAoGBAPPfw2ueOJhZDxaivfP2gJXeYa2IakmOOirRUDmlu82VLyADiKIn5IkB1/tvPFyHW8lBac+NR9vWNy89bq/Ry9KvDHYNQl4/dIv+PY7mRhKpg3edyT2nWnrCpl5jYGnL5K15bG7z1b7VU//bpGuTd8nAiPk1pbDljVmHFP4/8xEVAoGBAN7If64oO4YKiPozczJlsLh1Yf3EplparssROTo9tYyDo/dTf5Nlo0NMmpfFA105sxdjNhCUgPL7AA5qan6dOwiykrqUR/scfXhDpAFiOvqwB9LmvH4/A3uCZLY+Plaqs+Ckt2ViWVh+CtA/klPtCncrHyL0t+RUPUpgdjhCAD65AoGBANcu2ZsxQP7lK25TXE+7YmYekmvw1kDm6AepoSPOeIg4VsHlABabq4fykWViJjOGfioMion1IUmAx0fMyxOa/yNPKRzehdqwdeTvY34WehB3dAAhz67e9XtgdK2i+rez5+RcFGHmNRHLSh3+CYwYtP1Ph+rzGboz9TDgmg2DuFpdAoGAQYZDq7F66zBGZ7THh0sV2+ZJ/1f0sSm68G56Pk5ikr1OV81Do9KZbiWW4VgQDFxDu5WmfVjaodFaXpfE+BAga/hJRe9gdwhhI18wPKRrsyS3ActknZoQFUR3+V2Z3UFD9scIw3dHdGFOeXwb+pEDE/2fvvyujnMhFeAixUCWqQkCgYEA7WRehrbJo2p944HFNIIc3O3Z9i26wJH/Lc2abpIM26lnii3Ho48+jwPLR3f4inSNhlz9mySK9lo5DgbHZj5M0vpXhPy4d5OjrvpmqQ6xZzH9nfbOCceuORWUi+KeNganxRakx8vxy4aV4H0WhiTDUf3lW+6+SZwXMjAaw7KB5yM=",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "cd:9d:9c:cd:0c:a9:3f:f4:ac:70:b5:eb:d9:c6:5c:9d",
    },
    'PROD-CENTRAL': {
        "CURRDIRECTORY": "PwCIT_EXTRACT-PRD-CENTRAL/UDX",
        "PASSWORD": "10Eu_{Hl94jB",
        "USERNAME": "extract_udx_writer_prd_central",
        "HOSTADDRESS": "mft4app.pwcinternal.com",
        "KEY": "MIIEoQIBAAKCAQEAoHI8fgf39aKRbsA4sSbIrk/CEn3svV9LAKqIV0THr0+8AdQe+TsxJHq/ZXYsWIt6x4kAARkPDjk8goEn3s5QEYpRjWqFeZtNC58am6cw2MKVHFVXRLwMdos31zvyiaB5Uns31LB2NMibmExfglsImU800RaqLjAYiS6eHMeF5xuzMH/IiRjjegONpNYbVhsnKtlb11OHvywauxjvhF56FtpCeM64jpV+fz1SQg/zAFHy8Z0ACI0jUwJDhJ5KDIxmBB54G3dXa+dVXjBxXazaJhqiMx3xCQC9lDrde4sIAVVk37F+H6YDdbonFdtPxbiy2gtrEu5nZ7qG9TwoxjBHgQIBEQKCAQABLgQ1ofDhzl8v34fyPmcfZjvHqlZyVWgUtfW1WPnwWQ6PEoA6TZyYvSNKKGX5HySq4QHh4/LvKdUmm014OfzS89cwGUD7P0Jy6LME6MI91EEYrc3RclLqSI2WSdpBAw/zbhUdzJd1GBAz8YCzyDLjAnb6AgyLojxqa6MLgXeSo/TqCKDV16KnfTrjDEd0DhnmqIZz6rByn8wIKM18tYpxee+DmLtqeiKH/lYjs2tmDDVw4X5fgBWuCPEwUdTxOI+f1F62H3nWYhQzVT95DAfctJzlSLvCrxY3IEfXzANcXXwl6/9CkBssDYouQUh/o492TLCcJcfRKEYISAZ8bpfRAoGBAN0EdcbUEcApoT6dImV/dzAw67t9XAT11gVZfdMWVOaTFc9/1E53vVf/9g9ZowMLvgmG5F/Np4KaYmBv/HFgX1vcq9B/PPnWLEM50zG+h5EfFlR2lHKxYI9cbWgL7rRTKSUpjapqOYdhn1/BJSkVpF5HUvHYGGx0WgdYM4XPT+FxAoGBALnXdJAaeLpV2wZpLcAvJjRwZjLFScXkcbEL7fItmAc6/TUP1tmF9c5AKX/xG+G+F4Vy7X1+2hxD38FpKLiGIL0+oFbbgaOxoFbfE4dQwRvHEPPt5b7qnGDup0rLOoqcgkuQxDhY3gys6V7CB6LhMB25+qZGw9PV+0s2xTDcH78RAoGAaAIZToHqPE/TaMJqih3dvFNBwaRnid0Za+3g285GEicZUpaCBs7vsPDsQ3V541DR5l2YpY30ebIQLWHgNVqHOkm6Q/+kOVW6ef0YF2i4RErdVOyCF9sAQ3bKEthwVNvXIIwGblAbEohK/+Jr1xk+SnvqzCl0575Ie+1FbCVSxHECgYEAjh07BMjy6Ng+E/YT7VE7VUbkvWmw4p+iPBgubeahbvDfvyo64pOd2fTUcOWNyr6KdRumjSTE6HAjk+cBBZO+rtWJq9UIyHjF6BP/3/J1jbZYQhBGRrNohlwlk4xZ8Ya9/YzSSTTmCbFnKlgj9QaONNmDcBf/KYWD7jjxJV0JN8ECgYBaFNIr61BNtfuQP8qFbG0Ccv0exb/xaceeeDFi6/KNcoJJZu+KtCdHXdvXUHY4M0l+NPJa2bSbrznsmXH9DoXqCuD7hnhwK49XH6wte9Eg30b/Kr2yO7xAJfuPRRhd3tS9pjDDDQJAPveIxoR62kINCjhrUnWMz15+xvm96Qqt4g==",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "53:48:49:e5:b9:79:09:76:93:33:f9:a7:d0:7a:4a:a6",
    },
}

# If True, Use the PublicKey value from the ECF, and reject invalid keys
# If False, Use the hardcoded value below to encrypt EPF response files
# USE_PWC_ECF_PUBLIC_KEY = True

# Backup method used to encrypt EPF response files for testing / QA
PWC_EPF_PUBLIC_KEY = "MIIGdDCCBVygAwIBAgIKVpyTQgADABkWrjANBgkqhkiG9w0BAQUFADBUMRMwEQYKCZImiZPyLGQBGRYDY29tMRMwEQYKCZImiZPyLGQBGRYDcHdjMSgwJgYDVQQDEx9QcmljZXdhdGVyaG91c2VDb29wZXJzIElzc3VpbmcxMB4XDTE0MTAyMTIzNDY0NVoXDTE3MDQyMTIzNTY0NVowcDELMAkGA1UEBhMCVVMxCzAJBgNVBAgTAkZMMQ4wDAYDVQQHEwVUYW1wYTEMMAoGA1UEChMDUFdDMQwwCgYDVQQLEwNJRlMxKDAmBgNVBAMTH2F1cmFhdHN0c3R3aGQwMS5wd2NpbnRlcm5hbC5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDdJHQ53Mdjh+JdxA8z1syTylio6qV5gDNhB0FirOqHBa65hZyGjEmIifFg0POeJj84r1//Y0QqnXBpfMkhdSjfogN3qggdjXMX7/CSNlwNFzzprOC4oVEE0reblxodu0n/932OZA99Cn9szEt8ZiYCaxRWEhC/1NGCe85Ev649sWK4/az5IK/0c35gk1FETXh4cQThWTGNMFgMD3LwBZSArAUpTpg5PKSi9TSEw5czyoZNzl2OOkMMLXIEmrrbZmHkNDwm8nyPru9ubeLRHKyxgp9WHWMd4r/soTLTWOObRUVWWcjbv2O1EZaMUs5qvga0VMyosBs2vsPzAM8NEIa5AgMBAAGjggMqMIIDJjAOBgNVHQ8BAf8EBAMCBPAwEwYDVR0lBAwwCgYIKwYBBQUHAwEweAYJKoZIhvcNAQkPBGswaTAOBggqhkiG9w0DAgICAIAwDgYIKoZIhvcNAwQCAgCAMAsGCWCGSAFlAwQBKjALBglghkgBZQMEAS0wCwYJYIZIAWUDBAECMAsGCWCGSAFlAwQBBTAHBgUrDgMCBzAKBggqhkiG9w0DBzAdBgNVHQ4EFgQUQGq4zr5KvzGHHrCjChD24JL8fIMwHwYDVR0jBBgwFoAUQqa135u6wchnnl+neti+GnU/dO0wggELBgNVHR8EggECMIH/MIH8oIH5oIH2hlBodHRwOi8vY2VydGRhdGExLnB3Y2ludGVybmFsLmNvbS9DZXJ0RGF0YTEvUHJpY2V3YXRlcmhvdXNlQ29vcGVycyUyMElzc3VpbmcxLmNybIZQaHR0cDovL2NlcnRkYXRhMi5wd2NpbnRlcm5hbC5jb20vQ2VydERhdGEyL1ByaWNld2F0ZXJob3VzZUNvb3BlcnMlMjBJc3N1aW5nMS5jcmyGUGh0dHA6Ly9jZXJ0ZGF0YTMucHdjaW50ZXJuYWwuY29tL0NlcnREYXRhMy9QcmljZXdhdGVyaG91c2VDb29wZXJzJTIwSXNzdWluZzEuY3JsMIIBJgYIKwYBBQUHAQEEggEYMIIBFDCBhwYIKwYBBQUHMAKGe2h0dHA6Ly9jZXJ0ZGF0YTEucHdjaW50ZXJuYWwuY29tL0NlcnREYXRhMS9VU1RQQTNHVFNDQTAzLnVzLm5hbS5hZC5wd2NpbnRlcm5hbC5jb21fUHJpY2V3YXRlcmhvdXNlQ29vcGVycyUyMElzc3VpbmcxKDMpLmNydDCBhwYIKwYBBQUHMAKGe2h0dHA6Ly9jZXJ0ZGF0YTIucHdjaW50ZXJuYWwuY29tL0NlcnREYXRhMi9VU1RQQTNHVFNDQTAzLnVzLm5hbS5hZC5wd2NpbnRlcm5hbC5jb21fUHJpY2V3YXRlcmhvdXNlQ29vcGVycyUyMElzc3VpbmcxKDMpLmNydDAMBgNVHRMBAf8EAjAAMA0GCSqGSIb3DQEBBQUAA4IBAQB1vWzkxtKSqpR2ndRJLLCaRXDD9sATk/c5ziSvi1mNqDANXyDazbeJHk7d6XHmpQdrrEp/Z7JLJZreN392ZTPf9ltzzXKBfWaL7c434suROj74fmYqK5d3MVAnIe97V/KvJdJJ+n/ac+c0A8VkJ0REI8tXuqjF2D/M5AFAIBUQzdMaLSEbERpEq7xiDJTQStzcVUV7nkgOBZoQKBx/P8e94/QIjOPwm+RaGEQo1CHQz7UbJ8GppE86M0lWlkkB8tQT6RJXTy+1DVp9Q+p+EYvnWWWZSyOCArH7EIwt5heDCP2iy5V3nIcE2yN7EPjbUJAXOFx6BokCIk70M+Y+ULQJ"

# If True, do not show the worker timeout user config, and always use None
ALLOW_WORKER_TIMEOUT_SETTING = False

# If True, show the Content Preview dialog.
# If False, show a basic alert window with the SQL query for copying.
CONTENT_PREVIEW_ENABLED = False
