# List of binary names to exclude from build (in lowercase)
EXCLUDE_BINARIES = [
    'oci.dll',  # Oracle Driver
    'sapnwrfc.dll',  # SAP Netweaver SDK
]
EXCLUDE_DATA_FOLDERS = [
    'tests',  # IBM DB2 Cat pictures
    'certifi',  # SSL Certificates for `requests`
    'cryptography-1.5-py3.5.egg-info',  # Useless docs for `cryptography` package
    'lib2to3',  # Useless 'Grammar' files for converting Python2 code to Python3
    'clidriver',  # IBM DB2 Driver
    'ibm_db_dlls',  # IBM DB2 Driver
]
EXCLUDE_DATA_ITEMS = [
    'CHANGES',  # IBM DB2 Changelog
    'LICENSE',  # IBM DB2 License
    'README.md',  # IBM DB2 README
]

# List of binary names to explicitly include in the build
# ('filepath in output .exe', 'local filepath', 'BINARY')
INCLUDE_BINARIES = [
    # APSW Requirements (Zlib)
    ('zlibwapi.dll', 'deps/zlibwapi.dll', 'BINARY'),
    ('README.txt', 'README.txt', 'BINARY'),
]

a = Analysis(
    ['gui.py'],
    hiddenimports=[
        'cx_Oracle',
        'win32timezone',
        'pyextract.connect.db2',
        'pyextract.connect.mssql',
        'pyextract.connect.oracle',
        'pyextract.connect.sap',
        'pyextract.streams.sapstream',
    ]
)

# Remove excluded binaries and binary folders from the Analysis
a.binaries = [item for item in a.binaries
              if item[0].lower() not in EXCLUDE_BINARIES]
a.datas = [item for item in a.datas
           if item[0].split(os.sep)[0].lower() not in EXCLUDE_DATA_FOLDERS
           and item[0] not in EXCLUDE_DATA_ITEMS]

# Add images and other assets to the Analysis
a.datas += [
    ('assets/extract-logo.png', 'assets/extract-logo.png', 'Data'),
    ('assets/pwc-logo.png', 'assets/pwc-logo.png', 'Data'),
    ('chrome.zip', 'chrome.zip', 'Data'),
]
pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(
    pyz,
    a.scripts,
    exclude_binaries=True,
    name='PwC Extract',
    icon='assets/extract-logo.ico',
    debug=False,
    console=False,
    strip=False,
    upx=True,
)

collection = COLLECT(
    exe,
    a.binaries + INCLUDE_BINARIES,
    a.zipfiles,
    a.datas,
    strip=False,
    upx=True,
    name='PwC Extract'
)
