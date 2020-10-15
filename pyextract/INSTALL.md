# Installing PyExtract

Because PyExtract supports many complex database systems, it also requires many obscure dependencies from those database vendors to function.

Most of the dependencies needed can be found in the [PyExtract team Google Drive][GOOGLE-DRIVE].

We highly recommend using the files available in the Drive instead of dealing with the headache of finding and compiling these files yourself.

## Install Python 3.5 / Anaconda 4.2 (64-bit)

Download and install the latest version of [Python / Anaconda][ANACONDA]. Currently, this package requires a 64-bit file system and a 64-bit version of Python. You can check the architecture of your machine from the start menu by right-clicking `Computer > Properties`.

After installation, confirm Python was installed properly:

```bash
$ python --version
Python 3.5.2 :: Anaconda 4.2.0 (64-bit)
$ python -c 'import platform; print(platform.architecture())'
('64bit', 'WindowsPE')
```

## Clone the Git repo

If you haven't already, clone this repo from Git and `cd` into it.

```bash
$ git clone http://matlkatp2app025:8080/tfs/ECAP/PyTech/_git/pyextract
$ cd pyextract
```

## Install packages available from PyPI

Install packages available from the Python Package Index using `pip`:

```bash
$ pip install -r ./requirements.txt
```

## Install components for IBM DB2 extraction

Use `easy_install` to install the package and all the DLLs that are needed to build the executable (WARNING: `pip` will not download the needed DLLs, only the Python package).

```bash
$ easy_install ibm_db
```

## Install components for cross-platform encryption (Chilkat)

Get the `chilkat-9.5.0-python-3.5-x64.zip` file from the Google Drive. Unzip the file and navigate inside the top-level folder. Run the `installChilkat.py` file to install the program in your Python `site-packages` folder.

```bash
$ python installChilkat.py
```

## Install components for ODBC (MemSQL, MySQL, SQL Server) extraction

Download the appropriate [ceODBC 2.0.1][CEODBC] `.whl` for your version of python and `pip install` the file. After installing, test the installation by importing it in Python.

```bash
$ pip install ceODBC-2.0.1-cp35-cp35m-win_amd64.whl
$ python -c 'import ceODBC; print(ceODBC)'
<module 'ceODBC' from 'C:\\Anaconda3\\lib\\site-packages\\ceODBC.cp35-win_amd64.pyd'>
```

## Install the ODBC MySQL driver

Download and install the [ODBC MySQL connector 3.51][ODBC-CONNECTOR]. Run the program using all default settings.

You can verify the ODBC was installed by opening the start menu, and opening the `Microsoft ODBC Administrator` program. Under the `Drivers` tab on the main window, you should now see the `MySQL ODBC 3.51 Driver` listed.

## Install components for encrypted SQLite extraction

Download the [APSW][GOOGLE-DRIVE] `.whl` from the PyExtract Google Drive, and `pip install` the file. After installing, test the installation by importing it in Python.

```bash
$ pip install apsw-3.17.0.post1-cp35-cp35m-win_amd64.whl
$ python -c 'import apsw; print(apsw)'
<module 'apsw' from 'C:\\Anaconda3\\lib\\site-packages\\apsw.cp35-win_amd64.pyd'>
```

If you get the error `ImportError: DLL load failed: The specified module could not be found`, you will need two additional files.

Navigate to the `APSW\_sources\zlib` folder in the [APSW repo][PYTECH-APSW]. Copy the `zlibwapi.dll` and `zlibwapi.lib` files into your local Python base directory.

If you do not know the location of your Python root folder, you can find it with the following command:

```bash
$ python -c 'import sys; print(sys.exec_prefix)'
C:\Anaconda3
```

## Install package, client and DLLs for Oracle extraction

Download the file `cx_Oracle-5.2.1-11g.win-amd64-py3.5.exe` and the folder `instantclient_11_2` from [Google Drive][GOOGLE-DRIVE]. Place the client folder anywhere on your local machine, and remember its location to reference later.

*--or--*

Download the appropriate [cx_Oracle installer][CX-ORACLE] for your platform (probably Windows 64-bit), and use the default install in that. Follow [this guide][ORACLE-CLIENT-GUIDE] to download the [Oracle instant client (v11.2 Basic)][ORACLE-CLIENT].

## Install components for SAP extraction

Download the file `pyrfc-1.9.4-py3.5-win-amd64.egg` and the folder `nwrfcsdk` from [Google Drive][GOOGLE-DRIVE]. Place the SDK folder anywhere on your local machine, and remember its location to reference later. Install the `.egg` file using `easy_install`.

```bash
$ easy_install pyrfc-1.9.4-py3.5-win-amd64.egg
```

*--or--*

(Really, this is the most complex part of the installation, just use the pre-compiled module and SDK located in the Google Drive)

Download and install the [SAP NetWeaver RFC Library][SAP-NW-RFC] from the [SAP Software Download Center][SAP-SOFTWARE]. The specific library needed for 64-bit Windows is `NWRFC_39-20004568.SAR`, which can be found by searching for `SAP NW RFC SDK 7.20` in the main Software Download Center search box.

To unpack the `.SAR` file, you'll also need to download the `SAPCAR.EXE` program from the Download Center. Place the SAPCAR utility and the `.SAR` file in the same directory, and use the command:

```batch
.\SAPCAR.EXE -xvf "NWRFC_39-20004568.SAR"
```

This command will produce the `nwrfcsdk` directory. Place this folder anywhere on your local machine, and remember its location to reference later.

Next, clone the `pyrfc` module from Git and build it with Python. Install the newly created `.egg` file using `easy_install`.

```bash
$ git clone https://github.com/SAP/PyRFC
$ cd pyrfc
$ python setup.py clean --all
$ cd dist
$ easy_install pyrfc-1.9.4-py3.5-win-amd64.egg
```

## Create a `.env` file for dependency and credential information

When PyExtract is run using a graphic interface, the location of SAP and Oracle dependencies can be set on the 'Configs' page and dynamically imported.

For development / testing purposes, you will need to set an environment variable for each location needed. Currently, only SAP and Oracle components need locations set:

```bash
export ORACLE_HOME="C:\\instantclient_11_2"
export SAP_NETWEAVER_SDK="C:\\nwrfcsdk\\lib"
```

It is recommended that you put all environment variable settings into a `.env` file, exactly as shown above. This file **will not** be synced with the Git source control, so it is safe to store test credentials and API keys in that file.

Before running the program for development or testing, you will need to activate your environment using the `source .env` command in Bash.

## Done!

Installation of dependencies is now complete. Refer to `README.md` for further instructions on how to test and use the program.


[ANACONDA]: https://repo.continuum.io/archive/Anaconda3-4.2.0-Windows-x86_64.exe
[CEODBC]: http://www.lfd.uci.edu/~gohlke/pythonlibs/#ceodbc
[CX-ORACLE]: https://pypi.python.org/pypi/cx_Oracle
[GOOGLE-DRIVE]: https://drive.google.com/open?id=0B2RrjZ1HLSnjN3p5UGtFUjN5WlU
[ODBC-CONNECTOR]: https://dev.mysql.com/downloads/file/?id=406118
[ORACLE-CLIENT]: http://www.oracle.com/technetwork/topics/winx64soft-089540.html
[ORACLE-CLIENT-GUIDE]: http://stackoverflow.com/a/20193861
[PYTECH-APSW]: http://matlkatp2app025:8080/tfs/ECAP/PyTech/_git/APSW
[SAP-NW-RFC]: http://sap.github.io/PyRFC/install.html#install-c-connector
[SAP-SOFTWARE]: https://support.sap.com/software.html
