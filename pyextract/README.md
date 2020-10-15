# PyExtract (Python Data Extractor)

Package for high-performance data transfer between databases.

**Supports extraction from:**
* DB2
* MemSQL / MySQL
* Microsoft SQL Server
* Oracle
* SAP
* SAP (ABAP `.fil` files)
* SQLite

**Supports extraction into:**
* MemSQL / MySQL
* Microsoft SQL Server
* SQLite

PyExtract requires Python 3.5 (Anaconda 4.0) and a Windows 64-bit machine.

## About

The PyExtract program is a recreation of the GATT PwC Extract program, but written in Python instead of C# and .NET. Python allows us to make changes to the program quicker, makes the program more accessible to our Halo developers, and has a large ecosystem of data-centric packages.

PyExtract began development in September 2016, and was released for beta testing in May 2017. Full release is scheduled for July 2017, and the program will be used to facilitate data extraction for 2017 year-end audits.

If you'd like to learn more about PyExtract, or have feedback about the program, please send an email to `US_PyExtract@pwc.com`.

## Installing dependencies

See `/INSTALL.md` for installation and setup instructions.

## Testing the source code

On the first run from each terminal session, you must first set the environment variable `PYEXTRACT_PASSWORD`. This password will be used to unlock the `credentials.dat` vault and access all testing credentials.

You can get this password from the PyExtract development team.

To set the environment variable in Bash, use the following command:

```bash
$ export PYEXTRACT_PASSWORD="<password>"
```

To run all unit tests, use the following command from the top-level of this repo:

```bash
$ python -m unittest discover tests
```

As of 2017-06-20, all 82 unit tests take approximately 383 seconds.

## Building the executable

There are two executables that can be created, one for production / client use and one for QA / testing. The production build is more limited in order to enforce business rules (e.g. cannot select an encryption level of 'None').

In order to build the executable for client use, run the following script from the top-level of this repo:

```bash
sh scripts/prd_build.sh
```

For the QA build, run the QA-specific script instead:

```bash
sh scripts/qa_build.sh
```

The program will be output to the `/dist/` folder as `/PwC Extract/`, and can be run using the `./dist/PwC Extract/PwC Extract.exe` file.

## Deploying the program to ClickOnce

The primary method of deploying the PyExtract program to users is through the `ClickOnce` service.

The [production version][CLICKONCE-PROD] of the PyExtract ClickOnce service is hosted on the PwC server `matlkecapapp001` at port `7770`. The [QA / testing version][CLICKONCE-QA] of the PyExtract ClickOnce service is hosted on the same server at the port `7771`.

Once you've built an executable suitable for production, follow the below steps to publish the newest version. Note that this will immediately be available to all users of the program, so be sure that the build has passed QA testing before deployment.

To instead publish a `QA` version of the build for testing, replace every instance of `prod` with `qa` in the instructions below.

* Build the `PwC Extract` executable, and ensure it passes testing for all the major connection types.
* Log into the `matlkecapapp001` server using Windows authentication.
* Navigate to the location of the [Clickonce Repo][CLICKONCE-REPO] on that server, or clone it from TFS.
* Move the new `./dist/PwC Extract` folder into the `pyextract-clickonce/prod/PyExtract/` folder, replacing the old version of the folder if it exists already.
* Open the `pyextract-clickonce/prod/PyExtract.sln` file using Visual Studio 2015
* From the toolbar, build the project using `Build > Build PyExtract`.
* From the toolbar, open the `Project > PyExract Properties` window.
* On the `Publish` tab, increment the version number to match the current version of PyExtract.
* Click on the `Publish Now` button, and wait about a minute for the process to complete.
* Navigate to `http://matlkecapapp001:7770/publish.htm` from your laptop, and confirm that the new version has been published.

Now, users who click `Install` on that webpage will receive the newest version, and any users that have previously installed will be prompted to update the next time they run their `PyExtract` program.

## Building the program as a signed MSI installer

Since some PyExtract users do not have access to the PwC network, and to provide backup versions of the program, we also distribute major versions of the program as a MSI installer. These installers are located in the [PyExtract team Google Drive][GOOGLE-DRIVE] under the `Releases` folder.

To start a build remotely, you can use the `curl` command in the Bash script `build_jenkins.sh`. You'll need to set environment variables for your Jenkins crumb (found at the [Jenkins crumb issuer][JENKINS-CRUMB]) and your Jenkins API token (found at `http://cd-jenkins.pwcinternal.com/user/<username>/configure)`before running the script.

```bash
export JENKINS_CRUMB=<crumb>
export JENKINS_API_TOKEN=<token>
sh scripts/build_jenkins.sh
```

*-or-*

You can also build the installer manually from a web interface. Navigate to the [PyExtract task in Jenkins][JENKINS].

* After testing a version of the program built locally, push changes up to any branch of the PyExtract repo.
* On the [Jenkins page][JENKINS], select `Build with Parameters` from the menu.
* Leave the `SourceRepo` and `BuildLabel` values as their defaults, and use the name of the branch you want to build from in the `GitBranch` value.
* Click the `Build` button, and wait about 15 minutes for the process to complete.
* Follow the link on that page to the downstream project [`EIT-PythonExtract`][JENKINS-PART-2]
* Under the `Last Successful Artifacts` section, download the file `PwC_EITPythonExtract_2.0.0.<version>_EN_01.msi`
* Rename this file to `pyextract-<version>.msi`, and place it in the `Releases` folder of the [Google Drive][GOOGLE-DRIVE]

## Using PyExtract as a Python package

Either install the package to site-packages, or run scripts from the same directory that contains the `pyextract` folder. A sample data extraction script for calling this package from Python can be found at the `/sample.py` file.

To test the `/sample.py` script (after any modifications might be made), run the following command from the top-level folder in this repo:

```bash
$ python sample.py
```

## Using PyExtract with a Graphic Interface

Run the following command from the top-level folder in this repo:

```bash
$ python gui.py
```

## Frequently Asked Questions (FAQs)

### I'm getting an error `DLL load failed: The specified module could not be found.`

This error occurs when trying to import certain dependencies. The environment (terminal/shell) that you are running from does not have the right path set to know where dependencies like SAP or Oracle clients exist. Reference the `INSTALL.md` file for whichever module is giving you this issue, add the appropriate variables to CMD / Bash, and try again.


[CLICKONCE-PROD]: http://matlkecapapp001:7770/publish.htm
[CLICKONCE-REPO]: http://matlkatp2app025:8080/tfs/ECAP/PyTech/_git/pyextract-clickonce?path=%2FREADME.md&version=GBmaster&_a=contents
[CLICKONCE-QA]: http://matlkecapapp001:7771/publish.htm
[GOOGLE-DRIVE]: https://drive.google.com/open?id=0B2RrjZ1HLSnjN3p5UGtFUjN5WlU
[JENKINS]: http://cd-jenkins.pwcinternal.com/job/EIT/job/EIT-PyExtract/
[JENKINS-CRUMB]: http://cd-jenkins.pwcinternal.com/crumbIssuer/api/xml
[JENKINS-PART-2]: http://cd-jenkins.pwcinternal.com/job/EIT/job/EIT-PythonExtract/
