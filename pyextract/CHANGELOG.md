# Change Log

All notable changes to this project will be documented in this file.
This project adheres to [Semantic Versioning][SEMVER].
This project follows [these changelog guidelines][GUIDELINES].

## 2.2.0 [UnReleased]
* Fixed WhereClause ECF parameter
* ABAP Bug fix adding missing trailing parenthesis
* Added MySQL Connection
* Added package name to working directory path
* Added Re-Upload Feature
* Added DB2, MSSQL and MYSQL to docker container
* Added vm_lookup
* ABAP Bug Fix not validating Request ID in Log
* Added Warning (in log and in extraction complete dialog) when row skips are encountered on SAP.
* Allow user to specify MySQL Driver in config panel similar to what we did for MSSQL previously.  Also added MySQL and MSSQL defaults.
* Added DSN option to MySQL Connection Dialog

## 2.1.2 [Released]
* ABAP Bug fix removing trailing comma when length of tuple is 1.
* Oracle - Use ALU32UTF8 encoding.
* Generate hash of .data files in chunks.
* ABAP Bug fix for missing columns when using free selection '@'
* Bug fix for Connection Failed list index out of range.

## 2.1.1 [Released]
* Fixed broken previous button when continuing ABAP extraction
* Remove upper limit on SAP row size
* Updated ECF to ABAP parameter file conversion to use RFC structure instead of separate ABAP
* Re-establish SAP connection in controlled pause/resume.
* Removed reference to 'STRIPEKEY' in ABAP Output consumption
* Fixed image.IsOK() assertion error
* Fixed failure acquiring table_name because ecf was not provided in DataDefinition() for ABAP
* Added new row size config for non-SAP (50K default).
* Created FileHandler for logging vs. MemoryHandler.
* Merged docker components into main branch.
* Removed write worker timeout.
* Upgraded to cx_Oracle 6.0.2.
* Cast all cx_Oracle.NUMBER data types to decimal.Decimal to avoid loss in precision.
* Re-worked SAP connection dialogs to allow for Load Balanced Connection w/SNC.
* Add PX Version in Metadata.txt
* Add active configurations to log.
* Updated authorization checks to account for BBP_RFC_READ_TABLE.  PX will now check for both Data Services and BBP_RFC_READ_TABLE and would not stop a user from running an extraction if the client does not have Data Services, as long as they have BBP access.
* During resume, re-attempt tables that previously had errors (these were being skipped).
* Added log entry at the end to note "Package Name" and "Package Path" - just so its clear.

## [2.1.0]
* Added a 2 second wait after package is uploaded to MFT but before the package is appended with "_Complete"
* For SAP, honor the batch_size configured in PwC Extract - it was default to 1000
* Goodyear apsw.BusyError fix.
* Extractions stopping when encountering auth issue.  Extraction should log error and continue.
* Fixed permission error when trying to delete files while they are still being written/added to the package (.zip).  This gets triggered when files get larger.
* Changed the SAP Connection field `SNC MS Server` to be optional, so the user does not need to provide it in order to establish a Load Balanced Connection to SAP.
* Updated the tooltip text for `SNC MS Host` and `SNC MS Server` to better explain the difference between the fields.
* Removed configurable option `USE_PWC_ECF_PUBLIC_KEY` as this should always be True from now on.
* Fix Load Balanced connection in SAP ('sysid': connection_details.get('sysnr') --> 'sysid': connection_details.get('sysid'))
* Changed default chunk size to 500K
* Changed default batch size to 200
* Removed requirement that ABAP `.fil` filenames had to include a unique ID from the name of the folder containing them (because that folder might have been renamed).


## [2.0.7] - 2017-08-23 - Official Release!

### Added
* Added a `README.txt` file to the build which includes the licenses for the Python dependencies that are included.

### Changed
* Added a patched `ibm_db.py` file to the build that allows us to dynamically import DB2 dependencies.
* Removed expectation of time in Expiry Date and if there are any errors with Expiry date, just ignore the expiry date entirely.

### Fixed
* Bug 24403: Added a 200-character limit to names of saved connections.
* Bug 24409: Disabled the `Save Password` field on SAP ABAP Connection page.
* Bug 25689: Fixed typo in tooltip for Oracle `Value` user input.
* Bug 25984: If the 'Working Directory' value is set to blank (empty string) and saved, it will set the `working_directory` back to the default value.
* Bug 26084: Updated instructions on the 'SAP Connection' screen.


## [2.0.6] - 2017-08-10 - Bug fixes

### Added
* Added Display Size and Scale to TableMetaData table for Oracle to fix precision issues with loader.
* Added `gui.ManageSavedConnDialog` and `gui.SavedConnGrid` classes so users can delete and update saved database credentials from their `ConfigDatabase`.

### Changed
* Updated the 'Phase 2' of ABAP extractions to not require a specific folder name to run an extraction from ABAP CSV files.
* Replaced the `gui.apply_default_window_style` function with a `gui.DefaultDialog` class, which most GUI dialogs now inherit from.

### Removed
* Removed `core.sqlite_repeater` since the root cause of the `apsw.BusyError` in `core.update_total_record_counts` was fixed.


## [2.0.5] - Unreleased

### Added
* Added a `max_readers` user configuration for adjusting the amount of multiprocessing / threading that occurs during extraction.
* Added `core.sqlite_repeater` to avoid random `apsw.BusyError` that were happening during `core.update_total_record_counts`.

### Changed
* Renamed the final executable name from `python-extract` to `PwC Extract`.
* Changed the generic name `max_workers` to `max_writers` in `core.py` to avoid confusion with the new `max_readers` user config.
* Moved `ECF` files used for testing to a top-level `/ecfs/` folder. Removed all `ECF` files from the `/tests/assets/` folder.
* Hardcoded the value for `max_writers` to always be `1` throughout the application, to avoid errors encountered during multiprocess writes to SQLite.
* Removed the 'Content Preview' functionality and replaced it with a message box displaying basic SQL query / SAP parameters for the user to copy.
* Changed the SQLite database PRAGMAs to increase performance.

### Removed
* Removed support for 1.6 versions of `ECF` files. The Extract program will now only support `ECF` files of version 2.0. All 1.6 ECFs must be converted to 2.0 ECFs using the Extract ECF Text Editor tool.
* Removed `DEBUG:` logging calls for SAP function modules.
* Removed `GPG` and `gnupg` from the program entirely.


## [2.0.4] - 2017-08-07 - DB2 Removal, Auto-upload, Password Saving

### Added
* Added detailed mouse-over tooltips to the labels of all user inputs.
* Added "Auto Upload" to configuration panel. When set to "Yes", the upload will take place immediately after successfull extraction. Defaults to "No".
* Add error handling to `sapstream.get_parent_split_info` for malformed ECFs.
* Added `utils.NetworkDisconnectError` so disconnect code is more robust.
* Allow users to save their database connection passwords in the per-user encrypted config database.

### Changed
* For all new installations, the default working directory (`Desktop/PyExtractWD`) has been moved and renamed to `%LOCALAPPDATA%/PwC/Extract/Data`.
* IBM DB2 dependencies have been removed from the default build now that a way has been found to import them at runtime. Users will now need to specify a location on their local machine for the `IBM DB2 Driver` in order to run DB2 extractions.
* Changed ERP dependency validation to be based on expected names of content within a folder instead of by the name of the folder. This makes the application more secure and allows us to provide more useful error messages when dependency folders are not set up as expected.
* Updated the 'Extract' title on the home page to also be Italic.
* Updated upload success message to "Successfully uploaded data to PwC using <method>"
* Updated "Duplicate columns within ECF" message to include the table name.
* Update `sapstream.format_params` to allow `=` parameters and disallow `!=`.
* The 'Next' button on Connection screens will now be disabled unless all required inputs are filled in by the user.
* Changed timestamps in the GUI screen to include dates.
* Changed delimiter between log elements from 4 spaces (` `) to a tab (`\t`)
* Changed log message truncation of SAP where clause items to not truncate in the middle of a value, and to also include the number of items not shown.
* Changed naming of log levels when using the GUI, renaming `WARNING` to `WARN` and `CRITICAL` to `ERROR`.
* Renamed all instances of `MFT` to `SFTP` throughout the app to avoid ambiguity. The `SFTP` service itself remains unchanged, and the key `MFT_LFU` will still be the canonical value to include in ECF files.
* Allow 'Max seconds per request' (`worker_timeout`) to be blank or `0` to signify that workers should never timeout. Updated the default option to be `0` for this setting.

### Fixed
* Fixed a bug where the `lfu_location` and `sftp_location` panels in the Production build were not being hidden.
* Disable "Next" button on the ECF Selection screen if an ECF is invalid.
* Ensured that network disconnects during SAP reads are handled appropriately. This prevents records being skipped when an extraction continues after a disconnect.
* Made sure that ABAP extractions create a `TableExtractions` file.
* Fixed a minor bug that occured when switching to and from a Connection Dialog screen that does not have a password field, after having been on a screen with a password field.
* Ensured that ABAP connection screen 'Finish' button will enable when all required ABAP files (`abap_folder` and `abap_filename`) are provided.
* Fixed typo in Oracle connection error handling in `DataStream.start()`.

### Removed
* Removed the 'Setting Default User Configs' dialog that would display on initial program launch.
* Removed the `clidriver` and `ibm_db_dlls` folders from the program.
* Removed the behavior where passwords would be cleared when navigating between connection pages. Passwords will now remain with all other previous form data, and will continue to appear obscured (`*******`)


## [2.0.3] - 2017-07-31 - Bugfixes

### Added
* Added a timeout to SAP table validation requests, so that each table takes no longer than ten seconds to validate. Please note that table validation will be disabled while we determine the impact on the SAP server from timed out requests.
* Added a `SAP Batch Size` parameter to the Configs panel, which allows the user to control how many records should be requested per RFC call during SAP extractions.

### Changed
* Changed the label of the 'Open ECF' home page button to 'New Extraction'.
* Output tables will still be created, but empty, when no records are returned for any valid (non-errored) extraction.

### Fixed
* Fixed bug where uncreated Extract config databases were identified as unencrypted, failing to create the file.
* Fixed bug in `sap.where_clause_rfc_format` that caused certain values to be skipped when using large `IN ()` statements.
* Fixed `SAPMessenger.get_metadata_from_query` to work with ECF table sections that do not have a `Parameters` section.

### Removed
* Removed unused `certifi`, `cryptography`, and `lib2to3` folders from the build.


## [2.0.2] - 2017-07-24 - Bugfixes

### Added
* Added 'Advanced Options' section (with scrollbar) to Configs screen to help maintain the growing amount of user configs available.
* Added 'Log Level' option to Configs screen, which can be set to `INFO` (default) or `DEBUG` on a per-extraction basis.
* Added ability to right-click on any cell in the Content Preview screen to copy the text to your machine's clipboard.
* Added additional library validation and error handling to `gui.validate_sap_sdk`.
* Added `error_queue` so that errors during threaded reads in `SAPStream.threaded_extract` are appropriately raised to main process and GUI.
* Add check for SAP date formats in ECF, asserting that the format is either `YYYYMMDD` or `YYYY-MM-DD`.

### Changed
* Increased default `worker_timeout` from two minutes to 20 minutes.
* Now including SAP RFC 'where filters' to each record in the extraction log that reports how many records were extracted. If the filter is a large `WHERE IN ()` list, it will be truncated in the middle, and the full list of values can be found by turning on `DEBUG` level logging.
* Improved format of 'Date Started' information on the `ContinueExtraction` screen.
* Hiding the `ibm_dll_folder` panel until we actually remove those dependencies and require the user to set that up for themselves.

### Fixed
* Bug 5579: Removed 'Fatal Error' popup that occured at the end of extractions.
* Bug 15987: Restore the text log when resuming or continuing an extraction, so that the text log for multi-part extractions still contain all logging data.
* Bug 19846: Added vertical scroll bar to Configs screen to show all fields.
* Bug 20252: Errors for fields in Configs screen will now report their user-facing names in the error instead of their Pythonic names.
* Limit data pulled to validate tables in `SAPMessenger.validate_user_access` and do not error a table if a SAP database memory limit is reached.


## [2.0.1] - 2017-07-17 - Bugfixes and SFTP Port Options

### Added
* If uploading via MFT fails when an upload type of `MFT_LFU` is selected, an upload to the LFU location will be used instead.
* Moved production `config.py` settings into `defaultconfig.py`. Updated `prd_config.py` and `qa_config.py` to import default config values from that file, and only specify the values they need to change in the environment-specific files.
* Added additional `LOGGER.debug` calls in `sapstream.py` and `sap.py`.
* Added dropdown option on Configs dialog for MFT SFTP Port ('22' or '52222').

### Changed
* Refactored unit tests to reduce duplicates (removing `tests_bvt` folder).
* Sorted the values in `.gitignore` by name and granularity.
* Replaced the `dependency.config` file with two environment variables, and wrote instructures for using a `.env` file to store this information.
* Changed minimum value for `chunk_size` (rows extracted per request) to 1000.
* Replaced duplicate code in `gui.ConfigsDialog` with `StaticLabel`, `FolderSelectPanel`, and `UserInputPanel` classes.

### Fixed
* Bug 13158: LFU uploads will now properly raise an error when a network disconnect happens during the upload process.
* Bug 17814: Update `sapstream.py` error checks to raise `AssertionError` for ECF issues, so that the GUI will no longer treat them as unhandled errors.
* Bug 18291: User config values with ranges (e.g. `chunk_size`) will now always alert the allowable range on an input error.
* Removed `sap.validate_user_access` to prevent out-of-memory errors with huge client systems.


## [2.0.0] - 2017-07-01 - Release!

*Official release version of the Extract 2.0 program.*

### Changed
* Updated SQLite database encryption to always use AES-256.
* Updated EPF response file encryption to use the `PublicKey` value provided in the ECF file. ECF files without a valid `PublicKey` from the ECF Generation Service keystore can no longer be used with the Extract program.
* Updated data package upload process to always upload to a production server, using the `Territory` value in the ECF. Note that only the `West` and `Central` territories are supported.

### Fixed
* Added validation and error handling to ensure ECFs contain `Queries` and `ChannelLos` values before processing them.
* Set a consistent label size on the Configs screen to show the full label for 'SFTP Upload Location'
* Always check for and delete outdated content at the start on an extraction. This occurs when the user manually unzips data from a package and leaves it in the working directory.


## [0.8.5] - 2017-06-28 - Release Candidate

*This version will be the last major release before production (except for critical bugfixes)*

### Changed
* Changed `ExtractData.channel` to read the `ChannelLos` value from ALL ECFs,  instead of reading the `LineOfService` value in 2.0 ECFs.


## [0.8.4] - 2017-06-27 - Bugfixes

### Added
* Added option in build configuration `USE_PWC_ECF_PUBLIC_KEY`, which will allow the program to use either the `PublicKey` value from the ECF, or a hardcoded PwC public key, when encrypting EPF response files.
* In a build that requires providing a `PublicKey` in the ECF, ECF files that do not have this attribute are no longer considered valid, and will redirect the user to the ECF Generation site if selected.

### Fixed
* SAP ECFs that contain table-level `Parameters` without a `Meta` fields will be provided default values for batch and chunk sizes.


## [0.8.3] - 2017-06-16 - Minor bugfixes

### Added
* Added scripts to make building and deploying the application easier.

### Changed
* Updated 'Low Disk Space' message to be warning-level instead of error.
* Do not prompt user to confirm when closing the 'Continue Extraction' window.
* Made `sqlite.connect_to_sqlite` create all intermediate folders to the database if they don't exist, not just the base folder of the database (in case use deletes entire working directory mid-extraction).

### Fixed
* Bug 8280: Updated `ecfreader.DataDefinition` to validate that no duplicate columns exist in an ECF query or parameter section.
* Bug 12128: Updated `SQLiteMessenger.delete_duplicates` to not throw an error if the table provided does not exist.
* Bug 12541: Prevent application from closing while an upload is in progress.
* Bug 15630: Updated the messaging for record counts at extraction end.
* Bug 16437: Updated `ABAPMessenger` to use a metadata `sourceSystem` of `SAP` instead of `ABAP` for loader compatibility.
* Bug 16541: Raise error when user tries to set a base-level drive (e.g. `C:\`) as a writeable folder or depedency folder.
* Made 'Load Saved Connection' dropdown on DB2 Connection window default to no selection, instead of defaulting to the first item available.
* Made sure extractions end safely and silently if user closes application using the 'X' button mid-extraction.
* Made `SAPStream` log number of rows read with the integer comma format.

### Removed
* Removed code relating to the old 'Upload Package' button on front page.


## [0.8.2] - 2017-06-09 - Hotfix

### Fixed
* Fixed a bug that would prevent the application from opening if the user had no saved extractions.


## [0.8.1] - 2017-06-07 - ECF-based Uploads and MSI Installer

### Added
* Force production build to upload based on 'Territory' value in the ECF. Note that only the `West` and `Central` territories will be supported in production. The QA build will continue to upload to whatever location is selected on the Configs page.

### Changed
* Updated `README` and `INSTALL` documents to reflect current program state.
* Now doing post-connection query validation in its own thread, so that the GUI does not become unresponsive when validating large ECFs.
* Now saving connection details before validating queries, instead of after.

### Fixed
* Bug 9129: Fix 'Content Preview' screen so that resizing the grid columns does not create scrolling issues and hide data.
* Bug 9712: Trailing spaces in filepaths will no longer cause a 'relative filepath not allowed' error.
* Bug 10968: In ABAP, information message is displayed to user before the extraction screen to inform ABAP doesn't support pause/resume.
* Bug 11281: Disable Upload button after user clicks 'OK' on "Package Upload success message".
* Bug 12454: Upload success message doesn't include the URL, upload method and location included instead.
* Bug 12531: Clear all content before refreshing the 'ECF Preview' page when a new ECF is selected to avoid overwriting / overlapping information.
* Bug 12543: When user deletes a record in 'Continue Extraction' screen, the directory containing the zip file is also deleted.
* Bug 12548: In Continue Extraction screen, Next button is enabled only when extractions are present and user selects an extraction.
* Bug 13615: Validation of all user-set folders will now test for invalid and no-access folders, raising appropriate errors for each situation.
* Bug 13616: See Bug 13615.
* Bug 14321: In ABAP, table with zero records generates a .dat file.
* Bug 14896: Instructions text updated to remove the reference of 'Start' button and include the reference of 'Resume' button.
* Bug 14898: Error message displayed when non-numeric values entered in  chunk_size and queue_size in Configs screen.
* Bug 14899: See Bug 11281.
* Bug 15103: Included a note on ABAP screen to inform the user about  unsupported datatypes.
* Bug 15257: In ABAP, input file generated with complete table information.
* Prevent GUI from locking up when a connection fails due to a bad username and password combination.
* Prevent one source of hanging processes in the GUI, by making sure that all dialog windows have the `PyExtract` class as a parent.

### Removed
* Now hiding the `SFTP Location` and `LFU Location` options from the user Configs window in the Production build, since all uploads in production will be driven by the ECF.


## [0.8.0] - 2017-05-31 - Build updates and bugfixes

### Added
* Added ECF filename to the title of the 'Extract In Progress' dialog window.
* Added `EXCLUDE_BINARIES` and `EXCLUDE_BINARY_FOLDERS` logic to `build.spec` to easily exclude ERP dependencies without excluding their Python modules.
* Added folder selector on User Config screen to set location of the IBM DB2 `clidriver/bin` directory (currently non-functional).
* Added the ability to completely exit the program and resume an extraction with encrypted SQLite files.
* Added `version.py` file to track version displays around the application.

### Changed
* Extractions will now start automatically when user reaches the `ExtractionDialog` page, instead of needing to click the `Start` button.
* Moved 'Extraction complete' messages after the 'Packaging' step to reduce confusion based on user feedback.
* Made 'Creating table in SQLite' messages DEBUG level to reduce noise in the GUI log box based on user feedback.
* Moved all global variables into a config.py file.
* Created bash scripts for PRD vs QA builds in the `scripts` folder
* Removed the Oracle `oci.dll` dependency from the `build.spec` script.

### Fixed
* Bug 9179: User can browse ECF from Extract starting screen directly.
* Bug 9379: Updated log message language to read accurate record count.
* Bug 9712: Update `gui.valid_working_dir` to capture all 3 failure modes.
* Bug 10478: User can now delete multiple extractions at once on the 'Continue Extraction' screen
* Bug 10968: In ABAP, information message is displayed to user before the extraction screen to inform ABAP doesn't support pause/resume.
* Bug 11027: Add `gui._default_config_path` which always points to the users `AppData/Local` folder, for persistant `extract.config` settings.
* Bug 11281: Disable Upload button after user clicks 'OK' on        "Package Upload success message"
* Bug 11369: Updated ABAP messenger to ignore `ABAP` and `_` occuring in the starting of the filepath.
* Bug 11421: In ABAP,display a error message if ECF returns zero records.
* Bug 11563: In ABAP,display a message if invalid join is skipped.
* Bug 11619: In ABAP,display a message if invalid parameter is skipped.
* Bug 11807: See Bug 11619.
* Bug 12018: Null columns will no longer show as None, now 'Null'
* Bug 12071: Updated the content preview screen to display long queries.
* Bug 12544: When user closes any window, display confirmation message.
* Bug 12548: In Continue Extraction screen, Next button is enabled only when extractions are present and user selects an extraction.
* Bug 12635: In ABAP,when user enters duplicate filename, prompt user to check if file should be replaced or enter a new filename.
* Bug 13567: Changed what the targetTableName field would use as it's source to avoid any loader issues.
* Bug 13616: Program now validates ALL folder selections when user clicks 'Apply' when updating configuration and dependency settings.
* Bug 13617: Configs screen will show pop-up message only if user changes values.
* Bug 13723: Updated button label to display 'Resume' instead of 'Start'.
* Bug 13743: Fixed message to reference fields and queries.
* Bug 13983: SQL Server Connection screen displays error if port is non-numeric.
* Bug 14266: In ABAP, input file generated with complete table information.
* Bug 14774: Delete local `.log` file after packaging in extraction process since they will be included in the data package and uploaded to PwC.
* Bug 14896: Instructions text updated to remove the reference of 'Start' button and include the reference of 'Resume' button.
* Bug 14899: See Bug 11281.
* Bug 14900: Included DB2 dependencies again to avoid errors.
* Fixed `PyExtract.load_user_configs()` to update the configs dictionary rather than replace it, so any non-saved configs (like ECF filepath) do not get removed and cause runtime errors.
* Update user config `encryption` option on program load, to avoid an error when switching between the `QA` and `PROD` versions of the program.

### Removed
* Removed IBM DB2 dependencies from the build and made their locations user-configurable (just like SAP and Oracle).
* Removed `DB2WriteWorker` components that were never fully implemented.


## [0.7.1] - 2017-05-05 - Dependency decoupling, Clickonce removal

### Added
* Config value default setting enhanced for all configs (see `ConfigDatabase.set_default_user_configs()`).
* Config value checking for queue_size, chunk_size, worker_timeout.
* Add `BaseConnectionDialog.required_submodules` so that PyExtract dependencies only need to be loaded when relevant ECFs are used.
* Added an update to the `soruceRecordCount` field in the `TableExtractions` for the loader.
* Logging for record counts on a key report.

### Changed
* Always enable the 'Apply' button on Config panel, and just validate when the user clicks it, to avoid complex logic checks on every user action.
* Made `pyinstaller build.spec` only produce the one-folder configuration.
* Made SAP and Oracle dependencies optional and user-configurable.
* Moved `setup_multiproc_logger` from `core.py` to `util.py`.
* Broke out write workers into their own `workers.py` module.
* Updated SAP key reports to make multiple calls if system is unresponsive.

### Fixed
* Stopped error when 'Next' button clicked on 'Continue Extraction' screen before ever selecting a row.
* Bug 8455: Added MSSQL 'Port' option to the connection dialog.
* Bug 9090: Added year and © symbol to copyright statement.
* Bug 9249: Changed preview message when a query is invalid/errored.
* Bug 9520: Changed network disconnect message during preview.
* Bug 9312: Invalid working directory error message now shows as expected.
* Bug 9732: Extract working directory now updates immediately.
* Bug 9847: Flag incorrect data types and skip query
* Bug 11060: Continue Extraction now possible even if package doesn't exist.
* Bug 11305: Extract configs now update immediately when user applies them.
* Bug 11900: Duplicate metadata rows won't be created anymore on a pause resume
* Bug 11923: Fixed working directory config to not allow invalid paths.
* Bug 12093: Added MSSQL Port field to allow this connection.
* Bug 12200: Made submodule import success required to advance beyond ECF page.
* Bug 12212: Changed datetime format on ECF preview
* Bug 12326: Key report total record logging.
* Bug 12351: Improved format of 'SAP Connection Failed' message.
* Bug 13249: Fixed invalid parent reference in nesting.
* Bug 13253: Adjusted how SAP throws errors for nested tables
* Bug 13258: Check if table exists prior to getting record count.
* Bug 13260: Check the banned data types in SAP only against cols being extracted.
* Adjusted how SAP handles alphanumeric parameters.

### Removed
* Removed `Clickonce` code, future builds will be released standalone.
* Removed `cli` code, will be added back in future release.
* Removed `setup.py` code since this repo will not be deployed as a package.
* Removed SAP and Oracle clients / dependencies from `build.spec`


## [0.7.0] - 2017-04-28 - Beta

### Added
* Added a 'one folder' build to the `build.spec` file, which will be used in place of the current 'one file' build when finished testing.
* Added optional `dependency.config` file that can be used to locate PyExtract dependencies instead of user setting the system PATH.
* Allow user to set the location of their SAP Netweaver SDK from the GUI.
* Added error handling for errors occuring during extraction.
* Added a query timeout, such that the system call will error out and retry if a given query is taking too long.
* Thread in GUI for a package upload, allowing for use of tool during upload. Modal box (to be made into a progress bar) shows while package is uploading.
* Added a popup if an ECF has errors prior to the ECF preview screen. Also updated query/fields section to display the error for user to assess.
* Added data type checks for all databases/technologies.

### Fixed
* Removed BUFFER Error unit test for SAP as we not longer are bound by 512 character limitation.
* Fixed ECF preview screen to align w/ 1.6 spec.
* Fix `core.extract_from_ecf` and `core._create_pause_resume_table` to use the unique `extract_id` instead of `request_id`.
* Bug 7075: Added datatype checking for Oracle and query skipping if non-supported datatype is present.
* Bug 8280: Errors from workers will now propogate up to `Extraction`.
* Bug 9029: Removed prepopulated connection details.
* Bug 9109: Remove Upload button on home screen.
* Bug 9225: Removed instructions on GUI related to copy and paste.
* Bug 9235: Added 'Agree with Data Request?' prompt to Content Preview.
* Bug 9276: Added * to indicate option vs required fields.
* Bug 10741: Tables will now auto re-extract if local SQLite is missing.
* Bug 11318: Log all invalid columns for a table in SAP.
* Bug 11594: Updated 2.0 parsing logic for dates
* Bug 11580: Updated to show Oracle previews again.
* Bug 11560: Removed HANA option from SAP connections.
* Bug 11902: Large SAP file extractions error remedy
* Update datetime parse for expiry date formatting
* Bug 12041: fixed error typo
* Bug 9028: Added pause resume, minimize buttons throughout the tool.

### Removed
* Removed `sap` related imports from `pyextract` and `pyextract.connect` init files. All SAP modules will need to be imported directly.
* Removed BUFFER Error unit test for SAP as we not longer are bound by 512 character limitation


## [0.6.1] - 2017-04-20 - Upload locations, More bugfixes

### Added
* Add all LFU and SFTP upload locations to `/pyextract/config.py`
* Allow user to set the LFU / SFTP upload location from the GUI using the `ConfigsDialog` panel.
* Enabled `Next` button on the `ContinueExtractionDialog` window.
* Add `ContinueExtractionDialog.prompt_to_restore_ecf`, in case original ECF from an extraction no longer exists when user attempts to 'Continue'.
* Added back PRAGMA settings for encrypting SQLite files during operations

### Fixed
* If a `null` value exists in ECF data to display to user, display a blank string instead.
* Correctly pass in the chunk size to SAP MSGR.
* Bug 8043: Port numbers are now validated in connection screens. Orcl & DB2
* Bug 8073: Error message now raised for invalid MSSQL schemas
* Bug 8195: User now prompted to delete data package after successful upload.
* Bug 8280: Error message now raised for these situations.
* Bug 8353: Data package will always be created, regardless of error.
* Bug 9148: Instructions updated.
* Bug 9149: Now showing all ECF details on the file details screen.
* Bug 9153: Reset password field on navigation to a Connection dialog.
* Bug 9203: Updated instructions for DB2 Connection.
* Bug 9242: Error message is shown when previewing an invalid table.
* Bug 9301: Updated pause/resume log message to be more accurate.
* Bug 9309: Use Extract ID instead of timestamp to name log files.
* Bug 9305: Start/resume button behavior has been verified.
* Bug 9310: Same fix required for 9301.
* Bug 9313: Cancel button is no longer enabled while program is pausing.
* Bug 9321: Error raised if data package is deleted between pause/resume.
* Bug 9330: Oracle connection window will now populate saved 'User' value.
* Bug 9395: MSSQL preview will now return the right message with limited access.
* Bug 9471: Extraction process no longer hangs in this case.
* Bug 9510: Error now raised same as 9516.
* Bug 9516: Error will be raised to user if data integrity check fails during pause / resume (i.e. they delete a local database from the data package).
* Bug 9691: Fixed message to conform to spec for a pause.   
* Bug 9706: User now prompted to delete data package on extraction 'Cancel'.
* Bug 9732: Package now extracts to proper path within request ID folder.
* Bug 10686: Trim white space from columns in SAP ECFs.
* Bug 10825: Fixed spelling of the word 'timeout' on user config screen.

### Removed
* Removed `ContinueExtractionDialog.previous_button_pressed` because it is only a one-page dialog. Button will remain, just always inactive.


## [0.6.0] - 2017-04-17 - ABAP Workflow, General Bugfixes, Refactoring

### Added
* Function module check for SAP upon start of the GUI.
* Finished two-part ABAP workflow in the GUI.

### Changed
* Make SAP key reports stream in chunks rather than all at once to avoid overflowing the buffer.
* Removed the `core.extract` function and broke it into multiple helper functions in `gui2`. Since this function was only being called from `gui2`, and has gained a lot of `wx` and GUI-specific code, it makes more sense to break it out.
* Pass `**extract_kwargs` into `gui2.ExtractThread` instead of `core.extract`.
* Make `gui2.PyExtract.next_workflow_dialog` only return dialog name instead of returning the dialog object and its name.
* Moved all `*.ico` and `*.png` files to a new `/assets/` directory.

### Fixed
* Bug 8818: Updated SAP auth check to throw error if FM is not available.
* Bug 9072: Reject SAP tables with invalid data types.
* Bug 9132: ECFs with a past `ExpiryDate` will now raise an error on selection.
* Bug 9148: Fixed instructions on the 'File Detail' screen.
* Bug 9165: 'Save Connection' errors no longer occur due to duplicate names.
* Bug 9189: Updated 'Content Preview' so SAP uses column name 'Fields'
* Bug 9205: Change title on 'IBM DB2 Connection' screen.
* Bug 9206: Allow user to overwrite previously saved connections.
* Bug 9222: Change title on 'Content Preview (table)' dialog.
* Bug 9228: Error now raised when user attempts to access data they don't have access to in the Content Preview screen.
* Bug 9232: Raise error for SAP queries w/ an asterisk in the 'Fields'.
* Bug 9249 & 9707: Content preview now alerts user if query fails instead of hanging.
* Bug 9262: Change title on 'Oracle Connection' screen.
* Bug 9274: Updated 'Log' label on Extract screen to 'Extraction Log'.
* Bug 9275: Updated 'Progress' label on Extract screen to 'Status'.
* Bug 9368: Remove 'Windows Authentication' option from `OracleConnDialog`.
* Bug 9377: Updated column headers in Content Preview screen.
* Bug 9382: Provide more detailed error message for failed ECF parsing.
* Bug 9518: Content preview alerts user when query returns 0 records.
* Bug 9793: Deleting extractions now occurs at an Extract ID level instead of Request ID level.

### Removed
* Removed `utils.sql_escape_string` because we now properly escape columns in SQLite using double quotes, so special characters are not a problem.
* Removed `callback_success` from `core.extract` since that functionality has been replaced with `wx.Event` handlers surrounding the thread.
* Removed original `gui.py`, replaced by previously-named `gui2.py`.
* Removed `webapp` package, `config.py`, and `run_flask.py`, moving them to [their own repo in TFS](http://matlkatp2app025:8080/tfs/ECAP/PyTech/_git/pyextract-flask)
* Removed all `TODO` comments, and turned them into work items in VSTS.


## [0.5.1] - 2017-04-10 - Stop/Resume, GUI Content, General Bugfixes

### Added
* Created `gui2.GuidePanel` class for consistent look and feel of right-hand 'Instructions' pane within the GUI.
* Now setting defaults for the user 'Config' on first program launch.
* During ECF selection, if data for the ECF was previous extracted and can be resumed from, an alert will be shown to the user that prompts them to continue the extraction instead of restarting it.
* Now saving every extraction attempt to the local database so they can be restarted in case the program crashes or is closed.
* Added 'Continue Extraction' workflow from the home page.

### Changed
* Now using `TableExtractions.dat` SQLite database for pause/resume functionality in `gui2.py` (so an extraneous `pyextract.dat` file does not throw off the loader).
* Replaced custom class `gui2.BasicModal` with default `wx.MessageBox`.
* Replaced custom class `gui2.BusyModal` with default `wx.BusyInfo`.
* Rename class `gui2.Config` to `gui2.ConfigDatabase` to help distinguish it from the generic 'config' dictionaries of key-value pairs.
* Merge four functions `ConfigDatabase.does_*_conn_exist` into single function with argument `ConfigDatabase.conn_exists(erp)`.
* Merge four functions `ConfigDatabase.get_*_saved` into single function with argument `ConfigDatabase.saved_credential_names(erp)`.
* Merge four functions `ConfigDatabase.save_*_creds()` into single function with argument `ConfigDatabase.save_credentials(erp)`.
* Create base class `gui2.BaseConnectionDialog` for the methods that are shared between the subclassed ERP-specific dialogs.
* Changed fields being collected for SAP metadata, per convo on loade reqs
* Format checksums in EPF as list of dictionaries for GATT retriever service.
* Update `gui2.ContentPreviewDialog` so grid selection is row-based.
* Break out helper function `PyExtract._create_extract_kwargs` from inside of `PyExtract.open_ecf_button_pressed`.
* Removed `PASSWORD` fields from `ConfigDatabase` since we don't save them.

### Fixed
* Bug 7104: SourceFieldLength of SAP columns is now accurate.
* Bug 7110: Raise warning if disk space is too low for extraction.
* Bug 7117: SourceFieldLength of SAP columns is now accurate.
* Bug 8066: Cancel button now works as expected.
* Bug 8070: Check SAP Client value and raise error if not int of len 3.
* Bug 8074: Removed `pyextract.dat` from output packages.
* Bug 8142: Shows more specific error message around why a connection failed
* Bug 8178: Continue extraction for invalid queries (non SAP)
* Bug 8428: Modified row skip for large tables so workers don't shut down.
* Bug 8429: SAP connection now succeeds with this ECF.
* Bug 8495: Updated copy on 'zero records returned' error message.
* Bug 8558 & 8599 & 8612: Updated DB2 datatype mappings.
* Bug 8901: Modified check to raise an error for invalid tables and continue.
* Bug 8919: Extraction now works with this ECF.
* Bug 8946: Error for invalid SAP fields now raised properly.
* Bug 8971: Default worker timeout set higher to fix this defect.
* Bug 9082: Fixed SAP connection label names.
* Bug 9088: Updated copy on the 'SAP Connection' screen.
* Bug 9089: Now setting default config settings on first program launch instead of when the 'Open ECF' button is clicked.
* Bug 9090: Updated copy for the PwC legal disclaimer on landing page.
* Bug 9092: Title of all primary screens and dialogs will be 'Extract'.
* Bug 9107 & 9183: All instances of the title 'Guide' in the GUI have been changed to 'Instructions'.
* Bug 9108: Updated instruction text for SQL Server connections.
* Bug 9110: Updated title on the SQL Server connection screen.
* Bug 9111: Added 'Schema' input to MSSQL connection screen in place of the previous 'Port' input (which is not used for MSSQL).
* Bug 9112: Title on landing page changed to just 'Extract'.
* Bug 9128: User will be prompted to confirm when attempting to close app.
* Bug 9131: ECF file selection dialog now only shows `*.ecf` files.
* Bug 9172: Update error message copy if an invalid `*.ecf` file is selected.
* Bug 9181: Correct instructions now shown on 'Content Preview' screen.
* Bug 9301: User is now able to resume extractions after closing the program.
* Bug 9307: Correctly able to pause and resume, hanging DB connection was cause.
* Bug 9387: User can now deselect queries in content preview.

### Removed
* Removed extraneous default positional arguments from many object calls in `gui2.py` (i.e. turning `wx.Panel(self, wx.ID_ANY, wx.DefaultPosition, wx.DefaultSize, wx.TAB_TRAVERSAL)` into `wx.Panel(self)`)
* Removed all `PASSWORD` fields from the `ConfigDatabase` tables.
* Removed extra `Panel`s and `Sizer`s in `gui2.py` used for borders and alignment, replacing them with explicit border increases or alignment changes to the core content that they were surrounding.
* Removed calls in `gui2.py` to `wx.StaticText.Wrap(-1)`, since this is the default behavior.


## [0.5.0] - 2017-03-31 - New GUI, Pause/Resume, GATT Alignment

### Added
* Add `ABAPMessenger` as an input option for data extraction from a folder of ABAP `.fil` files (module `pyextract.connect.abap`).
* Add supporting unit test for an ABAP to SQLite extraction in the `tests.test_core.TestFromABAP` class (with ECF and `.fil` file `assets`).
* Allow `Extraction.extract_id` to be set explicitly during object creation by passing a value for `extract_id` to the `Extraction.__init__` method.
* Added database tracking of extraction status at both the query level and more granular where clause level for nested extractions
* Unexpected errors that occur during the extraction process will now be logged in the `.log` file with their complete traceback.
* Expected / accounted for errors that occur during extraction will not impede extraction of other tables, and will be logged in the `TableExtractions.dat` database.
* Add validation to `SAPMessenger.get_data_from_sap` so a more specific error is raised when a column listed in an ECF does not exist in the associated table in SAP.
* Pause resume base functionality and validation
* SAP SNC connection functionality to the new GUI
* Add progress bar to GUI to track progress from query to query in an ECF.
* Added abiiity to extract SAP Key Reports (only MB5B)

### Changed
* Switch naming conventions to follow GATT Extract v1.6 strictly. All databases will now end in `.dat` instead of `.db`.
* Replaced the standalone GUI with a new shiny GUI that closely resembles the .NET version of the Extract program.
* Massive unit test refactor, removing 300 total lines of code, and bringing pylint to 9.92, while maintaining 100% of old functionality.
* Move unit test Messenger creation into `setUpClass` methods, so a failing connection to one database (e.g. Oracle) does not prevent tests for other databases (e.g. MSSQL) from being run.
* Update `ecfreader.get_ecf_meta_data` and `utils.DataDefinition` to support `SAP > ABAP` ECFs and table-based metadata.
* Update `pyextract.streams.DataStream` so that it will `trim_data` if provided a `row_limit` smaller than the number of rows returned.
* Using a `@contextmanager` with APSW connections in the GUI so that all connections are closed safely every database update.
* Update `SAPMessenger` log messages to track seconds for each pull, not aggregate seconds.
* Now encrypting output EPF files using Chilkat and a standard PwC public key (NOT the public key from the ECF files).
* Now uploading all data to the QA1 servers instead of QA2.
* Make `core.save_extract_data` function less reliant on the `ecfreader.ExtractData` attribute names.
* Allow encryption to be `None`, `AES-128`, or `AES-256` in the GUI while we are still in testing mode.

### Fixed
* Changed SAP testing host from `pwcusadsap006-00004992.cloudapp.net` to the Halo-specific `pwcusadsap006-00005525.cloudapp.net`, which is more reliable because it has less traffic.
* Fixed error in new GUI that was causing ECF filepaths to not show up properly after they had been selected.
* Make `key_size` selection in the GUI have effect on AES encryption used.
* Fix where `PyExtract.init_dialogs` is called in the `while True` loop so that 'Cancel' buttons throughout the GUI work properly again.
* Fix `core._get_table_status` call so that it will properly return `not_started` where no row exists for a database table.

### Removed
* Removed unused `Extraction.save_configuration` function until we build it properly in Sprint 7.
* Removed method `Extraction.extract_from_ecf` and turned it into a top-level function in `core.py` called `extract_from_ecf`.


## [0.4.0] - 2017-03-17 - Encryption, SQLite Chunking, and Publishing

### Added
* Add AES-256 encryption as an option to SQLite inputs/outputs, in addition to the current unencrypted and AES-128 options.
* Allow SQLite outputs to write data as: one database for all data, one database per table, or one database per chunk of data.
* Update the 'Complete' panel in the GUI to show filepath of results.
* Log the time to complete all extractions and the total records extracted from all tables after an extraction from the GUI.
* Log a warning at the end of `pyextract.extract` if zero records were extracted for every call to `Extraction.extract`.
* Add button to upload data package to a remote SFTP site after extraction.
* Add button to upload data package to the PwC LFU service after extraction.
* Warnings that occur during an Extraction from the GUI will now appear as pop-up alert boxes once the Extraction completes (Bug #5571).

### Changed
* Now using more descriptive / specific labels for user inputs in GUI.
* Split `SQLiteWriteWorker` into two classes: one for writing to a single SQLite database (default), and a new `SQLiteChunkWorker` for writing to a different database every chunk.
* Moved `setup_multiproc_logger` from `utils.py` to `core.py` (the only module that function was used in).
* Modified metadata and logging tables to write to their own database if sqlite output is used, per loader requirements
* If an Extraction is cancelled before any log messages are written, the program will still include an empty log file in the final Package, and display a warning to the user.

### Fixed
* Bug 6853: Connection to MSSQL will error if the `database` or `server` value provided is a blank string ('').
* No longer showing multiple 'Extraction Canceled' messages if the Cancel button is hit repeatedly during an Extraction.

### Removed
* Removed unused `SQLiteMessenger.sftppath` attribute.


## [0.3.0] - 2017-03-03 - Packaging and polishing

### Added
* Add `callback_success` argument to `pyextract.extract`, which allows any function to be executed after an extraction (for GUI alerts / manipulation).
* Add `callback_error` argument to `pyextract.extract`, which will allow a function to execute when an error occurs, passing it that error.
* When extraction completes from the GUI, the 'Cancel' button will now change its label to 'Finish'.
* Add early validation and alert box for ECF selection in the GUI.
* Add early validation to the ECF parser functions so an error occurs if the ECF file contains a non-unique `NameAlias` for any query.
* Add `row_limit` to `SAPStream` class to improve unit test speed.
* Add `connection.setbusytimeout(50)` to SQLite connections, to ensure support for multiprocessing and multithreading.
* Added `*args` and `**kwargs` to MSSQL and Oracle connection functions to support the C2 method of instantiation with extra information.
* Made GUI resizeable during Wizard pages.

### Changed
* Renamed all test ECF files for consistency and clarity.
* Refactored ECF parsing to be consistant regardless of source.
* Refactored Metadata for consistancy and updates metadata to align with GATT loader requirements.
* Refactored SAP messenger and stream to be more modular and consistant with other DB stream/messenger functionality.
* Improved formatting of times and record counts in `SAPStream` log messages.
* Change name of `utils.clean_str_for_logging` to `utils.cleanstr` now that it is used more frequently.
* Now calling `pyextract.extract` from GUI using keyword arguments instead of positional ones for clarity.
* Now validating advanced options in the GUI earlier (on submitting the Advanced Options page).
* Combined `sap_to_sqlite_nested` and `sap_from_ecf_v1_6` unit tests into `TestExtractionFromSAP.test_sap_nested_from_ecf`.
* DB2 setup in the GUI will now default username to the logged in user.
* GUI now creates a ZIP package of files instead of placing them all on the desktop.
* Can now set a `output_folder` and `package_name` in the `pyextract.extract` function and in the GUI.

### Fixed
* Fixed the 'Confirmation Page' in the GUI to properly show selected values.
* Null values are no longer written to SQLite as the string 'None'
* SAP metadata is now written to master database during extraction.
* Clean all queries (replace tabs and newlines with spaces) before logging.
* If a `NameAlias` is provided for a query in an ECF, the output table will use that value for its name instead of the `Name` value.
* Make all SAP fields uppercase in `ECFParser.format_table_info` so that data is aligned with what is returned from SAP RFC function calls.
* Enclosed table and column names in SQLite with "double quotes" to allow more strings as identifiers.
* Fixed SAP `sapstream.get_header_split_docs` to work with schema-based databases (e.g. MSSQL).
* Updated MSSQL source-type mapping to return the best approximation of the source type in SQL Server syntax from the `ceODBC.Cursor.description` value.
* Fixed issue where GUI would display blank screen behind primary screen after selecting 'Another Extraction'.

### Removed
* Removed all unit tests and compatibility for v1.5 ECFs.
* Removed `total_rows_for_table` record count from `RfcTableExtractions` until it can be implemented reliably with multiprocessing.
* Removed setting of database `PRAGMA`s in SQLite (until we test performance).
* Simplified SAP extraction API so an `extraction_meta` namedtuple argument is no longer required to run `pyextract.extract`.


## [0.2.1] - 2017-02-16 - Standalone fixes and IBM DB2 support

### Added
* Added DB2 to the GUI as a source connection.
* Added unit test for DB2 to SQLite extraction.

### Fixed
* Removed hard coded SAP credentials from GUI to properly accept user input.
* Included DB2, GnuPG, Oracle, and SAP dependencies in `./build.spec` so the standalone program correctly runs on a no-dependency server (`US1HAL5W70048`)
* Fixed `pyextract.extract` to not create a duplicate `DataStream` when running SAP extractions (removes the 'Fatal Error' on SAP extraction start).
* Make 'Fatal Error' on extraction end auto-close with custom `forking.Popen` (https://github.com/pyinstaller/pyinstaller/wiki/Recipe-Multiprocessing)
* Replaced Oracle `instantclient_12_1` with `instantclient_11_2`, which works with both our `R12` and `11i` testing databases.


## [0.2.0] - 2017-02-08 - Wizard-style GUI, ClickOnce, SAP Nesting, and MemSQL

### Added
* GUI deployment now uses ClickOnce (http://matlkecapapp001:7770/publish.htm) so future updates to the executable will be automatically downloaded. See the 'Deploying' section of `./README.md` for more information.
* Extraction from and to MemSQL and MySQL is now supported.
* Extraction from SAP using N-level Nesting (e.g. BKPF/BSEG) is now supported.
* Extraction using a SQL query or an ECF file is now supported in the GUI.
* Extraction from Oracle using `service_name` or `tnsname` in the GUI (in addition to current `service_id` option) is now supported.
* Unit test for extracting from ECF files.
* Unit test for writing to non-chunked SQLite database.
* Unit test for writing to a specifically-named output table.
* Trial versions of IBM-DB2 and CSV messengers have been created, but are not fully tested or integrated into the user interfaces.

### Changed
* Turned standalone GUI into a wizard-based interface.
* Set default Oracle `NLS_DATE_FORMAT` to `DD-Mon-YY` to support GATT v1.6 ECFs.
* Made `Extraction` class require a `DataStream` object as the `source` instead of an `ABCMessenger` object. This will allow the program to use more complex sources (SAP N-nesting) instead of only SQL-based ones.
* Turned class-level properties (e.g. `DataReader.CHUNK_SIZE`) into instance-level properties to support parallel runs with different settings.
* Changed `row_limit` option of `SQLStream` to work on a per-table / per-extract basis instead of for the life of the object.
* Environment variable `PYEXTRACT_PASSWORD` must now be set in order to run unit tests (available from PyExtract developers).
* Replaced `secret.py` with `credentials.dat`, an encrypted SQLite database.
* Made `test_core.py` unit tests more robust: now checking for table existence and accurate row counts in the SQLite versions of tests.
* Updated logging messages to be more consistent and have appropriate levels.

### Fixed
* Moved `multiprocessing.freeze_support()` to the top-level `gui.py` file, which solves the issue where workers would occasionally not write to SQLite.
* Overloaded `multiprocessing.forking._Popen` class with custom version that makes Processes / Threads safe for freezing into a standalone EXE.
* Explicitly included GPG binary (`iconv.dll`) in the standalone EXE.
* Explicitly included Oracle binary (`oraociei12.dll`) in the standalone EXE.
* General Pylint, Typing, and PEP-0008 updates on all files.

### Removed
* Removed `_assets` from Git repo and history, moving them to a [Google Drive folder](https://drive.google.com/drive/folders/0B2RrjZ1HLSnjeXN1VlprbVpMdVk?usp=sharing)


## [0.1.0] - 2016-12-15 - Alpha

Initial version of Python Extract program.

### Features
* Extraction from MSSQL, Oracle, SQLite, and SAP single tables into MSSQL or SQLite, using standard SQL queries.
* Standalone executable graphic interface to run the program
* (Draft) Command-line interface for running Oracle to MSSQL extractions


[Unreleased]: http://matlkatp2app025:8080/tfs/ECAP/PyTech/_git/pyextract/branches?baseVersion=GBmaster&targetVersion=GBdevelop&_a=files
[0.1.0]: http://matlkatp2app025:8080/tfs/ECAP/PyTech/_git/pyextract?version=GBmaster

[GUIDELINES]: https://raw.githubusercontent.com/olivierlacan/keep-a-changelog/master/CHANGELOG.md
[SEMVER]: http://semver.org/
