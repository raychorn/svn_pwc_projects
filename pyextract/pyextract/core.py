"""Module to handle high-level extraction process, multiproc, and threading."""

import json
import multiprocessing
import os
from queue import Empty as QueueEmpty
import tempfile
import time
import uuid
import math
import apsw

try:
    from wx import CallAfter
except:
    pass
# from wx import CallAfter  # Avoid locking GUI with logging alerts

from . import config
from . import connect
from .connect.abap import ABAPMessenger
from .connect.mssql import MSSQLMessenger
from .connect.mysql import MySQLMessenger
from .connect.sqlite import SQLiteMessenger
from . import ecfreader
from . import utils
from .workers import SQLiteWriteWorker, SQLiteChunkTableWorker, \
                     MSSQLWriteWorker, MySQLWriteWorker

from cacheManager import redis_connection

LOGGER = multiprocessing.get_logger()


class Extraction(object):
    """Manages data extraction(s) from one database to another."""

    def __init__(self, source,  # type: DataStream
                 output: connect.ABCMessenger,
                 logfile: str = None, max_writers=1, worker_timeout=30,
                 chunk_results='one_db', extract_id: str = None) -> None:
        """Instantiate an object to manage data extraction(s).

        ARGS:
            source: A 'DataStream' connected to the database/schema
                where data will be extracted from.
            output: A Messenger connected to the database/schema
                where data will be extracted into.
            logfile: If True, logs to text file in addition to shell/DB.
            max_writers: Maximum number of worker processes to pull data.
            chunk_results: Determines what method will be used to write data
                to the local SQLlite file there are currently three supported.
                    1) 'one_db' - all data will be written to one database file
                    2) 'db_per_table' - each query/table will write to it's own db
                    3) 'db_per_chunk' - each chunk of data returned will be written
                        to it's own database
            extract_id: If an ECF is being used to initaite the extraction
                use the RequestID from the ECF

        OBJECT PROPERTIES:
            extract_id (str): Unique 8-character string for this extraction.
            logger (multiprocessing.Logger): Logger that spans processes and
                writes all log messages to disk and stdout.
            queue: Where data readers place data to be retrieved by writers.
            worker_msg_queue: Multiprocess Queue holding tuples of info about
                each attempted data extraction.
        """
        # TODO -- refactor this typecheck
        assert 'pyextract.streams' in str(type(source)), \
            'source must be a pyextract.Stream object'
        assert isinstance(output, connect.ABCMessenger), \
            'output must be a pyextract.Messenger object'
        assert chunk_results in ('db_per_chunk', 'one_db', 'db_per_table'), \
            'Invalid value for chunk_results:  {}'.format(chunk_results)

        self.stream = source
        self.output = output
        self.logfile = logfile

        self.max_writers = max_writers
        self.worker_timeout = worker_timeout
        self.chunk_results = chunk_results
        # Use ID provided, or generate a new unique ID
        self.extract_id = extract_id or str(uuid.uuid4())[:8]

        self.logger = multiprocessing.get_logger()
        self.queue = None  # type: multiprocessing.Queue
        self.worker_msg_queue = None  # type: multiprocessing.Queue

        # Track total records extracted across all function calls
        self.rows_read = 0

        self.warnings = False

        if self.logfile is None:
            self.logfile = tempfile.mktemp()
        utils.setup_multiproc_logger()

    def extract_from_query(self, query: str):
        """Perform a data extraction from a SQL query."""
        metadata = utils.DataDefinition(query, self.stream.messenger)
        _create_new_temp_status_tracker(self.output)
        _create_pause_resume_table(self.output)
        self.extract(metadata=metadata)

    def extract(self, metadata: utils.DataDefinition):

        """Configure the program and begin data extraction."""
        # Don't run an extraction if the input stream has been stopped by user
        if self.stream.is_stopped():
            self.logger.debug('Extraction stopped by user, skipping extraction.')
            return

        self.logger.info("Extraction started with ID:  %s", self.extract_id)
        starttime = time.time()
        self.queue = multiprocessing.Queue()
        self.worker_msg_queue = multiprocessing.Queue()

        self.logger.info("Creating table of general extract metadata")
        _create_metadata_table(self.output, metadata)

        # Create workers to write data
        number_workers = min(self.max_writers, multiprocessing.cpu_count())
        self.logger.info("Using %s subprocess(es) to write data", number_workers)
        workers = self._create_write_data_workers(number_workers)

        # Create the table in the output database
        if self.chunk_results == 'one_db':
            if not self.output.table_exists(metadata.target_table):
                self.logger.info("Creating output table:  %s", metadata.target_table)
                columns = [column['targetFieldName'] for column in metadata.columns]
                datatypes = datatypes_from_metadata(metadata.columns, type(self.output))
                self.output.create_table(metadata.target_table, columns, datatypes)
            else:
                self.logger.info("Output table exists:  %s", metadata.target_table)
        elif self.chunk_results == 'db_per_table':
            messenger = _create_sqlite_messenger(self.output, metadata.target_table)
            if not messenger.table_exists(metadata.target_table):
                self.logger.info("Creating new SQLite table: %s", metadata.target_table)
                columns = [column['targetFieldName'] for column in metadata.columns]
                datatypes = datatypes_from_metadata(metadata.columns, type(messenger))
                messenger.create_table(metadata.target_table, columns, datatypes)

        try:
            messenger._extract_cursor.close()
        except:
            pass
        try:
            messenger._conn.close()
        except:
            pass

        self.logger.info("Beginning data pull using metadata: %s", metadata)

        #Creating temporary tracking table for pause resume
        try:
            _update_pause_resume_table_status(self.extract_id, self.output, metadata.target_table, 'in_progress')
        except apsw.BusyError:
            self.logger.error("_update_pause_resume_table_status() failed.  Leaving blank...")

        try:
            self.stream.start(self.queue, metadata)
            self.warnings = self.stream.encountered_row_skips

        except KeyboardInterrupt:
            self.logger.warning("Extraction interrupted. Terminating workers.")
            for worker in workers:
                worker.terminate()
            raise
        except:
            self.logger.info('Waiting for write worker to finish...')
            self.queue.put(None, timeout=1)
            for worker in workers:
                self.logger.debug('Joining with worker thread %s', worker)
                worker.join()
                self.logger.debug('Finished joining with worker thread %s', worker)
            raise

        # Add rows extracted this call to the total
        self.rows_read += self.stream.rows_read

        ###### EBS #######
        # if self.stream.__module__ in ['pyextract.streams.sqlstream']:
        #     self.logger.info('Waiting for write worker to finish...')
        #     self.queue.put(None, timeout=1)
        #     for worker in workers:
        #         self.logger.debug('Joining with worker thread %s', worker)
        #         worker.join()
        #         self.logger.debug('Finished joining with worker thread %s', worker)
        #
        # ####### SAP #######
        # else:
        #     if self.queue.qsize() > 0:
        #         for worker in workers:
        #             self.logger.debug('Joining with worker thread %s', worker)
        #             worker.join()
        #             self.logger.debug('Finished joining with worker thread %s', worker)

        ####### 207 #######
        self.logger.info('Waiting for write worker to finish...')
        self.queue.put(None, timeout=1)
        for worker in workers:
            self.logger.debug('Joining with worker thread %s', worker)
            worker.join()
            self.logger.debug('Finished joining with worker thread %s', worker)

        # # Clean up workers
        # for worker in workers:
        #     worker.terminate()

        self.logger.debug('Finished closing data queue and worker processes.')
        self.logger.info('Collecting and validating subprocess messages...')
        worker_messages = []
        while True:
            try:
                worker_messages += [self.worker_msg_queue.get(timeout=1)]
            except QueueEmpty:
                break

        self.logger.info('Saving extraction progress in local database...')
        self._update_post_extract_status(metadata.target_table, worker_messages)

        # Create a unique output DB for the logs if it's a sqlite DB
        if isinstance(self.output, SQLiteMessenger):
            log_output = _create_sqlite_messenger(self.output, config.LOG_TABLE)
        else:
            log_output = self.output

        self.logger.info("Writing subprocess summary to master database...")
        write_chunk_summary_table(log_output, worker_messages)

        self.logger.info("End-to-end extraction time: %d seconds",
                         (time.time() - starttime))

        self.logger.info("Writing logs to master database and text file...")
        # utils.flush_log_to_file(self.logger, self.logfile)
        self.copy_log_to_messenger(log_output)

        self.logger.info("Text log is located at:  %s", self.logfile)
        self.logger.info("Output is located at:  %s", self.output)

        if self.stream.is_stopped():
            self.logger.info("Extraction procedures paused, select Resume to continue")
        else:
            update_total_record_counts(self.output, metadata, self.stream.rows_read)

    def _create_write_data_workers(self, number_workers: int) -> list:
        """Create and return a group of workers that write to the result DB."""
        # Set keyword arguments that are shared across all types of Workers
        kwargs = {
            'write_queue': self.queue,
            'extract_id': self.extract_id,
            'worker_msg_queue': self.worker_msg_queue,
            'logfile': self.logfile,
            'worker_timeout': self.worker_timeout,
        }

        # Determine the specific type of Worker to write results with
        if isinstance(self.output, SQLiteMessenger):
            # Update kwargs shared by all SQLite workers
            kwargs.update({
                'is_zipped': self.output.is_zipped,
                'password': self.output.password,
                'aes256': self.output.aes256,
            })
            # Determine specific worker type based on 'chunking'
            if self.chunk_results == 'one_db':
                worker_class = SQLiteWriteWorker
                kwargs.update({
                    'filepath': self.output.filepath
                })
            else:
                db_per_chunk = bool(self.chunk_results == 'db_per_chunk')
                worker_class = SQLiteChunkTableWorker
                kwargs.update({
                    'master_msgr': self.output,
                    'db_per_chunk': db_per_chunk
                })
        elif isinstance(self.output, MSSQLMessenger):
            worker_class = MSSQLWriteWorker
            kwargs.update({
                'connection_string': self.output.connection_string,
                'schema': self.output.schema,
            })
        elif isinstance(self.output, MySQLMessenger):
            worker_class = MySQLWriteWorker
            kwargs.update({
                'connection_string': self.output.connection_string,
            })

        # Create, start workers, and return them in a list
        processes = []
        for _ in range(number_workers):
            proc = worker_class(**kwargs)
            proc.start()
            processes += [proc]

        return processes

    def copy_log_to_messenger(self, messenger: connect.ABCMessenger=None,
                              table='_pyextract_full_log'):

        """Copy log data from text file to the output messenger."""

        columns = ('datetime', 'process', 'level', 'message', 'host')
        metadata = get_simple_text_metadata(columns)

        if not messenger.table_exists(table):
            datatypes = datatypes_from_metadata(metadata, type(messenger))
            messenger.create_table(table, columns, datatypes)

        if not os.path.exists(self.logfile):
            # self.logger.warning('No log data to write.')
            return

        with open(self.logfile, 'r') as log:
            data = log.readlines()

        if not data:
            # self.logger.warning('No log data to write.')
            return

        # Turn data into 2D array matching column order (sorted by timestamp)
        data = (line.split('\t') for line in data)
        # Remove lines that have been malformed by subprocesses
        data = sorted(line for line in data
                      if len(line) == len(columns))
        messenger.insert_into(table, data, metadata)

    def _update_post_extract_status(self, table: str, worker_messages: list):
        """Update pause/resume database based on extraction error/success."""
        errors = [row[-1] for row in worker_messages if row[-1] is not None]

        if errors:
            # Log the errors in GUI for user, and update Pause/Resume
            # table to reflect that an error occurs for this table
            for error in errors:
                self.logger.error(error)
            _update_pause_resume_table_status(
                extract_id=self.extract_id,
                output=self.output,
                table=table,
                status='error'
            )
            self.output.update_records(table=config.STATUS_TABLE,
                                       columns=['error_encountered'],
                                       updated_values=[True],
                                       where_condition="table_alias = '{}'" \
                                                       .format(table))
            _create_new_temp_status_tracker(self.output)

        elif not self.stream.is_stopped():
            # Update status table for the completed query/table
            # Drop temporary tracker table now that table has completed
            # (only if the stream was not stopped/paused by the user)
            _update_pause_resume_table_status(
                extract_id=self.extract_id,
                output=self.output,
                table=table,
                status='complete'
            )
            _create_new_temp_status_tracker(self.output)


def save_extract_data(output: connect.ABCMessenger,
                      ecf_data: ecfreader.ExtractData,
                      error: str = None):
    """Write data about an intended extraction to the output messenger.

    Data will be written to the standard PyExtract 'config.STATUS_TABLE'. If
    an error message is provided, this will also be captured so the user
    knows that an extraction did not occur for that table/dataset.
    """
    fields_values = (
        ('RequestID', ecf_data.request_id),
        ('SchemaName', ecf_data.schema),
        ('TableName', ecf_data.table_name),
        ('PwCSetId', ecf_data.set_id),
        # Following 4 fields will be used in future updates
        ('SourceRecordCount', None),
        ('TargetRecordCount', None),
        ('Difference', None),
        ('LoadID', None),
        ('table_alias', ecf_data.table_alias),
        ('query_text', json.dumps(ecf_data.query_text)),
        ('ecf_filename', ecf_data.ecf_filename),
        ('error_encountered', error),
    )

    #Check if the output source is sqlite, generate new messenger for a new DB
    if isinstance(output, SQLiteMessenger):
        output = _create_sqlite_messenger(output, config.STATUS_TABLE)

    if not output.table_exists(config.STATUS_TABLE):
        fields = [field for field, _ in fields_values]
        metadata = get_simple_text_metadata(fields)
        columns = [column['targetFieldName'] for column in metadata]
        datatypes = datatypes_from_metadata(metadata, type(output))
        output.create_table(config.STATUS_TABLE, columns, datatypes)

    # Convert ExtractData object into a one-row table of strings
    data = (tuple(value for _, value in fields_values), )
    output.insert_into(config.STATUS_TABLE, data)


def clean_up_local_dbs(output: connect.ABCMessenger):
    """At the end of an extraction removes duplicates records that may have
        been created as a result of the pause resume functionality
    """
    #Check if the output source is sqlite, generate new messenger for a new DB
    if isinstance(output, SQLiteMessenger):
        status_output = _create_sqlite_messenger(output, config.STATUS_TABLE)
        status_output.delete_duplicates(table=config.STATUS_TABLE,
                                        fields="RequestID, table_alias")
        meta_output = _create_sqlite_messenger(output, config.METADATA_TABLE)
        meta_output.delete_duplicates(table=config.METADATA_TABLE,
                                      fields="source_table_alias, sourceFieldName")


def _create_metadata_table(output, metadata: utils.DataDefinition) -> None:
    """Create a metadata table, if SQlite, create new DB for the package"""

    #Check if the output source is sqlite, generate new messenger for a new DB
    if isinstance(output, SQLiteMessenger):
        output = _create_sqlite_messenger(output, config.METADATA_TABLE)

    output.create_metadata_table(metadata, config.METADATA_TABLE)


def _create_pause_resume_table(output: connect.ABCMessenger):
    """Create a temporary status tracking table on given database.
    This table will support extract pause/resume functionality.
    """
    table = 'query_level_status'
    columns = ('extract_id', 'tab_name', 'status')
    if not output.table_exists(table):
        output.create_table(table=table, columns=columns)


def _create_new_temp_status_tracker(output: connect.ABCMessenger):
    """Create a status tracker table (overwriting current one if needed)."""
    table = "temp_tracker"
    columns = ('query_text', 'tab_name')
    output.drop_table_if_exists(table)
    output.create_table(table=table, columns=columns)


def _get_previously_extracted_row_count(extract_id: str, output: connect.ABCMessenger, table: str):
    """Updates the status tables for the extraction as it progresses
    """
    pass
    # where_clause = "tab_name = '{}'".format(table)
    # if not output.rows_exist('query_level_status', where_clause):
    #     return 0
    # else:
    #     # Update the current record with the new status
    #     output.fetch_data('SELECT SourceRecordCount FROM query_level_status WHERE {}'.format)


def _update_pause_resume_table_status(extract_id: str, output: connect.ABCMessenger,
                                      table: str, status: str = None):
    """Updates the status tables for the extraction as it progresses
    """
    where_clause = "tab_name = '{}'".format(table)
    if not output.rows_exist('query_level_status', where_clause):
        # Insert a new row for this table status
        data = ((extract_id, table, status),)
        output.insert_into('query_level_status', data)
    else:
        # Update the current record with the new status
        output.update_records(table='query_level_status',
                              columns=['status'],
                              updated_values=[status],
                              where_condition=where_clause)


def datatypes_from_metadata(metadata: list, result_type: connect.ABCMessenger):
    """Return a list of datatypes for a result create_table statement."""
    if result_type == MSSQLMessenger:
        datatypes = [column['mssql_datatype'] for column in metadata]
    else:
        datatypes = [column['sqlite_datatype'] for column in metadata]
    return datatypes


@redis_connection()
def update_progress(r, extract_key, increment_by):
    """ updates the stored progress values"""
    return r.incrby("extract:progress:{}".format(extract_key), increment_by)


def extract_from_ecf(ecf: str, source,  # type: DataStream
                     output: connect.ABCMessenger,
                     logfile: str = None, max_writers=1,
                     worker_timeout=30, chunk_results='one_db',
                     encrypted=True,
                     resume_extract=False,
                     extract_id: str = None,
                     gauge=None,
                     dockerPackageId=False)-> Extraction:
    """Perform one or more extractions from a PwC ECF file.

    ARGS:
        ecf: Local filepath of the ECF that defines this extraction.
        gauge: If provided, a wx.Gauge object to increment with each
            loop of the ECF.

    RETURNS:
        Extraction object used for the extraction, so the calling function
        has access to the filepath, rows_read, and similar attributes from
        that object.
    """
    errors = False
    warnings = False
    if not resume_extract:
        _create_new_temp_status_tracker(output)
    _create_pause_resume_table(output)

    #Raise an error if the database provided doesn't contain the required info to resume
    if resume_extract:
        _validate_pause_resume_db(output)
        LOGGER.info("Continuing extraction(s) using ECF file:  %s", ecf)
    else:
        LOGGER.info("Beginning extraction using ECF file:  %s", ecf)

    parsed_ecf_data = ecfreader.get_ecf_meta_data(ecf, encrypted)

    if not extract_id:
        # TODO -- should we use a UUID in this case instead?
        extract_id = parsed_ecf_data[0].request_id

    # If using a wx.Gauge (progress bar), set its range based on number
    # of queries / configs in the ECF file.
    if gauge:
        CallAfter(gauge.SetRange, len(parsed_ecf_data))

    #Valid pause resume information provided is correct prior to proceeding
    #Returns a modified list of ECF data for the tables/queries yet to be extracted
    if resume_extract:
        number_queries = len(parsed_ecf_data)
        LOGGER.info('Validating data from previous extraction')
        _validate_resumed_queries(source, output, parsed_ecf_data,
                                  chunk_results, extract_id)
        LOGGER.info('Current extraction status:')

        # Get should be order

        parsed_ecf_data = _manage_resumed_queries(output, parsed_ecf_data)

        # Put the resumed tables back in order.
        queries_skipped = number_queries - len(parsed_ecf_data)
    else:
        queries_skipped = 0

    # Convert ECF data into source-agnostic DataDefinitions
    datadefs = []

    if isinstance(source.messenger, ABAPMessenger):
        # If extracting for ABAP, ignore ECF 'Queries' section,
        # and just extract all the data available in the ABAP folder
        for table in source.messenger.list_all_tables():
            ecf_data = dummy_abap_ecf_data(parsed_ecf_data[0], table)
            datadefs += [utils.DataDefinition(parameters=table,
                                              source=source.messenger,
                                              ecf_data=ecf_data,
                                              output_table=table)]

            save_extract_data(output=output, ecf_data=ecf_data)
    else:
        # Otherwise, use the 'Queries' section provided in ECF,
        # and write the ECF data for each query into the output database
        LOGGER.debug("Creating table of metadata from ECF file")
        for ecf_data in parsed_ecf_data:
            try:

                datadef = utils.DataDefinition(parameters=ecf_data.query_text,
                                               source=source.messenger,
                                               ecf_data=ecf_data)

                _check_parent_error(data_definition=datadef,
                                    source=source.messenger,
                                    output=output)
                datadefs += [datadef]

                _update_pause_resume_table_status(extract_id=extract_id,
                                                  output=output,
                                                  table=ecf_data.table_alias,
                                                  status='not_started')
            except Exception as error:
                errors = True
                # An error we know about and raise explicitly occured.
                # Do not stop extracting, just fail for this table
                LOGGER.error(utils.cleanstr(str(error)))
                save_extract_data(output=output, ecf_data=ecf_data, error=True)

                #Log the table has an error in the status table
                _update_pause_resume_table_status(extract_id=extract_id,
                                                  output=output,
                                                  table=ecf_data.table_alias,
                                                  status='error')
            else:
                save_extract_data(output=output, ecf_data=ecf_data)

    # Create a shared Extraction class, then extract all data

    extraction = Extraction(source, output, logfile, max_writers,
                            worker_timeout, chunk_results,
                            extract_id=extract_id)

    if dockerPackageId:

        # Initially, increment progress by 10, out of the gate.
        update_progress(dockerPackageId, 10)

        # Total Number of queries
        total_no_queries = len(datadefs)

        # Increment By
        inc_by = math.floor(70/total_no_queries)

    for index, metadata in enumerate(datadefs):

        # If the extraction is paused, do not continue extractions
        if source.is_stopped():
            break

        # If using a gauge, set its progress to current position
        # If resuming from paused state, add offset for skipped queries
        if gauge:
            CallAfter(gauge.SetValue, index + queries_skipped)

        if dockerPackageId:
            # if total_progress + inc_by > 100:
            #     inc_by = 100 - total_progress
            update_progress(dockerPackageId, inc_by)
            # total_progress += inc_by

        try:
            _check_parent_error(data_definition=metadata,
                                source=source.messenger,
                                output=output)
            extraction.extract(metadata=metadata)
            warnings = extraction.warnings
            output.update_records(config.STATUS_TABLE, ['error_encountered'], ['False'],
                                  where_condition="[TableName] = '{}'".format(metadata.ecf_data.table_name))

        except Exception as error:
            # An error we know about and raise explicitly occured.
            # Do not stop extracting the ECF, just fail for this table

            rows_read_before_error = extraction.stream.rows_read
            errmsg = utils.cleanstr(str(error))
            LOGGER.error(errmsg)
            errors = True
            if "will be skipped" not in errmsg:
                output.update_records(config.STATUS_TABLE, ['SourceRecordCount'], [rows_read_before_error],
                                      where_condition="[TableName] = '{}'".format(metadata.ecf_data.table_name))
            output.update_records(config.STATUS_TABLE, ['error_encountered'], ['True'],
                                  where_condition="[TableName] = '{}'".format(metadata.ecf_data.table_name))
            _update_pause_resume_table_status(extract_id=extract_id,
                                              output=output,
                                              table=metadata.target_table,
                                              status='error')

    # TODO: need to integrate ABAP w/ these other technologies.
    if not isinstance(source.messenger, ABAPMessenger):
        clean_up_local_dbs(output)

    return extraction, errors, warnings


def _validate_resumed_queries(source,  # type: DataStream
                              output: connect.ABCMessenger,
                              parsed_ecf_data: ecfreader.ExtractData,
                              chunk_results: str,
                              extract_id: str):
    """Validate local pause/resume SQLite database before resuming.

    If any tables are marked as 'in progress' or 'complete', but local
    data no longer exists, they will be updated to 'not_started' so that
    they can get re-extracted.
    """
    for ecf_data in parsed_ecf_data:
        table = ecf_data.table_alias
        status = _get_table_status(output=output, table=table)

        if status in ['in_progress', 'error']:
            in_progess_table_validation(source, output, table, chunk_results)

        elif status == 'complete':
            if not valid_complete_table(output, table, chunk_results):
                _update_pause_resume_table_status(extract_id=extract_id,
                                                  output=output,
                                                  table=table,
                                                  status='not_started')


def _manage_resumed_queries(output: connect.ABCMessenger,
                            parsed_ecf_data: ecfreader.ExtractData) -> list:
    """Function to manage the local databases for resumed extractions
        if the extraction is non nesting it will remove the databse for the
        query which is in progress, if it is for nesting it will ensure the
        nesting database was provided so any potential nested tables will
        have a valid parent reference
    """
    data_to_extract = []
    for ecf_data in parsed_ecf_data:
        table = ecf_data.table_alias
        status = _get_table_status(output=output, table=table)

        if status == 'error':
            LOGGER.info('Table "%s" had an error and will be re-attempted', table)
            # data_to_extract.insert(0, ecf_data)
            data_to_extract.append(ecf_data)
        elif status == 'not_started':
            LOGGER.info('Table "%s" was not started and will be extracted', table)
            data_to_extract.append(ecf_data)
        elif status == 'in_progress':
            LOGGER.info('Table "%s" was in progress and will be resumed', table)
            # data_to_extract.insert(0, ecf_data)
            data_to_extract.append(ecf_data)
        elif status == 'complete':
            LOGGER.info('Table "%s" is already complete and will be skipped', table)


    if not data_to_extract:
        LOGGER.warning('No data left to extract based on pause/resume database')

    return data_to_extract


def valid_complete_table(output: connect.ABCMessenger, table: str,
                         chunk_results: str) -> bool:
    """Return True if a 'complete' table from pause/resume DB still exists."""
    assert chunk_results in ('db_per_table', 'one_db')
    if chunk_results == 'db_per_table':
        return sibling_database_exists(output, table)
    elif chunk_results == 'one_db':
        return output.table_exists(table)


def in_progess_table_validation(source,  # type: DataStream
                                output: connect.ABCMessenger, table: str,
                                chunk_results: str):
    """Validates that tables/databases with an in progess status are still
        present upon a resume
    """
    # TODO -- Refactor SAPMessenger typechecks to not use strings

    if chunk_results == 'db_per_table':
        if 'SAPMessenger' in str(type(source.messenger)):
            source.in_progress_tables.append(table)
        else:
            drop_local_database(output, table)

    elif chunk_results == 'one_db':
        if 'SAPMessenger' in str(type(source.messenger)):
            source.in_progress_tables.append(table)
        else:
            output.drop_table_if_exists(table)


def get_simple_text_metadata(columns: list) -> list:
    """Return simple metadata for auxillary text tables."""
    metadata = []  # type: List[Dict[str, str]]
    for col in columns:
        metadata += [{
            'targetFieldName': col,
            'sqlite_datatype': 'TEXT',
            'mssql_datatype': 'NVARCHAR(MAX)',
        }]
    return metadata


def _create_sqlite_messenger(output: SQLiteMessenger,
                             filename: str = None) -> SQLiteMessenger:
    """Return a new Messenger with an identical setup to the original,
    but at a different filename (database) in the same folder.
    """
    if filename:
        newname = "Encrypted_Content_{}.dat".format(filename)
    else:
        # Recreate a SQLiteMessenger with same name as the current one
        newname = os.path.basename(output.filepath).split('.')[0]

    folder = os.path.dirname(output.filepath)
    newpath = os.path.join(folder, newname)

    connection = SQLiteMessenger(is_zipped=output.is_zipped,
                                 password=output.password,
                                 aes256=output.aes256,
                                 filepath=newpath)
    return connection


def _validate_pause_resume_db(output: connect.ABCMessenger) -> None:
    """Validates the output database is from a previous extraction for a resume"""
    assert output.table_exists('query_level_status'), \
        'Please provide a valid resume database'


def _get_table_status(output: connect.ABCMessenger, table: str) -> str:
    """Return table status from a database for pause/resume feature."""
    statement = """
        SELECT status
        FROM 'query_level_status'
        WHERE tab_name = '{}'
        """.format(table)
    results = output.fetch_data(statement)
    # If there's data, and its valid, use it, otherwise 'not_started'
    if results and results[0]:
        if results[0][0]:
            status = results[0][0]
        else:
            status = 'in_progress'
    else:
        status = 'not_started'
    assert status in ('error', 'not_started', 'in_progress', 'complete')
    return status


def sibling_database_exists(output: SQLiteMessenger, table: str) -> bool:
    """Return True if a SQLite database for table exists alongside output."""
    folder = os.path.dirname(output.filepath)
    filename = "Encrypted_Content_{}.dat".format(table)
    filepath = os.path.join(folder, filename)
    return os.path.exists(filepath)


def drop_local_database(output: SQLiteMessenger, table: str):
    """Drop a local database if it still exists."""
    folder = os.path.dirname(output.filepath)
    filename = "Encrypted_Content_{}.dat".format(table)
    filepath = os.path.join(folder, filename)
    if os.path.exists(filepath):
        os.remove(filepath)


def _check_parent_error(data_definition: utils.DataDefinition,
                        source: connect.ABCMessenger,
                        output: connect.ABCMessenger):
    """Checks if a nested table's parent has errored out and will skip if so"""

    # TODO -- refactor this typecheck
    if 'SAPMessenger' in str(type(source)) \
        and isinstance(output, SQLiteMessenger):
        if data_definition.parameters['ParentSplitInfo']:
            for parent in data_definition.parameters['ParentSplitInfo']:
                assert _get_table_status(output, parent['ParentTableAlias']) != 'error', \
                    'Parent table {} encountered an error {} will be skipped' \
                    .format(parent['ParentTableAlias'], data_definition.target_table)


def _gui_parent_error_check(data_definition: utils.DataDefinition,
                            source: connect.ABCMessenger,
                            errored_tables: list) -> bool:
    """
    Only valid for nesting ECFs --

    Slimmed down version of the _check parent error function above
        doesn't rely on local databases, just a list that is in memory
    """
    if 'SAPMessenger' in str(type(source)):
        if data_definition.parameters['ParentSplitInfo']:
            for parent in data_definition.parameters['ParentSplitInfo']:
                assert parent['ParentTableAlias'] not in errored_tables, \
                    'Table will be skipped due to error in parent {}'\
                    .format(parent['ParentTableAlias'])


def gui_ecf_validation(ecf_meta_data: ecfreader.ExtractData,
                       messenger: connect.ABCMessenger):
    """Perfforms an initial validation via the GUI to inform the
        user which queries/tables within their ECF are invalid prior to
        starting an extraction.
    """
    errored_tables = []
    errors = []

    # Removing all validation for now.
    # for ecf_data in ecf_meta_data:
    #     try:
    #         pass
    #         datadef = utils.DataDefinition(parameters=ecf_data.query_text,
    #                                        source=messenger,
    #                                        ecf_data=ecf_data)
    #
    #         _gui_parent_error_check(data_definition=datadef,
    #                                 source=messenger,
    #                                 errored_tables=errored_tables)
    #
    #     except Exception as error:
    #         errored_tables += [ecf_data.table_alias]
    #         errors += [error]

    return errored_tables, errors


def update_total_record_counts(output: connect.ABCMessenger,
                               metadata: utils.DataDefinition,
                               rows_read: int):
    """Updated record counts in table extractions database for the source loader
        to reference
    """

    if not isinstance(output, SQLiteMessenger) or not metadata.ecf_data:
        return

    status_db = _create_sqlite_messenger(output, config.STATUS_TABLE)

    where_clause = "table_alias = '{}'".format(metadata.target_table)

    status_db.update_records(table=config.STATUS_TABLE,
                             columns=['SourceRecordCount'],
                             updated_values=[rows_read],
                             where_condition=where_clause)


def dummy_abap_ecf_data(original: ecfreader.ExtractData,
                        table_name: str) -> ecfreader.ExtractData:
    """Return dummy ExtractData based on an ABAP ECF and table name.
    Use the first entry in the ABAP ECF as a template.
    Blank any data that is unreliable for ABAP.
    """
    ecf_data = original
    ecf_data.schema_name = None
    ecf_data.table_name = table_name
    ecf_data.table_alias = table_name
    ecf_data.query_text = None
    return ecf_data


def write_chunk_summary_table(log_output: connect.ABCMessenger,
                              worker_messages: list = None,
                              table='_pyextract_chunk_log'):
    """Write summary of chunk databases to master result database."""
    fields = (
        'datetime_created', 'database_filename', 'database_schema',
        'targetTableName', 'number_records', 'process', 'error'
    )
    metadata = get_simple_text_metadata(fields)

    if not log_output.table_exists(table):
        columns = [column['targetFieldName'] for column in metadata]
        datatypes = datatypes_from_metadata(metadata, type(log_output))
        log_output.create_table(table, columns, datatypes)

    if not worker_messages:
        return  # No data to write

    formatted_data = tuple(
        tuple(str(item) for item in row) for row in worker_messages
    )
    log_output.insert_into(table, formatted_data, metadata)


if __name__ == '__main__':
    # On Windows calling this function is necessary for standalone EXEs
    multiprocessing.freeze_support()
