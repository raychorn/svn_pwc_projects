"""Classes to manage the flow of data from a SAP-based Messenger to a Queue."""

from datetime import datetime, timedelta
import itertools
import multiprocessing
import os
from queue import Empty as QueueEmpty
import re
import threading
import time

import pyrfc
import stopit

from ..connect import ABCMessenger, SQLiteMessenger
from ..connect.mssql import MSSQLMessenger
from ..connect.sap import SAPMessenger, where_clause_rfc_format
from ..saplogic import SAPKeyReports
from ..utils import NetworkDisconnectError
from .. import config


LOGGER = multiprocessing.get_logger()


class SAPStream(object):

    """Streams data from SAP into a Queue."""

    def __init__(self, messenger: ABCMessenger, queue_size=10,
                 row_limit: int = None, chunk_results='one_db',
                 batch_size=1000, chunk_size=150000, max_readers=4,
                 output: ABCMessenger = None,
                 stopevent: threading.Event=None,
                 in_progress_tables: list = []) -> None:
        """Instantiate an SAP data stream.

        ARGS:
            messenger: Messenger object that pulls data into the queue.
            queue_size: Maximum amount of calls that can be in the queue before a wait
            row_limit: Maximum number of rows to read before finishing
                an extraction. If None, will read all rows.
            chunk_results: Determines what method will be used to write data
                to the local SQLlite file there are currently three supported.
                    1) 'one_db' - all data will be written to one database file
                    2) 'db_per_table' - each query/table will write to it's own db
                    3) 'db_per_chunk' - each chunk of data returned will be written
                        to it's own database
            chunk_size: How many records to fetch from SAP at a time for raw
                table extractions
            output: Output SQLite messenger to store extraction metadata
            stopevent: If provided, an Event that can be .set() to signal
                an extraction should be paused/canceled.
            in_progress_tables: Tracks which tables were paused mid extraction

        ATTRIBUTES:
            logger: A logger shared across multiple processes.
            rows_read: A counter of the rows extracted for a given extraction
        """
        self.messenger = messenger
        self.queue_size = queue_size
        self.row_limit = row_limit
        self.chunk_results = chunk_results
        self.batch_size = batch_size
        self.chunk_size = chunk_size
        self.max_readers = max_readers
        self.output = output
        self.stopevent = stopevent
        self.in_progress_tables = in_progress_tables
        self.encountered_row_skips = False

        self.logger = multiprocessing.get_logger()

    def start(self, queue: multiprocessing.Queue, metadata=None):
        """Stream all data defined by metadata as chunks into the queue."""

        starttime = time.time()
        read_threads = []
        # self.rows_read = 0
        self.rows_read = self.output.get_rows_read_so_far(metadata.ecf_data.table_alias)
        if self.rows_read > 0:
            LOGGER.info("{} rows previously extracted.".format(self.rows_read))
        self.executed_calls = []
        self.write_queue_ = queue  # type: multiprocessing.Queue
        self.metadata_ = metadata

        #Set variables for the creation of the nesting logic
        ecf_info = metadata.ecf_data.ecfjson
        table_split_info = metadata.parameters

        if table_split_info['NameAlias'] in self.in_progress_tables:
            self.executed_calls = get_executed_sap_calls(self.output,
                                                         table_split_info['NameAlias'])

        if metadata.query_type.upper() == 'KEY_REPORT':
            #Temp work aroudn to call the key report
            key_rep = SAPKeyReports(self.messenger, queue, self.stopevent,
                                    self.in_progress_tables)
            key_rep.run_report(metadata)
            self.rows_read += key_rep.rows_read
            self.write_queue_.put((None,))
            return

        template = create_extraction_template(table_split_info, ecf_info,
                                              self.batch_size, self.chunk_size)
        LOGGER.debug('Template for extraction: {}'.format(template))

        if 'parent_split_info' not in template.keys():
            LOGGER.debug('Solo or header table detected, using simple generator')
            gen = rfc_single_read_table_generator(template)
        else:
            LOGGER.debug('Nested table detected, using local database {}'
                         .format(self.output))
            local_db_info = (self.chunk_results, self.output)
            gen = nested_extraction_generator(template, local_db_info, batch_size=self.batch_size)

        # Create a thread-safe generator for SAP RFC where clases
        self.yield_where_clauses = ThreadSafeIter(gen)

        # Create a queue for threads to write errors to and reference later
        error_queue = multiprocessing.Queue()

        LOGGER.debug('Creating %d threads to read data from SAP', self.max_readers)
        for _ in range(self.max_readers):
            kwargs = {'error_queue': error_queue}
            thread = threading.Thread(target=self.threaded_extract, kwargs=kwargs)
            read_threads.append(thread)

        # Start threads to extract and place data in queue
        for thread in read_threads:
            thread.daemon = True
            thread.start()

        # Force Threads to wait until nested tables (avoids nesting conflicts)
        for thread in read_threads:
            thread.join()

        # If an error occurred in a thread, raise it to parent processes
        try:
            raise error_queue.get(timeout=1)
        except QueueEmpty:
            pass

        # Wait until no data is left to write before moving to next table
        self.wait_for_writers()


        endtime = time.time()
        self.logger.info("Finished reading {:,} rows in {:.2f} seconds."
                         .format(self.rows_read, (endtime - starttime)))

        # Delete the list of executed where calls from memory
        try:
            del self.executed_calls
        except AttributeError:
            pass

        return self.encountered_row_skips

    def threaded_extract(self, error_queue: multiprocessing.Queue):
        """Run an SAP RFC extraction from within a Thread.
        Place returned data in a write_queue, and place any errors
        in an error_queue, so they can be access by the main Process.
        """
        # Use a temporary messenger for each threaded SAP call
        try:
            if self.metadata_.function_module:
                temp_msgr = SAPMessenger(
                    logon_details=self.messenger.logon_details_,
                    connection_type=self.messenger.connection_type,
                    function_module=self.metadata_.function_module,
                    package_size=self.chunk_size,
                )
            else:
                temp_msgr = SAPMessenger(
                    logon_details=self.messenger.logon_details_,
                    connection_type=self.messenger.connection_type,
                    package_size=self.chunk_size,
                )
        except pyrfc.CommunicationError:
            # Create a nicer, generic error to display to user
            error = NetworkDisconnectError('Network connection to SAP has been lost.')
            error_queue.put(error, timeout=1)
            return

        while True:
            if self.is_stopped():
                self.logger.info("Extraction stopped by user after {:,} rows."
                                 .format(self.rows_read))
                break

            if self.row_limit_reached():
                self.logger.info("Finished reading {:,} of {:,} maximum rows."
                                 .format(self.rows_read, self.row_limit))
                break

            if self.queue_limit_reached():
                self.logger.debug("Queue for storing data to write is full. "
                                  "Checking again in 5 seconds.")
                time.sleep(5)
                continue

            # Get the next where clause to read, stop reading if none
            try:
                query_info = next(self.yield_where_clauses)
            except StopIteration:
                LOGGER.debug('Where clause generator exhausted, ending read')
                break
            except Exception as error:
                # If unexpected error occurs, raise it to main process
                error_queue.put(error, timeout=1)
                break

            # Checks if the where clause should be checked
            if self.executed_calls:
                try:
                    self.executed_calls.remove(str(query_info[2]))
                    continue
                except ValueError:
                    pass

            start_row = 0
            # Read data from SAP, report error back to main if needed
            try:
                data = self.sap_messenger_call(temp_msgr, query_info=query_info)
            except Exception as error:
                error_queue.put(error, timeout=1)
                break

            if data and not self.row_limit_reached():
                trimmed_data = self.trim_data(data)
                record_count = len(trimmed_data)
                self.rows_read += record_count
                start_row += record_count
                self.write_queue_.put((True, trimmed_data, self.metadata_,
                                       query_info[2]))
                #Check if we are row skipping and address if so
                if record_count >= self.chunk_size:
                    self.encountered_row_skips = True
                    self.logger.warn("Beginning row skips which could result in an incomplete extraction, "
                                     "particularly if this is a transactional table.  To avoid row skips, the "
                                     "'SAP Rows per request' configuration would need to be "
                                     "increased (currently {:,}).  Avoid setting this option larger than 5M"
                                     "without first contacting a member of PwC Extract Support Team."
                                     .format(record_count, self.chunk_size))
                    self.row_skipping_handler(temp_msgr, query_info, start_row=start_row)
            else:
                # This is to handle SAP calls the return 0 rows
                # They have to be put in the queue to be logged to avoid
                # making duplicate calls to SAP upon a resume
                self.write_queue_.put((True, [], self.metadata_,
                                       query_info[2]))

        temp_msgr._conn.close()

    def trim_data(self, data: list) -> list:
        """Return data to fit the row limit (if applicable)."""
        if self.row_limit:
            rows_left = self.row_limit - self.rows_read
            if len(data) > rows_left:
                data = data[:rows_left]
        return data

    def row_limit_reached(self) -> bool:
        """Return True if the maximum number of rows have been read."""
        return bool(self.row_limit and self.rows_read >= self.row_limit)

    def queue_limit_reached(self) -> bool:
        """Return True if the queue to put data in is full."""
        return bool(self.write_queue_.qsize() >= self.queue_size)

    def is_stopped(self) -> bool:
        """Return True if there is a stop event"""
        return bool(self.stopevent and self.stopevent.is_set())

    def wait_for_writers(self):
        """Wait while writer threads finish writing header data.
        This avoids skipping any header rows when nesting starts.
        """
        time.sleep(3)
        while self.write_queue_.qsize() > 0:
            time.sleep(5)

    def row_skipping_handler(self, temp_msgr: ABCMessenger, query_info: tuple, start_row=0):
        """If row skipping will continue to call SAP incrementing up in rows
            until the calls are no longer maxing out the threshold
        """
        first_run = True

        while first_run or len(data) >= self.chunk_size:
            if self.is_stopped():
                self.logger.info("Extraction stopped by user after {:,} rows."
                                 .format(self.rows_read))
                break
            first_run = False

            data = self.sap_messenger_call(temp_msgr, query_info, skipping=True, start_row=start_row)

            trimmed_data = self.trim_data(data)
            cnt = len(trimmed_data)
            self.rows_read += cnt
            start_row += cnt
            self.write_queue_.put((True, trimmed_data, self.metadata_,
                                   query_info[2]))

    def sap_messenger_call(self, temp_msgr: ABCMessenger, query_info: tuple,
                           skipping=False, start_row=0):
        """Handles try except behavior for calls to SAP Messenger"""

        try:
            with stopit.ThreadingTimeout(config.QUERY_TIMEOUT, swallow_exc=False):
                kwargs = {
                    'table': query_info[1],
                    'columns': query_info[0],
                    'where': query_info[2],
                    'package_size': self.chunk_size
                }
                if skipping:
                    kwargs['from_row'] = start_row
                return temp_msgr.single_readtable(**kwargs)
        except pyrfc.CommunicationError:
            # Create a nicer, generic error to display to user
            raise NetworkDisconnectError('Network connection to SAP has been lost')
        except Exception as error:
            # self.write_queue_.put((False, str(error), self.metadata_))
            self.logger.error("Query failed due to error: {}".format(error))
            raise


class ThreadSafeIter(object):
    """Takes an iterator/generator and makes it thread-safe by
    serializing call to the `next` method of given iterator/generator.
    """
    def __init__(self, generator):
        self.generator = generator
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return next(self.generator)


###############################################################################
### Functions in this section are used to generate the where clauses
### for SAP calls
###############################################################################

def rfc_single_read_table_generator(table: dict, where_clause: list = None):
    """
        Pulls data from SAP either via a chunk pull or by splitting
        for header tables in an n nested extraction:

        table(dict): Dictonary containing the information required to split/limit
            calls to SAP per the ECF
        where_clause(list): if the table is nested this is the parent's where clause
            to be used in the generation the nested child's where clause split

    """
    LOGGER.debug('Initialize rfc_single_read_table_generator: table: {}'.format(table))
    LOGGER.debug('Initialize rfc_single_read_table_generator: where_clause: {}'.format(where_clause))

    filter_fields, filter_criteria = for_loop_where(table['for_loop_params'])
    for for_item in filter_criteria:
        #iterate over the date range generator
        for date_split in date_range_loop(table['date_range_param']):
            #iterate over the non date range param
            for non_dt_split in non_date_range_loop(table['non_date_range_params']):
                # Make copy of for loop for split calls to SAP
                if where_clause:
                    temp_where = where_clause.copy()
                else:
                    temp_where = list()
                try:
                    temp_where.extend(table['where_clause_list'].copy())
                except AttributeError:
                    continue

                if for_item != ' ':
                    #this iterates through the for loop criteria
                    #and matches the field to the column as we iterate
                    #through the permutations of the various for loops
                    for index, query_point in enumerate(filter_fields):
                        condition = "{} = '{}'".format(query_point, for_item[index])
                        temp_where.append(condition)

                # check if there is a value to be used in the sap call
                if date_split != ' ':
                    temp_where.append(date_split)
                if non_dt_split != ' ':
                    temp_where.append(non_dt_split)

                sap_where_clause = where_clause_rfc_format(temp_where)

                if not sap_where_clause:
                    sap_where_clause = []

                yield (table['columns'], table['tablename'], sap_where_clause)


def nested_extraction_generator(table, local_db_info, between_nest=False, batch_size=1000):
    """internal method to coorindate the nested extractions
        will identify if a table has more than one parent and consolidate
        header docs accordingly

        args:
            table(dict): the attributes to be used while curating the
                calls to SAP
            local_db_info(tuple): Tuple of the output and chunk method
            between_nest (bool): True will result in a between being used
                while nesting, False will use an IN
    """

    # Section is for organizing splits from parent and calling local
    # sqllite DB for nesting info
    LOGGER.debug('Initialize nested_extraction_generator: table: {}'.format(table))
    LOGGER.debug('Initialize nested_extraction_generator: local_db_info: {}'.format(local_db_info))
    LOGGER.debug('Initialize nested_extraction_generator: between_nest: {}'.format(between_nest))

    #Identify if child has one or more parents
    if len(table['parent_split_info']) == 1:
        parent_split_info = table['parent_split_info'][0]
        parent_param_gen = rfc_single_read_table_generator(parent_split_info)
        for parent_query in parent_param_gen:
            LOGGER.debug('Parent query parameters: {}'.format(parent_query))
            #par_where is a tuple of columns, table and where clause
            #Join incase the parent where splits across more than one line
            if parent_query[2]:
                par_where = [' '.join(parent_query[2])]
            else:
                par_where = parent_query[2]

            #Get info from template to collect from local SQL DB
            local_db_field = parent_split_info['local_db_field']['ParentField']
            from_table_parent = parent_split_info['tablealias']

            #Returns a messenger connected to the appropriate local DB
            local_db_conn = get_local_db_conn(local_db_info, from_table_parent)

            #Returns a list of docuements the child will split on from parent
            header_docs = get_header_split_docs(local_db_field=local_db_field,
                                                from_table_parent=from_table_parent,
                                                db_connection=local_db_conn,
                                                temp_where=par_where,)
            LOGGER.debug('Parsed header docs from parent table: {}'
                         .format(header_docs))

            #Get information required to modify the where statement
            #if the child and parent tables have different col names
            par_field_split_info = parent_split_info['field_mapping']
            modify_nest_where_for_cols(par_field_split_info=par_field_split_info,
                                       where_clause=par_where)

            #Get field to be used for the IN or BETWEEN calls from SAP
            sap_split_field = parent_split_info['local_db_field']['ChildField']

            #create the child generator for
            child_param_gen = rfc_single_read_table_generator(table, par_where)
            for item in child_param_gen:
                LOGGER.debug('Child query parameters: {}'.format(item))
                where = [' '.join(item[2])]
                if between_nest:
                    c_gen = between_nest_splits(table, header_docs, sap_split_field,
                                                where, batch_size=batch_size)
                else:
                    c_gen = in_nest_splits(table, header_docs, sap_split_field, where, batch_size=batch_size)
                while True:
                    try:
                        yield next(c_gen)
                    except StopIteration:
                        break

    #Child has more than one parent
    else:
        joint_header_docs = []
        sap_split_field = set()

        for parent_table in table['parent_split_info']:
        #Get info from template to collect from local SQL DB
            local_db_field = parent_table['local_db_field']['ParentField']
            from_table_parent = parent_table['tablealias']

            #Returns a messenger connected to the appropriate local DB
            local_db_conn = get_local_db_conn(local_db_info, from_table_parent)
            #Returns a list of docuements the child will split on from parent
            header_docs = get_header_split_docs(local_db_field=local_db_field,
                                                from_table_parent=from_table_parent,
                                                db_connection=local_db_conn)

            #retreive the parent docs from the local db
            joint_header_docs.extend(header_docs)

            #ID the SAP field to be used for splitting
            sap_split_field.add(parent_table['local_db_field']['ChildField'])

        assert len(sap_split_field) == 1, (
            'Local SAP field "{}" not consistant between parents'
            ).format(sap_split_field)

        sap_split_field = sap_split_field.pop()

        #create the child generator for
        child_param_gen = rfc_single_read_table_generator(table)

        for item in child_param_gen:
            where = item[2]
            if between_nest:
                c_gen = between_nest_splits(table, header_docs, sap_split_field,
                                            where, batch_size=batch_size)
            else:
                c_gen = in_nest_splits(table, header_docs, sap_split_field, where, batch_size=batch_size)
            while True:
                try:
                    yield next(c_gen)
                except StopIteration:
                    break


def between_nest_splits(table, header_doc_list, nested_field,
                        pre_where_clause=None, batch_size=1000):
    """function to break out the generation of BETWEEN calls of SAP nested
        table call"""
    for chunk in _between_nest(header_doc_list=header_doc_list,
                               split_field=nested_field, batch_size=batch_size):
        temp_where = pre_where_clause.copy()
        temp_where.append(chunk)
        where_clause = where_clause_rfc_format(temp_where)

        # make sure it's formatted for SAP if no params
        # prob
        if not where_clause:
            where_clause = ''
        yield (table['columns'], table['tablename'], where_clause)


def in_nest_splits(table, header_doc_list, nested_field, pre_where_clause=None, batch_size=1000):
    """function to break out the generation of IN calls of SAP nested
        table call"""
    LOGGER.debug("Initializing in_nest_splits: table: {}".format(table))
    LOGGER.debug("Initializing in_nest_splits: header_doct_list: {}".format(header_doc_list))
    LOGGER.debug("Initializing in_nest_splits: nested_field: {}".format(nested_field))
    LOGGER.debug("Initializing in_nest_splits: pre_where_clause: {}".format(pre_where_clause))

    for chunk in _in_nest(header_doc_list=header_doc_list, split_field=nested_field, batch_size=batch_size):
        temp_where = pre_where_clause.copy()
        temp_where.append(chunk)
        where_clause = where_clause_rfc_format(temp_where)
        if not where_clause:
            where_clause = ''

        yield (table['columns'], table['tablename'], where_clause)


def _between_nest(header_doc_list, split_field, batch_size=1000):

    """Internal method to extract data from SAP based upon a BETWEEN call."""
    index = 0
    current_batch = []
    batched_docs = []

    LOGGER.debug("Initializing _between_nest: header_doc_list: {}".format(header_doc_list))
    LOGGER.debug("Initializing _between_nest: split_field: {}".format(split_field))
    LOGGER.debug("Initializing _between_nest: batch_size: {}".format(batch_size))

    threshold = batch_size + 3

    while len(header_doc_list) > index:
        # strip zeros from doc num while keeping it a str
        doc_number = header_doc_list[index].replace('0', '')
        if not current_batch:
            current_batch.append(doc_number)
            index += 1
        elif int(doc_number) <= int(current_batch[0]) + threshold:
            current_batch.append(doc_number)
            if len(header_doc_list) == index + 1:
                yield _between_nest_where_cause(current_batch, split_field)
            index += 1
        else:
            # Save batch of documents to be used in where
            batched_docs.append(current_batch)
            yield _between_nest_where_cause(current_batch, split_field)

            current_batch = []


def _between_nest_where_cause(current_batch, split_field):
    """returns the where clause for the generator"""

    if len(current_batch) == 1:
        return "{} = '{}'".format(split_field, current_batch[0])
    else:
        low_doc = min(current_batch)
        high_doc = max(current_batch)
        return "{} BETWEEN '{}' AND '{}'".format(split_field,
                                                 low_doc,
                                                 high_doc)


def _in_nest(header_doc_list, split_field, batch_size=1000):
    # print("DEBUG: batch_size {}".format(batch_size))
    """
        internal method to extract data from SAP based upon an IN call
        will yield a string of the IN clause
    """
    LOGGER.debug('Initialize _in_nest: header_doc_list: {}'.format(header_doc_list))
    LOGGER.debug('Initialize _in_nest: split_field: {}'.format(split_field))
    LOGGER.debug('Initialize _in_nest: batch_size: {}'.format(batch_size))

    for i in range(0, len(header_doc_list), batch_size):
        yield "{} IN ('".format(split_field) +                \
              "','".join(header_doc_list[i:i + batch_size]) + "')"


def for_loop_where(for_loop_params: dict):
    """ method which creats a generator for all criteria which require a
        for loop to split on
    """
    # check if the table has no parameters otherwise return empty values
    try:
        if not for_loop_params:
            return ([' '], [' '])
    except AttributeError:
        return ([' '], [' '])

    fields = []
    values = []
    for criteria in for_loop_params:
        fields.append(criteria['Name'])
        values.append(criteria['Values'])

    # creates generator of all possible for loop combinations
    return fields, itertools.product(*values)


def date_range_loop(date_range_param: dict):
    """method only handles one date parameter for a table
        returns list of where clauses to be included in the SAP calls
    """
    # catch if the table has no parameters
    try:
        if not date_range_param:
            return [' ']
    except AttributeError:
        return [' ']

    assert len(date_range_param) <= 1, (
        'Can not use more than one date-range parameter in an ECF. Found {}'
        ).format(date_range_param)

    # If date is ISO 8601 (YYYY-MM-DD), strip away hyphens so it's SAP format
    start_date = date_range_param[0]['Values'][0].replace('-', '')
    end_date = date_range_param[0]['Values'][1].replace('-', '')

    # If dates are not valid SAP dates (YYYYMMDD), tell user to make a new ECF
    try:
        datetime.strptime(start_date, '%Y%m%d')
        datetime.strptime(end_date, '%Y%m%d')
    except ValueError:
        msg = (
            'Invalid SAP date range format in ECF: {}. '
            'Date values must be in the format "YYYYMMDD".'
            ).format([start_date, end_date])
        raise AssertionError(msg)

    field_name = date_range_param[0]['Name']
    loop = (
        "{} = '{}'".format(field_name, x)
        for x in get_date_ranges(start_date, end_date)
    )
    return loop


def non_date_range_loop(non_date_params: dict):
    """only handles one non_date range param returns list of
    where clauses to be included while compiling SAP calls"""
    try:
        if not non_date_params:
            return [' ']
    except AttributeError:
        return [' ']

    assert len(non_date_params) <= 1, (
        'Can not use more than one NON date-range parameter in an ECF. Found {}'
        ).format(non_date_params)

    start = non_date_params[0]['Values'][0]
    end = non_date_params[0]['Values'][1]
    field_name = non_date_params[0]['Name']
    return ("'{} = '{}'".format(x, field_name) for x in range(start, end + 1))


def get_date_ranges(start_date,
                    end_date,
                    date_in_format="%Y%m%d",
                    date_out_format="%Y%m%d"):
    """
    Generates a list containing days between start and end date
     Parameters:   start_date(str): Start of date range
                   end_date(str): End of date range
                   date_in_format(str):Date format of start and end dates %Y / %m / %d
                   date_out_format(str):Date format of output range %Y / %m / %d
     Returns:      List containing a string date value for each day in the date range
    """

    # Create date time objects for date math
    start = datetime.strptime(start_date, date_in_format)
    end = datetime.strptime(end_date, date_in_format)

    return [(start + timedelta(days=x)).strftime(date_out_format)
            for x in range(0, (end - start).days + 1)]


def select_parse(statement):
    """
    Parses a SQL statement into a format SAP's BPP module can raed to extract

       Arguments:
          statement (str): SQL statement to be used
    """
    statement = " ".join([x.strip('\t')
                          for x in statement.upper().split('\n')])

    if 'WHERE' not in statement:
        statement = statement + ' WHERE '

    regex = re.compile("SELECT(.*)FROM(.*)WHERE(.*)")

    parts = regex.findall(statement)
    parts = parts[0]
    select = [x.strip() for x in parts[0].split(',')]
    frm = parts[1].strip()
    where = parts[2].strip()

    # format for RFC_READ_TABLE
    pattern = re.compile(r"""((?:[^'"]|'[^']*'|"[^"]*")+)""")
    where = pattern.split(where)[1::2]
    cleaned = [select, frm, where]
    return cleaned


def get_split_values_from_parent(table):
    """ method which creats a generator for all criteria which require a
        for loop to split on
    """

    if not table.parent_split_info:
        return ['']

    fields = []
    values = []
    for param in table.parent_split_info:
        if param['UseParentFilter']:
            fields.append(param['Name'])
            values.append(param['Values'])

    # creates generator of all possible for loop combinations
    return fields, itertools.product(*values)


def get_header_split_docs(local_db_field, from_table_parent,
                          db_connection, temp_where=None):
    """funtion to curate query to make local DB call for
        the split informaton along with the field to be used
        when making calls to SAP for the nested table
    """

    header_docs = []

    if hasattr(db_connection, 'schema'):
        from_table_parent = \
            "[{}].[{}]".format(db_connection.schema, from_table_parent)

    if temp_where:
        where_part = ''.join(temp_where)
        query = (
            """SELECT DISTINCT {0} FROM {1} WHERE {2} ORDER BY {0}"""
            .format(local_db_field, from_table_parent, where_part)
        )
    else:
        query = (
            """SELECT DISTINCT {0} FROM {1} ORDER BY {0}"""
            .format(local_db_field, from_table_parent)
        )

    res = db_connection.fetch_data(query)

    if isinstance(db_connection, SQLiteMessenger):
        db_connection._conn.close()

    header_docs.extend([doc for sm in res for doc in sm])

    return sorted(header_docs)


def get_local_db_conn(local_db_info, parent_table_alias):
    """function which returns a messenger connected to the appropriate local
        sqlite DB required for nesting

        args:
            local_db_info(tuple): Tuple with the base messenger and chunk method
            parent_table_alias(str): Name of the parent table to assist in identifying
                the local DB
    """
    chunk_method, output = local_db_info

    #Check if all results are in one database, return that database
    if chunk_method == 'one_db':
        if isinstance(output, SQLiteMessenger):
            return SQLiteMessenger(is_zipped=output.is_zipped,
                                   password=output.password,
                                   aes256=output.aes256,
                                   filepath=output.filepath)
        else:
            return output
    #If each table has it's own DB, iterate through the directory to find right DB
    elif chunk_method in 'db_per_table':
        local_db_dir = os.path.dirname(output.filepath)
        local_dbs = [database for database in os.listdir(local_db_dir)
                     if database.endswith("{}.dat".format(parent_table_alias))]
        #Ensure the local file doesn't have more than one potential parent DB
        assert len(local_dbs) == 1, 'Not a one to one relationship found \
                                         between parent and local database files'
        #Establish a connection to the local database for nesting calls
        folder = os.path.dirname(output.filepath)
        db_name = os.path.join(folder, local_dbs[0])
        return SQLiteMessenger(is_zipped=output.is_zipped,
                               password=output.password,
                               aes256=output.aes256,
                               filepath=db_name)
    #SAP does not currently support anything but the two methods above for nesting
    else:
        raise NotImplementedError('Not supported for SAP currently')
        #local_nesting_db_controller(self.local_db, self.metadata_)


def modify_nest_where_for_cols(par_field_split_info, where_clause):
    """replace field names if the child col doesn't match the parent col"""

    #iterate through the parent field field mapping to determine if names differ
    for split_params in par_field_split_info:
        parent_field_name = split_params['ParentField']
        child_field_name = split_params['ChildField']
        #if so replace the string w/ the child field mapping
        if parent_field_name != child_field_name:
            for row, clause in enumerate(where_clause):
                if split_params['ParentField'] in clause:
                    where_clause[row] = clause.replace(parent_field_name,
                                                       child_field_name)


###############################################################################
### Functions in this section are associated with creating the Extraction
### template object which will be used to generate where clauses for the
### threaded SAP reads
###############################################################################


def create_extraction_template(table_info: dict, ecf_info: dict,
                               batch_size=1000, chunk_size=50000) -> dict:
    """Combine table-level data and ECF data into a template for streaming."""

    template = {
        'tablename': table_info['Name'],
        'tablealias': table_info['NameAlias'],
        'text': table_info,
        'columns': [col.strip() for col in table_info['Columns']],
        'local_sql_where': table_info['LocalDBWhereClause'],
    }

    if table_info.get('Meta'):
        template['batch_size'] = table_info['Meta']['BatchSize']
        template['chunk_size'] = table_info['Meta']['ChunkSize']
    else:
        template['batch_size'] = batch_size
        template['chunk_size'] = chunk_size

    #Partitions parameters into various groups for logical extraction splitting
    if table_info['Parameters']:
        format_params(table_info['Parameters'], template)
    else:
        #Create empty parameters
        shell_params(template)

    if table_info['WhereClause']:
        #if there is a where clause create a list attribute around the strings
        #will collect additional information as the splits are made later
        template['where_clause_list'] += [table_info['WhereClause']]

    if table_info['ParentSplitInfo']:
        template['parent_split_info'] = []
        #Iterate through the parent tables to be used to split
        get_parent_split_parameters(template, table_info, ecf_info)

    return template


def format_params(params, temp_template):
    """function which parses a tables filtering/splitting
        criteria into a number of buckets for further processing once an
        extractioni is started
    """
    # allows for inequalities on the params
    operations = ['>', '<', '>=', '<=', '<>', '=']

    temp_template['where_clause_list'] = []
    temp_template['for_loop_params'] = []
    temp_template['date_range_param'] = []
    temp_template['non_date_range_params'] = []

    for param in params:

        if param['Operation'] == 'BETWEEN':
            validate_between_split_info(param, temp_template)

        elif param['Operation'] == 'RANGE':
            validate_range_split_info(param, temp_template)

        # check if it's criteria to be looped through for splitting
        elif len(param['Values']) > 1 and param['Operation'] == 'LOOP':
            temp_template['for_loop_params'].append(param)

        # Check if it's a single limiting criteria
        elif len(param['Values']) == 1 and param['Operation'] == 'IN':
            temp_template['where_clause_list'].append("{} = '{}'".format(param['Name'],
                                                                         param['Values'][0]))

        #Departure from 1.6 this is will be assessed as an IN
        elif len(param['Values']) > 1 and param['Operation'] == 'IN':
            in_operation_handler(param, temp_template)

        elif (len(param['Values']) == 1) and (param['Operation'] in operations):
            where_clause = "{} {} '{}'".format(param['Name'],
                                               param['Operation'],
                                               param['Values'][0])
            temp_template['where_clause_list'].append(where_clause)
        else:
            raise AssertionError(
                'Not a valid ECF parameter config: {}'.format(param)
                )


def get_parent_split_parameters(temp_template, table_info, ecf_info):
    """Function to collect information from the parent template
        relevant for local DB calls to generate splits for SAP as
        well as collecting filters from the parent which should
        be applied to the child
    """

    parent_split_info = table_info['ParentSplitInfo']

    for parent in parent_split_info:
        collect_validate_parent_info(temp_template, parent, ecf_info)


def collect_validate_parent_info(temp_template, parent_table, ecf_info):
    """collects and formats parent split information for nested extraction"""

    #find appropriate parent_ecf
    for table in ecf_info['Queries']:
        if parent_table['ParentTableAlias'] == table['NameAlias']:
            parent_ecf_info = table

            #validate the links are valid
            validate_nesting_links(parent_table, parent_ecf_info)

            #collect parent split info
            get_parent_split_info(temp_template, parent_ecf_info,
                                  parent_table)


def validate_nesting_links(parent_split_info, parent_ecf_info):
    """checks if nested table has a valid link field"""

    for link in parent_split_info['SplitFields']:
        assert link['ParentField'] in parent_ecf_info['Columns'], \
            'Parent link field not in parent columns to be extracted'


def get_parent_split_info(temp_template, parent_ecf_info, parent_table):
    """Collect the split information from the parent table in the ECF
        create a subdictionary with that information to be used for the
        ordering of the splitting during the extraction
    """

    # Aggregate all parameters and splitting info from the ECF into two lists
    parent_parameters_split = []
    select_fields = []

    for split_param in parent_table['SplitFields']:
        # If parameter doesn't use parent table, just select on those
        if not split_param['UseParentFilter']:
            select_fields.append(split_param)
            continue

        # Otherwise, make sure the parent has Parameters to match with
        assert parent_ecf_info['Parameters'], (
            'Must provide "Parameters" for parent table if '
            'the child table selects "UseParentFilter"'
            )
        # Add parent parameters based on keys in this parameter template
        for parent_param in parent_ecf_info['Parameters']:
            if split_param['ParentField'] == parent_param['Name']:
                parent_parameters_split.append(parent_param)

    assert len(select_fields) == 1, (
        'ECF must contain exactly one non-link filter in '
        'the "Parent Split Info" section. Found: {}'
        ).format(select_fields)

    temp_template_parent = {
        'columns': [],
        'tablename': parent_table['ParentTableName'],
        'tablealias': parent_table['ParentTableAlias'],
        'field_mapping': parent_table['SplitFields'],
        'local_db_field': select_fields[0],
    }

    format_params(parent_parameters_split, temp_template_parent)

    temp_template['parent_split_info'].append(temp_template_parent)


def shell_params(temp_template):
    """function to populate empty parameters if the nested table doesn't use
            parameters other than the parent
    """
    temp_template['where_clause_list'] = []
    temp_template['for_loop_params'] = []
    temp_template['date_range_param'] = []
    temp_template['non_date_range_params'] = []


def validate_between_split_info(param, temp_template):
    """Validates information provided by ECF for BETWEEN calls is valid"""

    assert len(param['Values']) == 2, (
        'Too many values for parameter in ECF. '
        'BETWEEN operation requires exactly two values'
        )

    if param['Type'] == 'date':
        temp_template['date_range_param'].append(param)
    else:
        where_clause = (
            "{0} BETWEEN '{1}' AND '{2}'"
            ).format(param['Name'], param['Values'][0], param['Values'][1])
        temp_template['where_clause_list'].append(where_clause)


def validate_range_split_info(param, temp_template):
    """Validates RANGE parameters provided are valid"""
    # Check if it's a date
    if param['Type'] == 'date' and len(param['Values']) == 2:
        temp_template['date_range_param'].append(param)

    #Throw an error if there are not two dates to split on
    elif param['Type'] == 'date' and len(param['Values']) != 2:
        raise AssertionError('Date range clause requires exactly two dates')

    # Check if it's a non-date range (e.g. document line item)
    elif param['Type'] != 'date' and len(param['Values']) == 2:
        temp_template['non_date_range_params'].append(param)

    #Throw an error if there aren't two values to generate between
    elif param['Type'] == 'date' and len(param['Values']) != 2:
        raise AssertionError('Range clause requires exactly two values')


def in_operation_handler(param, temp_template):
    """
    Identifies the correct formatting of the filter based upon the ECF criteria
        for IN operations with more than one value
    """
    try:
        if param['Interpretation'] == 'STRICT':
            temp_template['where_clause_list'].append("{} IN ('{}')".format(param['Name'],
                                                                            "', '".join(param['Values'])))
        elif param['Interpretation'] == 'ITER':
            temp_template['for_loop_params'].append(param)

    except KeyError:
        temp_template['where_clause_list'].append("{} IN ('{}')".format(param['Name'],
                                                                        "', '".join(param['Values'])))


def local_nesting_db_controller(sqlite_db, metadata):
    """function to create a new table within the local DB if necessary"""
    if not sqlite_db.table_exists(metadata.target_table):
        columns = [column['targetFieldName'] for column in metadata.columns]
        datatypes = ['TEXT'] * len(metadata.columns)
        sqlite_db.create_table(metadata.target_table, columns, datatypes)


def get_in_progress_tabs(status_db: SQLiteMessenger) -> list:
    """Returns a list of items which were noted as in process on a pause"""
    statement = """
                SELECT tab_name
                FROM 'query_level_status'
                WHERE status = 'in_progress'
                """

    results = status_db.fetch_data(statement)

    assert len(results) <= 1, ('Invalid resume data, cannot have more than \
                               one_db table in progress')

    return [row[0] for row in results]


def get_executed_sap_calls(status_db: SQLiteMessenger, resuming_table: str) -> list:
    """Returns the calls which have already been made to SAP for resumed tables"""
    statement = """
                SELECT query_text
                FROM 'temp_tracker'
                WHERE tab_name = '{}'
                """.format(resuming_table)

    results = status_db.fetch_data(statement)

    return [row[0] for row in results]
