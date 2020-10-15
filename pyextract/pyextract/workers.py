"""Houses the logic for the write workers"""

# import logging
import multiprocessing
import os
import sys
import time
from queue import Empty as QueueEmpty
import psutil

import apsw

from . import core
from . import utils
from .connect import ABCMessenger, SQLiteMessenger
from .connect.mssql import MSSQLMessenger
from .connect.mysql import MySQLMessenger
import os

if os.name == 'nt':

    import multiprocessing.popen_spawn_win32 as forking

    class _Popen(forking.Popen):
        """Windows-forking-safe Process (http://stackoverflow.com/a/27694505)."""
        def __init__(self, *args, **kwargs):
            """Return a Popen that can properly target the Python env folder
            (sys._MEIPASS) from a frozen executable state.
            """
            # pylint: disable=no-member,protected-access
            if hasattr(sys, 'frozen'):
                # We have to set original _MEIPASS2 value from sys._MEIPASS
                # to get --onefile mode working.
                os.putenv('_MEIPASS2', sys._MEIPASS)
            try:
                super(_Popen, self).__init__(*args, **kwargs)
            finally:
                if hasattr(sys, 'frozen'):
                    # On some platforms (e.g. AIX) 'os.unsetenv()' is not
                    # available. In those cases we cannot delete the variable
                    # but only set it to the empty string. The bootloader
                    # can handle this case.
                    if hasattr(os, 'unsetenv'):
                        os.unsetenv('_MEIPASS2')
                    else:
                        os.putenv('_MEIPASS2', '')
    # Override 'Popen' class with our modified version
    forking.Popen = _Popen

    class Process(multiprocessing.Process):
        """A monkey-patched Process class that fixes frozen forking on Windows."""
        _Popen = _Popen

else:

    Process = multiprocessing.Process

class WriteWorker(Process):
    """A base worker that writes data in a queue to another location."""

    def __init__(self, write_queue: multiprocessing.Queue, extract_id: str,
                 worker_msg_queue: multiprocessing.Queue,
                 logfile: str, worker_timeout: int):
        """Instantiate the data write worker.

        ARGS:
            write_queue: The queue with data for writing.
            extract_id: The Extraction this worker belongs to.
            worker_msg_queue: A Queue to write messages and errors to
            logfile: The filepath to write log messages to.
            worker_timeout: Number of seconds a worker will wait for an item
                before giving up and shutting down.

        PROPERTIES:
            chunks_written: Number of chunks this worker has written.
            logger: The multiprocessing logger this worker can write with.
        """

        super(WriteWorker, self).__init__()
        self.daemon = True  # process can be closed / killed from main thread.

        self.chunks_written = 0
        self.logger = None  # type: multiprocessing.Logger

        self.queue = write_queue
        self.extract_id = extract_id
        self.worker_msg_queue = worker_msg_queue
        self.logfile = logfile
        self.worker_timeout = worker_timeout

    def __repr__(self):
        return '<%s>' % self.name

    def _create_logger(self):
        """Create a multiprocessing logger for this worker (or a dummy)."""
        self.logger = multiprocessing.get_logger()
        utils.setup_file_logger(self.logfile)
        # utils.setup_multiproc_logger()

    def run(self):
        """Main process invoked by calling process.start().

        Start processing data from queue until it is empty.

        Queue .get notes -- The queue takes a number of arguements.
        0th index (bool) - indicating if an error has been encountered on the stream side.
        1st (tuple/str) - If no error, contains the data extracted,
            if an error, contains the error message
        2nd index (metadata) - meatdata for the query (utils.DataDefinition)
        3rd (optional, for nesting) - Query text/where clause used to generate data
            used in the tracking of what has been extracted for pause resume funtionality.
        """

        self._create_logger()
        self.logger.info("Data write worker %s started", self)

        while True:
            try:
                item = self.queue.get()
            except QueueEmpty:
                self.logger.info("%s found no items in %i seconds. Shutting down.",
                                 self, self.worker_timeout)
                self.logger.setLevel(50)  # silence default Process shutdown messages
                break

            if item is None:
                # Check if any item exists after sentinel, otherwise shutdown
                # NOTE: this happens when a SQLite database is locked during
                # the very last write to disk. First the reader process finishes
                # and signals shutdown with a 'None' in the write queue.
                # Then the last writer fails, and puts the item it failed to
                # write back in the queue, now AFTER the sentinel 'None'.
                try:
                    item = self.queue.get_nowait()
                except QueueEmpty:
                    # Queue is empty as expected, write log to disk,
                    # and then put the sentinel back in queue for next process
                    self.logger.info("%s received signal to shutdown.", self)

                    # if self.logfile is not None and logging.getLogger().handlers:
                    #     self.logger.info('%s writing log from memory to %s...',
                    #                      self, self.logfile)
                        # utils.flush_log_to_file(self.logger, self.logfile)

                    self.queue.put(None, timeout=1)
                    break

                # Non-sentinel item still exists, put sentinel back in
                # at end of write queue, and process the new item
                self.queue.put(None, timeout=1)

            self.logger.info("%s got data to write from the queue.", self)
            #self.logger.info("*** %s" % (', '.join([str(type(t)) for t in item])))
            # If success flag is set to False, log an error
            if not item[0]:
                self.save_write_results(tablename=item[2].target_table,
                                        error=str(item[1]))
                # raise Exception(str(item[1]))
                continue

            # Attempt to process item using method from subclass
            # Put an error in the message List if it occurs so that
            # the parent Extraction class can raise it to the user
            try:
                self.process_item(*item[1:])
            except apsw.BusyError:  # pylint: disable=no-member
                # Database locked -- log an error -- and put data item
                # back in the queue for another worker to write it
                self.logger.warning('%s failed to write to SQLite table "%s" '
                                    '(database busy). Putting data back in queue',
                                    self, item[2].target_table)
                self.queue.put(item, timeout=1)
            except Exception as error:
                self.save_write_results(tablename=item[2].target_table,
                                        error=str(error))
                raise error

        p = psutil.Process(self.pid)
        p.kill()

    def save_write_results(self, location='', schema='', tablename='',
                           number_rows=0, error: str = None):
        """Save tuple of write result information to the results list.

        ARGS:
            location: Location where the chunk of data was written.
            schema: Database schema where this chunk was written.
            tablename: Name of the table where the chunk was written.
            number_rows: Number of rows that were written.
            error: Error message from failed write attempt (if applicable).
        """
        time_format = "%Y-%m-%d %H:%M:%S"
        write_time = time.strftime(time_format, time.localtime(time.time()))
        write_information = (
            write_time, location, schema, tablename,
            number_rows, str(self), error
        )
        self.worker_msg_queue.put(write_information, timeout=1)

    def process_item(self, data, metadata):
        """Do something with an item from queue. Overwritten by subclass."""
        raise NotImplementedError

    def detach_file_logger(self):
        for i, hndlr in enumerate(self.logger.handlers):
            if isinstance(hndlr, self.logger.FileHandler):
                hndlr.close()
                self.logger.removeHandler(hndlr)

class SQLiteWriteWorker(WriteWorker):
    """A write worker that writes all data to a single SQLite database."""

    def __init__(self, filepath: str, password: str = None,
                 is_zipped=False, aes256=False, **kwargs):
        """Instantiate the data write worker."""
        super().__init__(**kwargs)
        self.filepath = filepath
        self.password = password
        self.is_zipped = is_zipped
        self.aes256 = aes256

    def process_item(self, data: list, metadata: utils.DataDefinition,
                     query_text: str = None):
        """Unpack the item from the queue and write data to result database."""
        tablename = metadata.target_table
        # Create connection to SQLite DB
        # (must happen after __init__ for multiprocessing support)
        messenger = SQLiteMessenger(
            filepath=self.filepath, password=self.password,
            is_zipped=self.is_zipped, aes256=self.aes256
        )

        if not messenger.table_exists(tablename):
            self.logger.info("Creating new SQLite table: %s", tablename)
            columns = [column['targetFieldName'] for column in metadata.columns]
            datatypes = core.datatypes_from_metadata(metadata.columns, type(messenger))
            messenger.create_table(tablename, columns, datatypes)

        try:
            #Added if data check as SAP can return 0 rows but the where clause
            #Needs to be logged to avoid duplicate calls upon a resume
            if data:
                messenger.insert_into(tablename, data)
            #Check if query text is passed (might only be for SAP)
            if query_text:
                messenger.insert_into('temp_tracker', ((query_text, tablename),))
            self.logger.info("{} successfully wrote {:,} records.".format(self, len(data)))

        except RuntimeError:
            self.logger.error("%s job has FAILED.", self)

        else:
            self.logger.info("{} successfully wrote {:,} records.".format(self, len(data)))
            self.chunks_written += 1
            self.save_write_results(location=self.filepath,
                                    tablename=tablename,
                                    number_rows=len(data))


class SQLiteChunkTableWorker(WriteWorker):
    """A write worker that writes data to one SQLite database per chunk or
        one databse per table.
    """

    def __init__(self, master_msgr: ABCMessenger, password: str = None,
                 is_zipped=False, aes256=False, db_per_chunk=False, **kwargs):
        """Instantiate the data write worker.

        ARGS:
            folder: Filepath of the folder on the local machine
                where result databases will be written.
        """
        super().__init__(**kwargs)
        self.master_msgr = master_msgr
        self.password = password
        self.is_zipped = is_zipped
        self.aes256 = aes256
        self.db_per_chunk = db_per_chunk
        self.counts = {}

    def process_item(self, data: list, metadata: utils.DataDefinition,
                     query_text: str = None):
        """Unpack item from queue and write data to unique result database."""
        tablename = metadata.target_table
        folder = os.path.dirname(self.master_msgr.filepath)

        if tablename not in self.counts:
            self.counts[tablename] = 0

        #Check if it's a db per chunk extraction
        if self.db_per_chunk:
            filename = (
                "extract_{id}_{worker}_chunk_{chunk}.dat"
                .format(id=self.extract_id, worker=self.name,
                        chunk=self.chunks_written)
            )
        #If a db per table
        else:
            filename = (
                "Encrypted_Content_{tablealias}.dat"
                .format(tablealias=tablename)
            )

        filepath = os.path.join(folder, filename)

        if not os.path.exists(filepath):
            self.logger.debug("Created new SQLite database: %s", filepath)

        # Connect to the new SQLite database and insert metadata in it
        write_messenger = SQLiteMessenger(
            filepath=filepath, is_zipped=self.is_zipped,
            password=self.password, aes256=self.aes256
        )
        status_messenger = SQLiteMessenger(
            filepath=self.master_msgr.filepath, is_zipped=self.is_zipped,
            password=self.password, aes256=self.aes256
        )

        # Create new data/insert table for item processing
        if not write_messenger.table_exists(tablename):
            self.logger.info("Creating new SQLite table: %s", tablename)
            columns = [column['targetFieldName'] for column in metadata.columns]
            datatypes = core.datatypes_from_metadata(metadata.columns, type(write_messenger))
            write_messenger.create_table(tablename, columns, datatypes)

        # Insert data into the new table
        try:
            #Added if data check as SAP can return 0 rows but the where clause
            #Needs to be logged to avoid duplicate calls upon a resume
            if data:
                write_messenger.insert_into(tablename, data)
            #Check if query text is passed (might only be for SAP)
            #self.logger.info('*** query_text --> "%s".' % (query_text))
            if query_text:
                status_messenger.insert_into('temp_tracker', ((query_text, tablename),))

        except RuntimeError:
            self.logger.error("%s job has FAILED.", self)

        else:

            self.counts[tablename] += len(data)
            self.logger.info("{} successfully wrote {:,} records to {}.  {:,} total.".format(
                self, len(data), tablename, self.counts[tablename]))
            self.chunks_written += 1
            self.save_write_results(location=filepath, tablename=tablename,
                                    number_rows=len(data))
        finally:
            write_messenger._conn.close()
            status_messenger._conn.close()


class MSSQLWriteWorker(WriteWorker):
    """A write worker that writes to a single MSSQL database schema."""

    def __init__(self, connection_string, schema, **kwargs):
        """Instantiate the data write worker.

        ARGS:
            connection_string (str): The connection string to MSSQL.
            schema (str): The schema to write results to.
            **kwargs: See WriteWorker.__init__().
        """
        super().__init__(**kwargs)
        self.connection_string = connection_string
        self.schema = schema

    def process_item(self, data: tuple, metadata: utils.DataDefinition,
                     query_text: str = None):
        """Unpack the item from the queue and write data to result database."""

        messenger = MSSQLMessenger(self.schema, self.connection_string)
        tablename = metadata.target_table

        try:
            if data:
                messenger.insert_into(tablename, data, metadata.columns)
            #Check if query text is passed (might only be for SAP)
            if query_text:
                messenger.insert_into('temp_tracker', ((''.join(query_text),
                                                        tablename),))
        except RuntimeError:
            self.logger.error("%s job has FAILED.", self)
        else:
            self.logger.info("%s successfully completed job.", self)
            self.chunks_written += 1
            self.save_write_results(schema=self.schema, tablename=tablename,
                                    number_rows=len(data))


class MySQLWriteWorker(WriteWorker):
    """A write worker that writes to a single MSSQL database schema."""

    def __init__(self, connection_string: str, **kwargs):
        """Instantiate the data write worker."""
        super().__init__(**kwargs)
        self.connection_string = connection_string

    def process_item(self, data: tuple, metadata: utils.DataDefinition):
        """Unpack the item from the queue and write data to result database."""
        messenger = MySQLMessenger(self.connection_string)
        tablename = metadata.target_table
        try:
            messenger.insert_into(tablename, data, metadata.columns)
        except RuntimeError:
            self.logger.error("%s job has FAILED.", self)
        else:
            self.logger.info("%s successfully completed job.", self)
            self.chunks_written += 1
            self.save_write_results(tablename=tablename, number_rows=len(data))
