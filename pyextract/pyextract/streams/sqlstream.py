"""Classes to manage the flow of data from a SQL-based Messenger to a Queue."""

import multiprocessing
import threading
import time
import stopit

from ..connect import ABCMessenger
from ..utils import DataDefinition
from .. import config


class DataStream(object):
    """Streams data defined by DataDefinition in chunks using an ABCMessenger.

    As chunks are read, they are placed into a multiprocessing Queue
    for write workers to pick up and put into the output database.
    """

    def __init__(self, messenger: ABCMessenger, chunk_size=50000,
                 queue_size=10, row_limit: int = None,
                 stopevent: threading.Event=None) -> None:
        """Instantiate a data stream.

        ARGS:
            messenger: Messenger object that pulls data into the queue.
            chunk_size: Maximum number of records to read at a time.
            queue_size: Maximum number of chunks to hold in the queue.
            row_limit: Maximum number of rows to read before finishing
                an extraction. If None, will read all rows.
            stopevent: If provided, an Event that can be .set() to signal
                an extraction should be paused/canceled.

        ATTRIBUTES:
            queue: The queue to place data that was read from SQL.
            logger: A logger shared across multiple processes.
            rows_read: Number of rows that this reader has processed.
        """
        self.messenger = messenger
        self.chunk_size = chunk_size
        self.queue_size = queue_size
        self.row_limit = row_limit
        self.queue = None  # type: multiprocessing.Queue
        self.logger = multiprocessing.get_logger()
        self.rows_read = 0
        self.stopevent = stopevent
        # Limit chunk_size too if row_limit is super small
        if row_limit and row_limit < chunk_size:
            self.chunk_size = row_limit
        #To be used in the timeout when queries become unresponsive.
        self.timed_out = False
        self.encountered_row_skips = False ## Always will be false for non-sap

    def start(self, queue: multiprocessing.Queue, metadata: DataDefinition):

        """Stream all data defined by metadata as chunks into the queue."""
        starttime = time.time()
        self.rows_read = 0
        self.queue = queue
        self.messenger.begin_extraction(metadata, self.chunk_size)

        while True:
            if self.is_stopped():
                self.logger.info("Extraction stopped by user after {:,} rows."
                                 .format(self.rows_read))
                break
            elif self.row_limit_reached():
                self.logger.info("Finished reading {:,} of {:,} maximum rows."
                                 .format(self.rows_read, self.row_limit))
                break
            elif self.queue_limit_reached():
                self.logger.debug("Queue for storing data to write is full. "
                                  "Checking again in 5 seconds.")
                time.sleep(5)
                continue
            else:
                self.logger.debug("Fetching new rows from source database.")
                try:
                    with stopit.ThreadingTimeout(config.QUERY_TIMEOUT,
                                                 swallow_exc=False):
                        data = self.messenger.continue_extraction(self.chunk_size)
                except stopit.utils.TimeoutException as error:
                    if not self.timed_out:
                        self.logger.error(
                            'Query is unresponsive, trying again in '
                            '%i seconds...', config.QUERY_RETRY_WAIT_TIME
                        )
                        time.sleep(config.QUERY_RETRY_WAIT_TIME)
                        self.timed_out = True
                        self.start(queue, metadata)
                        break
                    self.queue.put((False, "Query Timeout Error", metadata))
                    self.logger.error("Query failed due to error: %s", error)
                    break
                except Exception as error:
                    self.logger.info("Error encountered during extraction: %s", error)
                    self.queue.put((False, str(error), metadata))
                    self.logger.error("Query failed due to error: %s", error)
                    break

                if data:
                    data = self.trim_data(data)
                    item = (True, data, metadata)
                    self.queue.put(item)
                    self.rows_read += len(data)
                    # import pdb; pdb.set_trace()
                    self.logger.info(
                        "{:,} new records read from source database ({:,} total)."
                        .format(len(data), self.rows_read)
                    )
                else:
                    self.logger.info("All data has been read from source database.")
                    break

        try:
            self.messenger.finish_extraction()
        except AttributeError:
            # TODO -- assess accuracy of this comment and need for try/except
            # In the instance of a query timeout, given the recursive
            # call to retry, this will return a None type failing the
            # .close of the cursor
            pass

        self.logger.info("Finished reading {:,} rows in {:.2f} seconds."
                         .format(self.rows_read, (time.time() - starttime)))


    def row_limit_reached(self):
        """Return True if the maximum number of rows have been read."""
        return bool(self.row_limit and self.rows_read >= self.row_limit)

    def queue_limit_reached(self):
        """Return True if the queue to put data in is full."""
        return bool(self.queue.qsize() >= self.queue_size)

    def is_stopped(self):
        """Return True if there is a stop event"""
        return bool(self.stopevent and self.stopevent.is_set())

    def trim_data(self, data: list) -> list:
        """Return data to fit the row limit (if applicable)."""
        if self.row_limit:
            rows_left = self.row_limit - self.rows_read
            if len(data) > rows_left:
                data = data[:rows_left]
        return data
