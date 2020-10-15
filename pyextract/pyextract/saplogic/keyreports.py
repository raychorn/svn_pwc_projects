"""Class to call a non-RFC Program (Key Report) in SAP.
Collects data from the SAP spool and parses the information into a queue.
"""

import datetime
import multiprocessing
import threading
import time
from typing import List

from .. import utils
from ..connect.sap import SAPMessenger

LOGGER = multiprocessing.get_logger()
REPORT_PAGE_CHUNK_SIZE = 10 #How many pages to call from SAP at a time


class SAPKeyReports(object):
    """Class to schedule a background job for non RFC enables programs to be
        executed, upon completion of the job the spool will be downloaded and
        parsed
    """
    def __init__(self, messenger: SAPMessenger,
                 write_queue: multiprocessing.Queue,
                 stopevent: threading.Event=None,
                 in_progress_tables: List[str] = None):
        """
        args:
            messenger: existing SAP Messenger
            write_queue: queue to be used to put data for write workers
                to pick up
            stopevent: If provided, an Event that can be .set() to signal
                an extraction should be paused/canceled.
            in_progress_tables: Tracks which tables were paused mid extraction

        properties:
            conn: new SAP connection for various FM calls
            condetails: SAP connection properties
            pages_read: Number of pages read from the spool for a given report
        """

        self.messenger = messenger
        self.write_queue = write_queue
        self.stopevent = stopevent
        self.in_progress_tables = in_progress_tables or []

        self.conn = messenger._conn
        self.condetails = self.conn.get_connection_attributes()
        self.pages_read = 0
        self.rows_read = 0

    def run_report(self, metadata: utils.DataDefinition):
        """
        run a report using a single variant
        """
        program = metadata.parameters['Name']
        variant_name = metadata.target_table
        variant = metadata.parameters['Parameters']

        if metadata.parameters['NameAlias'] in self.in_progress_tables:
            pass

        self._create_variant(program=program, variant_name=variant_name,
                             variant=variant)
        result = self.schedule_job_immediately(program, program, 'TEMPVARI')
        spoolid = self.determine_spool_id(program, result['JOBCOUNT'])
        self._wait_until_job_completed(program, result['JOBCOUNT'])
        report_length = self._get_report_size_in_pages(spoolid=spoolid)

        self.stream_download_spool(spoolid=spoolid, report_length=report_length,
                                   program=program, metadata=metadata)

    def _create_variant(self, program: str, variant_name: str, variant: list):
        """
        Checks if the variant has already been created in the SAP environment
            if not will create the variant
        """
        vari_text = list()
        vari_contents = list()

        temp = dict(MANDT=self.condetails['client'],
                    LANGU=u'EN',
                    REPORT=program,
                    VARIANT=variant_name,
                    VTEXT=u'temporary')

        vari_text.append(temp)

        vari_desc = dict(REPORT=program,
                         VARIANT=variant_name,
                         AEDAT=datetime.date.today(),
                         AETIME=datetime.time(00, 00))

        vari_contents.extend(variant)

        if self.check_if_variant_exists(program=program, variant=variant_name):
            self.conn.call('RS_CHANGE_CREATED_VARIANT_RFC',
                           CURR_REPORT=program, CURR_VARIANT=variant_name,
                           VARI_DESC=vari_desc, VARI_CONTENTS=vari_contents,
                           VARI_TEXT=vari_text)
        else:
            self.conn.call('RS_CREATE_VARIANT_RFC', CURR_REPORT=program,
                           CURR_VARIANT=variant_name, VARI_DESC=vari_desc,
                           VARI_CONTENTS=vari_contents, VARI_TEXT=vari_text)

    def check_if_variant_exists(self, program: str, variant: list):
        """
        Pings SAP to identify if program and the assocaited variant Exist
            use T-Code SE38 in the GUI to research parameters if variants needs
            to be created.
        """
        result = self.conn.call(self.messenger.function_module,
                                QUERY_TABLE='VARID', \
                                DELIMITER='|', \
                                FIELDS=[{'FIELDNAME':'VARIANT'}], \
                                OPTIONS=[{'TEXT':"REPORT EQ '" + program.upper()+"'"}])

        for record in result[result['OUT_TABLE']]:
            if record['WA'] == variant.upper():
                return True

    def schedule_job_immediately(self, jobname: str, program: str,
                                 variant: str = None):
        """
        Opens and XMI connection and will call various FMs to schedule the
            background job for the report to be executed
        """
        self.conn.call('BAPI_XMI_LOGON', EXTCOMPANY='LARS',
                       EXTPRODUCT='assessment', INTERFACE='XBP',
                       VERSION='2.0')

        result = self.conn.call('BAPI_XBP_JOB_OPEN', JOBNAME=jobname,
                                EXTERNAL_USER_NAME='AUDIT')

        jobcount = result['JOBCOUNT']

        if variant:
            self.conn.call('BAPI_XBP_JOB_ADD_ABAP_STEP', JOBNAME=jobname,
                           JOBCOUNT=jobcount, EXTERNAL_USER_NAME='AUDIT',
                           ABAP_PROGRAM_NAME=program, ABAP_VARIANT_NAME=variant)
        else:
            self.conn.call('BAPI_XBP_JOB_ADD_ABAP_STEP', JOBNAME=jobname,
                           JOBCOUNT=jobcount, EXTERNAL_USER_NAME='AUDIT',
                           ABAP_PROGRAM_NAME=program)

        self.conn.call('BAPI_XBP_JOB_CLOSE', JOBNAME=jobname,
                       JOBCOUNT=jobcount, EXTERNAL_USER_NAME='AUDIT')

        self.conn.call('BAPI_XBP_JOB_START_IMMEDIATELY', JOBNAME=jobname,
                       JOBCOUNT=jobcount, EXTERNAL_USER_NAME='AUDIT')

        return dict(JOBCOUNT=jobcount)

    def _wait_until_job_completed(self, jobname: str, jobcount: str) -> bool:
        """
        Checks whether a job is still running and waits until it completes.

        Returns a bool dictating whether the file will be saved locally to disk
            or passed via RFC and held in memory based on runtime in SAP.
        """
        jobstatus = 'X'

        while jobstatus not in ['F', 'A']:
            status = self.conn.call('SUBST_CHECK_BATCHJOB', JOBNAME=jobname,
                                    JOBCOUNT=jobcount)
            jobstatus = status['JOBSTATUS']
            time.sleep(3)

    def determine_spool_id(self, jobname: str, jobcount: str) -> int:
        """
        find the spool ID based on the job name and job count.
        """
        attempts = 0
        results = '0000000000'

        spool_where = "JOBNAME = '{}' AND JOBCOUNT = {} AND STEPCOUNT = 1".format(jobname,
                                                                                  jobcount)

        while attempts < 3 and results == '0000000000':
            time.sleep(6)
            try:
                results = self.messenger.single_readtable(
                    columns=['LISTIDENT'],
                    table='TBTCP',
                    where=[spool_where]
                    )[0][0]
            except IndexError:
                pass
            attempts += 1

        return int(results)

    def _get_report_size_in_pages(self, spoolid: int):
        """Queries SAP table to identify report length in pages, this will be
            used to chunk up the calls to SAP to now overflow the memory
        """
        spool_where = "RQIDENT = '{}'".format(spoolid)
        attempts = 0
        pages = 0

        while attempts < 3 and not pages:
            time.sleep(3)
            try:
                pages = int(self.messenger.single_readtable(
                    columns=['RQAPPRULE'],
                    table='TSP01',
                    where=[spool_where]
                    )[0][0])
            except IndexError:
                pass
            attempts += 1

        return pages

    def stream_download_spool(self, spoolid: int, report_length: int, program: str,
                              metadata: utils.DataDefinition):
        """
        Download Spool File from SAP in chunks if the report length is larger
            than the specified gloval length at the top of the file.
        param spoolid: spool ID of the job
        report_length: Length of report used in chunking up SAP calls
        program: name of the program (thus batch job name)
        metadata: to be passed along to the queue
        """
        self.conn.call('BAPI_XMI_LOGON',
                       EXTCOMPANY='linkies',
                       EXTPRODUCT='assessment',
                       INTERFACE='XBP',
                       VERSION='2.0')
        first_page = 1
        last_page = REPORT_PAGE_CHUNK_SIZE

        while self.pages_read < report_length:
            if self.is_stopped():
                LOGGER.info("Extraction stopped by user after {:,} pages."
                            .format(self.pages_read))
                break
            elif report_length < REPORT_PAGE_CHUNK_SIZE:
                result = self.conn.call('BAPI_XBP_JOB_READ_SINGLE_SPOOL',
                                        SPOOL_REQUEST=spoolid,
                                        EXTERNAL_USER_NAME='AUDIT')
                data = self.parse_data(program=program,
                                       spoolfile=result)
                self.write_queue.put((True, data, metadata))
                break
            else:
                result = self.conn.call('BAPI_XBP_JOB_READ_SINGLE_SPOOL',
                                        SPOOL_REQUEST=spoolid,
                                        EXTERNAL_USER_NAME='AUDIT',
                                        FIRST_PAGE=first_page,
                                        LAST_PAGE=last_page)
                data = self.parse_data(program=program,
                                       spoolfile=result)
                self.write_queue.put((True, data, metadata))
                self.pages_read += REPORT_PAGE_CHUNK_SIZE
                first_page += REPORT_PAGE_CHUNK_SIZE
                last_page += REPORT_PAGE_CHUNK_SIZE

    def parse_data(self, program: str, spoolfile: dict):
        """method to parse data (right now MB5B specific) and return a list of
            tuples
        """
        data = []

        #currently only supporting parsing for MB5B
        if program == 'RM07MLBD':
            for line in spoolfile['SPOOL_LIST_PLAIN']:
                if "|" in line['LINE'][1:-1] and 'ValA' not in line:
                    data.append(line['LINE'][1:-1].split("|"))

        #All other reports will be loaded as one column and one row per report line
        else:
            for line in spoolfile['SPOOL_LIST_PLAIN']:
                data.append([line['LINE']])

        self.rows_read += len(data)

        LOGGER.info('Extracted {:,} rows from {} (Total {:,})'
                    .format(len(data), program, self.rows_read))

        return data

    def is_stopped(self) -> bool:
        """Return True if there is a stop event"""
        return bool(self.stopevent and self.stopevent.is_set())
