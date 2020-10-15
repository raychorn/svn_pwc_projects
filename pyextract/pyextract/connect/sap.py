"""Query interface built on an open SAP connection."""

import multiprocessing
import re
import time
from typing import List

import pyrfc
from stopit import ThreadingTimeout

from .base import ABCMessenger
from .. import config

LOGGER = multiprocessing.get_logger()


class SAPMessenger(ABCMessenger):
    """Interfaces with a single SAP database."""

    OUTPUT_DELIMITER = '|'

    def __init__(self, logon_details: dict, connection_type='Direct Connection',
                 function_module='/BODS/RFC_READ_TABLE2', *args, **kwargs):
        """Instantiate a new instance of a connector.

        ARGS:
            *args, **kwargs: See connect_to_sap().
        """
        super().__init__()

        self.connection_type = connection_type
        self.function_module = function_module
        self.logon_details_ = logon_details
        self._conn = connect_to_sap(connection_type, logon_details)
        self._extract_query = None
        self._extract_row = 0


    def restablish_connection(self):
        self._conn = connect_to_sap(self.connection_type, self.logon_details_)

    def single_readtable(self, table: str, columns: list = None,
                         where: list = None, from_row=0,
                         delimiter='', package_size=150000):

        # print("DEBUG: package_size: {}".format(package_size))

        """A method to query SAP /BODS/RFC_READ_TABLE2
            This is the master SAP query function, regardless of the type selected.
            For example, single table split or neseted still leverage this function.

            args:
            columns list(str): list of headers to be extracted
            table (str): table to be extracted
            where (List[str]): limiting criteria, for example limiting results to EN
            from_row (int): If extraction is to be started at record other than 0,
                more commonly used internally for large tables

            Note: this will become a recursise function call if row skipping
                starts so we collect all records
        """
        starttime = time.time()

        if columns:
            fields = [{'FIELDNAME': column.strip()} for column in columns]
        else:
            fields = ''

        # the WHERE part of the query is called "options"
        if not where:
            where = []
        options = [{'TEXT': x} for x in where]

        # Call to SAP's RFC_READ_TABLE
        kwargs = {
            'QUERY_TABLE': table,
            'DELIMITER': delimiter,
            'FIELDS': fields,
            'OPTIONS': options,
            'ROWCOUNT': package_size,
            'ROWSKIPS': from_row,
        }
        LOGGER.debug('Calling SAP Function "%s" with Parameters: %s',
                     self.function_module, kwargs)
        tables = self._conn.call(self.function_module, **kwargs)

        if self.function_module in ['/SAPDS/RFC_READ_TABLE2', '/BODS/RFC_READ_TABLE2']:
            # checks which table the output was written to
            data_fields = tables[tables['OUT_TABLE']]
        else:
            # pull the data part of the result set
            data_fields = tables["DATA"]

        # pull the field name part of the result set
        data_names = tables["FIELDS"]

        col_len = [int(x['LENGTH']) for x in data_names]  # len for split
        col_start = [int(x['OFFSET']) for x in data_names]  # len for split
        long_fields = len(data_fields)  # data extraction

        # Print extraction status
        from_row += long_fields
        if from_row:
            message = (
                'Extracted {:,} records from {} in {:.2f} seconds'
                ).format(from_row, table, time.time() - starttime)
        else:
            message = (
                '{} returned zero records in {:.2f} seconds'
                ).format(table, time.time() - starttime)

        if where:
            # Truncate filter lists with more than 20 items.
            # Message will include number of hidden items, so a where
            # clause with 21 items will show 18 items and '...3 more...'
            excess = len(where) - 18
            if excess > 2:
                where = where[:13] + ['...{} more...'.format(excess)] + where[-5:]
            message += ' using filters {}'.format(where)

        # Log the status with newlines and tabs removed
        message += " using {}.".format(self.function_module)
        LOGGER.info(message.replace('\n', ' ').replace('\t', ' '))

        data = []
        for line in data_fields:
            if delimiter:
                data.append(line["WA"].split('|'))
            else:
                data.append(parse_columns(line["WA"], col_start, col_len))

        return data

    def abap_function_enabled(self, function):
        """Return True if ABAP function is enabled on this connection."""
        where_options = ["FMODE = 'R' AND FUNCNAME = '{}'".format(function)]
        response = self.single_readtable(table='TFDIR',
                                         where=where_options,
                                         package_size=1)
        return bool(response)

    def list_abap_functions(self, name_like=None, enabled_only=True,
                            names_only=True):
        """Return a list of SAP ABAP functions enabled for remote calls.

        ARGS:
            name_like (str): If provided, will only return functions that
                match the pattern in name_like (use '%' for wildcards).
            enabled_only (bool): If True, only return the functions that
                are enabled for remote RFC calls.
            names_only (bool): If True, returns only the function names in
                a list. If False, returns names and all metadata.

        RETURNS:
            (list[str] | dict[list[dict[str]]]): If names_only, returns
                a list of string function names. Otherwise, return the
                standard nested data structure from pyrfc.
        """

        where_options = []

        if name_like:
            where_options += ["FUNCNAME LIKE '{}'".format(name_like)]

        if enabled_only:
            where_options += ["FMODE = 'R'"]

        if names_only:
            select_fields = ("FUNCNAME", )
        else:
            select_fields = ''

        response = self.single_readtable(table='TFDIR',
                                         columns=select_fields,
                                         where=[' AND '.join(where_options)])

        return response

    def table_exists(self, table):
        """Return True if a table already exists in this database."""

        where_statement = "TABNAME = '{}'".format(table)
        call_options = {
            'QUERY_TABLE': 'DD03L',
            "FIELDS": [{"FIELDNAME": "FIELDNAME"}],
            "OPTIONS": [{"TEXT": where_statement}],
            'ROWCOUNT': 1,
            'DELIMITER': self.OUTPUT_DELIMITER,
        }
        response = self._conn.call('RFC_READ_TABLE', **call_options)
        table_exists = bool(response['DATA'])

        return table_exists

    def get_metadata_from_query(self, table: dict):
        """function to get meta data from SAP to be used while writing to SQLite"""

        assert 'Name' in table, 'metadata from ECF must include a "Name"'
        assert 'Columns' in table, 'metadata from ECF must include "Columns"'
        assert '*' not in table['Columns'], 'Queries with * are not supported for SAP'

        if table['Type'] == 'KEY_REPORT':
            sap_info = table['Columns']
        else:
            table_name = table['Name']
            column_names = [col.strip() for col in table['Columns']]

            # TODO -- Re-enable SAP table access validation after impact to
            # the SAP system can be determined. Even with a 10 second timeout
            # on the 'SELECT 1' query, we don't know if the big table scan
            # will still run server side, despite stopping the client request

            # LOGGER.debug('Validating user access to {}...'.format(table_name))
            # where = ["{} = '{}'".format(param['Name'], param['Values'][0])
            #          for param in table.get('Parameters', [])]
            # self.validate_user_access(table_name, column_names[0], where)

            sap_info = self.get_data_from_sap(table_name, column_names)

        metadata = [build_column_meta(field) for field in sap_info]
        return metadata

    def get_data_from_sap(self, table: str, column_names: list) -> list:
        """Return metadata from SAP to be used while writing to SQLite.

        RETURNS:
            A JSON list sorted by 'column_names', where each value is a
            dictionary with the keys listed in 'output_keys'.
        """
        output_keys = (
            'LENG', 'INTLEN', 'OUTPUTLEN', 'DATATYPE', 'INTTYPE',
            'DECIMALS', 'SIGN', 'FIELDTEXT', 'KEYFLAG', 'FIELDNAME'
        )

        try:
            response = self._conn.call("DDIF_FIELDINFO_GET", TABNAME=table)
        except pyrfc.CommunicationError:
            self.restablish_connection()
            try:
                response = self._conn.call("DDIF_FIELDINFO_GET", TABNAME=table)
            except:
                raise
        except pyrfc.ABAPApplicationError as error:
            raise Exception("Removing {} from extraction for "
                            "invalid table name. SAP ERROR {}"
                            .format(table, error.key))

        # Validate the data from the metadata response
        validate_column_dtypes(response, column_names)

        response_order = [info['FIELDNAME'] for info in response['DFIES_TAB']]

        return_data = []  # type: List[Dict[str,str]]
        invalid_columns = []

        for column in column_names:
            if column not in response_order:
                LOGGER.error('"%s" is not a valid field in SAP table "%s"',
                             column, table)
                invalid_columns.append(column)
            else:
                fieldinfo = response['DFIES_TAB'][response_order.index(column)]
                return_data += [{key: fieldinfo[key] for key in output_keys}]

        assert not invalid_columns, (
            '{} will be skipped due to invalid columns: {}'
            ).format(table, invalid_columns)

        return return_data

    def validate_user_access(self, table: str, column: str,
                             where: list = None):
        """Raise error if user does not have access to SAP table."""
        if where:
            where = where_clause_rfc_format(where)

        try:
            # If query takes over 10 seconds, user has access to table
            with ThreadingTimeout(10):
                self.single_readtable(table=table, columns=[column],
                                      where=where, package_size=1)
        except (pyrfc.ABAPRuntimeError, pyrfc.ABAPApplicationError) as error:
            if 'DBIF_RSQL_SQL_ERROR' in error.key:
                LOGGER.warning('Validating access to table "%s" reached '
                               'SAP memory limit', table)
                return
            LOGGER.error(str(error))
            raise error

    def get_table_columns(self, table):
        """Return a list of columns in an SAP table."""

        where_statement = "TABNAME = '{}'".format(table)
        call_options = {
            'QUERY_TABLE': 'DD03L',
            "FIELDS": [{"FIELDNAME": "FIELDNAME"}],
            "OPTIONS": [{"TEXT": where_statement}],
            'DELIMITER': self.OUTPUT_DELIMITER,
        }

        response = self._conn.call('RFC_READ_TABLE', **call_options)
        fields = [item["WA"] for item in response["DATA"]]

        # Filter out SAP metadata fields from primary data fields
        for pattern in ('.', '/', 'OFFSET_'):
            fields = [f for f in fields if not f.startswith(pattern)]

        return fields


def build_column_meta(field: dict):
    """Parse and format for sqlite ingestion"""

    field_config = dict()
    field_meta = field.copy()

    nullable = not bool(field_meta['KEYFLAG'].strip())  # True if '', False if 'X'

    field_config['sourceSystem'] = 'SAP'
    field_config['sourceFieldName'] = field_meta['FIELDNAME']
    field_config['targetFieldName'] = field_meta['FIELDNAME']
    field_config['sourceType'] = field_meta['INTTYPE']
    field_config['sourceFieldLength'] = field_meta['OUTPUTLEN']
    field_config['sourceFieldNumericPrecision'] = field_meta['DECIMALS']
    field_config['source_field_nullable'] = nullable
    field_config['isKey'] = not nullable
    field_config['SIGN'] = field_meta['SIGN']
    field_config['longDataType'] = field_meta['DATATYPE']

    field_config['longDataType'] = field_meta['DATATYPE']

    if field_meta['DATATYPE'] in ('CURR', 'DEC'):
        field_config['sqlite_datatype'] = 'REAL'
    else:
        field_config['sqlite_datatype'] = 'TEXT'
    if 'mssql_datatype' not in field_config:
        field_config['mssql_datatype'] = 'NVARCHAR(MAX)'
    return field_config


def validate_column_dtypes(sap_response: dict, ecf_cols: list):
    """Validates that columns being extracted are not from a
        disallowed data type
    """
    invalid_columns = []

    for column in sap_response['DFIES_TAB']:
        if (column['FIELDNAME'] in ecf_cols
                and column['DATATYPE'] in config.BANNED_SAP_DTYPES):
            LOGGER.error('Field "%s" in SAP table "%s" has an unsupported '
                         'data type ("%s").', column['FIELDNAME'],
                         column['TABNAME'], column['DATATYPE'])
            invalid_columns.append(column['FIELDNAME'])

    assert not invalid_columns, \
        "Removing {} from extraction due to unsupported data type(s)".format(column['TABNAME'])


def parse_columns(data, start, length):
    """function to parse data when no delimiter is used"""
    split_list = []
    for strt, end in zip(start, length):
        split_list.append(data[strt:strt + end])
    return split_list


def connect_to_sap(connection_type: str, connection_details: dict) -> pyrfc.Connection:
    """Return an open RFC connection to an SAP database.

    ARGS:
        connection_type: Method to be used to connect to SAP (e.g. SNC, Direct)

        connection_details: A dictionary housing the various arguments that may
            leveraged as there are a number of connection options.

        Supported connection types:

        Direct Connection:
            client: Client number of the SAP database.
            user: Username to authenticate with.
            password: Password to authenticate with.
            language: 2-digit language code to connect with.
            system: System number to connect to. If direct connection, is
                the 'system number'. If load balancing connection, 'system id'.
            ashost: Network location of the SAP server. If provided, will use
                a direct connection to SAP.

        Load Balanced Connection (Group/Server):
            client: Client number of the SAP database.
            user: Username to authenticate with.
            password: Password to authenticate with.
            language: 2-digit language code to connect with.
            mshost: A Message Server Host that will provide a load balancing
                connection to SAP.
            group: The group to use with the load-balancing messaging server.
            sysid: System number to connect to. If direct connection, is
                the 'system number'. If load balancing connection, 'system id'.

        RFC w/ SNC:
            client: Client number of the SAP database.
            ashost: Network location of the SAP server. If provided, will use
                a direct connection to SAP.
            language: 2-digit language code to connect with.
            snc_qop: Quality of protection (protection level) (3 default 0-9)
            snc_myname: SNC name of the RFC server program
            snc_partnername: SNC name of the communication partner (application server)
            snc_lib: file path to the DLL file/cert required to authenticate the user


    """

    # pylint: disable=too-many-arguments
    connection_options = {
        'client': validate_client(connection_details.get('client')),
        'lang': connection_details.get('language'),
    }

    if connection_type == 'Direct Connection':
        connection_options.update({
            'sysnr': connection_details.get('sysnr'),
            'ashost': connection_details.get('ashost'),
            'user': connection_details.get('user'),
            'passwd': connection_details.get('password'),
        })

    elif connection_type == "Load Balanced Connection":
        connection_options.update({
            'sysid': connection_details.get('sysid'),
            'mshost': connection_details.get('mshost'),
            'msserv': connection_details.get('msserv'),
            'group': connection_details.get('group'),
            'user': connection_details.get('user'),
            'passwd': connection_details.get('password'),
        })

    elif connection_type == "Direct Connection w/SNC":
        connection_options.update({
            'ashost': connection_details.get('ashost'),
            'snc_qop': connection_details.get('snc_qop'),
            'snc_myname': connection_details.get('snc_myname'),
            'snc_partnername': connection_details.get('snc_partnername'),
            'snc_lib': connection_details.get('snc_lib'),
        })

    elif connection_type == "Load Balanced w/SNC":
        connection_options.update({
            'sysid': connection_details.get('sysid'),
            'mshost': connection_details.get('mshost'),
            'msserv': connection_details.get('msserv'),
            'group': connection_details.get('group'),
            'user': connection_details.get('user'),
            'passwd': connection_details.get('password'),
            'snc_qop': connection_details.get('snc_qop'),
            'snc_myname': connection_details.get('snc_myname'),
            'snc_partnername': connection_details.get('snc_partnername'),
            'snc_lib': connection_details.get('snc_lib'),
        })

    elif connection_type == "SAP HANA":
        raise Exception('Hana is not yet supported in pyextract')

    # Remove any 'None' values from the logon creds
    clean_conn_options = {k: v for k, v in connection_options.items() if v}

    conn = pyrfc.Connection(**clean_conn_options)
    return conn


def validate_client(client_id: str):
    """Validates Client is in a valid format priot to calling SAP"""
    assert len(str(client_id)) == 3 and client_id.isdigit(), \
        'Client is invalid, please enter a 3 digit numeric value'
    return client_id


def check_func_mod_auths(connection_type: str, logon_details: dict, fms: list):
    """Validates the necessary function module access is available and returns
        the appropriate function module per the installed name space.
        e.g. BODS vs SAPDS
    """

    conn = connect_to_sap(connection_type, logon_details)

    working_fms = []
    failed_fms = []

    ds_errors = []
    bbp_errors = []

    for func_mod in fms:
        try:

            conn.call(func_mod, ROWCOUNT=1)
            working_fms.append(func_mod)

        except pyrfc.ABAPRuntimeError as err:
            failed_fms.append(func_mod)
            if func_mod in ['/SAPDS/RFC_READ_TABLE2', '/BODS/RFC_READ_TABLE2']:
                ds_errors.append(err)
            else:
                bbp_errors.append(err)

        except pyrfc.ABAPApplicationError as err:
            if err.key == 'TABLE_NOT_AVAILABLE':
                working_fms.append(func_mod)
            elif err.key == 'FU_NOT_FOUND':
                failed_fms.append(func_mod)
                if func_mod in ['/SAPDS/RFC_READ_TABLE2', '/BODS/RFC_READ_TABLE2']:
                    ds_errors.append(err)
                else:
                    bbp_errors.append(err)
            else:
                failed_fms.append(func_mod)
                if func_mod in ['/SAPDS/RFC_READ_TABLE2', '/BODS/RFC_READ_TABLE2']:
                    ds_errors.append(err)
                else:
                    bbp_errors.append(err)
        except:
            failed_fms.append(func_mod)
            if func_mod in ['/SAPDS/RFC_READ_TABLE2', '/BODS/RFC_READ_TABLE2']:
                ds_errors.append(err)
            else:
                bbp_errors.append(err)
        finally:
            conn.close()

    # Data Services is Required.
    missing_ds = False
    if '/SAPDS/RFC_READ_TABLE2' in fms or '/BODS/RFC_READ_TABLE2' in fms:
        if '/SAPDS/RFC_READ_TABLE2' not in working_fms and '/BODS/RFC_READ_TABLE2' not in working_fms:
            missing_ds = True

    # Data Services is Required.
    missing_bbp = False
    if 'BBP_RFC_READ_TABLE' in fms and 'BBP_RFC_READ_TABLE' not in working_fms:
        missing_bbp = True

    # Error
    if missing_ds or missing_bbp:
        if missing_ds and missing_bbp:
            message = "Data Services (/SAPDS/RFC_READ_TABLE2 or /BODS/RFC_READ_TABLE2) and BBP_RFC_READ_TABLE are " \
                      "required to perform this extraction.  However, you do not have access to use these modules.  SAP" \
                      " returnd the following errors:\n\n{}\n\n{}".format(ds_errors[0], bbp_errors[0])

        elif missing_ds:
            message = "Data Services (/SAPDS/RFC_READ_TABLE2 or /BODS/RFC_READ_TABLE2) is required to complete this " \
                     "extraction.  However, you do not have the necessary authorization(s) to use it.  SAP returned " \
                     "the following error:\n\n{}".format(ds_errors[0])
        elif missing_bbp:
            message = "BBP_RFC_READ_TABLE is required to complete this " \
                     "extraction.  However, you do not have the necessary authorization(s) to use it.  SAP returned " \
                     "the following error:\n\n{}".format(bbp_errors[0])

        else:
            message = ""

        if working_fms:
            return working_fms[0], message
        else:
            return [], message

    return working_fms[0], None

    #Given the FU not found is the only error we pass on, if nothing has been returned
    #at this point, the module isn't in the environment
    # raise Exception("Function modules: {} not available in environment".format(fms))


def where_clause_rfc_format(where_clause: List[str], char_lim=72):
    """Formats the where clause when doing a nested or split table extraction"""

    # Check for empty where clause
    if not where_clause:
        return []

    # Check if there is a blank value and the remainder of clause is empty
    if not where_clause[0] and len(where_clause) == 1:
        return []

    # Check if first value of where clause is empty and remove prior to formatting
    if not where_clause[0]:
        where_clause.pop(0)

    where_clause_formatted = ' AND '.join(where_clause).lstrip().rstrip()

    pattern = re.compile(r"""((?:[^'"]|'[^']*'|"[^"]*")+)""")

    split_wheres = pattern.split(where_clause_formatted)[1::2]
    long_where = ' '.join(split_wheres)

    # Split the single where clause into many lines for SAP RFC

    # First split out each AND clause onto its own line
    split_where = long_where.split(' AND ')
    # Add the 'AND' back in to the beginning of each non-first line
    for index, row in enumerate(split_where):
        if index != len(split_where) - 1 and row:
            row = row + ' AND '
        split_where[index] = row

    # Split the 'IN ()' clauses into one-item-per-row
    final = []
    for row in split_where:
        # Find the start of the IN keyword if applicable
        try:
            index = row.index(' IN (')
        except ValueError:
            final += [row]
            continue

        # Make sure the IN clause is well-formed
        assert row.endswith(')'), (
            'Invalid IN clause found in SAP RFC where options: {}'
            ).format(row)

        # Split the IN clause into one-item-per-row
        condition = row[:index+5]
        valuestring = row[index+5:]

        lines = [condition]

        values = valuestring.split(',')
        for index, value in enumerate(values):
            if index != len(values) - 1:
                value = value + ','
            lines += [value.strip()]

        final += lines

    # Make sure that each line in the output is now less than max
    for row in final:
        assert len(row) < char_lim, (
            'SAP where clause row is over the {} character limit: {}'
            ).format(char_lim, row)

    return final
