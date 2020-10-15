"""Module to read ABAP ecf and generate a parameter csv file
   and read the ABAP *.fil files and pass them to data stream"""

from datetime import datetime
import glob
import os
import tokenize
from typing import Dict

from .base import ABCMessenger

from ..ecfreader import read_encrypted_json


class ABAPMessenger(ABCMessenger):
    """Messenger to handle a folder of *.fil files as input."""

    def __init__(self, folder: str, ecf_requestid: str):
        """Return a new Messenger after validating the folder contents."""
        super().__init__()
        self._conn = None
        self.folder = folder

        # Tracking for in-progress extractions
        self.index = 0
        self.arg_queue = []

        # Convert the file location to ACL file name
        os.chdir(folder)
        file_acl = glob.glob("*.acl")
        self.prefix = file_acl[0].split('.')[0]
        self.project_file = os.path.join(folder, self.prefix + '.acl')

        self.ecf_requestid = ecf_requestid
        # self.validate_input(ecf_requestid)

    def get_table_def(self, table_name):
        """Get table definition directly from ACL project file.
        NOTE: This is really messy code... need to refactor this
        """
        f = 0
        block_len = 0
        table_def = []
        final = []
        n = []

        with tokenize.open(self.project_file) as token:
            file_encoding = token.encoding

        with open(self.project_file, encoding=file_encoding) as openfile:
            for line in openfile:
                if line[:7] == '^LAYOUT' and line[10:45].strip() == table_name:
                    f = 1
                if f:
                    if line.find(' AS ') != -1:
                        table_def.append(line.strip().strip('\r\n'))
                if line[:7] == '^LAYOUT' and line[10:45].strip() != table_name:
                    f = 0

        for i in table_def:
            temp = i.split(' ')
            for k in temp:
                if k != '':
                    n.append(k)
            final.append(n)
            n = []

        for i in final:
            block_len += int(i[3])

        final.append(block_len)
        return final

    def list_all_tables(self) -> list:
        """Return a list of tables that exist in this ABAP folder."""
        with tokenize.open(self.project_file) as token:
            file_encoding = token.encoding

        with open(self.project_file, encoding=file_encoding) as openfile:
            tables = [line[10:45].strip() for line in openfile
                      if line[:7] == '^LAYOUT']

        return sorted(tables)

    def process_table(self, chunk, table_def):
        """Parse individual fields for each chunk.
        Parsing is based on offsets retrieved from ACL file.
        Returns number of rows processed and chunks.
        """
        row_count = 0
        row = []
        chunk_data = []

        with tokenize.open(chunk) as token:
            file_encoding = token.encoding

        with open(chunk, encoding=file_encoding) as fil_file:
            for line in fil_file:
                row_count += 1
                for fields in table_def[:-1]:
                    start_pos = int(fields[2]) - 1
                    end_pos = int(fields[2]) + int(fields[3]) - 1
                    temp_field = line[start_pos:end_pos].strip()
                    row.append(temp_field)
                chunk_data.append(row)
                row = []

        #check if a fil file is empty and create a table with blank record
        if os.stat(chunk).st_size == 3:
            for line in range(len(table_def)):
                row.append('')
            chunk_data.append(row)

        return chunk_data, row_count

    def get_key(self, filename):
        """Return sort key to properly sort our directory listing.
        NOTE: from StackOverflow... but no attribution was saved.
        """
        file_text_name = os.path.splitext(os.path.basename(filename))
        file_last_num = os.path.basename(file_text_name[0]).split('_')
        return int(file_last_num[-1])

    def get_table_chunks(self, table_name):
        """Get the individual chunk files from the directory."""
        chunk_prefix = self.project_file.upper().split('.ACL')[0]
        table_fil = '_'.join([chunk_prefix, table_name, '*']) + '.fil'
        chunks = glob.glob(table_fil)
        return sorted(chunks, key=self.get_key)

    def read_ecf_data(self) -> Dict[str, str]:
        """Return ECF-related metadata from the ABAP log files.

        RETURNS:
            - REQUESTID: From the `*_log.txt` file (bcglog value is truncated
                so we have to get it from the other log)
            - STRIPEKEY: From the end of the `*_bcglog.txt` file
            - SCHEMA: From the end of the `*_bcglog.txt` file
        """
        # Read the ecf data from log file
        chunk_prefix = self.project_file.upper().split('.ACL')[0]
        logfile = '_'.join([chunk_prefix, 'bcglog']) + '.txt'
        logfile_2 = '_'.join([chunk_prefix, 'log']) + '.txt'
        with open(logfile, "r") as openfile:
            filelines = openfile.readlines()

        count = 0
        row_found = 0
        ecfinfo = {}
        for line in filelines:
            count = count + 1
            if "Table:" in line:    # if "Field" can also be done
                row_found = count

                tab_position1 = line.find(":")
                tab_position2 = line.find("\n")
                key = line[tab_position1+1:tab_position2]

            if count == row_found + 1 and "Field:" in line:
                tab_position1 = line.find(":")
                tab_position2 = line.find("\n")
                value = line[tab_position1+1:tab_position2]
                row_found = 0
                if value:
                    ecfinfo[key] = value

        with open(logfile_2, "r") as openfile:
            filelines = openfile.readlines()

        req_id = None
        for line in filelines:
            if "REQUESTID" in line and req_id == None:
                tab_position2 = line.find("\n")
                req_id = line[40:tab_position2]

        ecfinfo['REQUESTID'] = req_id
        return ecfinfo

    def get_meta_data(self, table_name):
        """Get the metadata file from the directory."""
        chunk_prefix = self.project_file.upper().split('.ACL')[0]
        metadata_fname = '_'.join([chunk_prefix, table_name, 'header']) + '.txt'
        metadata_file = glob.glob(metadata_fname)
        return metadata_file

    def process_meta_data(self, table_name):
        row_count = 0
        metadata = []

        #get ecfdata
        ecfinfo = self.read_ecf_data()
        source_schema = ""
        stripekey = ""

        #get metadata from header.txt file
        prefix = self.project_file.upper().split('.ACL')[0]
        file_name = '_'.join([prefix, table_name, 'header']) + '.txt'
        with open(file_name, "r") as myfile:
            filelines = myfile.readlines()
        config = {}
        for line in filelines:
            row_count += 1
            ls_line= line.split()
            if row_count != 1:
                config = {
                    'datetime' : datetime.now().strftime("%Y-%m-%d_%H-%M-%S"),
                    'sourceSystem': 'SAP',
                    'source_schema':  source_schema,
                    'sourceTableName': table_name,
                    'source_table_alias': table_name,
                    'sourceFieldName' : ls_line[0],
                    'sourceType' : ls_line[7],   # ABAP datatype
                    # check if longer datatype needs to be included as well
                    'sourceFieldLength' : ls_line[4],  # Output length
                    'sourceFieldNumericPrecision' : ls_line[5], #metadata[Dez.-stellen]
                    'source_field_nullable' : None,
                    'targetTableName' : table_name,
                    'targetFieldName' : ls_line[0],
                    'targetType' : 'TEXT', # or ls_line[6]
                    #'shardKey' : ?
                    #'isKey' : ?
                    'stripeKey' : stripekey,
                    'sqlite_datatype' : 'TEXT'
                    }
            if bool(config) is True:
                metadata += [config]
        return metadata

    def process_ecf_meta_data(self, metadata_file, table_name):
        ecf_metadata = []

        #get ecfdata
        ecfinfo = self.read_ecf_data()
        source_schema = ""
        stripekey = ecfinfo['STRIPEKEY']
        requestid = ecfinfo['REQUESTID']

        config = {
            'requestID': requestid,
            'schemaName':  source_schema,
            'tableName': table_name,
            'stripeKey': stripekey,
            'sourceRecordCount': None,
            'targetRecordCount': None,
            'difference' : None,
        }

        ecf_metadata += [config]
        return ecf_metadata

    def validate_input(self):
        """
        1. Is the ACL file present in the output folder and the count should only be 1
        2. Is the bcglog file present in the output folder and the count should only be 1
        3. All the file names in the folder begin with the same uniqueID and output folder
            is named in the format 'abap_uniqueid'
        4. At least one actual table data file is present in the folder i.e.
           at least one .fil file is present.There can be multiple fil files present
        5. Is header.txt file present for each SAP table i.e. for each new/unique .fil file
           there should be one and only one header .txt file
        6. Does the RequestID in the initial ECF match the RequestID in log file from ABAP
        """
        # Validations related to .ACL file
        ecf_requestid = self.ecf_requestid
        fileformat = os.path.join(self.folder, '*.ACL')
        acl_files = glob.glob(fileformat)
        assert len(acl_files) == 1, \
            'Exactly one ACL file should be present in the directory'

        # Validations related to bcglog file
        fileformat = os.path.join(self.folder, '*bcglog.txt')
        bcglog_files = glob.glob(fileformat)
        assert len(bcglog_files) == 1, \
            'Exactly one bcglog file should be present'

        # Validatons related to unique ID in filenames
        all_files_format = self.folder + '\\' + '*'
        correct_format = self.folder + '\\' + self.prefix + '*'
        all_files = glob.glob(all_files_format)
        correct_format_files = glob.glob(correct_format)
        assert len(all_files) == len(correct_format_files), (
            'all filenames in the directory should have the same prefix'
        )

        # Validations related to .fil files
        fileformat = self.folder + '\\' + '*' + '.fil'
        fil_files = glob.glob(fileformat)
        assert fil_files, (
            'No .FIL files found in the specified folder. '
            'Either the ECF returned 0 records or the .FIL '
            'files are missing in the specified folder'
        )

        # Get the list of tables from the ACL project file
        for table in self.list_all_tables():
            fileformat = self.folder + '\\' + '*' + table + '_header.txt'
            header_files = glob.glob(fileformat)
            assert len(header_files) == 1, \
                '{} should have exactly one header file'.format(table)

        # Validate RequestID from ECF file matches ABAP log file
        fileformat = os.path.join(self.folder, '*log.txt')

        chunk_prefix = self.project_file.upper().split('.ACL')[0]
        logfile_2 = '_'.join([chunk_prefix, 'log']) + '.txt'

        with open(logfile_2, 'r') as bcg_content:
            bcg_lines = bcg_content.readlines()

        bcg_ctr = 0
        bcg_reqid = None
        request_id_match = False
        for bcg_line in bcg_lines:
            bcg_ctr += 1

            if bcg_reqid is None:
                if 'REQUESTID' in bcg_line.upper():
                    tab_position2 = bcg_line.find("\n")
                    bcg_reqid = bcg_line[40:tab_position2]
                    if ecf_requestid == bcg_reqid:
                        request_id_match = True
                        break

        if not request_id_match:
            raise AssertionError('ECF RequestID does not match the RequestID '
                        'found in ABAP output log file')


    def get_metadata_from_query(self, table) -> list:
        """Return standard dictionary of metadata from a table."""
        return self.process_meta_data(table)

    def begin_extraction(self, metadata, chunk_size=None):
        """Prepare to pull data from the folder for this table."""
        self.arg_queue = []
        self.index = 0
        table_def = self.get_table_def(metadata.target_table)
        for chunk in self.get_table_chunks(metadata.target_table):
            self.arg_queue += [(chunk, table_def)]

    def continue_extraction(self, chunk_size=None):
        """Continue data extraction from this table until complete."""
        try:
            chunk, table_def = self.arg_queue[self.index]
        except IndexError:
            return None

        data, _ = self.process_table(chunk, table_def)

        self.index += 1
        return data

    def finish_extraction(self):
        """Extraction complete, delete references to old data."""
        del self.arg_queue


class ABAPInputGenerate(object):
    """Reads ECF file provided by the user and generates
       a parameter file which is later used by ABAP"""

    def __init__(self, ecf_location: str, generated_file: str):
        """ Read the file location of input ECF and
            output parameter csv file"""

        self.ecf_location = ecf_location
        self.generated_file = generated_file
        self.tables = []
        self.tables_fields = {}
        self.param_dict = {}
        self.relation_dict = {}
        self.ecfinfo_dict = {}
        self.added = []
        self.file_data = []
        self.free_sel_dict = {}
        self.warnings = []
        self.written = []

    def read_ecf_file(self):
        """reading the data from the ECF"""
        initial_data = read_encrypted_json(self.ecf_location, encrypted=True)

        # Validate that complex nested queries are not passed through from ECF
        queries = initial_data["Queries"]
        children = []

        for query in queries:
            child = query['Name']
            if query['ParentSplitInfo']:
                children.append(child)

        for query in queries:
            if 'ParentSplitInfo' in query:
                if query['ParentSplitInfo']:
                    for parent in query['ParentSplitInfo']:
                        if parent['ParentTableName'] in children:
                            raise Exception("PwC X-TRACT (ABAP) does not support multiple levels of nesting.")

        return initial_data

    def read_param_relation(self):
        """reading parameters and relation information"""

        initial_data = self.read_ecf_file()
        queries = initial_data["Queries"]

        param = []
        param_list = []
        relation_list = []

        for query in queries:
            Table_Name1 = query["Name"]
            self.tables.append(query["Name"])

        	# populate self.tables_fields dictionary
            Fields = query["Columns"]
            # Fields_list = Fields.split(',')
            self.tables_fields[Table_Name1] = Fields

        	# populate self.param_dict from Parameters in ECF
            parameters = query.get('Parameters')
            if parameters:
                for parameter in parameters:
                    # perform validations
                    for param_val in parameter["Values"]:
                        assert ';' not in param_val, 'ABAP does not support ; in ECF file'
                    param_temp = parameter["Operation"]
                    assert 'LIKE' not in param_temp.upper(),\
                        'ABAP does not support like keyword in where clause'

                    param.append(parameter["Name"])
                    if parameter["Operation"] == 'IN':
                        if len(parameter["Values"]) != 1:
                            value_list = parameter["Values"]
                            str_value = tuple(value_list)
                            #value_tuple = tuple(value_list)
                            #str_value = str(value_tuple)
                        else:
                            value_list = parameter["Values"]
                            str_value = "('{}')".format(value_list[0])
                        param.append(str_value)
                        param.append('')
                    elif parameter["Operation"] == 'BETWEEN' or parameter["Operation"] == 'RANGE':

                        if parameter['Type'].upper() == 'DATE':

                            start = parameter["Values"][0]
                            end = parameter["Values"][1]

                            try:
                                start = datetime.strptime(start, '%Y%m%d')
                            except:
                                try:
                                    start = datetime.strptime(start, '%Y-%m-%d')
                                except:
                                    raise Exception("Could not parse date value in ECF.  Acceptable formats include 'YYYY-MM-DD' or 'YYYYMMDD' ")

                            try:
                                end = datetime.strptime(end, '%Y%m%d')
                            except:
                                try:
                                    end = datetime.strptime(end, '%Y-%m-%d')
                                except:
                                    raise Exception("Could not parse date value in ECF.  Acceptable formats include 'YYYY-MM-DD' or 'YYYYMMDD' ")

                            param.append(start.strftime('%Y%m%d'))
                            param.append(end.strftime('%Y%m%d'))

                        else:
                            param.append(parameter["Values"][0])
                            param.append(parameter["Values"][1])
                    else:
                        param.append(parameter["Values"])
                        param.append('')
                    param.append(parameter["Operation"])
                    param_list += [param]
                    param = []
                self.param_dict[Table_Name1] = param_list
                param_list = []

            # populate self.relation_dict from ParentInfo in ECF
            parent_info = query.get('ParentSplitInfo')            
            if parent_info:
                for parent in parent_info:
                    relation_list.append('ParentTable')
                    relation_list.append(parent["ParentTableName"])

                    join_fields = parent_info[0]['SplitFields']
                    
                    parent_fields = []
                    child_fields = []
                    for field in join_fields:
                        parent_fields.append(field['ParentField'])
                        child_fields.append(field['ChildField'])

                    relation_list.append('ParentFields')
                    relation_list.append((',').join(parent_fields).split(', '))
                    relation_list.append('ChildFields')
                    relation_list.append((',').join(child_fields).split(', '))

                self.relation_dict[Table_Name1] = relation_list
                relation_list = []

    def read_additional_info(self):
        """populate self.ecfinfo_dict"""
        initial_data = self.read_ecf_file()
        self.ecfinfo_dict["RequestId"] = initial_data["RequestId"]
        self.ecfinfo_dict["SchemaName"] = initial_data["SchemaName"]
        if 'PwCSetId' in initial_data:
            self.ecfinfo_dict["PwCSetId"] = initial_data['PwCSetId']
        else:
            self.ecfinfo_dict["PwCSetId"] = initial_data['SetId']

    def build_file_data(self):
        """Building the file data based on the ECF info"""

        self.read_param_relation()
        self.read_additional_info()

        where_clause = []
        for key in self.param_dict:
            for row in self.param_dict[key]:
                temp = key + '-' + row[0]
                where_clause.append(temp)

        self.added = []
        row2 = []
        to_be_added_list = []
        self.file_data = []

        #list of parameters
        param_field_dict = {}

        for key_1, values_1 in self.param_dict.items():
            temp_list = []
            for row_1 in values_1:
                if row_1:
                    temp_list.append(row_1[0])
            param_field_dict[key_1] = temp_list

        #Check if fields in param_field_dict are present in tables_fields dict
        for i_key in self.tables_fields.keys():
            if i_key in param_field_dict.keys():
                for i_row in param_field_dict[i_key]:
                    if i_row not in self.tables_fields[i_key]:
                        message = (
                            "Invalid Parameter condition skipped: Parameter Field {row} "
                            "is not present in the field-list of table {key}"
                            ).format(row=i_row, key=i_key)
                        self.warnings.append(message)

        for key in self.tables_fields:
            for row in self.tables_fields[key]:
                for key1 in self.param_dict:
                    for row1 in self.param_dict[key1]:

                        if key == key1 and row == row1[0]:
                            if row1[3] == 'IN':
                                self.no_param(key, row)
                                self.handle_in_case(key, row, row1)
                            elif row1[3] == 'BETWEEN' or row1[3] == 'RANGE':
                                self.handle_between_case(key, row, row1)
                            else:
                                self.handle_else_case(key, row, row1)

                        elif (key + '-' + row not in self.added) and \
                             (key + '-' + row not in where_clause):
                            self.no_param(key, row)

                # when paramters list is an [] empty list
                if not self.param_dict:
                    if (key + '-' + row not in self.added) and \
                        (key + '-' + row not in where_clause):
                        self.no_param(key, row)
        
        #Move free selections to beginning of table list
        free_cntr = 0
        sorted_data = []
        table_list = []
        for item in self.file_data:

            #Initial table
            if not table_list:
                table_list.append(item)

                if item[0].split(';')[1] == '@':
                    free_cntr +=1

            #Check if still on current table
            elif item[0].split(';')[0] == table_list[len(table_list)-1][0].split(';')[0]:

                #If field signifies a free selection, add the line to the beginning of the table list. 
                #Track location of free selections so that additional lines are adding in the right order. 
                if item[0].split(';')[1] == '@':
                    table_list.insert(free_cntr,item)
                    free_cntr +=1
                else:
                    table_list.append(item)
            else:
                #Add table list to data and reset free counter
                sorted_data += table_list
                free_cntr = 0
                
                #Start new table
                table_list = []
                table_list.append(item)
                if item[0].split(';')[1] == '@':
                    free_cntr +=1
        
        #Add final table to sorted data
        sorted_data += table_list
        
        #Update self.file_data with the new sorted list. 
        self.file_data = sorted_data

    def no_param(self, key, row):
        """When fields have no parameters"""

        line = [';'.join([key, row, '', ''])]
        data = key + '-' + row
        self.added.append(data)
        self.file_data.append(line)

    def handle_in_case(self, key, row, row1):
        """handling IN case in parameters
        The content of all columns is limited to 72 characters. 
        This includes the tablename, fieldname, operator
        and values."""
        
        #Using 40 to be safe. SAP seems to be under the illiusion that characters exist
        #that don't. We can't rely on the len of the lines in python.
        max_line_len = 40

        temp_1 = ' ' + row1[3] + ' '
        temp = row1[1]

        chunked_list = []
        chunked_lists = []
        column_values = key + row + str(row1[1]) + row1[3]

        if len(column_values) >= max_line_len:

            for i_value in temp:
                chunked_list.append(i_value)
                in_string = str(chunked_list) + key + row + row1[3]
                if len(in_string) >= max_line_len:
                    if chunked_list:
                        chunked_lists.append(chunked_list)
                        chunked_list = []

            # needed at the end outside for loop
            if chunked_list:
                chunked_lists.append(chunked_list)

            i_ctr = 1
            last_ctr = len(chunked_lists)

            # chunked_lists = [['1000', '2000', '3000', '4000'],['5000']]
            # When there are two many elements in an IN statement, they need to broken up into multiple lines.

            for i_list in chunked_lists:

                i_tuple = tuple(i_list)
                i_string = str(i_tuple)

                # Remove trailing comma if there is only one element in the tuple.
                if len(i_list) == 1:
                    i_string = i_string.replace(",)", ")")

                # It is the first grouping.
                if i_ctr == 1:
                    if key in self.free_sel_dict.keys():
                        if self.free_sel_dict[key] == 'X':
                            i_third_col = ' AND ' + '( ' + row + ' IN ' + i_string
                    else:
                            i_third_col = '( ' + row + ' IN ' + i_string

                    if last_ctr == i_ctr:
                        i_third_col = i_third_col + ' )'

                    line = [str(';'.join([key, '@', i_third_col, row1[2]]))]

                    self.file_data.append(line)

                # It is not the first grouping.
                else:
                    if last_ctr == i_ctr:
                        i_third_col = ' OR ' + row + ' IN ' + i_string + ' )'
                    else:
                        i_third_col = ' OR ' + row + ' IN ' + i_string
                    line = [str(';'.join([key, '@', i_third_col, row1[2]]))]

                    self.file_data.append(line)
                i_ctr += 1
                self.free_sel_dict[key] = 'X'

        else:
            values_in_str = str(temp)
            line = [str(';'.join([key, row, temp_1+values_in_str, row1[2]]))]

            self.file_data.append(line)

        temp = ''
        data = key + '-' + row

        self.added.append(data)

    def handle_between_case(self, key, row, row1):
        """handling BETWEEN case in parameters """

        temp = ''
        temp_1 = row1[1]
        line = [';'.join([key, row, temp_1+temp, row1[2]])]
        temp = ''
        data = key + '-' + row
        self.added.append(data)
        self.file_data.append(line)

    def handle_else_case(self, key, row, row1):
        """handling remaining case in parameters """
        [temp_2] = row1[1]
        temp = "'"+temp_2+"'"
        temp_1 = ' '+ row1[3] + ' '
        line = [';'.join([key, row, temp_1+temp, row1[2]])]
        temp = ''
        data = key + '-' + row
        self.added.append(data)
        self.file_data.append(line)

    def build_excel_data(self):
        """modify file_data and add relation info, source:self.relation_dict """

        self.build_file_data()
        exceldata = []
        child_fields_dict = {}

        for [i] in self.file_data:
            for key2 in self.relation_dict:
                row2 = self.relation_dict[key2]

                # if key2 == i[0:4]:
                if key2 == i[0:4] and row2:
                    ParentsFields = row2[3]
                    ParentsFieldsList = ParentsFields[0].split(',')
                    ChildFields = row2[5]
                    ChildFieldsList = ChildFields[0].split(',')
                    child_fields_dict[key2] = ChildFieldsList

                    if i[5:10] in ChildFieldsList:
                        tab_position = ChildFieldsList.index(i[5:10])
                        to_be_added = '-'.join([row2[1], ParentsFieldsList[tab_position]])
                        temp = i[0:11] + ' ' + '=' + ' ' + to_be_added + ';'
                        i = temp
                        temp = None
                        exceldata.append([i])
                        self.written.append([i])

            # to exclude lines already added i.e. ones with relation info
            if [i] not in self.written:
                exceldata.append([i])

        if not self.relation_dict:
            exceldata = self.file_data

        #Check if fields in child_fields_dict are present in tables_fields dict
        for i_key2, i_values in child_fields_dict.items():
            for i_child in i_values:
                if i_child not in self.tables_fields[i_key2]:
                    message = "Join Skipped: Invalid Join Field "+i_child+" in table "+i_key2
                    self.warnings.append(message)

        return exceldata

    def write_additional_info(self):
        """info from ecf which will be used to create metadata"""

        exceldata = self.build_excel_data()
        temp = [';'.join(['RequestID;', self.ecfinfo_dict["RequestId"]]) + ';']
        exceldata.append(temp)
        temp = None

        temp = [';'.join(['stripeKey', self.ecfinfo_dict["PwCSetId"]]) + ';' + ';']
        exceldata.append(temp)
        temp = None

        temp = [';'.join(['Schema', self.ecfinfo_dict["SchemaName"]]) + ';' + ';']
        exceldata.append(temp)
        temp = None

        return exceldata

    def create_relation_graph(self):
        """logic for creating parent-child relationship graph"""

        initial_data = self.read_ecf_file()
        queries = initial_data["Queries"]
  
        rel_graph = dict()
        for node in queries:
            rel_graph[node['NameAlias']] = {}
            node_keys = node.keys()
            if 'ParentSplitInfo' in node_keys:
                for p_node in node['ParentSplitInfo']:
                    rel_graph[node['NameAlias']].update({
                        p_node['ParentTableAlias']: {},
                    })

        parent_tables = {}
        for g_key, g_val in rel_graph.items():
            if g_val:
                for gg_key, gg_val in g_val.items():
                    parent_tables[gg_key] = g_key

        one_data = []
        two_data = []
        written_data = []
        finaldata = []

        exceldata = self.write_additional_info()
        for p_key, p_val in parent_tables.items():
            for [e_line] in exceldata:
                position = e_line.find(";")
                if e_line[0:position] == p_key:
                    one_data.append([e_line])
                    written_data.append([e_line])
                if e_line[0:position] == p_val:
                    two_data.append([e_line])
                    written_data.append([e_line])
            finaldata += one_data
            finaldata += two_data

        for [e_line] in exceldata:
            if [e_line] not in written_data:
                finaldata.append([e_line])
        return finaldata

    def create_parameter_file(self):
        """write data to output csv file"""

        finaldata = self.create_relation_graph()
        header_line = 'Tablename;Fieldname;Value/From;To'
        with open(self.generated_file, 'w', newline='') as csvfile:
            csvfile.write(header_line)
            for i in finaldata:
                csvfile.write("\n"+",".join(map(str, i)))
        return self.warnings


if __name__ == '__main__':

    # g = ABAPInputGenerate(
    #     # r'C:\Users\bschwab003\Downloads\ddf810a4-d482-4c34-af3f-322c24f9a931.ecf',
    #     r'C:\Users\bschwab003\Downloads\Danfoss AS_SAP ECC 6.0 - P08_Extraction_20180301_095147.ecf',
    #     r'C:\Users\bschwab003\Downloads\dd.csv'
    # )
    # g.create_parameter_file()

    ABAPMessenger(r'C:\Users\bschwab003\Documents\ECFs\abap-bkpf-bseg', '5d4c1e2b-6279-4164-96cc-ba2cbkpfbse1')