"""Module to read and parse data from a PwC Extraction Config File (ECF)."""

from __future__ import unicode_literals
import base64
from collections import namedtuple
from datetime import datetime
import hashlib
import json
import math
import os
import sys
from typing import Dict, List

import chilkat

import config

JSONDict = Dict[str, object]  # pylint: disable=invalid-name
QueryData = namedtuple(
    'QueryData',
    ('tablename', 'table_alias', 'text')
)


class ExtractData(object):
    """Light weight class to keep track of meta data for packaging"""

    def __init__(self, ecfjson: JSONDict, table_name: str,
                 table_alias: str, query_text: str, ecf_filename: str):
        """Create a new set of ExtractData for data extraction."""
        self.ecfjson = ecfjson
        self.table_name = table_name
        self.table_alias = table_alias
        self.query_text = query_text
        self.ecf_filename = ecf_filename
        # Shortcut mappings and quick validation that keys exist in ECF
        self.request_id = ecfjson['RequestId']
        if 'PwCSetId' in ecfjson:
            self.set_id = ecfjson['PwCSetId']
        else:
            self.set_id = ecfjson['SetId']
        self.tags = ecfjson['AdditionalTags']
        self.publickey = ecfjson['PublicKey']
        self.channel = ecfjson['ChannelLos']
        self.client = ecfjson['ClientName']
        self.database = ecfjson['DatabaseName']
        self.server = ecfjson['DatabaseServerName']
        self.port = ecfjson['DatabasePort']
        self.schema = ecfjson.get('SchemaName')
        self.territory = ecfjson['Territory']
        self.destination = ecfjson['DataDestination']


class V20Table(object):
    """Class used to build v2.0 ECF structure during parsing of v1.6 ECF."""
    def __init__(self, name, namealias):
        self.name = name
        self.namealias = namealias
        self.columns = []
        self.parameters = []
        self.meta = {}
        self.parent_split_info = []
        self.local_db_where = []

    def set_batch_size(self, size):
        """set attribute batch size"""
        self.meta["BatchSize"] = size

    def set_chunk_size(self, size):
        """set attribute chunk size"""
        self.meta["ChunkSize"] = size

    def set_columns(self, columns):
        """set attribute columns"""
        self.columns = columns

    def add_parent_split_info(self, name, alias, split_fields):
        """set attribute parent split info"""
        self.parent_split_info.append({
            "ParentTableName": name,
            "ParentTableAlias": alias,
            "SplitFields": split_fields
        })

    def add_parameter(self, name, type_, values, operation):
        """set attribute parameter"""
        self.parameters.append({
            "Name": name,
            "Type": type_,
            "Values": values,
            "Operation": operation
        })

    def add_local_db_where(self, name, alias, type_, values, operation):
        """set attribute local db where"""
        if self.parameters is None:
            self.parameters = []
        self.parameters.append({
            "Name": name,
            'NameAlias': alias,
            "Type": type_,
            "Values": values,
            "Operation": operation
        })

    def get_parameters(self):
        """get params from ECF"""
        return {"Parameters": self.parameters}

    def get_local_db_where(self):
        """get local db"""
        if self.local_db_where:
            return {"LocalDBWhereClause": self.local_db_where}
        return {"LocalDBWhereClause": None}

    def get_parent_split_info(self):
        """get parent spli t info"""
        return {"ParentSplitInfo": self.parent_split_info}

    def get_meta(self):
        """collect meat key value from ECF"""
        return {"Meta": self.meta}

    def get_basics(self):
        """collect basic ecf info"""
        return {
            "Name": self.name,
            "NameAlias": self.namealias,
            "Type": "TABLE",
            "Columns": self.columns,
            "WhereClause": ""
        }

    def as_json(self):
        """update temp dict for parse"""
        temp = {}
        temp.update(self.get_basics())
        temp.update(self.get_meta())
        temp.update(self.get_parameters())
        temp.update(self.get_parent_split_info())
        temp.update(self.get_local_db_where())
        return temp


def _parse_query(each_query: dict, parent: str = None,
                 parent_alias=None) -> V20Table:
    """
    This is a private function that converts an individual query in
    "Queries" section of v1.6 ECF into an instance of V20Table() object.
    :param each_query: (dict) An individual query in "Queries" section of v1.6 ECF
    :param parent: (str) Table name of the parent (if applicable)
    """
    v20 = V20Table(each_query["Name"], each_query['NameAlias'])
    v20.set_columns(each_query["Script"].split(","))
    v20.set_batch_size(each_query["Meta"]["BatchSize"])
    v20.set_chunk_size(500000)

    # Translate Parameters.
    if each_query["Parameters"]:
        for each_parameter in each_query["Parameters"]:
            v20.add_parameter(
                each_parameter["Name"],
                each_parameter["Type"],
                each_parameter["Values"],
                each_parameter["Operation"]
            )
    # Translate Parent Split Info.
    if parent:
        fields = []
        for each_field_mapping in each_query["Meta"]["FieldParamMappings"]:
            fields.append({
                "ParentField": each_field_mapping["FromField"],
                "ChildField": each_field_mapping["ToField"],
                "UseParentFilter": each_field_mapping["IsLinkFilter"]
            })
        v20.add_parent_split_info(parent, parent_alias, fields)
    return v20


def _parse_sub_queries(query, parent, parent_alias, all_queries):
    """
    This is a private function that converts a list of queries from the
    "SubQueries" section of v1.6 ECF into an list of of V20Table() objects.
    This is a recursive function and all the queries found in "SubQueries"
    will be converted into V20Table() object and put in the all list
    :param query:  (list[dict]) An list of queries in "SubQueries" section of v1.6 ECF
    :param parent:  (str) Table name of the parent (if applicable)
    :param all:  (list[V20Table()]  A list of v20Table() objects.
    :return:   (list[V20Table()]  A list of v20Table() objects.
    """
    if parent:
        all_queries.append(_parse_query(query, parent, parent_alias))
    else:
        all_queries.append(_parse_query(query, None, None))
    if query["SubQueries"]:
        for each_sub in query["SubQueries"]:
            _parse_sub_queries(each_sub, query["Name"],
                               query["NameAlias"], all_queries)
    return all_queries


def _aggregate(tables):
    """function to consolidate tables in the 2.0 structure"""

    unique_tables = {}
    duplicates = []
    for each in tables:
        if each.namealias not in unique_tables:
            unique_tables[each.namealias] = each
        else:
            duplicates.append((each.name, each.namealias,
                               each.get_parent_split_info()))
    for name, _, parent_split_info in duplicates:
        for each in parent_split_info["ParentSplitInfo"]:
            unique_tables[name].add_parent_split_info(each["ParentTableName"],
                                                      each["ParentTableAlias"],
                                                      each["SplitFields"])

    return [v for k, v in unique_tables.items()]


def _convert_to_queries_dict(tables):
    """
    A private function that converts a list of v20Table() objects and returns a single dictionary.
    :param tables: (list[v20Table())  A list of v20Table() objects.
    :return: (dict)  A JSON compliant v2.0 ECF.
    """
    queries = {"Queries": [v.as_json() for v in tables]}
    conv_query = json.dumps(queries)
    return conv_query


def _translate_individual_query(query):
    """
    Private function to translate the an individual query from "Queries"
    section formatted in v.1.6 structure into a v20Table object.
    This function will also aggregate/consolidate nested tables that have more than one parent.
    :param query: (dict) Individual query from the "Queries" section formatted in v.1.6 structure.
    :return: (v20Table()) v20Table object.
    """
    # Parse ECF, creating V20Table object for each query/subquery.
    tables = _parse_sub_queries(query, None, None, [])
    # Look for nested tables being extracted more than once with different parents.
    # Aggregate the ParentSplitInfo for each parent so the table is only extracted once.
    tables = _aggregate(tables)
    return tables


def _translate_queries_section(queries_section):
    """
    Private function to translate the content of "Queries" section formatted
    in v.1.6 structure into a list of  v20Table objects.
    :param queries_section: (list) Content of "Queries" section formatted in v.1.6 structure.
    :return:  (dict) Content of "Queries" section formatted in v.2.0 structure.
    """
    tables = []
    for each_query in queries_section:
        tables += _translate_individual_query(each_query)
    queries = {"Queries": [v.as_json() for v in tables]}
    return queries


def convert_16_to_20(unencrypted_v16_ecf: JSONDict) -> JSONDict:
    """
    Public function to convert an unencrypted v1.6 ECF to v2.0 ECF.
    :param v16_ecf: (dict) A JSON compliant v1.6 ECF.
    :return: (dict) A JSON compliant v2.0 ECF.
    """
    queries_section = unencrypted_v16_ecf.pop("Queries")
    unencrypted_v16_ecf.update(_translate_queries_section(queries_section))
    unencrypted_v16_ecf["EcfVersion"] = 2.0
    return unencrypted_v16_ecf


def get_ecf_meta_data(filepath: str, encrypted=True) -> List[ExtractData]:
    """Function to read and collect meta data from an ECF"""
    status = {}
    ecfjson = read_encrypted_json(filepath, encrypted=encrypted, status=status)
    if (status.get('is_encrypted', False)):
        validate_ecfjson(ecfjson)
        validate_license(ecfjson)
    flag = ' '

    if ecfjson['DataSource']['Application'] == 'SAP' and \
            ecfjson['DataSource']['DataConnector'] != 'ABAP':
        table_meta = get_sap_meta(ecfjson, ecfjson['EcfVersion'])

    elif ecfjson['DataSource']['Application'] == 'SAP' and \
            ecfjson['DataSource']['DataConnector'] == 'ABAP':
        all_data = get_abap_meta(ecfjson, filepath)
        flag = 'X'

    else:
        if "Queries" in ecfjson:
            if "Query" not in ecfjson["Queries"][0].keys():
                table_meta = get_sap_meta(ecfjson, ecfjson['EcfVersion'])
            else:
                table_meta = query_meta_from_ecfjson(ecfjson)
        else:
            table_meta = query_meta_from_ecfjson(ecfjson)

    if flag != 'X':
        all_data = []
        for table in table_meta:
            data = {
                'table_name': table.tablename,
                'table_alias': table.table_alias,
                'query_text': table.text,
                # NOTE: ecfjson used for SAP nesting, not written to sqlite
                'ecfjson': ecfjson,
                'ecf_filename': os.path.basename(filepath),
            }
            all_data += [ExtractData(**data)]

    # Validate that no duplicate aliases exist in this ECF
    aliases = [data.table_alias for data in all_data]
    assert len(set(aliases)) == len(aliases), (
        'Duplicate table aliases are defined in this ECF. '
        'Each query in an ECF must have a unique NameAlias. '
        'Received aliases:  {}'
        ).format(sorted(aliases))

    return all_data


def get_abap_meta(ecfjson: JSONDict, filepath: str):
    """Return ExtractData for an ABAP extraction from an ECF."""
    queries = ecfjson["Queries"]
    query_text = queries

    all_data = []
    for query in queries:
        data = {
            'table_name': query["Name"],
            'table_alias': query["NameAlias"],
            'query_text': query_text,
            # ecfjson is used later in nesting, not written to SQLite
            'ecfjson': ecfjson,
            'ecf_filename': os.path.basename(filepath),
        }
        all_data += [ExtractData(**data)]
    return all_data


def validate_ecfjson(ecfjson: JSONDict):
    """Validate the JSON data in the ECF.
    Raise an AssertionError if any of the validation checks fail.
    """

    # Validate keys that must be in all ECFs
    standard_keys = ('RequestId', 'Queries', 'ChannelLos', 'EcfVersion')
    for key in standard_keys:
        assert ecfjson.get(key), 'ECF must include a "{}" value.'.format(key)

    # Validate keys with multiple names and nested keys
    assert 'SetId' or 'PwCSetId' in ecfjson, 'ECF must include a "SetId"'
    assert ('DataSource' in ecfjson and
            'Application' in ecfjson['DataSource']), \
        'ECF must include an "Application" within the "DataSource"'

    # If requiring ECFs to included a public key, validate that too
    # if config.USE_PWC_ECF_PUBLIC_KEY:
    assert 'PublicKey' in ecfjson, 'ECF must include a "PublicKey"'
    assert chilkat.CkPublicKey().LoadBase64(ecfjson['PublicKey']), \
        'PublicKey value in ECF is invalid'

    # Check that the ECF is not expired if it is set to
    if ecfjson.get('ExpiryDate'):

        # Remove micro seconds
        if "." in ecfjson['ExpiryDate']:
            expiry = ecfjson['ExpiryDate'].split(".")[0]
        else:
            expiry = ecfjson['ExpiryDate']

        try:
            # If the expiry date
            if 'T' in expiry:
                expires = datetime.strptime(expiry, '%Y-%m-%dT%H:%M:%S')
            else:
                expires = datetime.strptime(expiry, '%Y-%m-%d')
        except Exception:
            expires = None

        if expires is not None:
            assert datetime.now() < expires, (
                'ECF expired on {}, please generate a new ECF for this request'
                ).format(expires.strftime('%Y-%m-%d'))

            ecfjson['ExpiryDate'] = expires.strftime('%Y-%m-%d')
        else:
            ecfjson['ExpiryDate'] = ""

    acceptable_fms = ["BBP_RFC_READ_TABLE"]
    for query in ecfjson['Queries']:
        if "FunctionModule" in query:
            assert query["FunctionModule"] in acceptable_fms, \
                'Invalid "FunctionModule" identified in ECF.  Acceptable options include: {}.  '\
                    .format(", ".join(acceptable_fms))

def query_meta_from_ecfjson(ecfjson: JSONDict) -> List[QueryData]:
    """Return SQL queries for extraction from JSON ECF data."""
    queries = ecfjson['Queries']
    assert queries, 'No queries found in ECF data: %s' % ecfjson
    parsed = [parse_query_data(data) for data in queries]
    return parsed


def get_sap_meta(ecfjson: JSONDict, ecf_version: str):
    """function to check if SAP ECF needs to be converted to 2.0 structure
        and to collect SAP metadata blob
    """
    #See if ECF needs to be converted to 2.0 structure
    if ecf_version == '1.6':
        ecfjson = convert_16_to_20(ecfjson)

    #Reject non 1.6
    elif ecf_version not in ['2.0', '1.6']:
        raise Exception('SAP ECF Version not supported in this build of Extract')

    #Set meta data for SAP
    return nest_metadata_from_ecfjson(ecfjson)


def nest_metadata_from_ecfjson(ecfjson: JSONDict) -> list:
    """Parse nested SAP queries and return the relevant meta data"""

    order_of_extract = _create_extraction_graph_order(ecfjson)

    #create template objects out of ECF
    parsed_queries = []

    #iterate through order and match to the correct ECF parameters
    for table in order_of_extract:
        for query in ecfjson['Queries']:
            if table == query['NameAlias']:
                validate_sap_rfc_ecf_input(query)
                tablename = query['Name']
                table_alias = query['NameAlias']

                #Create named tuple for ease of assignment in meta creation
                parsed = QueryData(tablename, table_alias, query)

                parsed_queries.append(parsed)

    return parsed_queries


def validate_sap_rfc_ecf_input(table_info: dict) -> None:
    """validates that the columns are unique within and ecf"""
    #Will only validate when it's a TABLE/BAPI extraction, will pass if it's a key report
    try:
        assert len(table_info['Columns']) == len(set(table_info['Columns'])), \
        'Duplicate columns within table {} in ECF.  '.format(table_info["Name"])
    except TypeError:
        pass


def parse_query_data(data: JSONDict) -> QueryData:
    """Return QueryData using a JSON dictionary of data from an ECF."""
    tablename = data.get('Name', None)
    table_alias = data.get('NameAlias', None)
    query = data.get('Script', data.get('Query', None))

    # Validate data in query is valid for Extract purposes
    assert 'SELECT ' in query.upper(), (
        'Invalid query found in ECF:  "{}"'
        ).format(query)

    parsed = QueryData(tablename, table_alias, query)
    return parsed


def read_encrypted_json(filepath: str, encrypted=True, status={}) -> JSONDict:
    """
    Read and return JSON data from an encrypted ECF or EPF file.
    
    Note:  This function had a serious flaw in that it would fail whenever the ECF was
    expected to be encrypted but it was actually not.  The correction was to make this 
    function "assume" the ECF is not excrypted and then it that fails "assume" it is encrypted
    and if that fails then raise an exception.  This function becomes fail-safe in that it only
    fails when the ECF cannot be read which means it is either plain-text or encrypted 
    and then either valid JSON or not.  
    
    The expectation, that the file be encrypted or not, is logically meaningless.  The fact
    is that the file is either plain-text or encrypted but it must always be valid JSON.
    """
    with open(filepath, 'rb') as ecf:
        rawdata = ecf.read()

    response = None
    try:
        decoded = rawdata.decode()
        response = json.loads(decoded)
        status['is_plaintext'] = True
    except:
        status['is_plaintext'] = False
        response = None
            
    if (not response):
        try:
            decoded = _chilkat_decrypt(rawdata)
            response = json.loads(decoded)
            status['is_encrypted'] = True
        except:
            status['is_encrypted'] = False
            response = None
            
    if (not response):
        raise Exception('The file "{filepath}" could not be decrypted. Make sure you '
                        'selected a valid v2.0 ".ecf" file generated by the PwC '
                        'ECF Generation service.'
                        .format(filepath=os.path.abspath(filepath)))
    
    return response
    

def chilkat_encrypt_with_cert(jsondata: str) -> bin:
    """Encrypt data for an EPF file using hardcoded PwC Cert key."""
    encrypt = chilkat.CkCrypt2()
    unlock_chilkat(encrypt)
    encrypt.put_CryptAlgorithm("pki")
    encrypt.Pkcs7CryptAlg = "aes"
    encrypt.KeyLength = 256

    cert = chilkat.CkCert()
    cert.LoadFromBase64(config.PWC_EPF_PUBLIC_KEY)
    encrypt.SetEncryptCert(cert)

    outbytes = chilkat.CkByteData()
    encrypt.EncryptString(jsondata, outbytes)

    encrypted = outbytes.getEncoded('base64')
    encoded = base64.b64decode(encrypted)
    return encoded


def chilkat_encrypt(jsondata: str, publickey: str) -> bin:
    """Encrypt data for an EPF file using Chilkat and public key from ECF."""
    pkobj = chilkat.CkPublicKey()
    assert pkobj.LoadBase64(publickey), pkobj.lastErrorText()

    rsa = chilkat.CkRsa()
    unlock_chilkat(rsa)
    rsa.put_EncodingMode("base64")
    assert rsa.ImportPublicKeyObj(pkobj), rsa.lastErrorText()

    encrypted = rsa.encryptStringENC(jsondata, False)
    decoded = base64.b64decode(encrypted)
    return decoded


def _chilkat_decrypt(rawdata: str):
    """Helper function to decrypt an ECF using chilkat"""
    decrypt = chilkat.CkCrypt2()
    decrypt.UnlockComponent("PWTRCP.CBX1118_yuLAXzPxmR8B")
    decrypt.put_CryptAlgorithm("aes")
    decrypt.put_CipherMode("cfb")
    decrypt.put_KeyLength(256)
    decrypt.put_PaddingScheme(0)
    decrypt.put_EncodingMode("base64")
    decrypt.SetEncodedKey(_pwc_passphrase(), "base64")
    encoded = base64.b64encode(rawdata).decode("utf-8")
    return decrypt.decryptStringENC(encoded)


def _pwc_passphrase() -> str:
    """Generate a passphrase that can be algorithmically recreated."""
    rounded_pi = round(math.pi, 10)
    if sys.version_info >= (3, 0):
        salted = str(round(rounded_pi * 133, 9))
    else:
        salted = str(rounded_pi * 133)
    salted = salted.encode('utf-8')
    hashed = hashlib.sha256(salted).hexdigest().upper()
    return hashed


def _create_extraction_graph_order(ecfjson: JSONDict) -> list:
    """function to create a directed graph of the extraction templates
        and order the graph based upon the order in which the templates
        should be extracted
    """
    dict_graph = dict()

    for node in ecfjson['Queries']:
        dict_graph[node['NameAlias']] = {}
        if node['ParentSplitInfo']:
            for p_node in node['ParentSplitInfo']:
                dict_graph[node['NameAlias']].update({
                    p_node['ParentTableAlias']: {},
                })

    return _order_sort(dict_graph)


def _order_sort(graph, reverse=True):
    """internal function to order the extraction such that nested tables
        are always behind their parent
    """
    # pylint: disable=invalid-name
    seen = set()
    order = []
    explored = set()

    for v in graph.keys():     # process all vertices in graph
        if v in explored:
            continue
        fringe = [v]   # nodes yet to look at
        while fringe:
            w = fringe[-1]  # depth first search
            if w in explored:  # already looked down this branch
                fringe.pop()
                continue
            seen.add(w)     # mark as seen
            # Check successors for cycles and for new nodes
            new_nodes = []
            for n in graph[w]:
                if n not in explored:
                    if n in seen:  # CYCLE !!
                        raise Exception("Graph contains a cycle.")
                    new_nodes.append(n)
            if new_nodes:   # Add new_nodes to fringe
                fringe.extend(new_nodes)
            else:           # No new nodes so w is fully explored
                explored.add(w)
                order.append(w)
                fringe.pop()    # done considering this node

    if reverse:
        return order

    return list(reversed(order))


def unlock_chilkat(component=None):
    """Unlock the Chilkat library using PwC credentials."""
    key = "PWTRCP.CBX1118_yuLAXzPxmR8B"
    if not component:
        component = chilkat.CkRsa()
    assert component.UnlockComponent(key), \
        "Failed to unlock Chilkat library."


def validate_license(ecfjson: JSONDict, validate=False):
    """Will valid an ECF upon upload to ensure it is compatible with
        the user's build
    """
    #Adding this for now to skip the functionality -- to be removed when we
    # would like to impement checking.
    if not validate:
        return

    decoded_license = decode_license('license.txt')

    if decoded_license['license_type'] == 'guid':
        assert ecfjson['Username'] == decoded_license['guid'], \
            'Invalid license, GUID does not match between ECF and license.'
    else:
        assert ecfjson['ClientId'] == decoded_license['client_id'], \
            'Invalid license, Client ID does not match between ECF and license.'


def decode_license(license_file: str) -> dict:
    """Decode a license for tool validation upon ECF upload"""
    rsa = chilkat.CkRsa()
    assert rsa.UnlockComponent("PWTRCP.CBX1118_yuLAXzPxmR8B")

    with open(license_file, 'r') as stream:
        license_str = stream.read()

    # Get Random Factor.
    factor = int(license_str[-6:], 16)

    # Generate hash based on key's random factor.
    crypt = chilkat.CkCrypt2()
    crypt.put_HashAlgorithm("SHA256")
    crypt.put_EncodingMode("hex")
    aes_256_key = crypt.hashStringENC(str(round(math.pi, 10) * factor))

    # Decode License.
    crypt.SetEncodedKey(aes_256_key, "hex")
    decrypted = crypt.decryptStringENC(license_str[:-6])
    return json.loads(decrypted)
