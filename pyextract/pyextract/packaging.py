"""Module to support packaging of final deliverables before sending to PwC."""

import logging
import hashlib
import json
import math
import os
import queue
import time
import sys
from tempfile import NamedTemporaryFile
from threading import Thread
from typing import Dict
from zipfile import ZipFile

import multiprocessing
import chilkat
import requests
from requests_toolbelt import MultipartEncoder

from . import config
from . import ecfreader

from pyextract.version import EXTRACT_VERSION

LOGGER = multiprocessing.get_logger()
LOGGER.setLevel(logging.INFO)


class DataPackage(object):
    """A zipped folder with all data / metadata from a completed extract."""

    def __init__(self, ecfpath: str = None):
        """Instantiate a new Data Package before adding data / metadata.

        ARGS:
            ecfpath: Filepath of the ECF file that created this package.

        PROPERTIES:
            _contents: Dictionary of zipped output filenames to
                local filepaths of all objects in this data package.
            _sqlite_files: List of SQLite files added to the data package.
        """
        self._contents = {}  # type: Dict[str,str]
        self._sqlite_files = []  # type: List[str]
        self.ecfpath = ecfpath
        if self.ecfpath:  # Validate the ECF
            assert ecfreader.read_encrypted_json(self.ecfpath)

    def add_sqlite_file(self, filepath: str):
        """Add a SQLite database to the data package."""
        name = os.path.basename(filepath)
        self._contents[name] = filepath
        self._sqlite_files += [name]

    def add_text_file(self, filepath: str):
        """Add a text file to the data package."""
        name = os.path.basename(filepath)
        self._contents[name] = filepath

    def create(self, filepath: str, sqlite_password: str = None):
        """Create the DataPackage at the target location."""
        if self.ecfpath:
            # Validate the ECF, then add it to the package contents
            ecfdata = ecfreader.get_ecf_meta_data(self.ecfpath)[0]
            ecfname = os.path.basename(self.ecfpath)
            self._contents[ecfname] = self.ecfpath
            # Create the EPF response file from the ECF data
            checksums = {
                key: sqlite_checksum(self._contents[key])
                for key in self._sqlite_files
            }
            self._contents['Encrypted_RequestInfo.EPF'] = \
                create_epf_file(ecfdata, sqlite_password, checksums)
            # Create plaintext metadata from the ECF data
            self._contents['Metadata.txt'] = \
                create_metadata_file(ecfdata)
        # Zip all the contents into the target filepath
        with ZipFile(filepath, 'w') as zipper:
            for name, path in self._contents.items():
                zipper.write(path, name)


class SFTPClient(object):
    """A client for sending data to a remote SFTP site."""

    def __init__(self, creds: dict, territory: str = None, port=22, rename_wait=2):
        """Connect to an SFTP client from set of credentials or territory."""
        ecfreader.unlock_chilkat()
        self.sftp = chilkat.CkSFtp()
        self.basedir = creds["CURRDIRECTORY"]
        self.rename_wait = rename_wait

        if territory:
            # Ensure it is an accepted territory
            valid_territories = ("WEST", "EAST", "CENTRAL", "AU")
            assert territory.upper().strip() in valid_territories, (
                'Invalid territory "{}", must be one of:  {}'
                ).format(territory, valid_territories)

        # Attempt a connection to SFTP site
        assert self.sftp.Connect(creds["HOSTADDRESS"], port), (
            "Could not establish connection to SFTP server/port:  {},{}"
            ).format(creds["HOSTADDRESS"], port)

        if creds["USEPKAUTH"].upper().strip() == "TRUE":
            # Connect to SFTP Server using public-key authentication
            auth = chilkat.CkSshKey()
            auth.Password = creds["PASSWORD"]
            auth.FromRfc4716PublicKey(creds["KEY"])
        else:
            # Connect to SFTP Server using password authentication
            auth = creds["PASSWORD"]

        assert self.sftp.AuthenticatePk(creds["USERNAME"], auth), (
            "Authentication failed for SFTP server:  {}"
            ).format(creds["HOSTADDRESS"])

        # Verify SFTP server's fingerprint
        hostkey = self.sftp.hostKeyFingerprint().split()[-1]
        assert hostkey == creds["SRVRFINGERPRINT"], \
            "Server fingerprint not recognized."

        assert self.sftp.InitializeSftp(), (
            "Exception occurred during initialization. Error raised:  {}"
            ).format(self.sftp.lastErrorText())

    def send(self, datafile: str, newname: str = None, overwrite=False):
        """Send data from a local filepath over the SFTP connection.

        ARGS:
            datafile: Local filepath of the file to be uploaded.
            newname: Filename to upload the file as to the SFTP.
            overwrite: If True, overwrite files in the SFTP base directory
                that already have the target filename.

        RAISES:
            AssertionError: If any action that should succeed during the
                upload fails, especially if a file to be uploaded already
                exists and overwrite is not set to True.
        """
        assert os.path.exists(datafile), \
            "Local filepath does not exist:  %s" % datafile

        # Get current listing of files in the target directory
        handle = self.sftp.openDir(self.basedir)
        listing = self.sftp.ReadDir(handle)
        contents = [listing.GetFileObject(index)
                    for index in range(listing.get_NumFilesAndDirs())]
        sftpfiles = [item.filename() for item in contents]

        # Determine filename that should be written to the SFTP
        filename = newname or os.path.basename(datafile)

        # Two files must not already exist on the SFTP site: 'filename.zip',
        # which will be renamed to 'filename_Complete.zip'.
        # If either name exists, we must delete it (if overwriting),
        # or raise an error to the user.
        completename = "{}_Complete.{}".format(
            '.'.join(filename.split('.')[:-1]),
            filename.split('.')[-1]
        )
        for name in (filename, completename):
            if name in sftpfiles:
                path = '/{}/{}'.format(self.basedir, name)
                assert overwrite, (
                    'File already exists at SFTP path:  {}'
                    ).format(path)
                LOGGER.warning('Overwriting file at SFTP path: %s', path)
                self.sftp.RemoveFile(path)
                assert self.sftp.get_LastMethodSuccess(), (
                    'Failed to remove SFTP file with error:  {}'
                    ).format(self.sftp.lastErrorText())

        # Attempt to run the upload, and wait for success confirmation
        destpath = '/{}/{}'.format(self.basedir, filename)
        uploaded = self.sftp.ResumeUploadFileByNameAsync(destpath, datafile).RunSynchronously()
        if not uploaded:
            LOGGER.error("Exception occurred during upload. Error raised:  {}".format(self.sftp.lastErrorText()))
            raise Exception("Exception occurred during upload. Error raised:  {}".format(self.sftp.lastErrorText()))

        # Bug Fix here - sleeping X seconds before attempting to rename.
        LOGGER.info("Upload complete.  Waiting {} seconds before renaming file.".format(self.rename_wait))
        time.sleep(self.rename_wait)
        LOGGER.info("Waiting complete.  Appending '_Complete' suffix to file on SFTP server.")

        # Append "_Complete" suffix to file on SFTP Server
        completepath = '/{}/{}'.format(self.basedir, completename)
        change_success = self.sftp.RenameFileOrDirAsync(destpath, completepath).RunSynchronously()
        if not change_success:
            LOGGER.error("Failed appending '_Complete' to file on SFTP Server.")
            raise Exception("Failed appending '_Complete' to file on SFTP Server.")



class LFUDataChunk(object):
    """Represents a chunk of a file to transmit via LFU (usually a .zip file).
    Only the LFUDataFile() class should instantiate this object.
    """
    def __init__(self, parent: object, index: int,
                 data: Dict[str, str], is_last=False):
        """Create a new DataChunk with given properties."""
        self.parent = parent
        self.index = index
        self.data = data
        self.is_last = is_last

    def get_hash_code(self) -> str:
        """Return the SHA256 hash of this data without hyphens."""
        hasher = hashlib.sha256()
        hasher.update(self.data)
        hash_str = str(hasher.hexdigest()).upper().replace("-", "")
        return hash_str

    def get_upload_data(self) -> Dict[str, str]:
        """Return JSON payload for a data chunk upload POST request."""
        return {
            "identifier": self.parent.file_id,
            "filename": self.parent.filename,
            "totalsize": str(self.parent.filesize),
            "totalchunks": str(self.parent.totalchunks),
            "chunknumber": str(self.index),
            "chunkSize": str(len(self.data)),
            "chunkchecksum": self.get_hash_code(),
        }

    def get_last_upload_data(self) -> Dict[str, str]:
        """Return JSON data of last chunk upload with most recent checksum."""
        data = self.get_upload_data()
        data["filechecksum"] = self.parent.get_hash_code()
        return data


class LFUDataFile(object):
    """This class represents a file to transmit via LFU (usually a .zip file).
    Only the LFUCLient() class should instantiate this object.
    """
    def __init__(self, filepath: str, chunk_size: int):
        """Return a new instance of a data file."""
        assert os.path.exists(filepath), \
            'file does not exist at given path:  {}'.format(filepath)
        assert chunk_size > 0, 'chunk_size must be a positive integer'
        self.filepath = filepath
        self.chunk_size = chunk_size
        self.filesize = os.path.getsize(self.filepath)
        self.filename = os.path.basename(self.filepath)

    @property
    def totalchunks(self) -> int:
        """Return total number of chunks that will be uploaded."""
        return math.ceil(self.filesize / self.chunk_size)

    @property
    def file_id(self) -> str:
        """Return a SHA256 hashed version of this file as a unique ID."""
        hasher = hashlib.sha256()
        hasher.update(self.filename.encode("ascii"))
        hash_str = str(hasher.hexdigest()).upper().replace("-", "")
        return hash_str

    def get_hash_code(self) -> str:
        """Return SHA256 hash of all content in this file."""
        with open(self.filepath, 'rb') as stream:
            data = stream.read()
        hasher = hashlib.sha256()
        hasher.update(data)
        hash_str = str(hasher.hexdigest()).upper().replace("-", "")
        return hash_str

    def read_chunk(self, index: int) -> LFUDataChunk:
        """Return a chunk of data from this file."""
        with open(self.filepath, 'rb') as stream:
            # Based on the index, seek to the location of the file.
            if index == 1:
                seek_val = 0
            else:
                seek_val = (index - 1) * self.chunk_size

            stream.seek(seek_val)
            # Read chunk size
            data = stream.read(self.chunk_size)

        # Create and instance of LFUDataChunk() and return it
        if index == self.totalchunks:
            chunk = LFUDataChunk(self, index, data, True)
        else:
            chunk = LFUDataChunk(self, index, data)

        return chunk


class LFUClient(object):
    """This class serves as the LFU client. It is instantiated using a host
    and a corresponding token for authentication. This class supports one
    primary (public) function called send() which will transmit a file
    at a particular file in chunks to the corresponding LFU server.
    """

    UPLOAD_PATH = "/api/upload"
    UPLOAD_PATH_TEST = "/api/upload/test"
    UPLOAD_PATH_CANCEL = "/api/upload/cancel"

    def __init__(self, host: str, token: str):
        """Create a new client to interact with the LFU REST API.

        ARGS:
            host: Fully-qualified domain of the REST API.
            token: API token to authorize use of the API.
        """
        self._host = host
        self._token = token
        self._file = None
        self.errors = queue.Queue()

    def send(self, filepath: str, chunk_size: int, test=False):
        """Function transmits the given file to the LFU host using HTTPS in
        chunk sizes also provided. The read/upload process is performed
        concurrently using different threads. The LFU service at the other
        end will put the chunks back together and validate the file received.

        ARGS:
            test: If True, upload files to test dir instead of production.
        """
        assert os.path.exists(filepath), \
            "Local filepath does not exist:  {}".format(filepath)

        self._file = LFUDataFile(filepath=filepath, chunk_size=chunk_size)

        LOGGER.info("Total File Size: %i", self._file.filesize)
        LOGGER.info("Chunk Size: %i", chunk_size)
        LOGGER.info("Total Chunks: %i", self._file.totalchunks)
        LOGGER.info("Starting extraction...")

        # Start final chunk first. The final chunk includes the hash code
        # for the file within the 'filechecksum' header in the HTTP Request
        # and generating the hash code for the entire file will take longer
        # than any individual chunk.
        kwargs = {
            'errors': self.errors,
            'index': self._file.totalchunks,
            'test': test,
        }
        thread = Thread(target=self._read_task, kwargs=kwargs)
        thread.start()
        thread.join()

        # Use threads to perform read/upload for all other chunks (all but the last).
        for index in range(1, self._file.totalchunks):
            kwargs = {
                'errors': self.errors,
                'index': index,
                'test': test,
            }
            thread = Thread(target=self._read_task, kwargs=kwargs)
            thread.start()
            thread.join()

        # Check if there were any errors during upload
        try:
            error = self.errors.get(timeout=1)
        except queue.Empty:
            pass
        else:
            raise error

    def _read_task(self, errors: queue.Queue, index: int, test=False):
        """Try to read a chunk and send it via HTTPS. Put any errors in Queue.

        ARGS:
            test: If True, upload files to test dir instead of production.
        """
        try:
            self._read_task_inner(index, test)
        except Exception as error:
            errors.put(error)

    def _read_task_inner(self, index: int, test=False):
        """Inner component of _read_task, surrounded by try/except."""
        LOGGER.info('reading chunk %d', index)
        chunk = self._file.read_chunk(index)

        # Get upload data for request.
        if chunk.is_last:
            upload_data = chunk.get_last_upload_data()
        else:
            upload_data = chunk.get_upload_data()

        upload_data["chunk"] = (upload_data["filename"], chunk.data)

        # Create Multipart form-data object.
        encoder = MultipartEncoder(fields=upload_data)

        # Assemble the URL to Post the request to
        if test:
            url = self._host + self.UPLOAD_PATH_TEST
        else:
            url = self._host + self.UPLOAD_PATH

        # Create and send upload HTTP Request
        LOGGER.info('writing chunk %d', index)
        response = requests.post(
            url=url,
            data=encoder,
            verify=False,
            headers={
                "Accept": "application/json",
                "Content-Type": encoder.content_type,
                "Authorization": "Bearer %s" % self._token,
                "Expect": "100-continue"
            },
        )
        assert response.status_code == 200, (
            'Error uploading chunk {}, POST failed with status code:  {}'
            ).format(index, response.status_code)


def sqlite_checksum(filepath: str) -> str:
    """Return a checksum for a SQLite file at a given filepath."""

    BUF_SIZE = 65536  # lets read stuff in 64kb chunks!
    sha256 = hashlib.sha256()
    with open(filepath, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha256.update(data)

    return sha256.hexdigest()

    # with open(filepath, 'rb') as stream:
    #     checksum = hashlib.sha256(stream.read()).hexdigest()
    # return checksum


def create_metadata_file(ecfdata: ecfreader.ExtractData) -> str:
    """Return filepath to a temporary JSON file containing
    high-level package metadata in plaintext.

    ARGS:
        ecfdata: Parsed data from ECF that defined the extract.
    """
    topic = (
        '{channel}.{destination}.2_0.{tags}'
        ).format(channel=ecfdata.channel,
                 destination=ecfdata.destination,
                 tags=ecfdata.tags)
    data = {
        # Pattern matching on RabbitMQ on the 'Topic' field will
        # determine what data loader will be used on this package,
        # and that matching is case-sensitive (uppercase)
        'Topic': topic.upper(),
        'PackagePath': None,  # Always NULL when created by our program
        'PublicKey': ecfdata.publickey,
        'RequestId': ecfdata.request_id,
        'PxVersion': "v{}".format(EXTRACT_VERSION)
    }
    with NamedTemporaryFile('w', delete=False) as metadatafile:
        metadatafile.write(json.dumps(data))
        return metadatafile.name


def create_epf_file(ecfdata: ecfreader.ExtractData,
                    sqlite_password: str = None,
                    checksums: dict = None) -> str:
    """Return filepath to an encrypted EPF file in a temp
    directory containing extraction response metadata.

    ARGS:
        ecfdata: Parsed data from ECF that defined the extract.
        sqlite_password: The password that can be used to decrypt
            the SQLite files present in this data package.
        checksums: Dict of SQLite filenames in package to checksums
            of the databases.
    """
    # Format checksums as list of dictionaries for GATT retriever service
    checksums = [{key: value} for key, value in sorted(checksums.items())]

    data = {
        'RequestKey': ecfdata.request_id,
        'PublicKey': sqlite_password,  # This field intentionally named wrong
        'CheckSums': checksums or None,
        'Status': None,  # Used downstream of our program
        'ChannelType': ecfdata.channel,
        'ClientName': ecfdata.client,
        'DatabaseName': ecfdata.database,
        'DatabaseServerName': ecfdata.server,
        'DatabasePort': ecfdata.port,
        'Schema': ecfdata.schema,
        'SetID': ecfdata.set_id,
        'Territory': ecfdata.territory,
    }
    with NamedTemporaryFile('w', suffix='.EPF', delete=False) as stream:
        filepath = stream.name

    # Encrypt the JSON data and write it to a filepath
    jsondata = json.dumps(data)

    # !!!! Always encrypt using ECF public key
    # if config.USE_PWC_ECF_PUBLIC_KEY:
    if True:
        encrypted = ecfreader.chilkat_encrypt(jsondata, ecfdata.publickey)
    else:
        encrypted = ecfreader.chilkat_encrypt_with_cert(jsondata)

    with open(filepath, 'wb') as stream:
        stream.write(encrypted)

    return filepath


if __name__ == '__main__':

    cfg = {
        "CURRDIRECTORY": "PwCIT_HALOERP-QA/UDX",
        "PASSWORD": "Of9zsDDX59",
        "USERNAME": "haloerp_udx_writer_qa",
        "HOSTADDRESS": "mft4app-west-stage.pwc.com",
        "KEY": "MIIEoAIBAAKCAQEAwXKCDYS4A+rJjlK5+AFH4HF6wJDB/Qg4z44RaSmGWRYCapQSg+EaHYXZxB8+Mrgko3iOgeNWghTj+sq7un9gac3/jdEA/kBklxAkZHxtrARjE6tn4mXXeo6UOC9Y/mjRlG29W6ko3k8P6Gs0nPEJ02SrLjiZ4AcoAGoZV5VuzAVkn4yJECWlVMCBxCcOc3lOePGKKzU3e353hVPy7YaJ8/ZlvyphGDZKyBxacomFsnOjj3Ot42Rad56WNBR7sKzywgsfzi3BQSX+PEqtAZFBniyt4KJmifp3gEAo45wS1UoF8Av2Vhh7WsaR1HMat5kd4T2ByGxUE+UEBnnM3iP/9wIBEQKCAQAREaIQQGqW7xHIyxBotNGi3NYgDMXSj8jHBQGJR228kQA2lJg40BjkfMBkIN/XTHu0EiqxG5axH/X/ilvbv/HrN9LIv540jTYNVD9yRzbaeNuQy1v9Y1bHDJSbi7UHY5oFkTZa6Ude6NvBrxs7BjWSpwALm5UbS+zw+k2HulyZh9oTs3HkuUcSWkocY7GHmuX4gGif/fRfe5V5yTTggRkoMEw8ZPAjwacFQ3/oEXxpvP/ofQELmVjQz8mVx37vnD+LeDsQJ0x+A5Sg5W67gMQ/lh7bRTEUCGu4t55/gQ3JyUeiP+Zk/sKuTnaDVHqRKQ0LBrfylPQEsjyiCCuHwzUNAoGBAOAhTLtvN+u5/P5GEq53xulhCZXVDo4ExwblstMADHIWeV8CwqpeMIuvtzaSS9Ql4d2tSd4+lPkiaC/ff+9b8TGDeivm2ouPfNtfk/I3b9HSXjN7WGsgvOyCl4Ij/MUwCaCudt/rGwFnatajfFFsG8Y+YqZkTsX+MqdpPUb+ET6lAoGBANz0ThgrZJQgGZDm/9lQLDbCOKfrky08lCfmDh39GFAWBFFlRRUkyFoyEs+aIi+I3nGXS8PMBXLBWM8IakMOe5u8k6va6ygdgI5n49PSbhT7KRywOc8w3ht63PfFPga2VzloFjidC4Y74nwLOVwzrBkl5Av8MBXgeQLGNUMyw71rAoGATxrPya7IcVCzpQmsPZOvnanHJdK518Vza9iZd6WqCiYMuB8Xh2yJmrZ85hWELMITe2pWTnBw0GZ/H/SHgaf6xi5nWsnyx9hKL07o7BOQ4KSZt9EuQ+1v2wDqLe6VcsWpC4jeqWIJiAZh8WbglTU2+qybSckMvlm3hmFg6+EzQ0kCgYAz/T+NN2L1rTMxJ0tCMPtYLbL6VYwKpNeQ6tYlLH4w9h8iNfIjF7arz4v0nLy/4/gaui/x1abt0yP0enNbEndv8CK/BlVy2cPlRZ8EqfvIs5Ez7TrHVsrZShXf8iy2SQV261CFyplMwsvg85UGor8U+dtOHTh9njqXPbIt7dO0GQKBgH6gpr04QNhcacPblsA4TZr7RN/9p2sVJ1kEuPATLAUvEHViczlGRFW696ZTUJF/tTg2wDyFfuz9hVEboU2yXAdYLunl6z4td0qjYQXVSFKPaNa8ablkT1n5nZsobuYiNM9ZjeRuMzv3vcuYewwr0SkxPVxkd1xeZkjILSIDUTi3",
        "USEPKAUTH": "true",
        "SRVRFINGERPRINT": "ec:80:8e:3e:70:79:6a:44:cb:7c:df:4c:0e:89:c0:63",
    }
    c = SFTPClient(cfg, territory='WEST', port=22, rename_wait=2)
    c.send(
        datafile='Q:\\QA1\\Package_6c36d918-c7ba-4aaa-86c6-d46e1969cb50_Complete.zip',
        newname='Package_faked918-c7ba-4aaa-86c6-d46e1969cb50.zip',
        overwrite=False
    )