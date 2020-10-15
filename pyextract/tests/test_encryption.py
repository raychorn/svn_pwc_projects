""" Tests for the Encryption_Keystore module of the pyextract program."""

import base64
import json
import shutil
import tempfile
import unittest

import chilkat

from pyextract import ecfreader


chilkat.CkRsa().UnlockComponent("PWTRCP.CBX1118_yuLAXzPxmR8B")


def get_id_key_pair(request_id):
    """
    Generates a unique identifier (uuid) and a corresponding public/private key pair.
    The private key is password protected using the unique identifier.
    :return: a tuple containing the unique identifier, public key, and private key
    """
    rsa = chilkat.CkRsa()
    success = rsa.GenerateKey(4096)
    if not success:
        err = rsa.lastErrorText()
        return err, 500

    public_key_obj = rsa.ExportPublicKeyObj()
    private_key_obj = rsa.ExportPrivateKeyObj()

    public_key = public_key_obj.getEncoded(False, "base64")
    private_key = private_key_obj.getPkcs8EncryptedENC("base64", request_id)

    return public_key, private_key


def encrypt_epf(epf, public_key):
    """
    :param epf: EPF as python dictionary
    :param public_key: A Base64 Encoded Public Key
    :return: Encrypted EPF binary
    """
    # Create Chilkat Public Key Object.
    ck_public_key = chilkat.CkPublicKey()
    assert ck_public_key.LoadBase64(public_key)

    # Create Chilkat RSA Object and Add CK PublicKey
    encrypter = chilkat.CkRsa()
    encrypter.put_EncodingMode("base64")
    assert encrypter.ImportPublicKeyObj(ck_public_key)

    # Convert EPF to a string.
    epf_str = json.dumps(epf)

    # Encrypt EPF (now a string) and return encrypted binary, base64 encoded
    encrypted_data = encrypter.encryptStringENC(epf_str, False)
    assert encrypted_data

    # Return encrypted binary.
    return base64.b64decode(encrypted_data)


def decrypt_epf(encrypted_epf_bin, request_id, private_key):

    """
    :param encrypted_epf_bin: Encrypted EPF binary
    :param request_id: A str.  Request ID of the ECF
    :param private_key: A Base64 Encoded Private Key
    :return:
    """
    private_key_binary = chilkat.CkByteData()
    private_key_binary.appendEncoded(private_key, "base64")

    ck_private_key = chilkat.CkPrivateKey()
    success = ck_private_key.LoadPkcs8Encrypted(private_key_binary, request_id)
    if not success:
        raise Exception(private_key.lastErrorText())


    decrypter = chilkat.CkRsa()
    decrypter.ImportPrivateKeyObj(ck_private_key)

    encrypted_epf_b64 = base64.b64encode(encrypted_epf_bin).decode("utf-8")
    unencrypted_data = decrypter.decryptStringENC(encrypted_epf_b64, True)

    return json.loads(unencrypted_data)


class TestEndtoEndencryption(unittest.TestCase):
    """Tests to ensure valid EndtoEndencryption"""

    def setUp(self):
        """Create testing folder."""
        self.folder = tempfile.mkdtemp()

    def tearDown(self):
        """Remove testing folder."""
        shutil.rmtree(self.folder, ignore_errors=True)

    def test_decryption(self):
        """Test that a message can be decrypted."""
        request_id = '1234'
        epf = {"some": "data"}

        public, private = get_id_key_pair(request_id)
        encrypted_bin = encrypt_epf(epf, public)
        decrypt = decrypt_epf(encrypted_bin, request_id, private)

        self.assertEqual(epf, decrypt)

class TestChilkatEncryptions(unittest.TestCase):
    """Tests to ensure valid EndtoEndChilkatencryption"""
    def setUp(self):
        """Create testing folder."""
        self.folder = tempfile.mkdtemp()

    def tearDown(self):
        """Remove testing folder."""
        shutil.rmtree(self.folder, ignore_errors=True)

    def test_encryption_with_cert(self):
        """Test the a message that can be encrypted using hardcoded PwC Cert key."""
        # pylint: disable=unused-variable
        jsondata = 'str'
        result = ecfreader.chilkat_encrypt_with_cert(jsondata='str')
        self.assertTrue(result)

    def test_chilkat_encrypt(self):
        """Test the a message that can be chilkat encrypted"""
        request_id = '1234'
        jsondata = '{"k": "v"}'
        publickey, _ = get_id_key_pair(request_id)
        result = ecfreader. chilkat_encrypt(jsondata=jsondata, publickey=publickey)
        self.assertTrue(result)

    def test_chilkat_decrypt(self):
        """Test the a message that can be chilkat decrypted"""
        rawdata = bytes('str', encoding='utf8')
        # pylint: disable=protected-access
        result = ecfreader._chilkat_decrypt(rawdata=rawdata)
        self.assertTrue(result)
