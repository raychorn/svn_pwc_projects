print('BEGIN: 1')
from ecdsa import SigningKey
sk = SigningKey.generate() # uses NIST192p
vk = sk.get_verifying_key()
signature = sk.sign("message".encode('utf-8'))
print(signature)
try:
    assert vk.verify(signature, "message".encode('utf-8'))
    print('Done... no exceptions.')
except Exception as ex:
    print(ex)
print('END!!! 1')


print('BEGIN: 2')
from ecdsa import SigningKey, NIST384p
sk = SigningKey.generate(curve=NIST384p)
vk = sk.get_verifying_key()
signature = sk.sign("message".encode('utf-8'))
print(signature)
try:
    assert vk.verify(signature, "message".encode('utf-8'))
    print('Done... no exceptions.')
except Exception as ex:
    print(ex)
print('END!!! 2')


print('BEGIN: 3')
from ecdsa import SigningKey, NIST384p
sk = SigningKey.generate(curve=NIST384p)
sk_string = sk.to_string()
print(sk_string)
sk2 = SigningKey.from_string(sk_string, curve=NIST384p)
sk2_string = sk2.to_string()
# sk and sk2 are the same key
try:
    assert sk_string == sk2_string, 'Hmmm... Maybe not so equal...'
    print('Done... no exceptions.')
except Exception as ex:
    print(ex)
print('END!!! 3')


print('BEGIN: 4')
from ecdsa import SigningKey, NIST384p
sk = SigningKey.generate(curve=NIST384p)
sk_string = sk.to_string()
sk_pem = sk.to_pem()
sk2 = SigningKey.from_pem(sk_pem)
sk2_string = sk2.to_string()
# sk and sk2 are the same key
try:
    assert sk_string == sk2_string, 'Hmmm... Maybe not so equal...'
    print('Done... no exceptions.')
except Exception as ex:
    print(ex)
print('END!!! 4')
