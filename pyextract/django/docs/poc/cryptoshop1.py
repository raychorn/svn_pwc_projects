

from cryptoshop import encryptstring
from cryptoshop import decryptstring

# No need to specify algo. Cryptoshop use cascade encryption with Serpent, AES and Twofish.
result1 = encryptstring(string= "my string to encrypt" , passphrase= "mypassword")
print(result1)

result2 = decryptstring(string= result1 , passphrase= "mypassword")
print(result2)

