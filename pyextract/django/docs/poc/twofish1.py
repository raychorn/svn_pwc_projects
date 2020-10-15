'''
Secure signature method 1 - CRC32 on fixed blocks.
'''
from twofish import Twofish

def unit_test1():
    '''
    baseline test.
    '''
    __testkey = '\xD4\x3B\xB7\x55\x6E\xA3\x2E\x46\xF2\xA2\x82\xB7\xD4\x5B\x4E\x0D\x57\xFF\x73\x9D\x4D\xC9\x2C\x1B\xD7\xFC\x01\x70\x0C\xC8\x21\x6F'
    __testdat = '\x90\xAF\xE9\x1B\xB2\x88\x54\x4F\x2C\x32\xDC\x23\x9B\x26\x35\xE6'
    try:
        assert 'l\xb4V\x1c@\xbf\n\x97\x05\x93\x1c\xb6\xd4\x08\xe7\xfa' == Twofish(__testkey).encrypt(__testdat)
        assert __testdat == Twofish(__testkey).decrypt('l\xb4V\x1c@\xbf\n\x97\x05\x93\x1c\xb6\xd4\x08\xe7\xfa')
        print('Test(s) completed without errors.')
    except Exception as ex:
        print('Test(s) failed because %s' % (ex))

def grouper(iterable, n=2):
    from more_itertools import chunked
    return chunked(iterable, n)

def __hex__(n):
    normalize = lambda item:item[0:2] + '0' + item[-1:]
    i = hex(n)
    return i if (len(i) == 4) else normalize(i)


def unit_test2():
    '''
    This will become a signing method however it does self-validate because it reverses the process.
    '''
    __testkey = '\xD4\x3B\xB7\x55\x6E\xA3\x2E\x46\xF2\xA2\x82\xB7\xD4\x5B\x4E\x0D\x57\xFF\x73\x9D\x4D\xC9\x2C\x1B\xD7\xFC\x01\x70\x0C\xC8\x21\x6F'
    __testdat = '\x90\xAF\xE9\x1B\xB2\x88\x54\x4F\x2C\x32\xDC\x23\x9B\x26\x35\xE6' * 10
    
    s_secret = Twofish(__testkey).encrypt(__testdat)
    s_secret_ints = [ord(i) for i in s_secret]
    s_secret_hexes = [__hex__(i)[2:] for i in s_secret_ints]
    s_secret_hex = ''.join([i for i in s_secret_hexes])
    print('s_secret --> %s' % (s_secret))
    print('s_secret_ints --> %s' % (s_secret_ints))
    print('s_secret_hexes --> %s' % (s_secret_hexes))
    print('s_secret_hex --> %s' % (s_secret_hex))
    '''
    1. gather fixed number of hex digitis, say 17.  3 is too few and more than 16 is too many, 17 is an odd number and makes no sense however this is ideal for security.
    2. swap msb with lsb.
    3. CRC32 each group and store CRC32 as hex digits.
    4. concat all blocks and sign using ECDSA.
    5. write to disk or transmit.
    '''
    s_secret_hex_grouped = ['0x' + ''.join(i) for i in grouper(s_secret_hex)]
    print('s_secret_hex_grouped --> %s' % (s_secret_hex_grouped))
    s_secret_hex_grouped_ints = [int(i, 16) for i in s_secret_hex_grouped]
    try:
        i = 0
        for ch in s_secret_ints:
            assert ch == s_secret_hex_grouped_ints[i], 'Something wrong with data at %s. Expected %s  but got %s.' % (i, ch, s_secret_hex_grouped_ints[i])
            i += 1
        print('No issues...')
    except Exception as ex:
        print(ex)
        
if (__name__ == '__main__'):
    unit_test1()
    
    unit_test2()

