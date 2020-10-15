import time

SECRET_KEY = 'bx0hpc1rwrvljb@u02lreu0%dlhrsd(j7e9ra01kmww@0w6@s*'.upper()
SECRET_KEY = ''.join([ch for ch in SECRET_KEY if (str(ch).isalpha())])[0:16]

if (__name__ == '__main__'):
    import pyotp
    __interval__ = 5
    totp = pyotp.TOTP(SECRET_KEY, digits=10, interval=__interval__)
    the_code = totp.now()
    print("Current OTP:", the_code)
    
    __is__ = totp.verify(the_code)
    print('Is %s valid %s' % (the_code, __is__))
    
    new_interval = __interval__+5
    print('Sleeping for %s secs.  code should faill.' % (new_interval))
    time.sleep(new_interval)

    __is__ = totp.verify(the_code)
    print('Is %s valid %s' % (the_code, __is__))
