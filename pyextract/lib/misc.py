import math
__int__ = lambda x:int(math.floor(float(str(x)))) if (x is not None) else x

def xrange(x, y=None, inc=None):
    return iter(range(__int__(x)) if (y is None) else range(__int__(x),__int__(y)) if (inc is None) else range(__int__(x),__int__(y), __int__(inc)))


def str_to_bytes(s):
    b = bytearray()
    b.extend(map(ord, s))
    return b

import struct

def pack_bytes_into(target, block, func=lambda x:struct.pack("<L", x)):
    normalize = lambda some_bytes:''.join([chr(b) for b in some_bytes])
    for t in block:
        target += normalize(func(t))
    return target


def is_float(value):
    try:
        x = float(str(value))
        return True
    except:
        pass
    return False


def is_int(value):
    try:
        x = int(str(value))
        return True
    except:
        pass
    return False


def is_number(value):
    return is_float(value) or is_int(value)


def parse_number(value_string):
    if (value_string.find('.') > -1):
        try:
            value = float(str(value_string))
            return value
        except:
            pass
    else:
        try:
            value = int(str(value_string))
            return value
        except:
            pass
    return None


def pretty_print_number(item):
    __classes__ = [str, int, float]
    if (not any([issubclass(type(item), t) for t in __classes__])):
        return item
    
    try:
        resp = "{:,}".format(int(str(item)) if (str(item).isdigit()) else float(str(item)) if (is_float(item)) else item)
    except:
        resp = item
    return resp
    

def pretty_print_numbers(items):
    if (issubclass(type(items), dict)):
        try:
            __iter__ = (k for k in items.keys())
        except:
            __iter__ = (k for k in items)
        for k in __iter__:
            items[k] = pretty_print_number(items[k])
        return items
    elif (issubclass(type(items), list)):
        return [pretty_print_number(item) for item in items]
    
    return pretty_print_number(items)
    

def split_date_strings(s):
    import re
    return [t for t in re.split(r'(\d+)', s) if (len(t) > 0) and (str(t).isdigit())]


def normalize_date_string(s):
    toks = [int(t) if (str(t).isdigit()) else t for t in split_date_strings(s)]
    if (toks[0] >= 1900) and (toks[0] <= 9999):
        toks.append(toks[0])
        del toks[0]
    resp = []
    for t in toks:
        fmt = '%04d'
        if (t < 99):
            fmt = '%02d'
        resp.append(fmt % (t))
    return ''.join(resp)


def is_valid_date(mm, dd, yyyy):
    return ( (yyyy >= 1900) and (yyyy <= 9999) ) and ( (mm >= 1) and (mm <= 12) ) and ( (dd >= 1) and (dd <= 31) )


def is_yyyymmdd(data, is_recursive=False):
    toks = split_date_strings(data)
    if (len(toks) == 3):
        yyyy, mm, dd = [toks[0],toks[1],toks[-1]]
    else:
        yyyy, mm, dd = [data[0:4],data[4:4+2],data[4+2:4+2+2]]
    if (not is_recursive):
        if (len(mm) > 2) or (len(dd) > 2) or (len(yyyy) != 4):
            return is_mmddyyyy(data, is_recursive=True)
    is_yyyy_digits = yyyy.isdigit()
    i_yyyy = int(yyyy) if (is_yyyy_digits) else -1
    is_mm_digits = mm.isdigit()
    i_mm = int(mm) if (is_mm_digits) else -1
    is_dd_digits = dd.isdigit()
    i_dd = int(dd) if (is_dd_digits) else -1
    return (len(mm+dd+yyyy) == 8) and (is_valid_date(i_mm, i_dd, i_yyyy))


def is_mmddyyyy(data, is_recursive=False):
    toks = split_date_strings(data)
    if (len(toks) == 3):
        mm, dd, yyyy = [toks[0],toks[1],toks[-1]]
    else:
        mm, dd, yyyy = [data[0:2],data[2:2+2],data[2+2:2+2+4]]
    if (not is_recursive):
        if (len(mm) > 2) or (len(dd) > 2) or (len(yyyy) != 4):
            return is_yyyymmdd(data, is_recursive=True)
    is_mm_digits = mm.isdigit()
    i_mm = int(mm) if (is_mm_digits) else -1
    is_dd_digits = dd.isdigit()
    i_dd = int(dd) if (is_dd_digits) else -1
    is_yyyy_digits = yyyy.isdigit()
    i_yyyy = int(yyyy) if (is_yyyy_digits) else -1
    return (len(mm+dd+yyyy) == 8) and (is_valid_date(i_mm, i_dd, i_yyyy))


def is_mmddyyyy_or_yyyymmdd(data):
    __is__ = is_mmddyyyy(data) or is_yyyymmdd(data)
    return __is__


def parse_dates(value):
    import datetime
    format_str = '%m/%d/%Y' if (is_mmddyyyy(value, is_recursive=True)) else '%Y/%m/%d' if (is_yyyymmdd(value, is_recursive=True)) else None
    seps = list(set([c for c in value if (not str(c).isdigit())]))
    __value__ = value.replace(seps[0],'/')
    __toks__ = [int(n) for n in __value__.split('/')]
    parts = ['%m', '%d']
    formats = []
    for t in __toks__:
        if ((t >= 1) and (t <= 12)) or ((t >= 1) and (t <= 31)):
            formats.append(parts[0])
            del parts[0]
        else:
            formats.append('%Y')
    format_str = '/'.join(formats)
    datetime_obj = None
    if (format_str):
        datetime_obj = datetime.datetime.strptime(__value__, format_str)
        return datetime_obj.date()
    return None


def date_range_generator(date1, date2, fmt="%m-%d-%Y"):
    from datetime import date
    from dateutil.rrule import rrule, DAILY
    
    for dt in rrule(DAILY, dtstart=min(date1,date2), until=max(date1,date2)):
        yield dt.strftime(fmt)
        
import json
import inspect

class ObjectEncoder(json.JSONEncoder):
    def default(self, obj):
        if hasattr(obj, "to_json"):
            return self.default(obj.to_json())
        elif hasattr(obj, "__dict__"):
            d = dict(
                (key, value)
                for key, value in inspect.getmembers(obj)
                if not key.startswith("__")
                and not inspect.isabstract(value)
                and not inspect.isbuiltin(value)
                and not inspect.isfunction(value)
                and not inspect.isgenerator(value)
                and not inspect.isgeneratorfunction(value)
                and not inspect.ismethod(value)
                and not inspect.ismethoddescriptor(value)
                and not inspect.isroutine(value)
            )
            return self.default(d)
        return obj
    
    
def fibonacci():
    """ Generator yielding Fibonacci numbers
    
    :returns: int -- Fibonacci number as an integer
    """
    x, y = 0, 1
    while True:
        yield x
        x, y = y, x + y
        
        
def circular(items=[]):
    while (1):
        for connection in items:
            yield connection


def fibs(n=7, items=8):
    fibs = []
    b = fibonacci()
    while (1):
        f = next(b)
        if (f > n):
            fibs.append(f)
            if (len(fibs) == items):
                break
            
    return circular(items=fibs)
    