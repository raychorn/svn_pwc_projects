import json

from misc import is_number
from misc import parse_dates
from misc import parse_number
from misc import date_range_generator
from misc import is_mmddyyyy_or_yyyymmdd

from misc import xrange

from itertools import product

from lib.sqlutils import SQLCoder

class ParametersProcessor(object):
    def __init__(self, parameters=[]):
        self.parameters = parameters
        self.new_parameters = []

    def on(self, parameters=[]):
        if (len(parameters) > 0):
            self.parameters = parameters
        __lists__ = []
        __sql_coder__ = SQLCoder({})
        for parm in self.parameters:
            parm_Interpretation = parm.get('Interpretation', None)
            parm_Operation = parm.get('Operation', None)
            parm_Values = parm.get('Values', None)
            parm_Name = parm.get('Name', None)
            __list__ = []
            if (str(parm_Interpretation).upper() == 'ITER'):
                if (str(parm_Operation).upper() == 'BETWEEN'):
                    is_dates = all([is_mmddyyyy_or_yyyymmdd(item) for item in parm_Values])
                    is_numbers = all([is_number(item) for item in parm_Values])
                    if (is_dates or is_number):
                        if (is_dates):
                            values = [parse_dates(value) for value in parm_Values]
                            gen = date_range_generator(values[0], values[-1])
                            for item in gen:
                                __list__.append('%s = %s' % (parm_Name, __sql_coder__.normalize_sql_date(item)))
                        elif (is_number):
                            values = [parse_number(value) for value in parm_Values]
                            values.sort()
                            for item in xrange(min(values[0], values[-1]), y=max(values[0], values[-1])):
                                __list__.append('%s = %s' % (parm_Name, item))
                        else:
                            raise ValueError
                elif (str(parm_Operation).upper() == 'IN'):
                    is_numbers = all([is_number(item) for item in parm_Values])
                    if (is_numbers):
                        values = [parse_number(value) for value in parm_Values]
                        values.sort()
                        for item in values:
                            __list__.append('%s = %s' % (parm_Name, item))
                    else:
                        try:
                            parm_Values.sort()
                        except:
                            pass
                        for item in parm_Values:
                            __list__.append('%s = %s' % (parm_Name, item if (not isinstance(item, str)) else "'%s'" % (item)))
                parm['__list__'] = __list__
                __lists__.append(__list__)
        
        self.new_parameters = (item for item in list(product(*__lists__)))
    
if (__name__ == '__main__'):
    fname = "C:/Users/rhorn010/AppData/Local/PwC/Extract/Data/1c5b4f21-deec-4005-b8b1-10dc9cc12969/datadefs_GL_JE_HEADERS_debug.json"
    
    fIn = open(fname, mode='r')
    data = json.load(fIn)
    fIn.close()

    parameters = data.get('parameters', {}).get('Parameters', [])
    p = ParametersProcessor()
    p.on(parameters=parameters)
    
    __dot__ = p.new_parameters
    
    print('BEGIN:')
    for item in __dot__:
        print(item)
    print('END!!!')
    
    print('-'*30)
    