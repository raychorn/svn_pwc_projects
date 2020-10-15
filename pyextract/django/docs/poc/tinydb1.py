import os, sys

import uuid

from tinydb import TinyDB, Query

from tinydb.storages import JSONStorage
from tinydb.middlewares import CachingMiddleware

if (__name__ == '__main__'):
    fpath = os.path.abspath('.')
    fname = os.sep.join([fpath, 'tinydb1.json'])
    db = TinyDB(fname, storage=CachingMiddleware(JSONStorage))
    
    sessionid = str(uuid.uuid4())
    
    aQuery = Query()
    while (1):
        result = db.search(aQuery.sessionid == sessionid)
        if (len(result) == 0):
            db.insert({'sessionid': sessionid, 'value': 11111})
        else:
            print('result has %s items -- > %s' % (len(result), ''.join([str(r) for r in result])))
            break
        
    print('\n')
    print('BEGIN:')
    for item in db:
        print(item)
    print('END!!!')
        
    db.close()
