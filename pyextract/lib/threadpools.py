import sys
import time
from threading import Thread
import threading

from misc import xrange

def threaded(func):
    def proxy(*args, **kwargs):
        thread = Thread(target=func, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return proxy

from queue import Queue

class ThreadQueue(Queue):
    def __init__(self, maxsize, isDaemon=False, is_multi_threaded=False):
        '''
        maxsize is ignored when is_multi_threaded is True otherwise it is used.
        is_multi_threaded --> treats each instance as a separate thread-pool to allow for a more granular way to manage threads and threading.
        '''
        self.isDaemon = isDaemon
        self.is_multi_threaded = is_multi_threaded
        self.__stopevent = threading.Event()
        if (not is_multi_threaded):
            assert maxsize > 0, 'maxsize > 0 required for ThreadQueue class'
        else:
            maxsize = 1
        self.maxsize = maxsize
        Queue.__init__(self, maxsize)
        for i in xrange(maxsize):
            thread = Thread(target = self._worker)
            thread.setDaemon(isDaemon)
            thread.start()

    def getIsRunning(self):
        return not self.__stopevent.isSet()

    def setIsRunning(self,isRunning):
        if (not isRunning):
            self.__stopevent.set()

    def _worker(self):
        while not self.__stopevent.isSet():
            if (not self.isRunning):
                break
            try:
                func, args, kwargs = self.get()
                func(*args, **kwargs)
            except Exception as details:
                self.task_done()
                self.join()
                raise
            else:
                self.task_done()

    def addJob(self, func, *args, **kwargs):
        self.put((func, args, kwargs))

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_value, traceback):
        self.__shutdown__()

    def __shutdown__(self):
        self.__stopevent.set()
        self.join()

    isRunning = property(getIsRunning, setIsRunning)
    
__queue__ = Queue(maxsize=1000)
__queue_allocated__ = 0
__instances__ = {}

import uuid

class MultiThreadQueue(ThreadQueue):
    def __init__(self):
        #super(ThreadQueue, self).__init__(1, isDaemon=True, is_multi_threaded=True)
        super().__init__(1, isDaemon=True, is_multi_threaded=True)
        self.__id__ = uuid.uuid4().hex
        __instances__[self.__id__] = self
        return
    
    def get_id(self):
        return self.__id__  # this is the handle when using this thread-queue.

    id = property(get_id)

    def get_Q(self):
        return __instances__.get(self.id)

    Q = property(get_Q)


def threadify(threadQ):
    assert threadQ.__class__ in [ThreadQueue], 'threadify decorator requires a ThreadQueue or Queue object instance, use Queue when threading is not required.'
    def decorator(func):
        def proxy(*args, **kwargs):
            threadQ.put((func, args, kwargs))
            return threadQ
        return proxy
    return decorator


class BadThreadingHandleException(Exception):
    pass

def multi_thread(id=None, handle=None):
    def wrap(f):
        def wrapped_f(*args, **kwargs):
            threadQ = __instances__.get(id)
            if (not threadQ):
                threadQ = __instances__.get(handle)
            if (threadQ):
                threadQ.put((f, args, kwargs))
            else:
                raise BadThreadingHandleException
        return wrapped_f
    return wrap

