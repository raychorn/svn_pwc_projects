from queue import Queue

from threadpools import multi_thread
from threadpools import MultiThreadQueue

import decorators

class ThreadedWorker(object):
    __workers__ = []

    def __init__(self, results_queue_size=1000):
        self.__resultsQ__ = Queue(results_queue_size)
        self.__isDaemon__ = True
        self.__task__ = None # tuple with func, args, kwargs,
        self.__Q__ = MultiThreadQueue()
        self.__thread_handle__ = self.__Q__.id

    @decorators.classproperty
    def workers(cls):
        return cls.__workers__
    
    def task():
        doc = "task property"
        def fget(self):
            return self.__task__
        def fset(self, task):
            self.__task__ = task
            #self.__Q__.put(task)
        return locals()
    task = property(**task())

    def isDaemon():
        doc = "isDaemon property"
        def fget(self):
            return self.__isDaemon__
        return locals()
    isDaemon = property(**isDaemon())

    def resultsQ():
        doc = "resultsQ property"
        def fget(self):
            return self.__resultsQ__
        return locals()
    resultsQ = property(**resultsQ())

    def thread_handle():
        doc = "thread_handle property"
        def fget(self):
            return self.__thread_handle__
        return locals()
    thread_handle = property(**thread_handle())

    def id():
        doc = "thread_handle or id property"
        def fget(self):
            return self.__thread_handle__
        return locals()
    id = property(**id())

    def Q():
        doc = "Thread Q instance property"
        def fget(self):
            return self.__Q__
        return locals()
    Q = property(**Q())
    
    def __join__(self):
        if (len(self.workers)):
            for worked in self.workers:
                worker.__join__()
        else:
            self.__Q__.join()

    def run(self, *args, **kwargs):
        @multi_thread(handle=self.thread_handle)
        def __run__(*args, **kwargs):
            func, args, kwargs = self.task
            args = args if (isinstance(args, list)) else [args]
            func(*args, **kwargs)

        __run__(args, kwargs)

