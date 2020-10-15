import time

def timeit(stats=None):
    def wrap(f):
        def wrapped_f(*args, **kwargs):
            __is__ = isinstance(stats, list)
            if (__is__):
                start_timestamp = time.time()
            try:
                f(*args, **kwargs)
            except:
                pass
            if (__is__):
                duration = time.time() - start_timestamp
                stats.append({'duration': duration, 'args': args, 'kwargs': kwargs})
        return wrapped_f
    return wrap

def profileit(stats=None):
    def wrap(f):
        def wrapped_f(*args, **kwargs):
            import cProfile, pstats, io
            pr = cProfile.Profile()
            pr.enable()
            ######################################
            try:
                f(*args, **kwargs)
            except:
                pass
            ######################################
            pr.disable()
            s = io.StringIO()
            sortby = 'cumulative'
            ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
            ps.print_stats()
            if (isinstance(stats, list)):
                stats.append(s.getvalue())
        return wrapped_f
    return wrap


class classproperty(property):
    def __get__(self, cls, owner):
        return classmethod(self.fget).__get__(None, owner)()
    
if (__name__ == '__main__'):
    __stats__ = []
    @timeit(stats=__stats__)
    def sayHello(*args, **kwargs):
        print('sayHello arguments.1:', args)
        print('sayHello arguments.2:', kwargs)
        
    foo = 'bar'
    sayHello("a", "different", "set of", "arguments", foo=foo)
    
    print('BEGIN:')
    for item in __stats__:
        print(item)
    print('END!!!')
    
