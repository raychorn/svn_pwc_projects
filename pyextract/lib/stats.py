class RuntimeStats(object):
    '''
    def stats():
        doc = "stats property"
        def fget(self):
            return self.__stats__
        def fset(self, value):
            self._foo = value
        def fdel(self):
            del self._foo
        return locals()  # credit: David Niergarth
    foo = property(**foo())

    '''
    def __init__(self):
        self.__selector__ = None
        self.__analysis__ = {}
        self.__stats__ = []
        
    def stats():
        doc = "stats property"
        def fget(self):
            return self.__stats__
        return locals()
    stats = property(**stats())

    def analysis():
        doc = "analysis property"
        def fget(self):
            return self.__analysis__
        def fset(self, selector):
            self.__selector__ = selector
            total = 0.0
            count = 0
            for item in self.__stats__:
                __is__ = isinstance(item, dict)
                total += item.get(self.__selector__, 0.0) if (__is__) else 0.0
                if (__is__):
                    count += 1
            self.__analysis__['duration'] = total
            self.__analysis__['average'] = (total / count) if (count > 0) else float('nan')
        return locals()
    analysis = property(**analysis())
       
