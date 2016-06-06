from itertools import groupby
from collections import namedtuple

"""
DataSet context
"""
class DsContext(object):
    def __init__(self):
        self.workers = 8

"""
DataSet defaults. Can be overriden by config.yml.
"""
class Defaults(object):
    def __init__(self):
        """
        Buffer size for splitting ChainableLists across nodes.
        """
        self.dsBufferSize = 128
        
        """
        Default number of workers.
        """
        self.workers = 8

"""
List with chainable function methods.
"""
class ChainableList(list):
    def __init__(self, l):
        list.__init__(self,l)

    def map(self, f):
        return ChainableList(map(f, self[:]))

    def flatMap(self, f):
        return ChainableList(chain.from_iterable(imap(f, self)))

    def filter(self, f):
        return ChainableList(filter(f, self[:]))

    def reduce(self, f):
        return ChainableList(reduce(f, self[:]))

    def head(n=5):
        return ChainableList(self[:n])

    def tail(n=5):
        return ChainableList(self[:n])

    def dict(f):
        return dict(f(x) for x in self[:])

    # list items must be tuples of the form (key, ChainableList(values))
    def reduceByKey(self, f):
        groups = ChainableList([ (key, ChainableList(group)) for key, group in groupby(sorted(self), lambda x: x[0])])\
            .map(lambda (key, items): (key, items.map(lambda x: x[1])))

        res = ChainableList([])

        for key, values in groups:
            res.append((key, reduce(f, values)))

        return res

"""
Worker
"""
class Worker(object):
    def __init__(self):
        self.offsets = {}

    def pushOffset(self, dataSetKey, offset):
        if dataSetKey not in self.offsets:
            self.offsets[dataSetKey] = []
        
        self.offsets[dataSetKey].append(offset)
    
    def clearKey(self, dataSetKey):
        if dataSetKey in self.offsets:
            del self.offsets[dataSetKey]

"""
DataSet
"""
class DataSet(ChainableList):
    def __init__(self, context, key, clist):
        ChainableList.__init__(self,clist)
        
        self.workers = {}
        self.context = context
        self.key = key
        self.clist = clist
        self.defaults = Defaults()
        self.dsLen = len(clist)
        
        # Define total number of buffers
        self.buffers = self.dsLen / self.defaults.dsBufferSize
        self.buffers = self.buffers + 1 if self.dsLen % self.defaults.dsBufferSize > 0 else self.buffers
        
        for workerId in xrange(0, self.defaults.workers):
            self.workers[workerId] = Worker()
        
            for i in xrange(0, self.buffers):
                if i % (workerId + 1) == 0:
                    self.workers[workerId].pushOffset(key, i)
    
    def _pushWorker(self, workerId, clist):
        self.workers[workerId] = clist
        
    def map(self, f):
        return ChainableList(map(f, self[:]))

    def filter(self, f):
        return ChainableList(filter(f, self[:]))

    def reduce(self, f):
        return ChainableList(reduce(f, self[:]))

    # list items must be tuples of the form (key, ChainableList(values)) - like spark's LabeledPoint
    def reduceByKey(self, f):
        res = ChainableList([])
        
        groups = ChainableList([ (key, ChainableList(group)) for key, group in groupby(sorted(self), lambda x: x[0]) ])\
            .map(lambda (key, items): (key, items.map(lambda x: x[1]))) 
        
        for key, values in groups:
            res.append((key, reduce(f, values)))
        
        return res

 
l = ChainableList(xrange(0, 10000))
ds = DataSet({}, "foo", l)

ds.workers[7].offsets








