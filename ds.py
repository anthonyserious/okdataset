from cloud.serialization.cloudpickle import dumps as pickle_dumps
from collections import namedtuple
from itertools import groupby
import pickle

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

    def head(self, n=5):
        return ChainableList(self[:n])

    def tail(self, n=5):
        return ChainableList(self[:n])

    def dict(self,f):
        return dict(f(x) for x in self[:])

    # list items must be tuples of the form (key, ChainableList(values))
    def reduceByKey(self, f):
        groups = ChainableList([ (key, ChainableList(group)) for key, group in groupby(sorted(self), lambda x: x[0])])\
            .map(lambda (key, items): (key, items.map(lambda x: x[1])))

        res = ChainableList([])

        for key, values in groups:
            res.append((key, reduce(f, values)))

        return res

# Trivial cache for just bulding this thing out
class Cache(object):
    def __init__(self):
        self.cache = {}

    def pushBuffer(self, dataSetKey, offset, buf):
        if dataSetKey not in self.cache:
            self.cache[dataSetKey] = {}
        
        self.cache[dataSetKey][offset] = buf
        
    def getBuffer(self, dataSetKey, offset):
        return self.cache[dataSetKey][offset]

"""
Worker
"""
class Worker(object):
    def __init__(self, cache):
        self.cache = cache
        self.offsets = {}

    def pushOffset(self, dataSetKey, offset):
        if dataSetKey not in self.offsets:
            self.offsets[dataSetKey] = []
        
        self.offsets[dataSetKey].append(offset)
    
    def clearKey(self, dataSetKey):
        if dataSetKey in self.offsets:
            del self.offsets[dataSetKey]
    
    def apply(self, method, f, dataSetKey):
        for offset in self.offsets.get(dataSetKey, []):
            buf = self.cache.getBuffer(dataSetKey, offset)
            #print buf
            yield getattr(buf, method)(pickle.loads(f))

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
        
        for i in xrange(0, self.buffers):
            start = self.defaults.dsBufferSize * i
            end = self.defaults.dsBufferSize * (i + 1)
            
            self.context["cache"].pushBuffer(key, i, ChainableList(clist[start:end]))
        
        for workerId in xrange(0, self.defaults.workers):
            self.workers[workerId] = Worker(context["cache"])
        
            for i in xrange(1, self.buffers + 1):
                if i % (workerId + 1) == 0:
                    self.workers[workerId].pushOffset(key, i - 1)
    
    def _pushWorker(self, workerId, clist):
        self.workers[workerId] = clist
    
    def map(self, f):
        pf = pickle_dumps(f)
        
        for w in self.workers.keys():
            worker = self.workers[w]
            for r in worker.apply("map", pf, self.key):
                print r

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
