from okdataset.clist import ChainableList
from okdataset.logger import Logger
from okdataset.profiler import Profiler, Timer
from okdataset.cache import Meta

from cloud.serialization.cloudpickle import dumps as pickle_dumps
import json
import pickle
from itertools import groupby
import uuid
import zmq

"""
DataSet
"""
class DataSet(object):
    def __init__(self, context, clist=None, label=None, fromExisting=False, bufferSize=None):
        self.context = context
        self.cache = context.cache
        self.meta = Meta(self.cache)
        self.label = label if label else "okds_%s" % uuid.uuid1()
        self.opsList = []
        
        localTimer = Timer()

        self.profiler = Profiler()
        
        if clist is None and not fromExisting:
            raise ValueError("Must provide either clist or fromExisting")

        if clist is not None and fromExisting:
            raise ValueError("Cannot provide both clist and fromExisting")

        if fromExisting and bufferSize is not None:
            raise ValueError("Cannot specify bufferSize for existing dataset")

        if clist is not None:
            self.dsLen = len(clist)
        
        if bufferSize is not None:
            self.bufferSize = bufferSize
        else:
            self.bufferSize = self.context.config["cache"]["io"]["bufferSize"]

        self.logger = Logger("dataset '" + self.label + "'")

        """
        Store the current working dataset label.  This will change as new intermediary
        datasets are created.
        """
        self.currentDsLabel = label
        self.currentIsIntermediary = False

        if fromExisting:
            self.dsLen = self.cache.len(label)
        else:
            # Set total number of buffers
            self.buffers = self.dsLen / self.bufferSize
            self.buffers = self.buffers + 1 if self.dsLen % self.bufferSize > 0 else self.buffers

            for i in xrange(0, self.buffers + 1):
                start = self.bufferSize * i
                end = self.bufferSize * (i + 1)
                
                pickleTimer = Timer()
                buf = pickle_dumps(ChainableList(clist[start:end]))
                self.profiler.add("masterPickle", pickleTimer.since())

                cacheTimer = Timer()
                self.cache.pushBuffer(label, i, buf)
                self.profiler.add("masterCache", cacheTimer.since())
            
            self.logger.debug("Initialized with %d buffers" % self.buffers)
            self.logger.debug(json.dumps(self.profiler.toDict(), indent=2))

    def compute(self):
        self.context.master.compute(self.currentDsLabel, self.createIntermediary(), self.opsList)
        return self

    def createIntermediary(self):
        prefix = self.label + "_intermediary_"
        
        timer = Timer()
        self.currentDsLabel = prefix + str(self.cache.incr(prefix))
        self.logger.debug("Creating intermediary '%s'" % self.currentDsLabel)
        self.profiler.add("masterCache", timer.since())

        self.currentIsIntermediary = True
        
        return self.currentDsLabel

    def flatMap(self, fn):
        self.opsList.append({ "method": "flatMap", "fn": fn })
        return self

    def map(self, fn):
        self.opsList.append({ "method": "map", "fn": fn })
        return self

    def filter(self, fn):
        self.opsList.append({ "method": "filter", "fn": fn })
        return self

    def reduce(self, fn):
        return ChainableList(reduce(fn, self[:]))

    # list items must be tuples of the form (key, ChainableList(values)) - like spark's LabeledPoint
    def reduceByKey(self, f):
        res = ChainableList([])
        
        groups = ChainableList([ (key, ChainableList(group)) for key, group in groupby(sorted(self), lambda x: x[0]) ])\
            .map(lambda (key, items): (key, items.map(lambda x: x[1]))) 
        
        for key, values in groups:
            res.append((key, reduce(f, values)))
        
        return res

    def collect(self):
        self.profiler = Profiler()
        localTimer = Timer()
 
        self.compute()
        res = ChainableList([])

        for k in sorted(self.cache.getKeys(self.currentDsLabel), key=lambda x: int(x)):
            cacheTimer = Timer()
            buf = self.cache.get(self.currentDsLabel, k)
            self.profiler.add("collectCache", cacheTimer.since())

            res.extend(buf)

        self.profiler.add("collectMaster", localTimer.since())

        if self.currentIsIntermediary:
            self.logger.debug("Removing intermediary" + self.currentDsLabel)
            self.meta.remove(self.currentDsLabel)

        self.currentDsLabel = self.label

        return pickle_dumps(res)


    def getProfile(self, f=None):
        if f:
            f(self.profiler.toDict())
        else:
            return self.profiler.toDict()

    def label(self, label):
        self.meta.rename(self.currentDsLabel, label)
        self.currentDsLabel = label
        self.currentIsIntermediary = False

    def __del__(self):
        self.meta.remove(self.currentDsLabel)

