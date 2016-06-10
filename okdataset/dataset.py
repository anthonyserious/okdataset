from okdataset.clist import ChainableList
from okdataset.worker import Worker

from cloud.serialization.cloudpickle import dumps as pickle_dumps
import pickle
from itertools import groupby

"""
DataSet
"""
class DataSet(ChainableList):
    def __init__(self, context, label, clist):
        ChainableList.__init__(self,clist)
        
        self.workers = context.workers
        self.context = context
        self.cache = context.cache
        self.label = label
        self.clist = clist
        self.dsLen = len(clist)
        self.bufferSize = self.context.config["cache"]["io"]["bufferSize"]

        """
        Store the current working dataset label.  This will change as new intermediary
        datasets are created.
        """
        self.currentDsLabel = label
        
        # Set total number of buffers
        self.buffers = self.dsLen / self.bufferSize
        self.buffers = self.buffers + 1 if self.dsLen % self.bufferSize > 0 else self.buffers

        for i in xrange(0, self.buffers):
            start = self.bufferSize * i
            end = self.bufferSize * (i + 1)

            self.cache.pushBuffer(label, i, ChainableList(clist[start:end]))

        for workerId in self.workers:
            for i in xrange(0, self.buffers):
                if i % (workerId + 1) == 0:
                    self.workers[workerId].pushOffset(self.label, i)

    def createIntermediary(self):
        prefix = self.label + "_intermediary_"
        self.currentDsLabel = prefix + str(self.cache.incr(prefix))
        return self.currentDsLabel

    def map(self, f):
        pf = pickle_dumps(f)

        for w in self.workers.keys():
            worker = self.workers[w]
            worker.apply("map", pf, self.currentDsLabel, self.createIntermediary())
        return self

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

    def collect(self):
        pass

