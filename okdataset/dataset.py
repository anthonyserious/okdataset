from okdataset.clist import ChainableList
from okdataset.logger import Logger

from cloud.serialization.cloudpickle import dumps as pickle_dumps
import pickle
from itertools import groupby
import zmq

"""
DataSet
"""
class DataSet(ChainableList):
    def __init__(self, context, label, clist=None, fromExisting=False, bufferSize=None):
        self.context = context
        self.cache = context.cache
        self.label = label
        
        if clist is None and not fromExisting:
            raise ValueError("Must provide either clist or fromExisting")

        if clist is not None and fromExisting:
            raise ValueError("Cannot provide both clist and fromExisting")

        if fromExisting and bufferSize is not None:
            raise ValueError("Cannot specify bufferSize for existing dataset")

        if clist is not None:
            ChainableList.__init__(self, clist)
        
            self.clist = clist
            self.dsLen = len(clist)
        
        if bufferSize is not None:
            self.bufferSize = bufferSize
        else:
            self.bufferSize = self.context.config["cache"]["io"]["bufferSize"]

        self.logger = Logger("dataset '" + self.label + "'")

        """
        zmq init
        """
        try:
            raw_input
        except NameError:
            # Python 3
            raw_input = input
        
        context = zmq.Context()

        self.sender = context.socket(zmq.PUSH)
        self.sender.bind("tcp://*:" + str(self.context.config["cluster"]["send"]["port"]))
        self.logger.debug("Initialized sender socket")

        self.sink = context.socket(zmq.PULL)
        self.sink.bind("tcp://*:" + str(self.context.config["cluster"]["return"]["port"]))
        self.logger.debug("Initialized sink socket")

        """
        Store the current working dataset label.  This will change as new intermediary
        datasets are created.
        """
        self.currentDsLabel = label
        if fromExisting:
            self.cache.len(label)
        else:
            # Set total number of buffers
            self.buffers = self.dsLen / self.bufferSize
            self.buffers = self.buffers + 1 if self.dsLen % self.bufferSize > 0 else self.buffers

            for i in xrange(0, self.buffers + 1):
                start = self.bufferSize * i
                end = self.bufferSize * (i + 1)

                self.cache.pushBuffer(label, i, pickle_dumps(ChainableList(clist[start:end])))
            
            self.logger.debug("Initialized with %d" % self.buffers)

    def createIntermediary(self):
        prefix = self.label + "_intermediary_"
        self.currentDsLabel = prefix + str(self.cache.incr(prefix))
        return self.currentDsLabel

    def map(self, f):
        self.logger.debug("Starting map on %s" % self.currentDsLabel)

        keys = self.cache.getKeys(self.currentDsLabel)
        self.logger.debug("Got %d keys" % len(keys))
        
        source = self.currentDsLabel
        dest = self.createIntermediary()
        
        for key in keys:
            self.logger.trace("Sending key %s" % key)
            self.sender.send(pickle_dumps({
                "method": "map",
                "fn": f,
                "offset": key,
                "sourceLabel": source,
                "destLabel": dest
        }))

        results = 0

        while results != len(keys) - 1:
            self.logger.trace("Received %d out of %d results" % (results, len(keys) - 1))
            res = self.sink.recv_pyobj()
            results = results + 1
        
        self.logger.debug("map complete")

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

