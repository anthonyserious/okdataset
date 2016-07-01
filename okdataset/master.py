from okdataset.clist import ChainableList
from okdataset.logger import Logger
from okdataset.profiler import Profiler, Timer
from okdataset.cache import Meta

from cloud.serialization.cloudpickle import dumps as pickle_dumps
import json
import pickle
from itertools import groupby
import zmq

"""
Master
"""
class Master(ChainableList):
    def __init__(self, cache, bufferSize):
        self.cache = cache
        self.meta = Meta(self.cache)
        
        localTimer = Timer()
        self.profiler = Profiler()

        """
        zmq init
        """
        try:
            raw_input
        except NameError:
            # Python 3
            raw_input = input
        
        zmqTimer = Timer()
        context = zmq.Context()

        self.sender = context.socket(zmq.PUSH)
        self.sender.bind("tcp://*:" + str(self.context.config["cluster"]["send"]["port"]))
        self.logger.debug("Initialized sender socket")

        self.sink = context.socket(zmq.PULL)
        self.sink.bind("tcp://*:" + str(self.context.config["cluster"]["return"]["port"]))
        self.logger.debug("Initialized sink socket")
        
        self.profiler.add("localZmq", zmqTimer.since())

        """
        Store the current working dataset label.  This will change as new intermediary
        datasets are created.
        """
        self.currentDsLabel = label
        self.currentIsIntermediary = False

        if fromExisting:
            self.cache.len(label)
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
            
            self.logger.debug("Initialized with %d" % self.buffers)
            self.logger.debug(json.dumps(self.profiler.toDict(), indent=2))


    def createIntermediary(self):
        prefix = self.label + "_intermediary_"
        
        timer = Timer()
        self.currentDsLabel = prefix + str(self.cache.incr(prefix))
        self.logger.debug("Creating intermediary '%s'" % self.currentDsLabel)
        self.profiler.add("masterCache", timer.since())

        self.currentIsIntermediary = True
        
        return self.currentDsLabel

    def map(self, fn):
        self.opsList.append({ "method": "map", "fn": fn })
        return self

    def filter(self, f):
        self.opsList.append({ "method": "filter", "fn": fn })
        return self

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
        self.profiler = Profiler()
        localTimer = Timer()
 
        self.compute()
        res = ChainableList([])

        for k in sorted(self.cache.getKeys(self.currentDsLabel), key=lambda x: int(x)):
            cacheTimer = Timer()
            buf = self.cache.get(self.currentDsLabel, k)
            self.profiler.add("collectCache", cacheTimer.since())

            pickleTimer = Timer()
            buf = pickle.loads(buf)
            self.profiler.add("collectPickle", pickleTimer.since())

            res.extend(buf)

        self.profiler.add("collectMaster", localTimer.since())

        if self.currentIsIntermediary:
            self.logger.debug("Removing intermediary" + self.currentDsLabel)
            self.meta.remove(self.currentDsLabel)

        self.currentDsLabel = self.label

        return res

    def compute(self):
        self.logger.debug("Starting compute on %s" % self.currentDsLabel)

        self.profiler = Profiler()
        localTimer = Timer()
        
        cacheTimer = Timer()
        keys = self.cache.getKeys(self.currentDsLabel)
        self.profiler.add("computeCache", cacheTimer.since())

        self.logger.debug("Got %d keys" % len(keys))
        
        source = self.currentDsLabel
        dest = self.createIntermediary()

        self.meta.register(dest, {
            "opsList": self.opsList, 
            "buffers": len(keys),
            "isIntermediary": True
        })
        
        for key in keys:
            self.logger.trace("Sending key %s" % key)
            
            pickleTimer = Timer()
            msg = pickle_dumps({
                "offset": key,
                "sourceLabel": source,
                "destLabel": dest
            })
            self.profiler.add("computePickle", pickleTimer.since())

            zmqTimer = Timer()
            self.sender.send(msg)
            self.profiler.add("computeZmq", zmqTimer.since())

        results = 0

        while results != len(keys) - 1:
            self.logger.trace("Received %d out of %d results" % (results, len(keys) - 1))

            zmqTimer = Timer()
            
            res = self.sink.recv_pyobj()
            
            self.profiler.add("computeZmq", zmqTimer.since())
            self.profiler.append(res["profiler"])
            
            results = results + 1
        
        self.profiler.add("computeOverall", localTimer.since())
        
        self.logger.info("map complete")
        self.logger.debug(json.dumps(self.profiler.toDict(), indent=2))

        return self


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

