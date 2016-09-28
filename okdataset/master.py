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
    def __init__(self, config, cache, bufferSize):
        self.cache = cache
        self.config = config
        self.logger = Logger("master")
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

        # Sender
        self.sender = context.socket(zmq.PUSH)
        self.sender.bind("tcp://*:" + str(self.config["cluster"]["send"]["port"]))
        self.logger.debug("Initialized sender socket")

        # Sink
        self.sink = context.socket(zmq.PULL)
        self.sink.bind("tcp://*:" + str(self.config["cluster"]["return"]["port"]))
        self.logger.debug("Initialized sink socket")
        
        # Server
        self.server = context.socket(zmq.REP)
        self.socket.bind("tcp://*:" + str(self.config["cluster"]["return"]["port"]))
        self.logger.debug("Initialized server socket")

        self.profiler.add("localZmq", zmqTimer.since())

    def compute(self, label, intermediaryLabel, opsList):
        self.logger.debug("Starting compute on %s" % label)

        self.profiler = Profiler()
        localTimer = Timer()
        
        cacheTimer = Timer()
        keys = self.cache.getKeys(label)
        self.profiler.add("computeCache", cacheTimer.since())

        self.logger.debug("Got %d keys" % len(keys))
        
        source = label
        dest = intermediaryLabel

        self.meta.register(dest, {
            "opsList": opsList, 
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
        
        self.logger.info("compute complete")
        self.logger.debug(json.dumps(self.profiler.toDict(), indent=2))

        return self


    def getProfile(self, f=None):
        if f:
            f(self.profiler.toDict())
        else:
            return self.profiler.toDict()

