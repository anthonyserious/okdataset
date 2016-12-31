#!/usr/bin/env python

from okdataset.clist import ChainableList
from okdataset.dataset import DataSet
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
    """
    Master is both the client-facing server process and also the grid controller.
    """
    def __init__(self, config, cache, bufferSize):
        self.cache = cache
        self.config = config
        self.logger = Logger("master")
        self.meta = Meta(self.cache)
        self.dataSets = {}
        
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
        self.sender.bind("tcp://*:" + str(self.config["cluster"]["master"]["port"]))
        self.logger.debug("Initialized sender socket")

        # Sink
        self.sink = context.socket(zmq.PULL)
        self.sink.bind("tcp://*:" + str(self.config["cluster"]["return"]["port"]))
        self.logger.debug("Initialized sink socket")
        
        # Server
        self.server = context.socket(zmq.REP)
        self.server.bind("tcp://*:" + str(self.config["server"]["port"]))
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

    def getProfile(self, fn=None):
        if fn:
            fn(self.profiler.toDict())
        else:
            return self.profiler.toDict()

    def mainLoop(self):
        while True:
            self.logger.debug("Receiving")
            req = pickle.loads(self.server.recv())
            data = req.get("data")

            if req["method"] == "create":
                self.logger.debug("create called for dataset %s, id %s" % (req["data"]["label"], req["id"]))
                ds = DataSet(self.cache, self.config, data["clist"], label=data["label"], fromExisting=data["fromExisting"], bufferSize=data["bufferSize"])
                print(ds.currentDsLabel)
                self.dataSets[req["id"]] = ds
                self.server.send(pickle_dumps({ "status": "ok" }))

            elif req["method"] in [ "map", "flatMap", "reduceByKey", "filter" ]:
                ds = self.dataSets[req["id"]]
                self.logger.debug("%s called for dataset %s, id %s" % (req["method"], ds.currentDsLabel, req["id"]))

                getattr(ds, req["method"])(data)
                self.server.send(pickle_dumps({ "status": "ok" }))

            elif req["method"] in [ "collect", "reduce" ]:
                ds = self.dataSets[req["id"]]
                self.logger.debug("%s called for dataset %s, id %s" % (req["method"], ds.currentDsLabel, req["id"]))

                self.compute(ds.currentDsLabel, ds.createIntermediary(), ds.opsList)
                res = ds.collect()

                if req["method"] == "reduce":
                    res = res.reduce(data)

                self.server.send(pickle_dumps({"status": "ok", "data": res}))

            elif req["method"] == "compute":
                ds = self.dataSets[req["id"]]
                self.logger.debug("compute called for dataset %s, id %s" % (ds.currentDsLabel, req["id"]))

                self.compute(ds.currentDsLabel, ds.createIntermediary(), ds.opsList)
                self.server.send(pickle_dumps({ "status": "ok" }))

            else:
                self.logger.debug("unknown method %s" % req["method"])


if __name__ == "__main__":
    import doctest, sys
    from okdataset.mocks import Logger
    import okdataset.mocks.zmq as zmq

    sys.exit(doctest.testmod()[0])

