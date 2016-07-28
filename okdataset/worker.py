from okdataset.cache import Cache, Meta
from okdataset.logger import Logger
from okdataset.profiler import Profiler, Timer
from cloud.serialization.cloudpickle import dumps as pickle_dumps

import pickle
import zmq

"""
Worker
"""
class Worker(object):
    def __init__(self, config):
        self.logger = Logger("worker")
        self.offsets = {}
        
        cache = Cache(config["cache"]["redis"])
        meta = Meta(cache)

        self.currentDestLabel = ""
        self.opsList = None
        
        cluster = config["cluster"]
        
        context = zmq.Context()
        
        receiver = context.socket(zmq.PULL)
        receiver.connect("tcp://" + cluster["send"]["host"] + ":" + str(cluster["send"]["port"]))
        self.logger.debug("Initialized receiver socket")

        returner = context.socket(zmq.PUSH)
        returner.connect("tcp://" + cluster["return"]["host"] + ":" + str(cluster["return"]["port"]))
        self.logger.debug("Initialized returner socket")
        
        self.logger.info("Worker initialized")

        while True:
            profiler = Profiler()
            local = Timer()

            zmqTimer = Timer()
            msg = receiver.recv()
            profiler.add("workerZmq", zmqTimer.since())
            
            pickleTimer = Timer()
            msg = pickle.loads(msg)
            profiler.add("workerPickle", pickleTimer.since())
            self.logger.trace("Received message: " + str(msg))

            if self.currentDestLabel != msg["destLabel"]:
                self.opsList = meta.get(msg["destLabel"])["opsList"]
                self.currentDestLabel = msg["destLabel"]
            
            cacheTimer = Timer()
            buf = cache.get(msg["sourceLabel"], msg["offset"])
            profiler.add("workerCache", cacheTimer.since())

            pickleTimer = Timer()
            buf = pickle.loads(buf)
            profiler.add("workerPickle", pickleTimer.since())

            self.logger.trace("Received buffer")
            
            res = buf
            for op in self.opsList:
                res = getattr(res, op["method"])(op["fn"])
            
            self.logger.trace("Processed buffer")

            reply = {
                "destLabel": msg["destLabel"],
                "offset": msg["offset"],
                "status": "ok",
                "profiler": profiler
            }

            # in case of flatMap
            if len(res) != len(buf):
                reply["size"] = len(res)

            pickleTimer = Timer()
            res = pickle_dumps(res)
            profiler.add("workerPickle", pickleTimer.since())

            cacheTimer = Timer()
            cache.pushBuffer(msg["destLabel"], msg["offset"], res)
            profiler.add("workerCache", cacheTimer.since())

            self.logger.trace("Processed buffer")

            profiler.add("workerOverall", local.since())

            returner.send_pyobj(reply)
            self.logger.trace("Reply sent")


