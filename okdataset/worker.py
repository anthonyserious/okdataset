from okdataset.cache import Cache
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
            msg = pickle.loads(receiver.recv())
            profiler.add("workerZmq", zmqTimer.since())

            self.logger.trace("Received message: " + str(msg))
            
            cacheTimer = Timer()
            buf = pickle.loads(cache.getBuffer(msg["sourceLabel"], msg["offset"]))
            profiler.add("workerCache", cacheTimer.since())

            self.logger.trace("Received buffer")
            
            res = getattr(buf, msg["method"])(msg["fn"])
            self.logger.trace("Processed buffer")

            cacheTimer = Timer()
            cache.pushBuffer(msg["destLabel"], msg["offset"], pickle_dumps(res))
            profiler.add("workerCache", cacheTimer.since())

            self.logger.trace("Processed buffer")

            profiler.add("workerOverall", local.since())

            reply = {
              "destLabel": msg["destLabel"],
              "offset": msg["offset"],
              "status": "ok",
              "profiler": profiler
            }

            returner.send_pyobj(reply)
            self.logger.trace("Reply sent")


