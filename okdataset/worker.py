from okdataset.cache import Cache
from okdataset.logger import Logger
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
            msg = pickle.loads(receiver.recv())
            self.logger.trace("Received message: " + str(msg))
            
            buf = pickle.loads(cache.getBuffer(msg["sourceLabel"], msg["offset"]))
            self.logger.trace("Received buffer")
            
            res = getattr(buf, msg["method"])(msg["fn"])
            self.logger.trace("Processed buffer")

            cache.pushBuffer(msg["destLabel"], msg["offset"], pickle_dumps(res))
            self.logger.trace("Processed buffer")

            reply = {
              "destLabel": msg["destLabel"],
              "offset": msg["offset"],
              "status": "ok"
            }

            returner.send_pyobj(reply)
            self.logger.trace("Reply sent")


