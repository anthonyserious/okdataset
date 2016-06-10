from okdataset.cache import Cache
from cloud.serialization.cloudpickle import dumps as pickle_dumps

import pickle
import zmq

"""
Worker
"""
class Worker(object):
    def __init__(self, config):
        self.offsets = {}
        cache = Cache(config["cache"]["redis"])
        
        cluster = config["cluster"]
        
        context = zmq.Context()
        
        receiver = context.socket(zmq.PULL)
        receiver.connect("tcp://" + cluster["send"]["host"] + ":" + str(cluster["send"]["port"]))
        
        returner = context.socket(zmq.PUSH)
        returner.connect("tcp://" + cluster["return"]["host"] + ":" + str(cluster["return"]["port"]))

        while True:
            msg = pickle.loads(receiver.recv())
            print "Got msg: ", msg
            
            buf = pickle.loads(cache.getBuffer(msg["sourceLabel"], msg["offset"]))
            print type(buf)
            print "Got bug"
            res = getattr(buf, msg["method"])(msg["fn"])
            print "Got res"

            cache.pushBuffer(msg["destLabel"], msg["offset"], pickle_dumps(res))

            reply = {
              "destLabel": msg["destLabel"],
              "offset": msg["offset"],
              "status": "ok"
            }

            returner.send_pyobj(reply)


