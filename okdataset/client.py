from cloud.serialization.cloudpickle import dumps as pickle_dumps
import zmq

class Client(object):
    def __init__(self, config, cache, bufferSize):
        self.cache = cache
        self.config = config
        self.logger = Logger("master")
        self.meta = Meta(self.cache)

        self.context = zmq.Context()

        self.socket = context.socket(zmq.REQ)
        socket.connect("tcp://%s:%d" % (self.config["master"]["host"], self.config["master"]["port"]))



