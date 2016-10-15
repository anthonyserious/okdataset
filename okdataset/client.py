from cloud.serialization.cloudpickle import dumps as pickle_dumps
from logger import Logger
from cache import Meta
import pickle
import zmq

class Client(object):
    def __init__(self, config, cache, bufferSize):
        self.cache = cache
        self.config = config
        self.logger = Logger("master")
        self.meta = Meta(self.cache)

        self.context = zmq.Context()

        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect("tcp://%s:%d" % (self.config["server"]["host"], self.config["server"]["port"]))

    def send(self, data):
        self.socket.send(pickle_dumps(data))
        return pickle.loads(self.socket.recv())
        

