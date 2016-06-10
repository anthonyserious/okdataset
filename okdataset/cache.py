import redis
import hiredis

class Cache(object):
    def __init__.py(self, config):
        self.r = redis.StrictRedis(host=config["redis"]["host"], port=config["redis"]["port"], db=0)

    def put(self, dsLabel, offset, buffer):
        pass

    def get(self, dsLabel, offet):
        pass

    def uniqueId(self, key):
        pass

    def getKeys(self, dsLabel):
        pass


