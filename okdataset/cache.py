from cloud.serialization.cloudpickle import dumps as pickle_dumps
import hiredis
from okdataset.logger import Logger
import pickle
import redis


class Cache(object):
    def __init__(self, config):
        self.r = redis.StrictRedis(host=config["host"], port=config["port"], db=0)

    def pushBuffer(self, dsLabel, offset, buf):
        return self.r.hset(dsLabel, offset, buf)
    
    def hset(self, label, key, val):
        return self.r.hset(label, key, val)

    def get(self, label, key):
        return self.r.hget(label, key)

    def incr(self, key, amount=1):
        return self.r.incr(key, amount=amount)

    def getKeys(self, dsLabel):
        return self.r.hkeys(dsLabel)

    def len(self, dsLabel):
        return self.r.hlen(dsLabel)

    def delete(self, dsLabel):
        return self.r.delete(dsLabel)

    def hdel(self, label, key):
        return self.r.hdel(label, key)

"""
Stores metadata about labeled and intermediary datasets, including:

- Dataset label
- Function to apply
- Buffer size

It also is used for dataset removal.

"""
class Meta(object):
    def __init__(self, cache):
        self.label = "okmeta"
        self.cache = cache
        self.logger = Logger(self.label)

    def register(self, dsLabel, obj):
        self.logger.debug("Registering '%s'" % dsLabel)
        self.cache.hset(self.label, dsLabel, pickle_dumps(obj))

    def get(self, dsLabel):
        self.logger.debug("Getting '%s'" % dsLabel)
        return pickle.loads(self.cache.get(self.label, dsLabel))

    """
    create intermediary label
    append operating + fn to a list
    """
    def createIntermediary(self, ds):
        self.logger.debug("Creating intermediary '%s'" % dsLabel)
        prefix = "%s_intermediary_" % ds.label
        
        timer = Timer()
        
        self.currentDsLabel = prefix + str(self.cache.incr(prefix))
        
        self.profiler.add("masterCache", timer.since())
        
        return self.currentDsLabel
   
    def remove(self, dsLabel):
        self.logger.debug("Removing '%s'" % dsLabel)
        self.cache.hdel(self.label, dsLabel)
        self.cache.delete(dsLabel)

    def rename(self, label, newLabel):
        self.cache.hset(self.label, newLabel, self.cache.get(self.label, label))


