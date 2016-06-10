import redis
import hiredis

class Cache(object):
    def __init__(self, config):
        self.r = redis.StrictRedis(host=config["host"], port=config["port"], db=0)

    def pushBuffer(self, dsLabel, offset, buf):
        self.r.hset(dsLabel, offset, buf)

    def getBuffer(self, dsLabel, offset):
        return self.r.hget(dsLabel, offset)

    def incr(self, key, amount=1):
        return self.r.incr(key, amount=amount)

    def getKeys(self, dsLabel):
        return self.r.hkeys(dsLabel)

    def len(self, dsLabel):
        return self.r.hlen(dsLabel)

    def delete(self, dsLabel):
        return self.r.delete(dsLabel)

