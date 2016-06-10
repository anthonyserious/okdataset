from okdataset.cache import Cache

"""
Worker
"""
class Worker(object):
    def __init__(self, config):
        self.config = config
        self.offsets = {}
        self.cache = Cache(config["redis"])

    def pushOffset(self, dataSetKey, offset):
        if dataSetKey not in self.offsets:
            self.offsets[dataSetKey] = []
        
        self.offsets[dataSetKey].append(offset)
    
    def clearKey(self, dataSetKey):
        if dataSetKey in self.offsets:
            del self.offsets[dataSetKey]

    def apply(self, method, f, dsLabel, intermediaryLabel):
        for offset in self.offsets.get(dsLabel, []):
            buf = self.cache.getBuffer(dsLabel, offset)
            #print buf
            yield getattr(buf, method)(pickle.loads(f))

