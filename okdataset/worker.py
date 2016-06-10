"""
Worker
"""
class Worker(object):
    def __init__(self):
        self.offsets = {}

    def pushOffset(self, dataSetKey, offset):
        if dataSetKey not in self.offsets:
            self.offsets[dataSetKey] = []
        
        self.offsets[dataSetKey].append(offset)
    
    def clearKey(self, dataSetKey):
        if dataSetKey in self.offsets:
            del self.offsets[dataSetKey]

