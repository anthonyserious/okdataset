"""
DataSet
"""
class DataSet(ChainableList):
    def __init__(self, context, key, clist):
        ChainableList.__init__(self,clist)
        
        self.workers = {}
        self.context = context
        self.key = key
        self.clist = clist
        self.defaults = Defaults()
        self.dsLen = len(clist)
        
        # Define total number of buffers
        self.buffers = self.dsLen / self.defaults.dsBufferSize
        self.buffers = self.buffers + 1 if self.dsLen % self.defaults.dsBufferSize > 0 else self.buffers
        
        for workerId in xrange(0, self.defaults.workers):
            self.workers[workerId] = Worker()
        
            for i in xrange(0, self.buffers):
                if i % (workerId + 1) == 0:
                    self.workers[workerId].pushOffset(key, i)
    
    def _pushWorker(self, workerId, clist):
        self.workers[workerId] = clist
        
    def map(self, f):
        return ChainableList(map(f, self[:]))

    def filter(self, f):
        return ChainableList(filter(f, self[:]))

    def reduce(self, f):
        return ChainableList(reduce(f, self[:]))

    # list items must be tuples of the form (key, ChainableList(values)) - like spark's LabeledPoint
    def reduceByKey(self, f):
        res = ChainableList([])
        
        groups = ChainableList([ (key, ChainableList(group)) for key, group in groupby(sorted(self), lambda x: x[0]) ])\
            .map(lambda (key, items): (key, items.map(lambda x: x[1]))) 
        
        for key, values in groups:
            res.append((key, reduce(f, values)))
        
        return res

