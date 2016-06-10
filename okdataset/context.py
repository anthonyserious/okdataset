"""
DataSet context
"""
class DsContext(object):
    def __init__(self, config="okdataset.yml"):
        self.workers = 8
