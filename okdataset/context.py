from okdataset.cache import Cache
from okdataset.dataset import DataSet
from master import Master

import os
import yaml

"""
DataSet context
"""
class Context(object):
    def __init__(self, master=True, configFile=os.path.dirname(os.path.realpath(__file__)) + "/../config.yml"):
        self.config = yaml.load(open(configFile).read())

        self.bufferSize = self.config["cache"]["io"]["bufferSize"]
        self.cache = Cache(self.config["cache"]["redis"])

        if master:
            self.master = Master(self.config, self.cache, self.bufferSize)
    
    def dataSet(self, *args, **kwargs):
        return DataSet(self, *args, **kwargs)


