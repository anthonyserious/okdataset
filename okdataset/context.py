from okdataset.cache import Cache
from okdataset.worker import Worker

import os
import yaml

"""
DataSet context
"""
class Context(object):
    def __init__(self, configFile=os.path.dirname(os.path.realpath(__file__)) + "/../config.yml"):
        self.config = yaml.load(open(configFile).read())
        
        self.bufferSize = self.config["cache"]["io"]["bufferSize"]
        self.cache = Cache(self.config["cache"]["redis"])
        self.master = self.config["master"]

        self.workers = {}

        for workerId in xrange(0, self.config["workers"]):
            self.workers[workerId] = Worker(self.config)
        

