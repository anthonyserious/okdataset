from okdataset.cache import Cache
from okdataset.datasetproxy import DataSetProxy
from master import Master
from client import Client
from worker import Worker

import os
import yaml

"""
DataSet context
"""
class Context(object):
    def __init__(self, client=True, configFile=os.path.dirname(os.path.realpath(__file__)) + "/../config.yml"):
        self.config = yaml.load(open(configFile).read())

        self.bufferSize = self.config["cache"]["io"]["bufferSize"]
        self.cache = Cache(self.config["cache"]["redis"])

        if client:
            self.client = Client(self.config, self.cache, self.bufferSize)

        print(self.client)

    def createMaster(self):
        return Master(self.config, self.cache, self.bufferSize)

    def createWorker(self):
        return Worker(self.config, self.cache)

    def dataSet(self, *args, **kwargs):
        print(kwargs)
        return DataSetProxy(self.client, self.config, *args, **kwargs)


