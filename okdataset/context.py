from okdataset.cache import Cache

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
        self.master = Master(self.config, self.cache, self.bufferSize)


