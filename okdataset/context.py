from okdataset.cache import Cache

import os
import yaml

"""
DataSet context
"""
class Context(object):
    def __init__(self, config=os.path.dirname(os.path.realpath(__file__)) + "/../config.yml"):
        self.config = yaml.load(open(config).read())
        
        self.bufferSize = config["cache"]["bufferSize"]
        self.cache = Cache(config["cache"]["redis"])
        self.master = config["master"]


