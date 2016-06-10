import yaml
import os

"""
DataSet context
"""
class Context(object):
    def __init__(self, config=os.path.dirname(os.path.realpath(__file__)) + "/../okdataset.yml"):
        self.workers = 8
        self.config = yaml.load(open(config).read())

