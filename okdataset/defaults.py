"""
DataSet defaults. Can be overriden by config.yml.
"""
class Defaults(object):
    def __init__(self):
        """
        Buffer size for splitting ChainableLists across nodes.
        """
        self.dsBufferSize = 128
        
        """
        Default number of workers.
        """
        self.workers = 8
