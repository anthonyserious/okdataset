class LoggerMock(object):
    def __init__(self, name):
        self.name = name

    def info(self, fields, msg=None):
        pass

    def debug(self, fields, msg=None):
        pass

    def trace(self, fields, msg=None):
        pass

    def error(self, fields, msg=None):
        pass

    def fatal(self, fields, msg=None):
        pass

    def warn(self, fields, msg=None):
        pass

