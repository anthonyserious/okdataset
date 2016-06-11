import datetime
import json
import os
import platform

"""
Logger compatible with node.js's llog.

{
    "name":"myapp",
    "hostname":"banana.local",
    "pid":40161,
    "level":30,
    "msg":"hi",
    "time":"2013-01-04T18:46:23.851Z",
    "v":0
}

Log levels:
    60: fatal
    50: error
    40: warn
    30: info
    20: debug
    10: trace

"""

class Logger(object):
    def __init__(self, name):
        self.name = name
        
        self.hostname = platform.node()
        self.pid = os.getpid()
        
    def _pre(self, fields, msg=None):
        log = {
            "v": 0,
            "hostname": self.hostname,
            "pid": self.pid,
            "time": datetime.datetime.now().isoformat(),
            "name": self.name
        }
        
        if msg is None:
            log["msg"] = fields
        else:
            log.update(fields)
            log["msg"] = msg
        
        return log
        
    def fatal(self, fields, msg=None):
        log = self._pre()
        log["level"] = 60
        print log
        
    def error(self, fields, msg=None):
        log = self._pre(fields, msg=msg)
        log["level"] = 50
        print log
        
    def warn(self, fields, msg=None):
        log = self._pre(fields, msg=msg)
        log["level"] = 40
        print log
        
    def info(self, fields, msg=None):
        log = self._pre(fields, msg=msg)
        log["level"] = 30
        print log

    def debug(self, fields, msg=None):
        log = self._pre(fields, msg=msg)
        log["level"] = 20
        print log

    def trace(self, fields, msg=None):
        log = self._pre(fields, msg=msg)
        log["level"] = 10
        print log
