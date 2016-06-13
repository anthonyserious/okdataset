import datetime
import json
import os
import platform
import sys

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

        # default to INFO level logging
        self.level = int(os.environ.get("LOG_LEVEL", 30))
        
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
    
    def _print(self, log):
        print json.dumps(log)
        sys.stdout.flush()
        
    def fatal(self, fields, msg=None):
        if self.level > 60:
            return
        log = self._pre()
        log["level"] = 60
        self._print(log)
        
    def error(self, fields, msg=None):
        if self.level > 50:
            return
        log = self._pre(fields, msg=msg)
        log["level"] = 50
        self._print(log)
        
    def warn(self, fields, msg=None):
        if self.level > 40:
            return
        log = self._pre(fields, msg=msg)
        log["level"] = 40
        self._print(log)
        
    def info(self, fields, msg=None):
        if self.level > 30:
            return
        log = self._pre(fields, msg=msg)
        log["level"] = 30
        self._print(log)

    def debug(self, fields, msg=None):
        if self.level > 20:
            return
        log = self._pre(fields, msg=msg)
        log["level"] = 20
        self._print(log)

    def trace(self, fields, msg=None):
        if self.level > 10:
            return
        log = self._pre(fields, msg=msg)
        log["level"] = 10
        self._print(log)
