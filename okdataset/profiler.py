import time

class Profiler(object):
    def __init__(self):
        pass

    def start(self):
        self.t = time.time()
        self.c = time.clock()

    def since(self):
        return {
            "time": time.time() - self.t,
            "clock": time.clock() - self.c
        }

