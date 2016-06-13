import time

class Timer(object):
    def __init__(self):
        self.t = time.time()
        self.c = time.clock()

    def since(self):
        return {
            "time": time.time() - self.t,
            "clock": time.clock() - self.c
        }

    def reset(self):
        self.t = time.time()
        self.c = time.clock()

