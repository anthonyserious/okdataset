import time

class Time(object):
    def __init__(self, t=None, c=None):
        if t is not None and c is not None:
            self.time = t
            self.cpu = c
        else:
            self.time = time.time()
            self.cpu = time.clock()

    def __add__(self, t):
        return Time(
            self.time + t.time, 
            self.cpu + t.cpu
        )

    def toDict(self):
        return { "time": self.time, "cpu": self.cpu }

class Timer(object):
    def __init__(self):
        self.t = Time()

    def since(self):
        return Time(
            t = time.time() - self.t.time,
            c = time.clock() - self.t.cpu
        )

    def reset(self):
        self.t = Time()

class Profiler(object):
    def __init__(self):
        self.timings = {}

    def add(self, key, t):
        if key not in self.timings:
            self.timings[key] = t
        else:
            self.timings[key] += t
    
    def getTimings(self):
        return self.timings

    def toDict(self):
        return dict((k, v.toDict()) for k, v in self.timings.iteritems())


    # Appends all profiler data from p
    def append(self, p):
        for k, v in p.getTimings().iteritems():
            self.add(k, v)


