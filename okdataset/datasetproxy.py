import uuid

class DataSetProxy(object):
    def __init__(self, client, config, clist=None, label=None, fromExisting=False, bufferSize=None):
        self.opsList = []
        self.clist = clist
        self.client = client


        # unique dataset handle ID
        self.id = uuid.uuid1()
        
        if client is None:
            raise ValueError("Must provide a client")
        
        if clist is None and not fromExisting:
            raise ValueError("Must provide either clist or fromExisting")

        if clist is not None and fromExisting:
            raise ValueError("Cannot provide both clist and fromExisting")

        if fromExisting and bufferSize is not None:
            raise ValueError("Cannot specify bufferSize for existing dataset")

        if bufferSize is not None:
            self.bufferSize = bufferSize
        else:
            self.bufferSize = config["cache"]["io"]["bufferSize"]

        self.client.send({
            "id": self.id,
            "method": "create",
            "data": {
                "clist": clist,
                "label": label,
                "fromExisting": fromExisting,
                "bufferSize": self.bufferSize,
            }
        })


    def map(self, fn):
        self.client.send({ "id": self.id, "method": "map", "data": fn })
        return self

    def flatMap(self, fn):
        self.client.send({ "id": self.id, "method": "flatMap", "data": fn })
        return self

    def filter(self, fn):
        self.client.send({ "id": self.id, "method": "filter", "data": fn })
        return self

    def reduce(self, fn):
        return self.client.send({ "id": self.id, "method": "reduce", "data": fn })

    def reduceByKey(self, fn):
        self.client.send({ "id": self.id, "method": "reduceByKey", "data": fn })
        return self

    def collect(self):
        res = self.client.send({ "id": self.id, "method": "collect" })
        if res["status"] == "ok":
            return res["data"]

    def compute(self):
        self.client.send({"id": self.id, "method": "compute"})
        return self
