class DataSetProxy(object):
    def __init__(self, client=None, clist=None, label=None, fromExisting=False, bufferSize=None):
        self.opsList = []
        self.clist = clist
        self.client = client
        
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
            self.bufferSize = self.context.config["cache"]["io"]["bufferSize"]

        self.client.send({
            "method": "create",
            "data": {
                "clist": clist,
                "label": label,
                "fromExisting": fromExisting,
                "bufferSize": self.bufferSize,
            }
        })


    def map(self, fn):
        return self.client.send({ "method": "map", "data": fn })

    def collect(self):
        return self.client.send({ "method": "collect" })

