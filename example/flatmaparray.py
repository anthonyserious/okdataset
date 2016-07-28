#!/usr/bin/env python

from okdataset.clist import ChainableList
from okdataset.context import Context
from okdataset.logger import Logger

logger = Logger("flatmaparray example")

context = Context()
logger.info("Building big list")
l = ChainableList([ x for x in xrange(0, 100) ])

logger.info("Building dataset")
ds = context.dataSet(l, label="flatMap list", bufferSize=1)

logger.info("Calling flatMap")

def fm(x):
    for i in [ "a", "b", "c" ]:
        yield [ x, i ]

res = ds.flatMap(fm).collect()
print(res)
logger.info("All done!")

