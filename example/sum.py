#!/usr/bin/env python

from okdataset import ChainableList, Context, Logger

logger = Logger("sum example")

context = Context()
logger.info("Building list")
l = ChainableList([ x for x in xrange(1, 30) ])

logger.info("Building dataset")
ds = context.dataSet(l, label="sum", bufferSize=1)

logger.info("Calling reduce")
print ds.reduce(lambda x, y: x + y)
logger.info("All done!")

