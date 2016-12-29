#!/usr/bin/env python

from okdataset import ChainableList, Context, Logger

logger = Logger("avg example")

context = Context()
logger.info("Building big list")
l = ChainableList([ x for x in xrange(1, 30) ])

logger.info("Building dataset")
ds = context.dataSet(l, label="avg", bufferSize=1)

logger.info("Calling reduce")
print ds.reduce(lambda x, y: (x + y) / 2.)
logger.info("All done!")

