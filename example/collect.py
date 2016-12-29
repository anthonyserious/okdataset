#!/usr/bin/env python

from okdataset import ChainableList, Context, Logger

logger = Logger("collect example")

logger.info("Building big list")
l = ChainableList([ x for x in xrange(0, 100) ])

logger.info("Creating context")
context = Context()
logger.info("Building dataset")
ds = context.dataSet(l, bufferSize=1)

a = 1
logger.info("Calling map")
ds.map(lambda x: x * 2 + 1)

res = ds.collect()
print res

