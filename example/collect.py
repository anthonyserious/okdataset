#!/usr/bin/env python

from okdataset.clist import ChainableList
from okdataset.context import Context
from okdataset.dataset import DataSet
from okdataset.logger import Logger

logger = Logger("maparray example")

logger.info("Building big list")
l = ChainableList([ x for x in xrange(0, 100) ])

logger.info("Building dataset")
context = Context()
ds = context.dataSet(l, bufferSize=1)

a = 1
logger.info("Calling map")
ds.map(lambda x: x * 2 + a)

res = ds.collect()
print res

