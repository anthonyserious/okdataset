#!/usr/bin/env python

from okdataset.clist import ChainableList
from okdataset.context import Context
from okdataset.logger import Logger

logger = Logger("maparray example")

context = Context()
logger.info("Building big list")
l = ChainableList([ x for x in xrange(0, 1000000) ])

logger.info("Building dataset")
ds = context.dataSet(l, label="big list")

logger.info("Calling map")
ds.map(lambda x: x * 2)
ds.compute()
logger.info("All done!")

