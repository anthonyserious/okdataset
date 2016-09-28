#!/usr/bin/env python

from okdataset.clist import ChainableList
from okdataset.context import Context
from okdataset.dataset import DataSet
from okdataset.logger import Logger

logger = Logger("maparray example")

logger.info("Building big list")
l = ChainableList([
    1,
    1,
    1,
    2,
    2,
    3,
    6,
    9,
    9,
    9,
    12
])

logger.info("Building dataset")
context = Context()
ds = context.dataSet(l, bufferSize=1)

a = 1
logger.info("Calling chain")
ds.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+ y)

print ds.collect()

