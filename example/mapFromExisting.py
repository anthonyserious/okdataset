#!/usr/bin/env python

from okdataset.clist import ChainableList
from okdataset.context import Context
from okdataset.dataset import DataSet
from okdataset.logger import Logger

logger = Logger("map from existing example")

context = Context()
logger.info("Building dataset from existing")
ds = DataSet(context, "big list", fromExisting=True)

logger.info("Calling map")
ds.map(lambda x: x * 2)

logger.info("All done!")

