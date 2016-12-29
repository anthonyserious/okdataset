okdataset
====

*okdataset* is a (BETA) proof of concept of a lightweight, on demand map-reduce cluster.  A few motivations for creating okdataset:

1. An API providing the majority of the PySpark API.
2. No JVMs - tuning JVMs is just tedious and unnecessary, and this is all Python anyways.
3. Supportability - simple JSON-structured logging, and basic fine-grained profiling.
4. Containers containers containers!

Architecture
===
okdataset uses ZeroMQ for middleware, and Redis as a distributed cache.  There is a single server process which implements a REQ/REP pattern for client-server connectivity, and a ventilator/sink pattern for master/worker calculation task delegation.

The `DataSet` class is a subclass of the `ChainableList` class, which is where the PySpark API subset is implemented, providing the usual `map`, `reduce`, `filter`, `flatMap`, and `reduceByKey`.  Clients create `DataSet` objects from lists, push the data to the master (and thus into the cache), push serialized (cloud pickle) methods to the server, and `collect` calls trigger method chains to be applied to buffers of the dataset on workers.  Results are stored in the cache and aggregated by the master for return to the client.



