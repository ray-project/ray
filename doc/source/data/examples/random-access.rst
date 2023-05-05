.. _datasets_random_access:

---------------------------------
Random Data Access (Experimental)
---------------------------------

Any Arrow-format datastream can be enabled for random access by calling ``ds.to_random_access_dataset(key="col_name")``. This partitions the data across the cluster by the given sort key, providing efficient random access to records via binary search. A number of worker actors are created, each of which has zero-copy access to the underlying sorted data blocks of the Datastream.

.. code-block:: python

    # Generate a dummy embedding table as an example.
    ds = ray.data.range(100)
    ds = ds.add_column("embedding", lambda b: b["id"] ** 2)
    # -> schema={id: int64, embedding: int64}

    # Enable random access on the datastream. This launches a number of actors
    # spread across the cluster that serve random access queries to the data.
    rmap = ds.to_random_access_dataset(key="id", num_workers=4)

    # Example of a point query by key.
    ray.get(rmap.get_async(2))
    # -> {"id": 2, "embedding": 4}

    # Queries to missing keys return None.
    ray.get(rmap.get_async(-1))
    # -> None

    # Example of a multiget query.
    rmap.multiget([4, 2])
    # -> [{"id": 4, "embedding": 16}, {"id": 2, "embedding": 4}]

Similar to Datastream, a RandomAccessDataset can be passed to and used from any Ray actor or task.

Architecture
------------

RandomAccessDataset spreads its workers evenly across the cluster. Each worker fetches and pins in shared memory all blocks of the sorted source data found on its node. In addition, it is ensured that each block is assigned to at least one worker. A central index of block to key-range assignments is computed, which is used to serve lookups.

Lookups occur as follows:

* First, the id of the block that contains the given key is located via binary search on the central index.
* Second, an actor that has the block pinned is selected (this is done randomly).
* A method call is sent to the actor, which then performs binary search to locate the record for the key.

This means that each random lookup costs ~1 network RTT as well as a small amount of computation on both the client and server side.

Performance
-----------

Since actor communication goes directly from worker to worker in Ray, the throughput of a RandomAccessDataset scales linearly with the number of workers available. As a rough measure, a single worker can provide ~2k individual gets/s and serve ~10k records/s for multigets, and this scales linearly as you increase the number of clients and workers for a single RandomAccessDataset. Large workloads may require hundreds of workers for sufficient throughput. You will also generally want more workers than clients, since the client does less computation than worker actors do.

To debug performance problems, use ``random_access_ds.stats()``. This will return a string showing the actor-side measured latencies as well as the distribution of data blocks and queries across the actors. Load imbalances can cause bottlenecks as certain actors receive more requests than others. Ensure that load is evenly distributed across the key space to avoid this.

It is important to note that the client (Ray worker process) can also be a bottleneck. To scale past the throughput of a single client, use multiple tasks to gather the data, for example:

.. code-block:: python

    @ray.remote
    def fetch(rmap, keys):
        return rmap.multiget(keys)

    # Split the list of keys we want to fetch into 10 pieces.
    pieces = np.array_split(all_keys, 10)

    # Fetch from the RandomAccessDataset in parallel using 10 remote tasks.
    print(ray.get([fetch.remote(rmap, p) for p in pieces]))

Fault Tolerance
---------------

Currently, RandomAccessDataset is not fault-tolerant. Losing any of the worker actors invalidates the dataset, and it must be re-created from the source data.
