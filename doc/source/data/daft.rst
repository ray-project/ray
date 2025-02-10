.. _daft-on-ray:

Daft (Ray-native distributed data engine)
=========================================

Daft is a data engine library that is deeply integrated into Ray as an execution backend. It is a fully-featured ETL library that runs on your Ray cluster without any additional dependencies,
supporting both SQL as well as a Python dataframe API as a user interface.

You can use Daft locally without a Ray cluster (in which case it runs in the current process just like a library such as Pandas, but is much more performant),
or point it to an existing Ray cluster to utilize distributed compute resources.

.. code-block:: python

    import daft
    import ray

    ray.init()
    df = daft.read_parquet("s3://my-bucket/*.parquet")


How Daft uses Ray
-----------------

Daft constructs a "Query Plan" when users interact with it, either using SQL or the Python DataFrame API. This plan is then compiled to run against different backends depending on the user's intent.

1. When run natively without Ray, this plan is compiled into a streaming execution plan that runs on an efficient multithreaded async Rust runtime on the current machine. This lets Daft run extremely efficiently on a single machine, without the overheads of multiple processes and heavyweight data transfer abstractions.
2. When run with a Ray cluster, this plan is compiled into a distributed execution plan that will then be executed on the Ray cluster using Ray core primitives.

Specifically, Daft's distributed scheduler chunks data into *partitions*. It then uses Ray for performing work on those partitions:

1. Running tasks on partitions - placement of tasks makes heavy use of Ray's resource requesting mechanisms (e.g. for tasks that may require a GPU) to ensure the appropriate amount of task parallelism
2. Moving task input/output partitions between nodes via the Ray object store for distributed operations such as a global sort, groupby or join (also called a *data shuffle*)

Ray Data integrations
---------------------

Daft is an ETL tool that is used upstream of Ray Data.

To integrate with Ray Data, simply use the Ray Dataset's `from_daft` and `to_daft` methods. This results in zero-copy data transfer between Daft and Ray Data, as they both utilize the Apache Arrow storage
format under the hood.
