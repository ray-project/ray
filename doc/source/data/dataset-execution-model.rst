Dataset Execution Model
=======================

This page overviews the execution model of Datasets in detail, which may be useful for understanding and tuning performance.

Reading Data
------------

Datasets uses Ray tasks to read data from remote storage. When reading from an file-based datasource (e.g., S3, GCS), a number of read tasks equal to the specified read parallelism (200 by default) will be created, and one or more files will be assigned to each read task. Each read task reads its assigned files and produces one or more output blocks (Ray objects):

.. image:: dataset-read.svg

..
  https://docs.google.com/drawings/d/15B4TB8b5xN15Q9S8-s0MjW6iIvo_PrH7JtV1fL123pU/edit

In the common case, each read task produces a single output block. Read tasks may produce multiple output blocks if the data exceeds the target max block size (500MB by default). This automatic block splitting avoids out of memory errors when reading very large single files (e.g., a 100-gigabyte CSV file). All the built-in datasources except for JSON currently support automatic block splitting.

Deferred Read Task Execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a Dataset is created using ``ray.data.read_*``, only the first read task will be executed by default. This avoids blocking Dataset creation on the reading of all data files, enabling inspection functions like ``ds.schema()`` and ``ds.show()`` to be used right away. Executing further transformations on the Dataset will trigger execution of all read tasks.

Transforming Datasets
---------------------
task vs actor compute provider

Locality
~~~~~~~~
locality scheduling for tasks




Distributed Shuffle
-------------------
talk about groupby, sort, random shuffle, fast repartition



Memory Management
-----------------
talk about blocks representation

Data Balancing
~~~~~~~~~~~~~~
Ray tries to spread read tasks out to balance memory usage

Spilling
~~~~~~~~
spill to object store on out of memory

Reference Counting
~~~~~~~~~~~~~~~~~~
mention how blocks are pinned until deleted


Performance Tuning
------------------

Tuning Read Parallelism
~~~~~~~~~~~~~~~~~~~~~~~

By default, Ray requests 0.5 CPUs per read task, which means two read tasks can concurrently execute per CPU. For datasources that can benefit from higher degress of I/O parallelism, you can specify a lower ``num_cpus`` value for the read function via the ``ray_remote_args`` parameter. For example, use ``ray.data.read_parquet(path, ray_remote_args={"num_cpus": 0.25})`` to allow up to four read tasks per CPU.

The number of read tasks can also be increased by increasing the ``parallelism`` parameter. For example, use ``ray.data.read_parquet(path, parallelism=1000)`` to create up to 1000 read tasks. Typically increasing the number of read tasks only helps if you have more cluster CPUs than the default parallelism.

Tuning Max Block Size
~~~~~~~~~~~~~~~~~~~~~

The max target block size can be adjusted via Dataset context API. For example, to configure a max target block size of 64MB, run ``ray.data.context.DatasetContext.get_current().target_max_block_size = 64_000_000`` prior to creating the Dataset. Lower block sizes reduce the max amount of object store and Python heap memory required during execution. However, having too many blocks may introduce task scheduling overheads. We do not recommend adjusting this value for most workloads.

Note that the number of blocks a Dataset created from ``ray.data.read_*`` contains is not fully known until all read tasks are fully executed. The number of blocks printed in the Dataset's string representation is initially set to the number of read tasks generated. To view the actual number of blocks created after block splitting, use ``len(ds.get_internal_block_refs())``, which will block until all data has been read.
