.. _data_key_concepts:

============
Key Concepts
============

To work with Ray Data, you need to understand how Datasets and Dataset Pipelines work.
You might also be interested to learn about the execution model of Ray Data operations


.. _dataset_concept:

--------
Datasets
--------

Ray Data implements `Distributed Arrow <https://arrow.apache.org/>`__.
A Dataset consists of a list of Ray object references to *blocks*.
Each block holds a set of items in either an `Arrow table <https://arrow.apache.org/docs/python/data.html#tables>`__
or a Python list (for Arrow incompatible objects).
Having multiple blocks in a dataset allows for parallel transformation and ingest of the data
(e.g., into :ref:`Ray Train <train-docs>` for ML training).

The following figure visualizes a Dataset that has three Arrow table blocks, each block holding 1000 rows each:

.. image:: images/dataset-arch.svg

..
  https://docs.google.com/drawings/d/1PmbDvHRfVthme9XD7EYM-LIHPXtHdOfjCbc1SCsM64k/edit

Since a Ray Dataset is just a list of Ray object references, it can be freely passed between Ray tasks,
actors, and libraries like any other object reference.
This flexibility is a unique characteristic of Ray Data.

Compared to `Spark RDDs <https://spark.apache.org/docs/latest/rdd-programming-guide.html>`__
and `Dask Bags <https://docs.dask.org/en/latest/bag.html>`__, Ray Data offers a more basic set of features,
and executes operations eagerly for simplicity.
It is intended that users cast Datasets into more feature-rich dataframe types (e.g., ``ds.to_dask()``) for advanced operations.

.. _dataset_pipeline_concept:

-----------------
Dataset Pipelines
-----------------


Datasets execute their transformations synchronously in blocking calls. However, it can be useful to overlap dataset computations with output. This can be done with a `DatasetPipeline <package-ref.html#datasetpipeline-api>`__.

A DatasetPipeline is an unified iterator over a (potentially infinite) sequence of Ray Datasets, each of which represents a *window* over the original data. Conceptually it is similar to a `Spark DStream <https://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams>`__, but manages execution over a bounded amount of source data instead of an unbounded stream. Ray computes each dataset window on-demand and stitches their output together into a single logical data iterator. DatasetPipeline implements most of the same transformation and output methods as Datasets (e.g., map, filter, split, iter_rows, to_torch, etc.).

.. _dataset_execution_concept:

-----------------------
Dataset Execution Model
-----------------------

This page overviews the execution model of Datasets, which may be useful for understanding and tuning performance.

Reading Data
============

Datasets uses Ray tasks to read data from remote storage. When reading from a file-based datasource (e.g., S3, GCS), it creates a number of read tasks equal to the specified read parallelism (200 by default). One or more files will be assigned to each read task. Each read task reads its assigned files and produces one or more output blocks (Ray objects):

.. image:: images/dataset-read.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/15B4TB8b5xN15Q9S8-s0MjW6iIvo_PrH7JtV1fL123pU/edit

In the common case, each read task produces a single output block. Read tasks may split the output into multiple blocks if the data exceeds the target max block size (2GiB by default). This automatic block splitting avoids out-of-memory errors when reading very large single files (e.g., a 100-gigabyte CSV file). All of the built-in datasources except for JSON currently support automatic block splitting.

.. note::

  Block splitting is currently off by default. See the block size tuning section below on how to enable block splitting (beta).

Deferred Read Task Execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a Dataset is created using ``ray.data.read_*``, only the first read task will be executed initially. This avoids blocking Dataset creation on the reading of all data files, enabling inspection functions like ``ds.schema()`` and ``ds.show()`` to be used right away. Executing further transformations on the Dataset will trigger execution of all read tasks.


Dataset Transforms
==================

Datasets use either Ray tasks or Ray actors to transform datasets (i.e., for ``.map``, ``.flat_map``, or ``.map_batches``). By default, tasks are used (``compute="tasks"``). Actors can be specified with ``compute="actors"``, in which case an autoscaling pool of Ray actors will be used to apply transformations. Using actors allows for expensive state initialization (e.g., for GPU-based tasks) to be re-used. Whichever compute strategy is used, each map task generally takes in one block and produces one or more output blocks. The output block splitting rule is the same as for file reads (blocks are split after hitting the target max block size of 2GiB):

.. image:: images/dataset-map.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/1MGlGsPyTOgBXswJyLZemqJO1Mf7d-WiEFptIulvcfWE/edit

Shuffling Data
==============

Certain operations like ``.sort`` and ``.groupby`` require data blocks to be partitioned by value. Datasets executes this in three phases. First, a wave of sampling tasks determines suitable partition boundaries based on a random sample of data. Second, map tasks divide each input block into a number of output blocks equal to the number of reduce tasks. Third, reduce tasks take assigned output blocks from each map task and combines them into one block. Overall, this strategy generates ``O(n^2)`` intermediate objects where ``n`` is the number of input blocks.

You can also change the partitioning of a Dataset using ``.random_shuffle`` or ``.repartition``. The former should be used if you want to randomize the order of elements in the dataset. The second should be used if you only want to equalize the size of the Dataset blocks (e.g., after a read or transformation that may skew the distribution of block sizes). Note that repartition has two modes, ``shuffle=False``, which performs the minimal data movement needed to equalize block sizes, and ``shuffle=True``, which performs a full (non-random) distributed shuffle:

.. image:: images/dataset-shuffle.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/132jhE3KXZsf29ho1yUdPrCHB9uheHBWHJhDQMXqIVPA/edit

Memory Management
=================

This section deals with how Datasets manages execution and object store memory.

Execution Memory
~~~~~~~~~~~~~~~~

During execution, certain types of intermediate data must fit in memory. This includes the input block of a task, as well as at least one of the output blocks of the task (when a task has multiple output blocks, only one needs to fit in memory at any given time). The input block consumes object stored shared memory (Python heap memory for non-Arrow data). The output blocks consume Python heap memory (prior to putting in the object store) as well as object store memory (after being put in the object store).

This means that large block sizes can lead to potential out-of-memory situations. To avoid OOM errors, Datasets tries to split blocks during map and read tasks into pieces smaller than the target max block size. In some cases, this splitting is not possible (e.g., if a single item in a block is extremely large, or the function given to ``.map_batches`` returns a very large batch). To avoid these issues, make sure no single item in your Datasets is too large, and always call ``.map_batches`` with batch size small enough such that the output batch can comfortably fit into memory.

Object Store Memory
~~~~~~~~~~~~~~~~~~~

Datasets uses the Ray object store to store data blocks, which means it inherits the memory management features of the Ray object store. This section discusses the relevant features:

**Object Spilling**: Since Datasets uses the Ray object store to store data blocks, any blocks that can't fit into object store memory are automatically spilled to disk. The objects are automatically reloaded when needed by downstream compute tasks:

.. image:: images/dataset-spill.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/1H_vDiaXgyLU16rVHKqM3rEl0hYdttECXfxCj8YPrbks/edit

**Locality Scheduling**: Ray will preferentially schedule compute tasks on nodes that already have a local copy of the object, reducing the need to transfer objects between nodes in the cluster.

**Reference Counting**: Dataset blocks are kept alive by object store reference counting as long as there is any Dataset that references them. To free memory, delete any Python references to the Dataset object.

**Load Balancing**: Datasets uses Ray scheduling hints to spread read tasks out across the cluster to balance memory usage.
