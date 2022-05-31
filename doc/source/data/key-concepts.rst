.. _data_key_concepts:

============
Key Concepts
============

To work with Ray Datasets, you need to understand how Datasets and Dataset Pipelines work.
You might also be interested to learn about the execution model of Ray Datasets operations.


.. _dataset_concept:

--------
Datasets
--------

Ray Datasets implements `Distributed Arrow <https://arrow.apache.org/>`__.
A Dataset consists of a list of Ray object references to *blocks*.
Each block holds a set of items in either an `Arrow table <https://arrow.apache.org/docs/python/data.html#tables>`__
(when creating from or transforming to tabular or tensor data), a `Pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__
(when creating from or transforming to Pandas data), or a Python list (otherwise).
Having multiple blocks in a dataset allows for parallel transformation and ingest of the data
(e.g., into :ref:`Ray Train <train-docs>` for ML training).

The following figure visualizes a Dataset that has three Arrow table blocks, each block holding 1000 rows each:

.. image:: images/dataset-arch.svg

..
  https://docs.google.com/drawings/d/1PmbDvHRfVthme9XD7EYM-LIHPXtHdOfjCbc1SCsM64k/edit

Since a Dataset is just a list of Ray object references, it can be freely passed between Ray tasks,
actors, and libraries like any other object reference.
This flexibility is a unique characteristic of Ray Datasets.

Compared to `Spark RDDs <https://spark.apache.org/docs/latest/rdd-programming-guide.html>`__
and `Dask Bags <https://docs.dask.org/en/latest/bag.html>`__, Ray Datasets offers a more basic set of features,
and executes operations eagerly for simplicity.
It is intended that users cast Datasets into more feature-rich dataframe types (e.g.,
:meth:`ds.to_dask() <ray.data.Dataset.to_dask>`) for advanced operations.

.. _dataset_pipeline_concept:

-----------------
Dataset Pipelines
-----------------


Datasets execute their transformations synchronously in blocking calls. However, it can be useful to overlap dataset computations with output. This can be done with a `DatasetPipeline <data-pipelines-quick-start>`__.

A DatasetPipeline is an unified iterator over a (potentially infinite) sequence of Ray Datasets, each of which represents a *window* over the original data. Conceptually it is similar to a `Spark DStream <https://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams>`__, but manages execution over a bounded amount of source data instead of an unbounded stream. Ray computes each dataset window on-demand and stitches their output together into a single logical data iterator. DatasetPipeline implements most of the same transformation and output methods as Datasets (e.g., map, filter, split, iter_rows, to_torch, etc.).

.. _dataset_execution_concept:

------------------------
Datasets Execution Model
------------------------

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

  Block splitting is off by default. See the :ref:`performance section <data_performance_tips>` on how to enable block splitting (beta).

.. _dataset_defeferred_reading:

Deferred Read Task Execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When a Dataset is created using ``ray.data.read_*``, only the first read task will be
executed initially. This avoids blocking Dataset creation on the reading of all data
files, enabling inspection functions like :meth:`ds.schema() <ray.data.Dataset.schema>``
and :meth:`ds.show() <ray.data.Dataset.show>` to be used right away. Executing further
transformations on the Dataset will trigger execution of all read tasks.

Dataset Transforms
==================

Datasets use either Ray tasks or Ray actors to transform datasets (i.e., for
:meth:`ds.map_batches() <ray.data.Dataset.map_batches>`,
:meth:`ds.map() <ray.data.Dataset.map>`, or
:meth:`ds.flat_map() <ray.data.Dataset.flat_map>`). By default, tasks are used (``compute="tasks"``). Actors can be specified with ``compute="actors"``, in which case an autoscaling pool of Ray actors will be used to apply transformations. Using actors allows for expensive state initialization (e.g., for GPU-based tasks) to be re-used. Whichever compute strategy is used, each map task generally takes in one block and produces one or more output blocks. The output block splitting rule is the same as for file reads (blocks are split after hitting the target max block size of 2GiB):

.. image:: images/dataset-map.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/1MGlGsPyTOgBXswJyLZemqJO1Mf7d-WiEFptIulvcfWE/edit

Shuffling Data
==============

Certain operations like :meth:`ds.sort() <ray.data.Dataset.sort>` and
:meth:`ds.groupby() <ray.data.Dataset.groupby>` require data blocks to be partitioned by value. Datasets executes this in three phases. First, a wave of sampling tasks determines suitable partition boundaries based on a random sample of data. Second, map tasks divide each input block into a number of output blocks equal to the number of reduce tasks. Third, reduce tasks take assigned output blocks from each map task and combines them into one block. Overall, this strategy generates ``O(n^2)`` intermediate objects where ``n`` is the number of input blocks.

You can also change the partitioning of a Dataset using :meth:`ds.random_shuffle()
<ray.data.Dataset.random_shuffle>` or
:meth:`ds.repartition() <ray.data.Dataset.repartition>`. The former should be used if you want to randomize the order of elements in the dataset. The second should be used if you only want to equalize the size of the Dataset blocks (e.g., after a read or transformation that may skew the distribution of block sizes). Note that repartition has two modes, ``shuffle=False``, which performs the minimal data movement needed to equalize block sizes, and ``shuffle=True``, which performs a full (non-random) distributed shuffle:

.. image:: images/dataset-shuffle.svg
   :width: 650px
   :align: center

..
  https://docs.google.com/drawings/d/132jhE3KXZsf29ho1yUdPrCHB9uheHBWHJhDQMXqIVPA/edit

Resource Allocation Model
=========================

Unlike other libraries in Ray's ML ecosystem, such as Tune and Train, Datasets does not
natively use placement groups to allocate resources for Datasets workloads (tasks and
actor pools). Instead, Datasets makes plain CPU/GPU resource requests to the cluster,
and in order to not compete with Tune/Train for resources within those library's
placement groups, Datasets **escapes placement groups by default**. Any Datasets
tasks launched from within a placement group will be executed outside of that placement
group by default. This can be thought of as Datasets requesting resources from the
margins of the cluster, outside of those ML library placement groups.

Although this is the default behavior, you can force all Datasets workloads to be
scheduled within a placement group by specifying a placement group as the global
scheduling strategy for all Datasets tasks/actors, using the global
:class:`DatasetContext <ray.data.DatasetContext>`.

.. note::

  This is an experimental feature subject to change as we work to improve our
  resource allocation model for Datasets.

.. literalinclude:: ./doc_code/key_concepts.py
  :language: python
  :start-after: __resource_allocation_begin__
  :end-before: __resource_allocation_end__

Memory Management
=================

This section deals with how Datasets manages execution and object store memory.

Execution Memory
~~~~~~~~~~~~~~~~

During execution, certain types of intermediate data must fit in memory. This includes the input block of a task, as well as at least one of the output blocks of the task (when a task has multiple output blocks, only one needs to fit in memory at any given time). The input block consumes object stored shared memory (Python heap memory for non-Arrow data). The output blocks consume Python heap memory (prior to putting in the object store) as well as object store memory (after being put in the object store).

This means that large block sizes can lead to potential out-of-memory situations. To
avoid OOM errors, Datasets can split blocks during map and read tasks into pieces
smaller than the target max block size. In some cases, this splitting is not possible
(e.g., if a single item in a block is extremely large, or the function given to
:meth:`ds.map_batches() <ray.data.Dataset.map_batches>` returns a very large batch). To
avoid these issues, make sure no single item in your Datasets is too large, and always
call :meth:`ds.map_batches() <ray.data.Dataset.map_batches>` with batch size small enough such that the output batch can comfortably fit into memory.

.. note::

  Block splitting is off by default. See the :ref:`performance section <data_performance_tips>` on how to enable block splitting (beta).

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

Lazy Execution Mode
===================

.. note::

  Lazy execution mode is experimental. If you run into any issues, please reach
  out on `Discourse <https://discuss.ray.io/>`__ or open an issue on the
  `Ray GitHub repo <https://github.com/ray-project/ray>`__.

By default, all Datasets operations are eager (except for data reading, which is
semi-lazy; see the :ref:`deferred reading docs <dataset_deferred_reading>`), executing
each stage synchronously. This provides a simpler iterative development and debugging
experience, allowing you to inspect up-to-date metadata (schema, row count, etc.) after
each operation, greatly improving the typical "Getting Started" experience.

However, this eager execution mode can result in less optimal (i.e. slower) execution
and increased memory utilization compared to what's possible with a lazy execution mode.
That's why Datasets offers a lazy execution mode, which you can transition to after
you're done prototyping your Datasets pipeline.

Lazy execution mode can be enabled by calling
:meth:`ds = ds.experimental_lazy() <ray.data.Dataset.experimental_lazy()>`, which
returns a dataset whose all subsequent operations will be **lazy**. These operations
won't be executed until the dataset is consumed (e.g. via
:meth:`ds.take() <ray.data.Dataset.take>`,
:meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>`,
:meth:`ds.to_torch() <ray.data.Dataset.to_torch>`, etc.) or if
:meth:`ds.fully_executed() <ray.data.Dataset.fully_executed>` is called to manually
trigger execution.

The big optimizations that lazy execution enables are **automatic stage fusion** and
**block move semantics**.

Automatic Stage Fusion
~~~~~~~~~~~~~~~~~~~~~~

Automatic fusion of stages/operations can significantly lower the Ray task overhead of
Datasets workloads, since a chain of reading and many map-like transformations will be
condensed into a single stage of Ray tasks; this results in less data needing to be put
into Ray's object store and transferred across nodes, and therefore resulting in lower
memory utilization and faster task execution.

Datasets will automatically fuse together lazy operations that are compatible:

* Same compute pattern: embarrassingly parallel map vs. all-to-all shuffle
* Same compute strategy: Ray tasks vs Ray actors
* Same resource specification, e.g. ``num_cpus`` or ``num_cpus`` requests

Read and subsequent map-like transformations
(e.g. :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`,
:meth:`ds.filter() <ray.data.Dataset.filter>`, etc.) will usually be fused together.
All-to-all transformations such as
:meth:`ds.random_shuffle() <ray.data.Dataset.random_shuffle>` can be fused with earlier
map-like stages, but not later stages.

.. note::

  For eager mode Datasets, reads are semi-lazy, so the transformation stage right after
  the read stage (that triggers the full data read) will fuse with the read stage. Note
  that this currently incurs re-reading of any already-read blocks (a fix for this is
  currently in progress.)


You can tell if stage fusion is enabled by checking the :ref:`Dataset stats <data_performance_tips>` and looking for fused stages (e.g., ``read->map_batches``).

.. code-block::

    Stage N read->map_batches->shuffle_map: N/N blocks executed in T
    * Remote wall time: T min, T max, T mean, T total
    * Remote cpu time: T min, T max, T mean, T total
    * Output num rows: N min, N max, N mean, N total

Block Move Semantics
~~~~~~~~~~~~~~~~~~~~

In addition to fusing together stages, lazy execution mode further optimizes memory
utilization by eagerly releasing the data produced by intermediate operations in a
chain.

For example, if you have a chain of ``read_parquet() -> map_batches() -> filter()`` operations:

.. literalinclude:: ./doc_code/key_concepts.py
  :language: python
  :start-after: __block_move_begin__
  :end-before: __block_move_end__

that, for the sake of this example, aren't fused together, Datasets can eagerly release
the outputs of the ``read_parquet()`` stage and the ``map_batches()`` stage before the
subsequent stage (``map_batches()`` and ``filter()``, respectively) have finished. This
was not possible in eager mode, since every operation materialized the data and returned
the references back to the user. But in lazy execution mode, we know that the outputs of
the ``read_parquet()`` and ``map_batches()`` stages are only going to be used by the
downstream stages, so we can more aggressively release them.

Dataset Pipelines Execution Model
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To avoid unnecessary data movement in the distributed setting,
:class:`DatasetPipelines <ray.data.dataset_pipelines.DatasetPipeline>` will always use
these lazy execution optimizations (stage fusion and block move semantics)
under-the-hood. Because a ``DatasetPipeline`` doesn't support creating more than one
``DatasetPipeline`` from a ``DatasetPipeline`` (i.e. no fan-out), we can clear block
data extra aggressively.

.. note::

  When creating a pipeline (i.e. calling :meth:`ds.window() <ray.data.Dataset.window>`
  or :meth:`ds.repeat() <ray.data.Dataset.repeat>`) immediately after a read stage, any
  already read data will be dropped, and the read stage will be absorbed into the
  pipeline and be made fully lazy. This allows you to easily create ML ingest pipelines
  that re-read data from storage on every epoch, as well as streaming batch inference
  pipelines that window all the way down to the file reading.

.. literalinclude:: ./doc_code/key_concepts.py
  :language: python
  :start-after: __dataset_pipelines_execution_begin__
  :end-before: __dataset_pipelines_execution_end__
