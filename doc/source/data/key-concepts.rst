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


Datasets execute their transformations synchronously in blocking calls. However, it can be useful to overlap dataset computations with output. This can be done with a `DatasetPipeline <package-ref.html#datasetpipeline-api>`__.

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

See the :ref:`Creating Datasets guide <creating_datasets>` for details on how to read
data into datasets.

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

See the :ref:`Transforming Datasets guide <transforming_datasets>` for an in-depth guide
on transforming datasets.

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
  
Fault tolerance
===============

Datasets relies on :ref:`task-based fault tolerance <task-fault-tolerance>` in Ray core. Specifically, a ``Dataset`` will be automatically recovered by Ray in case of failures. This works through **lineage reconstruction**: a Dataset is a collection of Ray objects stored in shared memory, and if any of these objects are lost, then Ray will recreate them by re-executing the task(s) that created them.

There are a few cases that are not currently supported:
1. If the original creator of the ``Dataset`` dies. This is because the creator stores the metadata for the :ref:`objects <object-fault-tolerance>` that comprise the ``Dataset``.
2. For a :meth:`DatasetPipeline.split() <ray.data.DatasetPipeline.split>`, we do not support recovery for a consumer failure. When there are multiple consumers, they must all read the split pipeline in lockstep. To recover from this case, the pipeline and all consumers must be restarted together.
3. The ``compute=actors`` option for transformations.

Execution and Memory Management
===============================

See :ref:`Execution and Memory Management <data_advanced>` for more details about how Datasets manages memory and optimizations such as lazy vs eager execution.

-------------------------
Resource Allocation Model
-------------------------

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

Example: Datasets in Tune
=========================

.. _datasets_tune:

Here's an example of how you can configure Datasets to run within Tune trials, which
is the typical case of when you'd encounter placement groups with Datasets. Two
scenarios are shown: running outside the trial group, and running within the trial placement group.

.. tabbed:: Outside Trial Placement Group

    By default, Dataset tasks escape the trial placement group. This means they will use
    spare cluster resources for execution, which can be problematic since the availability
    of such resources is not guaranteed.

    .. literalinclude:: ./doc_code/key_concepts.py
      :language: python
      :start-after: __resource_allocation_1_begin__
      :end-before: __resource_allocation_1_end__

.. tabbed:: Inside Trial Placement Group

    Datasets can be configured to use resources within the trial's placement group. This
    requires you to explicitly reserve resource bundles in the placement group for
    use by Datasets.

    .. literalinclude:: ./doc_code/key_concepts.py
      :language: python
      :start-after: __resource_allocation_2_begin__
      :end-before: __resource_allocation_2_end__

    .. note::

      This is an experimental feature subject to change as we work to improve our
      resource allocation model for Datasets.
