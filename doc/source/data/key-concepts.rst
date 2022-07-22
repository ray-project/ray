.. _data_key_concepts:

============
Key Concepts
============

.. _dataset_concept:

--------
Datasets
--------

A Dataset consists of a list of Ray object references to *blocks*.
Each block holds a set of items in either `Arrow table format <https://arrow.apache.org/docs/python/data.html#tables>`__
or a Python list (for non-tabular data).
Having multiple blocks in a dataset allows for parallel transformation and ingest of the data.

The following figure visualizes a Dataset that has three Arrow table blocks, each block holding 1000 rows each:

.. image:: images/dataset-arch.svg

..
  https://docs.google.com/drawings/d/1PmbDvHRfVthme9XD7EYM-LIHPXtHdOfjCbc1SCsM64k/edit

Since a Dataset is just a list of Ray object references, it can be freely passed between Ray tasks,
actors, and libraries like any other object reference.
This flexibility is a unique characteristic of Ray Datasets.

.. _dataset_pipeline_concept:

Reading Data
============

Datasets uses Ray tasks to read data from remote storage. When reading from a file-based datasource (e.g., S3, GCS), it creates a number of read tasks proportional to the number of CPUs in the cluster. Each read task reads its assigned files and produces an output block:

.. image:: images/dataset-read.svg
   :align: center

..
  https://docs.google.com/drawings/d/15B4TB8b5xN15Q9S8-s0MjW6iIvo_PrH7JtV1fL123pU/edit

The parallelism can also be manually specified, but the final parallelism for a read is always capped by the number of files in the underlying dataset. See the :ref:`Creating Datasets Guide <creating_datasets>` for an in-depth guide
on creating datasets.

Dataset Transforms
==================

Datasets use either Ray tasks or Ray actors to transform datasets (i.e., for
:meth:`ds.map_batches() <ray.data.Dataset.map_batches>`,
:meth:`ds.map() <ray.data.Dataset.map>`, or
:meth:`ds.flat_map() <ray.data.Dataset.flat_map>`). By default, tasks are used (``compute="tasks"``). Actors can be specified with ``compute="actors"``, in which case an autoscaling pool of Ray actors will be used to apply transformations. Using actors allows for expensive state initialization (e.g., for GPU-based tasks) to be re-used:

.. image:: images/dataset-map.svg
   :align: center

..
  https://docs.google.com/drawings/d/12STHGV0meGWfdWyBlJMUgw7a-JcFPu9BwSOn5BjRw9k/edit

See the :ref:`Transforming Datasets Guide <transforming_datasets>` for an in-depth guide
on transforming datasets.

Shuffling Data
==============

[todo: simplify]
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

-----------------
Dataset Pipelines
-----------------

Sometimes, you may want to execute your Datasets transformations


Datasets execute their transformations synchronously in blocking calls. However, it can be useful to overlap dataset computations with output. This can be done with a `DatasetPipeline <package-ref.html#datasetpipeline-api>`__.

A DatasetPipeline is an unified iterator over a (potentially infinite) sequence of Ray Datasets, each of which represents a *window* over the original data. Conceptually it is similar to a `Spark DStream <https://spark.apache.org/docs/latest/streaming-programming-guide.html#discretized-streams-dstreams>`__, but manages execution over a bounded amount of source data instead of an unbounded stream. Ray computes each dataset window on-demand and stitches their output together into a single logical data iterator. DatasetPipeline implements most of the same transformation and output methods as Datasets (e.g., map, filter, split, iter_rows, to_torch, etc.).

.. _dataset_execution_concept:


[todo: move this to separate page]

-------------------
Reserving Resources
-------------------

Unlike libraries like Tune and Train, Datasets does not use placement groups to allocate
resources for execution (its tasks and actor pools). Instead, Datasets makes plain
CPU/GPU resource requests to the cluster, *ignoring placement groups by default*. This
can be thought of as Datasets requesting resources from the margins of the cluster,
outside of those ML library placement groups.

To avoid hangs or CPU starvation of Datasets when used with Tune or Train, you can
exclude a fraction of CPUs from placement group scheduling, using the
``_max_cpu_fraction_per_node`` placement group option (Experimental).

Example: Datasets in Tune
=========================

.. _datasets_tune:

Here's an example of how you can configure Datasets to run within Tune trials, which
is the typical case of when you'd encounter placement groups with Datasets. Two
scenarios are shown: running outside the trial group using spare resources, and running with reserved resources.

.. tabbed:: Using Spare Cluster Resources

    By default, Dataset tasks escape the trial placement group. This means they will use
    spare cluster resources for execution, which can be problematic since the availability
    of such resources is not guaranteed.

    .. literalinclude:: ./doc_code/key_concepts.py
      :language: python
      :start-after: __resource_allocation_1_begin__
      :end-before: __resource_allocation_1_end__

.. tabbed:: Using Reserved CPUs (Experimental)

    The ``_max_cpu_fraction_per_node`` option can be used to exclude CPUs from placement
    group scheduling. In the below example, setting this parameter to ``0.8`` enables Tune
    trials to run smoothly without risk of deadlock by reserving 20% of node CPUs for
    Dataset execution.

    .. literalinclude:: ./doc_code/key_concepts.py
      :language: python
      :start-after: __resource_allocation_2_begin__
      :end-before: __resource_allocation_2_end__

    .. warning::

        ``_max_cpu_fraction_per_node`` is experimental and not currently recommended for use with
        autoscaling clusters (scale-up will not trigger properly).
