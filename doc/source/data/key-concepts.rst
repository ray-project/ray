.. _data_key_concepts:

============
Key Concepts
============

.. _dataset_concept:

----------
Dataset
----------

A :term:`Dataset <Dataset (object)>` operates over a sequence of Ray object references to :term:`blocks <Block>`.
Each block holds a set of records in an `Arrow table <https://arrow.apache.org/docs/python/data.html#tables>`_ or
`pandas DataFrame <https://pandas.pydata.org/docs/reference/frame.html>`_.
Having multiple blocks in a dataset allows for parallel transformation and ingest.

For ML use cases, Dataset natively supports mixing tensors with tabular data. To
learn more, read :ref:`Working with tensor data <working_with_tensors>`.

The following figure visualizes a dataset with three blocks, each holding 1000 rows. Note that certain blocks
may not be computed yet. Normally, callers iterate over dataset blocks in a streaming fashion, so that not all
blocks need to be materialized in the cluster memory at once.

.. image:: images/dataset-arch.svg

..
  https://docs.google.com/drawings/d/1PmbDvHRfVthme9XD7EYM-LIHPXtHdOfjCbc1SCsM64k/edit

Reading Data
============

Dataset uses Ray tasks to read data from remote storage in parallel. Each read task reads one or more files and produces one or more output blocks:

.. image:: images/dataset-read.svg
   :align: center

..
  https://docs.google.com/drawings/d/15B4TB8b5xN15Q9S8-s0MjW6iIvo_PrH7JtV1fL123pU/edit

You can increase or decrease the number of output blocks by changing the ``parallelism`` parameter.

For an in-depth guide on creating datasets, read :ref:`Loading Data <loading_data>`.

Transforming Data
=================

Dataset uses either Ray tasks or Ray actors to transform data blocks. By default, it uses tasks.

To use Actors, pass an :class:`ActorPoolStrategy` to ``compute`` in methods like
:meth:`~ray.data.Dataset.map_batches`. :class:`ActorPoolStrategy` creates an autoscaling
pool of Ray actors. This allows you to cache expensive state initialization
(e.g., model loading for GPU-based tasks).

.. image:: images/dataset-map.svg
   :align: center
..
  https://docs.google.com/drawings/d/12STHGV0meGWfdWyBlJMUgw7a-JcFPu9BwSOn5BjRw9k/edit

For an in-depth guide on transforming datasets, read :ref:`Transforming Data <transforming_data>`.

Shuffling Data
==============

Operations like :meth:`~ray.data.Dataset.sort` and :meth:`~ray.data.Dataset.groupby`
require blocks to be partitioned by value or *shuffled*. Dataset uses tasks to shuffle blocks in a map-reduce
style: map tasks partition blocks by value and then reduce tasks merge co-partitioned
blocks.

Call :meth:`~ray.data.Dataset.repartition` to change the number of blocks in a :class:`~ray.data.Dataset`.
Repartition has two modes:

* ``shuffle=False`` - performs the minimal data movement needed to equalize block sizes
* ``shuffle=True`` - performs a full distributed shuffle

.. image:: images/dataset-shuffle.svg
   :align: center

..
  https://docs.google.com/drawings/d/132jhE3KXZsf29ho1yUdPrCHB9uheHBWHJhDQMXqIVPA/edit

Dataset can shuffle multi-terabyte datasets, leveraging the Ray object store for disk spilling. For an in-depth guide on shuffle performance, read :ref:`Performance Tips and Tuning <shuffle_performance_tips>`.
Note that operations like shuffle materialize the entire Dataset prior to their execution (shuffle execution is not streamed through memory).

Iteration and materialization
=============================

Most transformations on a dataset are lazy. They don't execute until you iterate over the dataset or call
:meth:`Dataset.materialize() <ray.data.Dataset.materialize>`. When a Dataset is materialized, its
type becomes a `MaterializedDataset`, which indicates that all its blocks are materialized in Ray
object store memory.

Dataset transformations are executed in a streaming way, incrementally on the data and
with operators processed in parallel, see :ref:`Streaming Execution <streaming_execution>`.

Datasets and MaterializedDatasets can be freely passed between Ray tasks, actors, and libraries without
incurring copies of the underlying block data (pass by reference semantics).

Fault tolerance
===============

Dataset performs *lineage reconstruction* to recover data. If an application error or
system failure occurs, Dataset recreates lost blocks by re-executing tasks. If ``compute=ActorPoolStrategy(size=n)`` is used, then Ray
restarts the actor used for computing the block prior to re-executing the task.

Fault tolerance is not supported if the original worker process that created the Dataset dies.
This is because the creator stores the metadata for the :ref:`objects <object-fault-tolerance>` that comprise the Dataset.
