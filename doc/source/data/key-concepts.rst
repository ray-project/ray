.. _data_key_concepts:

============
Key Concepts
============

.. _dataset_concept:

Datasets and blocks
===================

A :class:`Dataset <ray.data.Dataset>` operates over a sequence of Ray object references
to :term:`blocks <Block>`. Each block contains a disjoint subset of rows, and Ray Data
loads and transforms these blocks in parallel.

.. tip::

    To change the number of blocks in a :class:`~ray.data.Dataset`, call
    :meth:`~ray.data.Dataset.repartition`.

The following figure visualizes a dataset with three blocks, each holding 1000 rows.
Note that Ray Data is lazy, so some blocks might be unmaterialized.

.. image:: images/dataset-arch.svg

..
  https://docs.google.com/drawings/d/1PmbDvHRfVthme9XD7EYM-LIHPXtHdOfjCbc1SCsM64k/edit

Execution model
===============

Ray Data is lazy and streaming:

- **Lazy**: This means that transformations on a :class:`~ray.data.Dataset` aren't
  executed until you call am method like :meth:`Dataset.iter_batches() <ray.data.Dataset.iter_batches>`
  or :meth:`Dataset.materialize() <ray.data.Dataset.materialize>`.
- **Streaming**: This means that transformations are executed
  incrementally (instead of on all of the data at once) and with steps overlapped.

For more information, see :ref:`Streaming Execution <streaming_execution>`.

Loading data
============

Ray Data lets you load data from local disk and cloud storage.

.. testcode::

    import ray

    ds = ray.data.read_parquet("...")

    print(ds.schema())

.. testsetup::

    ...

Ray Data uses :ref:`Ray tasks <task-key-concept>` to read files in parallel. Each read
task reads one or more files and produces an output block:

.. image:: images/dataset-read.svg
   :align: center

..
  https://docs.google.com/drawings/d/15B4TB8b5xN15Q9S8-s0MjW6iIvo_PrH7JtV1fL123pU/edit

For more information on loading data, see :ref:`Loading data <loading-data>`.

Transforming data
=================

Transformations let you process and modify your dataset. You can compose transformations
to express a chain of computations.

.. testcode::

    # TODO

Ray Data uses either :ref:`Ray tasks <task-key-concept>` or
:ref:`Ray actors <actor-key-concept>`to transform blocks. By default, it uses tasks.

Tasks are stateless. So, if you want to cache expensive initialization like downloading
model weights, use actors.

.. image:: images/dataset-map.svg
   :align: center
..
  https://docs.google.com/drawings/d/12STHGV0meGWfdWyBlJMUgw7a-JcFPu9BwSOn5BjRw9k/edit

For more information on transforming data, see
:ref:`Transforming data <transforming_data>`.

Shuffling data
==============

Ray Data can shuffle multi-terabyte datasets, leveraging the Ray object store for disk spilling.

.. testcode::

    import ray

    ds.range(10000).shuffle()

Dataset uses tasks to shuffle blocks in a map-reduce style: map tasks partition blocks
by value and then reduce tasks merge co-partitioned blocks.

.. note::

    In addition to shuffle, methods like :meth:`~ray.data.Dataset.sort` and :meth:`~ray.data.Dataset.groupby`
    also shuffle blocks. These methods materialize the entire
    :class:`~ray.data.Dataset`. In other words, shuffle execution isn't streamed through
    memory.

For an in-depth guide on shuffle performance, see :ref:`Performance Tips and Tuning <shuffle_performance_tips>`.

Fault tolerance
===============

Ray Data performs *lineage reconstruction* to recover data. If an application error or
system failure occurs, Ray Data recreates blocks by re-executing tasks.

.. note::

    Fault tolerance isn't supported if the process that created the
    :class:`~ray.data.Dataset` dies.
