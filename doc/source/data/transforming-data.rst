.. _transforming_data:

=================
Transforming Data
=================

Datastream transforms take in datastreams and produce new datastreams. For example, *map_batches*
is a transform that applies a
:ref:`user-defined function <transform_datastreams_writing_udfs>` on each data record
and returns a new datastream as the result. Datastream transforms can be composed to
express a chain of computations.

--------
Overview
--------

There are two main types of supported transforms:

* One-to-one: each input block will contribute to only one output
  block, such as :meth:`ds.map_batches() <ray.data.Datastream.map_batches>`.
* All-to-all: input blocks can contribute to multiple output blocks,
  such as :meth:`ds.random_shuffle() <ray.data.Datastream.random_shuffle>`.

.. list-table:: Common Ray Data transforms.
   :header-rows: 1

   * - Transform
     - Type
     - Description
   * - :meth:`ds.map() <ray.data.Datastream.map>`
     - One-to-one
     - Apply a given function to individual data records.
   * - :meth:`ds.map_batches() <ray.data.Datastream.map_batches>`
     - One-to-one
     - Apply a given function to batches of records.
   * - :meth:`ds.repartition() <ray.data.Datastream.repartition>`
     - All-to-all
     - | Repartition the datastream into N blocks.
   * - :meth:`ds.random_shuffle() <ray.data.Datastream.random_shuffle>`
     - All-to-all
     - | Randomly shuffle the datastream.
   * -  :meth:`ds.groupby().\<agg\>() <ray.data.Datastream.groupby>`
     - All-to-all
     - | Group data by column and aggregate each group.
   * -  :meth:`ds.groupby().map_groups() <ray.data.grouped_data.GroupedData.map_groups>`
     - All-to-all
     - | Group data by column and transform each group.

.. _transform_datastreams_writing_udfs:

--------------
Map transforms
--------------

Use ``map_batches`` to efficiently transform records in batches, or ``map`` to transform records individually:

.. tab-set::

    .. tab-item:: Map Batches

      Call `map_batches`` to transform batches of records. Each batch has type``Dict[str, np.ndarray]``. The below example shows how to use ``map_batches`` to convert text records to lowercase:

      .. literalinclude:: ./doc_code/transforming_data.py
        :language: python
        :start-after: __map_batches_begin__
        :end-before: __map_batches_end__

    .. tab-item:: Map

       Records can also be transformed one at a time using the ``map`` function, which takes records encoded as ``Dict[str, Any]]``. The below example shows how to convert text records to lowercase:

       .. literalinclude:: ./doc_code/transforming_data.py
         :language: python
         :start-after: __map_begin__
         :end-before: __map_end__

Configuring resources
=====================

By default, each task used for  (e.g., `map` or `map_batches`) requests 1 CPU from Ray.
To increase the resources reserved per task, you can increase the CPU request by specifying
``.map_batches(..., num_cpus=<N>)``, which will instead reserve ``N`` CPUs per task:

.. code-block:: python

    # Run each function with 1 CPU each (default).
    ds.map_batches(func)

    # Run each function with 4 CPUs each.
    ds.map_batches(func, num_cpus=4)

To request tasks be run on a GPU, use ``.map_batches(..., num_gpus=1)``, etc. In addition to
``num_cpus`` and ``num_gpus``, any kwarg from ``@ray.remote`` can be passed to customize
the resource scheduling of tasks:

.. code-block:: python

    # Run each function with 1 GPU each.
    ds.map_batches(func, num_gpus=1)

    # Can also customize other ray remote args such as `max_retries`.
    ds.map_batches(func, num_gpus=1, max_retries=10)

Configuring batch size
======================

An important parameter to set for :meth:`ds.map_batches() <ray.data.Datastream.map_batches>`
is ``batch_size``, which controls the size of the batches provided to the your transform function. The default
batch size is `4096` for CPU tasks. For GPU tasks, an explicit batch size is always required:

.. code-block:: python

    # Each batch sent to `func` will have up to 4096 records (default).
    ds.map_batches(func)

    # Reduce the batch size to 64 records per batch.
    ds.map_batches(func, batch_size=64)

Increasing ``batch_size`` can improve performance for transforms that take advantage of vectorization, but will also result in higher memory utilization, which can lead to out-of-memory (OOM) errors. If encountering OOMs, decreasing your ``batch_size`` may help. Note also that if the ``batch_size`` becomes larger than the number of records per block, multiple blocks will be bundled together into a single batch, potentially reducing the parallelism available.

.. _transform_datastreams_batch_formats:

Configuring batch format
========================

Customize the format of data batches using the ``batch_format`` argument to :meth:`ds.map_batches() <ray.data.Datastream.map_batches>`. The following are examples in each available batch format.

Transform functions do not have to return data in the same format as the input batch. For example, you could return a ``pd.DataFrame`` even if the input was in NumPy format.

.. tab-set::

    .. tab-item:: NumPy (default)

      The ``"numpy"`` option presents batches as ``Dict[str, np.ndarray]``, where the
      `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__
      values represent a batch of record field values.

      .. literalinclude:: ./doc_code/transforming_data.py
        :language: python
        :start-after: __writing_numpy_udfs_begin__
        :end-before: __writing_numpy_udfs_end__

    .. tab-item:: Pandas

      The ``"pandas"`` batch format presents batches in
      `pandas.DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__
      format.

      .. literalinclude:: ./doc_code/transforming_data.py
        :language: python
        :start-after: __writing_pandas_udfs_begin__
        :end-before: __writing_pandas_udfs_end__

    .. tab-item:: PyArrow

      The ``"pyarrow"`` batch format presents batches in
      `pyarrow.Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
      format.

      .. literalinclude:: ./doc_code/transforming_data.py
        :language: python
        :start-after: __writing_arrow_udfs_begin__
        :end-before: __writing_arrow_udfs_end__

.. _transforming_data_actors:

Reduce setup overheads using actors
===================================

Data transforms can be executed by either :ref:`Ray tasks <ray-remote-functions>`
or :ref:`Ray actors <actor-guide>`. By default, ``map_batches`` uses tasks.
For transforms that require expensive setup,
it's preferrable to use actors, which are stateful and allow setup to be reused
for efficiency. For a fixed-size actor pool, specify ``compute=ActorPoolStrategy(size=n)``.
For an autoscaling actor pool, use ``compute=ray.data.ActorPoolStrategy(min_size=m, max_size=n)``.

When using actors, you must also specify your transform as a callable class type instead of a plain function. The following is an example of using actors for batch inference:

.. literalinclude:: ./doc_code/transforming_data.py
   :language: python
   :start-after: __datastream_compute_strategy_begin__
   :end-before: __datastream_compute_strategy_end__

Reduce memory usage using generators
====================================

Transform functions can also be written as Python generators, yielding multiple outputs for a batch or row instead of a single item. Generator UDFs are useful when returning large objects. Instead of returning a very large output batch, ``fn`` can instead yield the output batch in chunks to avoid excessive heap memory usage.

.. literalinclude:: ./doc_code/transforming_data.py
  :language: python
  :start-after: __writing_generator_udfs_begin__
  :end-before: __writing_generator_udfs_end__

------------------
Shuffle transforms
------------------

Shuffle transforms change the organization of the data, e.g., increasing the number of blocks, or the order of records in each block, without changing the record contents.

Repartitioning data
===================

Call :meth:`Datastream.repartition() <ray.data.Datastream.repartition>` to change the
number of blocks of the datastream. This may be useful to break up your dataset into small
pieces to enable more fine-grained parallelization, or to reduce the number of files
produced as output of a write operation.

.. literalinclude:: ./doc_code/transforming_data.py
  :language: python
  :start-after: __shuffle_begin__
  :end-before: __shuffle_end__

Random shuffle
==============

Call :meth:`Datastream.random_shuffle() <ray.data.Datastream.random_shuffle>` to
globally shuffle the order of data records.

.. doctest::

    >>> import ray
    >>> datastream = ray.data.range(10)
    >>> datastream.random_shuffle().take_batch()  # doctest: +SKIP
    {'id': array([7, 0, 9, 3, 5, 1, 4, 2, 8, 6])}

For reduced overhead during training ingest, use local shuffles. Read 
:ref:`Shuffling Data <air-shuffle>` in the AIR user guide to learn more.

.. _data-groupbys:

------------------
Grouped transforms
------------------

Ray Data supports grouping data by column and applying aggregations to each group. This is supported via the :meth:`ds.groupby() <ray.data.Datastream.groupby>` call.

Aggregations
============

Aggregations can be performed per group:

.. code-block:: python

    ds = ray.data.from_items([
        {"A": x % 3, "B": 2 * x, "C": 3 * x}
        for x in range(10)
    ])

    # Group by the A column and calculate the per-group mean for B and C columns.
    ds.groupby("A").mean(["B", "C"]).to_pandas()
    # ->
    #    A  mean(B)  mean(C)
    # 0  0      9.0     13.5
    # 1  1      8.0     12.0
    # 2  2     10.0     15.0

Aggregations can also be applied globally:

.. code-block:: python

    from ray.data.aggregate import Mean, Std

    # Global mean on B and C columns.
    ds.mean(["B", "C"])
    # -> {'mean(B)': 9.0, 'mean(C)': 13.5}

    # Multiple global aggregations on multiple columns.
    ds.aggregate(Mean("B"), Std("B", ddof=0), Mean("C"), Std("C", ddof=0))
    # -> {'mean(A)': 0.9, 'std(A)': 0.8306623862918076, 'mean(B)': 9.0, 'std(B)': 5.744562646538029}

Note that Ray Data currently only supports grouping by a single column. In order to group by multiple columns, you can first compute the grouping key using ``map_batches`` prior to calling ``groupby``.

Map Groups
==========

Arbitrary processing can be applied to each group of records using :meth:`ds.groupby().map_groups() <ray.data.GroupedData.map_groups>`. For example, this could be used to implement custom aggregations, train a model per group, etc.

.. literalinclude:: ./doc_code/transforming_data.py
  :language: python
  :start-after: __map_groups_begin__
  :end-before: __map_groups_end__

Note that when using ``map_groups``, all records of the same group will be gathered into the same batch,
which may lead to out-of-memory errors if the group size exceeds the capacity of a single machine.
