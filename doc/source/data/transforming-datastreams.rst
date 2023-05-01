.. _transforming_datastreams:

========================
Transforming Data
========================

Datastreams transformations take in datastreams and produce new datastreams. For example, *map_batches*
is a transformation that applies a
:ref:`user-defined function <transform_datastreams_writing_udfs>` on each data record
and returns a new datastream as the result. Datastreams transformations can be composed to
express a chain of computations.

.. _transform_datastreams_transformations:

--------
Overview
--------

There are two main types of supported transformations, summarized in the below table:

* One-to-one: each input block will contribute to only one output
  block, such as :meth:`ds.map_batches() <ray.data.Datastream.map_batches>`.
* All-to-all: input blocks can contribute to multiple output blocks,
  such as :meth:`ds.random_shuffle() <ray.data.Datastream.random_shuffle>`.

.. list-table:: Common Ray Data transformations.
   :header-rows: 1

   * - Transformation
     - Type
     - Description
   * - :meth:`ds.map() <ray.data.Datastream.map>`
     - One-to-one
     - Apply a given function to individual records of this datastream.
   * - :meth:`ds.map_batches() <ray.data.Datastream.map_batches>`
     - One-to-one
     - Apply a given function to batches of records of this datastream.
   * - :meth:`ds.streaming_split() <ray.data.Datastream.split>`
     - One-to-one
     - | Split the datastream into N disjoint iterators.
   * - :meth:`ds.repartition(shuffle=False) <ray.data.Datastream.repartition>`
     - One-to-one
     - | Repartition the datastream into N blocks, without shuffling the data.
   * - :meth:`ds.repartition(shuffle=True) <ray.data.Datastream.repartition>`
     - All-to-all
     - | Repartition the datastream into N blocks, shuffling the data during repartition.
   * - :meth:`ds.random_shuffle() <ray.data.Datastream.random_shuffle>`
     - All-to-all
     - | Randomly shuffle the elements of this datastream.
   * -  :meth:`ds.sort() <ray.data.Datastream.sort>`
     - All-to-all
     - | Sort the datastream by a sortkey.
   * -  :meth:`ds.groupby() <ray.data.Datastream.groupby>`
     - All-to-all
     - | Group the datastream by a groupkey.

.. _transform_datastreams_writing_udfs:

--------------
Map transforms
--------------

Use ``map_batches`` to efficiently transform records in batches, or ``map`` to transform records individually:

.. tab-set::

    .. tab-item:: Map Batches
        
      Records can be transformed in batches of ``Dict[str, np.ndarray]`` using the ``map_batches`` function. The below example shows how to use ``map_batches`` to normalize the contents of an image column:

      .. literalinclude:: ./doc_code/transforming_datastreams.py
        :language: python
        :start-after: __writing_numpy_udfs_begin__
        :end-before: __writing_numpy_udfs_end__

    .. tab-item:: Map

       Records can also be transformed one at a time using the convenience ``map`` function, which takes and returns records encoded as ``Dict[str, Any]]``. The below example shows how to convert text records to lowercase:

       .. literalinclude:: ./doc_code/transforming_datastreams.py
         :language: python
         :start-after: __writing_dict_out_row_udfs_begin__
         :end-before: __writing_dict_out_row_udfs_end__

Configuring resources
=====================

By default, each task used for transformation (e.g., `map` or `map_batches`) will request 1 CPU from Ray.
To increase the resources reserved per task, you can increase the CPU request by specifying
``.map_batches(..., num_cpus=<N>)``, which will instead reserve ``N`` CPUs per task.

To request tasks be run on a GPU, use ``.map_batches(..., num_gpus=1)``, etc. In addition to
``num_cpus`` and ``num_gpus``, any kwarg from ``@ray.remote`` can be passed to customize
the resource scheduling of transformation tasks.

Configuring batch size
======================

An important parameter to set for :meth:`ds.map_batches() <ray.data.Datastream.map_batches>`
is ``batch_size``, which controls the size of the batches provided to the your transform function. The default
batch size is `4096` for CPU tasks. For GPU tasks, an explicit batch size is always required.

Increasing ``batch_size`` can improve performance for transforms that take advantage of vectorization, but will also result in higher memory utilization, which can lead to out-of-memory (OOM) errors. If encountering OOMs, decreasing your ``batch_size`` may help. Note that if you set a ``batch_size`` that's larger than the number of records per block, Datastreams will bundle multiple blocks together into a single batch, potentially reducing the parallelism available.

Configuring batch format
========================

Customize the *format* of data batches passed to your transformation function using the ``batch_format`` argument to :meth:`ds.map_batches() <ray.data.Datastream.map_batches>`. The following are examples in each available batch format.

Note that you do not have to return data in the same batch format as specified in the input.
For example, you could return a ``pd.DataFrame`` even if the input was in NumPy format.

.. tab-set::

    .. tab-item:: NumPy (default)

      The ``"numpy"`` option presents batches as ``Dict[str, np.ndarray]``, where the
      `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__
      values represent a batch of record field values.

      .. literalinclude:: ./doc_code/transforming_datastreams.py
        :language: python
        :start-after: __writing_numpy_udfs_begin__
        :end-before: __writing_numpy_udfs_end__

    .. tab-item:: Pandas

      The ``"pandas"`` batch format presents batches in
      `pandas.DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__
      format.

      .. literalinclude:: ./doc_code/transforming_datastreams.py
        :language: python
        :start-after: __writing_pandas_udfs_begin__
        :end-before: __writing_pandas_udfs_end__

    .. tab-item:: PyArrow

      The ``"pyarrow"`` batch format presents batches in
      `pyarrow.Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
      format.

      .. literalinclude:: ./doc_code/transforming_datastreams.py
        :language: python
        :start-after: __writing_arrow_udfs_begin__
        :end-before: __writing_arrow_udfs_end__

    .. tab-item:: None

      Specifying ``None`` will tell Ray Data to choose the most performant batch format
      for the operation.

      See the :ref:`Format overheads <data_format_overheads>` section for an overview
      of format conversion overheads.

Reduce setup overheads using actors
===================================

Ray Data transformations are executed by either :ref:`Ray tasks <ray-remote-functions>`
or :ref:`Ray actors <actor-guide>` across a Ray cluster. By default, tasks are
used. For transformations that require expensive setup,
it's preferrable to use actors, which are stateful and allow setup to be reused
for efficiency. For a fixed-size actor pool, specify ``compute=ActorPoolStrategy(size=n)``.
For an autoscaling actor pool, use ``compute=ray.data.ActorPoolStrategy(min_size=m, max_size=n)``.

When using actors, you must also specify your transformation function as a callable class typeinstead of a plain function. The following is an example of using actors for batch inference:

.. literalinclude:: ./doc_code/transforming_datastreams.py
   :language: python
   :start-after: __datastream_compute_strategy_begin__
   :end-before: __datastream_compute_strategy_end__

Reduce memory usage using generators
====================================

Transformations can also be written as Python generators, yielding multiple outputs for a batch or row instead of a single item. Generator UDFs are useful when returning large objects. Instead of returning a very large output batch, ``fn`` can instead yield the output batch in chunks to avoid excessive heap memory usage.

.. literalinclude:: ./doc_code/transforming_datastreams.py
  :language: python
  :start-after: __writing_generator_udfs_begin__
  :end-before: __writing_generator_udfs_end__
.. _data-groupbys:

--------------------------
Group-bys and aggregations
--------------------------

Unlike mapping operations, groupbys and aggregations are global. Grouped aggregations
are executed lazily. Global aggregations are executed *eagerly* and block until the
aggregation has been computed.

.. code-block:: python

    ds: ray.data.Datastream = ray.data.from_items([
        {"A": x % 3, "B": 2 * x, "C": 3 * x}
        for x in range(10)])

    # Group by the A column and calculate the per-group mean for B and C columns.
    agg_ds: ray.data.Datastream = ds.groupby("A").mean(["B", "C"]).materialize()
    # -> Sort Sample: 100%|███████████████████████████████████████| 10/10 [00:01<00:00,  9.04it/s]
    # -> GroupBy Map: 100%|███████████████████████████████████████| 10/10 [00:00<00:00, 23.66it/s]
    # -> GroupBy Reduce: 100%|████████████████████████████████████| 10/10 [00:00<00:00, 937.21it/s]
    # -> Datastream(num_blocks=10, num_rows=3, schema={})
    agg_ds.to_pandas()
    # ->
    #    A  mean(B)  mean(C)
    # 0  0      9.0     13.5
    # 1  1      8.0     12.0
    # 2  2     10.0     15.0

    # Global mean on B column.
    ds.mean("B")
    # -> GroupBy Map: 100%|███████████████████████████████████████| 10/10 [00:00<00:00, 2851.91it/s]
    # -> GroupBy Reduce: 100%|████████████████████████████████████| 1/1 [00:00<00:00, 319.69it/s]
    # -> 9.0

    # Global mean on multiple columns.
    ds.mean(["B", "C"])
    # -> GroupBy Map: 100%|███████████████████████████████████████| 10/10 [00:00<00:00, 1730.32it/s]
    # -> GroupBy Reduce: 100%|████████████████████████████████████| 1/1 [00:00<00:00, 231.41it/s]
    # -> {'mean(B)': 9.0, 'mean(C)': 13.5}

    # Multiple global aggregations on multiple columns.
    from ray.data.aggregate import Mean, Std
    ds.aggregate(Mean("B"), Std("B", ddof=0), Mean("C"), Std("C", ddof=0))
    # -> GroupBy Map: 100%|███████████████████████████████████████| 10/10 [00:00<00:00, 1568.73it/s]
    # -> GroupBy Reduce: 100%|████████████████████████████████████| 1/1 [00:00<00:00, 133.51it/s]
    # -> {'mean(A)': 0.9, 'std(A)': 0.8306623862918076, 'mean(B)': 9.0, 'std(B)': 5.744562646538029}

Combine aggreations with batch mapping to transform datastreams using computed statistics.
For example, you can efficiently standardize feature columns and impute missing values
with calculated column means.

.. code-block:: python

    # Impute missing values with the column mean.
    b_mean = ds.mean("B")
    # -> GroupBy Map: 100%|███████████████████████████████████████| 10/10 [00:00<00:00, 4054.03it/s]
    # -> GroupBy Reduce: 100%|████████████████████████████████████| 1/1 [00:00<00:00, 359.22it/s]
    # -> 9.0

    def impute_b(df: pd.DataFrame):
        df["B"].fillna(b_mean)
        return df

    ds = ds.map_batches(impute_b, batch_format="pandas")
    # -> MapBatches(impute_b)
    #    +- Datastream(num_blocks=10, num_rows=10, schema={A: int64, B: int64, C: int64})

    # Standard scaling of all feature columns.
    stats = ds.aggregate(Mean("B"), Std("B"), Mean("C"), Std("C"))
    # -> MapBatches(impute_b): 100%|██████████████████████████████| 10/10 [00:01<00:00,  7.16it/s]
    # -> GroupBy Map: 100%|███████████████████████████████████████| 10/10 [00:00<00:00, 1260.99it/s]
    # -> GroupBy Reduce: 100%|████████████████████████████████████| 1/1 [00:00<00:00, 128.77it/s]
    # -> {'mean(B)': 9.0, 'std(B)': 6.0553007081949835, 'mean(C)': 13.5, 'std(C)': 9.082951062292475}

    def batch_standard_scaler(df: pd.DataFrame):
        def column_standard_scaler(s: pd.Series):
            s_mean = stats[f"mean({s.name})"]
            s_std = stats[f"std({s.name})"]
            return (s - s_mean) / s_std

        cols = df.columns.difference(["A"])
        df.loc[:, cols] = df.loc[:, cols].transform(column_standard_scaler)
        return df

    ds = ds.map_batches(batch_standard_scaler, batch_format="pandas")
    ds.materialize()
    # -> Map Progress: 100%|██████████████████████████████████████| 10/10 [00:00<00:00, 144.79it/s]
    # -> Datastream(num_blocks=10, num_rows=10, schema={A: int64, B: double, C: double})

--------------
Shuffling data
--------------

Call :meth:`Datastream.random_shuffle() <ray.data.Datastream.random_shuffle>` to
perform a global shuffle.

.. doctest::

    >>> import ray
    >>> datastream = ray.data.range(10)
    >>> datastream.random_shuffle().take_batch()  # doctest: +SKIP
    {'id': array([7, 0, 9, 3, 5, 1, 4, 2, 8, 6])}

For better performance, perform a local shuffle. Read 
:ref:`Shuffling Data <air-shuffle>` in the AIR user guide to learn more.
