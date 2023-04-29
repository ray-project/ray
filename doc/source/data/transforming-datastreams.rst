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

---------------
Transformations
---------------

There are two main types of transformations:

* One-to-one: each input block will contribute to only one output
  block, such as :meth:`ds.map_batches() <ray.data.Datastream.map_batches>`.
* All-to-all: input blocks can contribute to multiple output blocks,
  such as :meth:`ds.random_shuffle() <ray.data.Datastream.random_shuffle>`.

Here is a table listing some common transformations supported by Ray Data.

.. list-table:: Common Ray Data transformations.
   :header-rows: 1

   * - Transformation
     - Type
     - Description
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

.. tip::

    Datastreams also provides the convenience transformation methods :meth:`ds.map() <ray.data.Datastream.map>`,
    :meth:`ds.flat_map() <ray.data.Datastream.flat_map>`, and :meth:`ds.filter() <ray.data.Datastream.filter>`,
    which are not vectorized (slower than :meth:`ds.map_batches() <ray.data.Datastream.map_batches>`), but
    may be useful for development.

The following is an example to make use of those transformation APIs for processing
the Iris datastream.

.. literalinclude:: ./doc_code/transforming_datastreams.py
   :language: python
   :start-after: __datastream_transformation_begin__
   :end-before: __datastream_transformation_end__

.. _transform_datastreams_writing_udfs:

-------------------------------------
Writing User-defined Functions (UDFs)
-------------------------------------

User-defined functions (UDFs) are routines that apply on one row (e.g.
:meth:`.map() <ray.data.Datastream.map>`) or a batch of rows (e.g.
:meth:`.map_batches() <ray.data.Datastream.map_batches>`) of a datastream. UDFs let you
express your customized business logic in transformations. Here we will focus on
:meth:`.map_batches() <ray.data.Datastream.map_batches>` as it's the primary mapping
API in Datastreams.

Here are the basics that you need to know about UDFs:

* A UDF can be either a function, a generator, or if using the :ref:`actor compute strategy <transform_datastreams_compute_strategy>`, a :ref:`callable class <transform_datastreams_callable_classes>`.
* The UDF output type determines the Datastream schema of the transformation result.
* (Optional) Change the UDF input :ref:`batch format <transform_datastreams_batch_formats>` using the ``batch_format`` argument.

.. _transform_datastreams_callable_classes:

Types of UDFs
=============
There are three types of UDFs that you can use with Ray Data: Function UDFs, Callable Class UDFs, and Generator UDFs.

.. tab-set::

    .. tab-item:: "Function UDFs"

      The most basic UDFs are functions that take in a batch or row as input, and returns a batch or row as output. See :ref:`transform_datastreams_batch_formats` for the supported batch formats.

      .. literalinclude:: ./doc_code/transforming_datastreams.py
        :language: python
        :start-after: __writing_default_udfs_tabular_begin__
        :end-before: __writing_default_udfs_tabular_end__

    .. tab-item:: "Callable Class UDFs"

      With the actor compute strategy, you can use per-row and per-batch UDFs
      *callable classes*, i.e., classes that implement the ``__call__`` magic method. You
      can use the constructor of the class for stateful setup, and it is only invoked once
      per worker actor.

      Callable classes are useful if you need to load expensive state (such as a model) for the UDF. By using an actor class, you only need to load the state once in the beginning, rather than for each batch.

      .. note::
        These transformation APIs take the uninstantiated callable class as an argument,
        not an instance of the class.

      .. literalinclude:: ./doc_code/transforming_datastreams.py
        :language: python
        :start-after: __writing_callable_classes_udfs_begin__
        :end-before: __writing_callable_classes_udfs_end__

    .. tab-item:: "Generator UDFs"

      UDFs can also be written as Python generators, yielding multiple outputs for a batch or row instead of a single item. Generator UDFs are useful when returning large objects. Instead of returning a very large output batch, ``fn`` can instead yield the output batch in chunks to avoid excessive heap memory usage.

      .. warning::
        When applying a generator UDF on individual rows, make sure to use the :meth:`.flat_map() <ray.data.Datastream.flat_map>` API and not the :meth:`.map() <ray.data.Datastream.map>` API.

      .. literalinclude:: ./doc_code/transforming_datastreams.py
        :language: python
        :start-after: __writing_generator_udfs_begin__
        :end-before: __writing_generator_udfs_end__


.. _transform_datastreams_batch_formats:

UDF Input Batch Format
======================

Choose the *batch format* of the data given to UDFs
by setting the ``batch_format`` option of :meth:`.map_batches() <ray.data.Datastream.map_batches>`.
Here is an overview of the available batch formats:

.. tab-set::

    .. tab-item:: "numpy" (default)

      The ``"numpy"`` option presents batches as ``Dict[str, np.ndarray]``, where the
      `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__
      values represent a batch of record field values.

      .. literalinclude:: ./doc_code/transforming_datastreams.py
        :language: python
        :start-after: __writing_numpy_udfs_begin__
        :end-before: __writing_numpy_udfs_end__

    .. tab-item:: "pandas"

      The ``"pandas"`` batch format presents batches in
      `pandas.DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__
      format.

      .. literalinclude:: ./doc_code/transforming_datastreams.py
        :language: python
        :start-after: __writing_pandas_udfs_begin__
        :end-before: __writing_pandas_udfs_end__

    .. tab-item:: "pyarrow"

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

Converting between the internal block types (Arrow, Pandas)
and the requested batch format (``"numpy"``, ``"pandas"``, ``"pyarrow"``)
may incur data copies; which conversions cause data copying is given in the below table:


.. list-table:: Data Format Conversion Costs
   :header-rows: 1
   :stub-columns: 1

   * - Block Type x Batch Format
     - ``"pandas"``
     - ``"numpy"``
     - ``"pyarrow"``
     - ``None``
   * - Pandas Block
     - Zero-copy
     - Copy*
     - Copy*
     - Zero-copy
   * - Arrow Block
     - Copy*
     - Zero-copy*
     - Zero-copy
     - Zero-copy

.. note::
  \* No copies occur when converting between Arrow, Pandas, and NumPy formats for columns
  represented in the Ray Data tensor extension type (except for bool arrays).

.. _transform_datastreams_batch_output_types:

Batch UDF Output Types
======================

The following output types are allowed for batch UDFs (e.g.,
:meth:`ds.map_batches() <ray.data.Datastream.map_batches>`). The following describes
how they are interpreted to create the transformation result:

.. tab-set::

    .. tab-item:: Dict[str, np.ndarray]

      .. literalinclude:: ./doc_code/transforming_datastreams.py
        :language: python
        :start-after: __writing_numpy_out_udfs_begin__
        :end-before: __writing_numpy_out_udfs_end__

    .. tab-item:: pd.DataFrame

      .. literalinclude:: ./doc_code/transforming_datastreams.py
        :language: python
        :start-after: __writing_pandas_out_udfs_begin__
        :end-before: __writing_pandas_out_udfs_end__

    .. tab-item:: pa.Table

      .. literalinclude:: ./doc_code/transforming_datastreams.py
        :language: python
        :start-after: __writing_arrow_out_udfs_begin__
        :end-before: __writing_arrow_out_udfs_end__

.. _transform_datastreams_row_output_types:

Row UDF Output Types
====================

When using :meth:`ds.map() <ray.data.Datastream.map>`, the output type must always be ``Dict[str, Any]``.


.. literalinclude:: ./doc_code/transforming_datastreams.py
  :language: python
  :start-after: __writing_dict_out_row_udfs_begin__
  :end-before: __writing_dict_out_row_udfs_end__

.. _transform_datastreams_configuring_batch_size:

----------------------
Configuring Batch Size
----------------------

An important parameter to set for :meth:`ds.map_batches() <ray.data.Datastream.map_batches>`
is ``batch_size``, which controls the size of the batches provided to the UDF. The default
batch size is `4096` for CPU tasks. For GPU tasks, an explicit batch size is always required.

Increasing ``batch_size`` can result in faster execution by better leveraging vectorized
operations and hardware, reducing batch slicing and concatenation overhead, and overall
saturation of CPUs/GPUs, but will also result in higher memory utilization, which can
lead to out-of-memory failures. If encountering OOMs, decreasing your ``batch_size`` may
help.

If you set a ``batch_size`` that's larger than your ``Datastream`` blocks, Datastreams
will bundle multiple blocks together for a single task in order to better satisfy
``batch_size``. If ``batch_size`` is a lot larger than your ``Datastream`` blocks (e.g. if
your datastream was created with too large of a ``parallelism`` and/or the ``batch_size``
is set to too large of a value for your datastream), the number of parallel tasks
may be less than expected.

.. note::
  The size of the batches provided to the UDF may be smaller than the provided
  ``batch_size`` if ``batch_size`` doesn't evenly divide the block(s) sent to a given
  task.

.. _transform_datastreams_compute_strategy:

----------------
Compute Strategy
----------------

Datastreams transformations are executed by either :ref:`Ray tasks <ray-remote-functions>`
or :ref:`Ray actors <actor-guide>` across a Ray cluster. By default, Ray tasks are
used. For transformations that require expensive setup,
it's preferrable to use Ray actors, which are stateful and allow setup to be reused
for efficiency. For a fixed-size actor pool, specify ``compute=ActorPoolStrategy(size=n)``.
For an autoscaling actor pool, use ``compute=ray.data.ActorPoolStrategy(min_size=m, max_size=n)``.

The following is an example of using the Ray tasks and actors compute strategy
for batch inference:

.. literalinclude:: ./doc_code/transforming_datastreams.py
   :language: python
   :start-after: __datastream_compute_strategy_begin__
   :end-before: __datastream_compute_strategy_end__

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
