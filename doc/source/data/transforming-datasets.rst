.. _transforming_datasets:

=====================
Transforming Datasets
=====================

The Ray Datasets transformations take in datasets and produce new datasets.
For example, *map* is a transformation that applies a user-defined function (UDF)
on each row of input dataset and returns a new dataset as result. The Datasets
transformations are **composable**. Operations can be further applied on the result
dataset, forming a chain of transformations to express more complex computations.
Transformations are the core for expressing business logic in Datasets.

.. _transform_datasets_transformations:

---------------
Transformations
---------------

In general, we have two types of transformations:

* **One-to-one transformations:** each input block will contribute to only one output
  block, such as :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`. In other
  systems this may be called narrow transformations.
* **All-to-all transformations:** input blocks can contribute to multiple output blocks,
  such as :meth:`ds.random_shuffle() <ray.data.Dataset.random_shuffle>`. In other
  systems this may be called wide transformations.

Here is a table listing some common transformations supported by Ray Datasets.

.. list-table:: Common Ray Datasets transformations.
   :header-rows: 1

   * - Transformation
     - Type
     - Description
   * - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
     - One-to-one
     - Apply a given function to batches of records of this dataset. 
   * - :meth:`ds.split() <ray.data.Dataset.split>`
     - One-to-one
     - | Split the dataset into N disjoint pieces.
   * - :meth:`ds.repartition(shuffle=False) <ray.data.Dataset.repartition>`
     - One-to-one
     - | Repartition the dataset into N blocks, without shuffling the data.
   * - :meth:`ds.repartition(shuffle=True) <ray.data.Dataset.repartition>`
     - All-to-all
     - | Repartition the dataset into N blocks, shuffling the data during repartition.
   * - :meth:`ds.random_shuffle() <ray.data.Dataset.random_shuffle>`
     - All-to-all
     - | Randomly shuffle the elements of this dataset.
   * -  :meth:`ds.sort() <ray.data.Dataset.sort>`
     - All-to-all
     - | Sort the dataset by a sortkey.
   * -  :meth:`ds.groupby() <ray.data.Dataset.groupby>`
     - All-to-all
     - | Group the dataset by a groupkey.

.. tip::

    Datasets also provides the convenience transformation methods :meth:`ds.map() <ray.data.Dataset.map>`,
    :meth:`ds.flat_map() <ray.data.Dataset.flat_map>`, and :meth:`ds.filter() <ray.data.Dataset.filter>`,
    which are not vectorized (slower than :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`), but
    may be useful for development.

The following is an example to make use of those transformation APIs for processing
the Iris dataset.

.. literalinclude:: ./doc_code/transforming_datasets.py
   :language: python
   :start-after: __dataset_transformation_begin__
   :end-before: __dataset_transformation_end__

.. _transform_datasets_writing_udfs:

------------
Writing UDFs
------------

User-defined functions (UDFs) are routines that apply on one row (e.g.
:meth:`.map() <ray.data.Dataset.map>`) or a batch of rows (e.g.
:meth:`.map_batches() <ray.data.Dataset.map_batches>`) of a dataset. UDFs let you
express your customized business logic in transformations. Here we will focus on
:meth:`.map_batches() <ray.data.Dataset.map_batches>` as it's the primary mapping
API in Datasets.

A batch UDF can be a function or, if using the
:ref:`actor compute strategy <transform_datasets_compute_strategy>`, a
:ref:`callable class <transform_datasets_callable_classes>`.
These UDFs have several :ref:`batch format options <transform_datasets_batch_formats>`,
which control the format of the batches that are passed to the provided batch UDF.
Depending on the underlying :ref:`dataset format <transform_datasets_dataset_formats>`,
using a particular batch format may or may not incur a data conversion cost
(e.g. converting an Arrow Table to a Pandas DataFrame, or creating an Arrow Table from a
Python list, both of which would incur a full copy of the data).

.. _transform_datasets_dataset_formats:

Dataset Formats
===============

A **dataset format** refers to how Datasets represents data under-the-hood as data
**blocks**.

* **Tabular (Arrow or Pandas) Datasets:** Represented under-the-hood as
  `Arrow Tables <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  or
  `Pandas DataFrames <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__.
  Tabular datasets are created by reading on-disk tabular data (Parquet, CSV, JSON),
  converting from in-memory tabular data (Pandas DataFrames, Arrow Tables,
  dictionaries, Dask DataFrames, Modin DataFrames, Spark DataFrames, Mars DataFrames)
  and by returning tabular data (Arrow, Pandas, dictionaries) from previous batch
  UDFs.

  Datasets will minimize conversions between the Arrow and Pandas
  representation: tabular data read from disk will always be read as Arrow Tables, but
  in-memory conversions of Pandas DataFrame data to a ``Dataset`` (e.g. converting from
  Pandas, Dask, Modin, Spark, and Mars) will use Pandas DataFrames as the internal data
  representation in order to avoid extra data conversions/copies.

* **Tensor Datasets:** Represented under-the-hood as a single-column
  `Arrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  or
  `Pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__,
  using our :ref:`tensor extension type <dataset-tensor-extension-api>` to embed the
  tensor in these tables under a single ``"__value__"`` column. Tensor datasets are
  created by reading on-disk tensor data (NPY), converting from in-memory tensor data
  (NumPy), and by returning tensor data (NumPy) from previous batch UDFs.

  Note that converting the underlying tabular representation to a NumPy ndarray is
  zero-copy, including converting the entire column to an ndarray and converting just
  a single column element to an ndarray.

* **Simple Datasets:** Represented under-the-hood as plain Python lists. Simple
  datasets are created by reading on-disk binary and plain-text data, converting from
  in-memory simple data (Python lists), and by returning simple data (Python lists)
  from previou batch UDFs.

  Simple datasets are mostly used as an escape hatch for data that's not cleanly
  representable in Arrow Tables and Pandas DataFrames.

.. _transform_datasets_batch_formats:

Batch Formats
=============

The **batch format** is the format of the data that's given to batch UDFs, e.g.
Pandas DataFrames, Arrow Tables, NumPy ndarrays, or Python lists. The 
:meth:`.map_batches() <ray.data.Dataset.map_batches>` API has a ``batch_format: str``
parameter that allows the user to dictate the batch format; we dig into the details of
each ``batch_format`` option below:

.. tabbed:: "native" (default)

  The ``"native"`` batch format presents batches in the canonical format for each dataset
  type:

  * **Tabular (Arrow or Pandas) Datasets:** Each batch will be a
    `pandas.DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__.
    If the dataset is represented as Arrow Tables under-the-hood, the conversion to the
    Pandas DataFrame batch format will incur a conversion cost (i.e., a full copy of
    each batch will be created in order to convert it).
  * **Tensor Datasets:** Each batch will be a
    `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__.
  * **Simple Datasets:** Each batch will be a Python list.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_native_udfs_begin__
    :end-before: __writing_native_udfs_end__

.. tabbed:: "pandas"

  The ``"pandas"`` batch format provides batches in a
  `pandas.DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__
  format. If converting a simple dataset to Pandas DataFrame batches, a single-column
  dataframe with the column ``"value"`` will be created.

  If the underlying datasets data is not already in Pandas DataFrame format, the
  batch format conversion will incur a copy for each batch.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_pandas_udfs_begin__
    :end-before: __writing_pandas_udfs_end__

.. tabbed:: "pyarrow"

  The ``"pyarrow"`` batch format provides batches in a
  `pyarrow.Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  format. If converting a simple dataset to Arrow Table batches, a single-column table
  with the column ``"value"`` will be created.

  If the underlying datasets data is not already in Arrow Table format, the
  batch format conversion will incur a copy for each batch.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_arrow_udfs_begin__
    :end-before: __writing_arrow_udfs_end__

.. tabbed:: "numpy"

  The ``"numpy"`` batch format provides batches in a
  `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__
  format. The following details how each dataset format is converted into a NumPy batch:

  * **Tensor Datasets:** Each batch will be a single
    `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__
    containing the single-tensor-column for this batch.

    Note that this conversion should always be zero-copy.

  * **Tabular (Arrow or Pandas) Datasets:** Each batch will be a dictionary of NumPy
    ndarrays (``Dict[str, np.ndarray]``), with each key-value pair representing a column
    in the table.

    Note that this conversion should usually be zero-copy.

  * **Simple Datasets:** Each batch will be a single NumPy ndarray, where Datasets will
    attempt to convert each list-batch to an ndarray.

    Note that this conversion will
    incur a copy for primitive types that are representable as a NumPy dtype (since
    NumPy will create a continguous ndarray in that case), and will not incur a copy if
    the batch items aren't supported in the NumPy dtype system (since NumPy will create
    an ndarray of ``np.object`` pointers to the batch items in that case).

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_numpy_udfs_begin__
    :end-before: __writing_numpy_udfs_end__

The following table summarizes the conversion costs from a particular dataset format to
a particular batch format:

.. list-table:: Batch format conversion costs - is the conversion zero-copy?
   :header-rows: 1
   :stub-columns: 1
   :widths: 40 15 15 15 15
   :align: center

   * - Dataset Format -> Batch Format
     - ``"native"``
     - ``"pandas"``
     - ``"pyarrow"``
     - ``"numpy"``
   * - Tabular - Pandas
     - Zero-Copy
     - Zero-Copy
     - Copy
     - Zero-Copy
   * - Tabular - Arrow
     - Copy
     - Copy
     - Zero-Copy
     - Zero-Copy
   * - Tensor - Pandas
     - Zero-Copy
     - Zero-Copy
     - Copy
     - Zero-Copy
   * - Tensor - Arrow
     - Zero-Copy
     - Copy
     - Zero-Copy
     - Zero-Copy
   * - Simple - List
     - Zero-Copy
     - Copy
     - Copy
     - Copy

You should reference the `pyarrow.Table APIs
<https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__, the
`pandas.DataFrame APIs <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__,
or the `numpy.ndarray APIs <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__
when writing batch UDFs.

.. tip::

   Write your UDFs to leverage built-in vectorized operations on the ``pandas.DataFrame``,
   ``pyarrow.Table``, and ``numpy.ndarray`` abstractions for better performance. For
   example, suppose you want to compute the sum of a column in ``pandas.DataFrame``:
   instead of iterating over each row of a batch and summing up values of that column,
   you should use ``df_batch["col_foo"].sum()``.

.. _transform_datasets_callable_classes:

Callable Class UDFs
===================

When using the actor compute strategy, per-row and per-batch UDFs can also be
**callable classes**, i.e. classes that implement the ``__call__`` magic method. The
constructor of the class can be used for stateful setup, and will be only invoked once
per actor worker.

.. note::
  These transformation APIs take the uninstantiated callable class as an argument,
  **not** an instance of the class!

.. literalinclude:: ./doc_code/transforming_datasets.py
   :language: python
   :start-after: __writing_callable_classes_udfs_begin__
   :end-before: __writing_callable_classes_udfs_end__

.. _transform_datasets_batch_output_types:

Batch UDF Output Types
======================

The return type of a batch UDF will determine the format of the resulting dataset. This
allows you to dynamically convert between data formats during batch transformations.

The following details how each **batch** UDF (as used in
:meth:`ds.map_batches() <ray.data.Dataset.map_batches>`) return type will be interpreted
by Datasets when constructing its internal blocks:

.. tabbed:: pd.DataFrame

  Tabular dataset containing
  `Pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__
  blocks.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_pandas_out_udfs_begin__
    :end-before: __writing_pandas_out_udfs_end__

.. tabbed:: pa.Table

  Tabular dataset containing
  `pyarrow.Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  blocks.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_arrow_out_udfs_begin__
    :end-before: __writing_arrow_out_udfs_end__

.. tabbed:: np.ndarray

  Tensor dataset containing
  `pyarrow.Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  blocks, representing the tensor using our
  :ref:`tensor extension type <dataset-tensor-extension-api>` to embed the tensor in
  these tables under a single ``"__value__"`` column.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_numpy_out_udfs_begin__
    :end-before: __writing_numpy_out_udfs_end__

.. tabbed:: Dict[str, np.ndarray]

  Tabular dataset containing
  `pyarrow.Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  blocks, where each key-value pair treats the key as the column name as the value as
  the column tensor.

  If a column tensor is 1-dimensional, then the native Arrow 1D list
  type is used; if a column tensor has 2 or more dimensions, then we use our
  :ref:`tensor extension type <dataset-tensor-extension-api>` to embed these
  n-dimensional tensors in the Arrow table.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_numpy_dict_out_udfs_begin__
    :end-before: __writing_numpy_dict_out_udfs_end__

.. tabbed:: list

  Simple dataset containing Python list blocks.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_simple_out_udfs_begin__
    :end-before: __writing_simple_out_udfs_end__

.. _transform_datasets_row_output_types:

Row UDF Output Types
====================

The return type of a row UDF will determine the format of the resulting dataset. This
allows you to dynamically convert between data formats during row-based transformations.

The following details how each **row** UDF (as used in
:meth:`ds.map() <ray.data.Dataset.map>`) return type will be interpreted by Datasets
when constructing its internal blocks:

.. tabbed:: dict

  Tabular dataset containing
  `pyarrow.Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  blocks, treating the keys as the column names and the values as the column elements.

  If Arrow Table construction fails due to a value type in the dictionary not
  being supported by Arrow, then Datasets will fall back to a simple dataset containing
  Python list blocks.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_dict_out_row_udfs_begin__
    :end-before: __writing_dict_out_row_udfs_end__

.. tabbed:: PandasRow

  Tabular dataset containing
  `Pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__
  blocks.

  A ``PandasRow`` object is a zero-copy row view on an underlying Pandas DataFrame block
  that Datasets provides to per-row UDFs (:meth:`ds.map() <ray.data.Dataset.map>`) and
  returns in the row iterators (:meth:`ds.iter_rows <ray.data.Dataset.iter_rows>`).
  This row view provides a dictionary interface and can be transparently used as a
  dictionary. See the :class:`TableRow API <ray.data.row.TableRow>` for more information
  on this row view object.

  Note that a ``PandasRow`` is immmutable, so this row mapping cannot be updated
  in-place. If wanting to update the row, copy this zero-copy row view into a plain
  Python dictionary with :meth:`TableRow.as_pydict() <ray.data.row.TableRow.as_pydict>`
  and then mutate and return that dictionary.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_table_row_out_row_udfs_begin__
    :end-before: __writing_table_row_out_row_udfs_end__

.. tabbed:: ArrowRow

  Tabular dataset containing
  `pyarrow.Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  blocks.

  An ``ArrowRow`` object is a zero-copy row view on an underlying Arrow Table block
  that Datasets provides to per-row UDFs (``ds.map()``) and returns in the row
  that Datasets provides to per-row UDFs (:meth:`ds.map() <ray.data.Dataset.map>`) and
  returns in the row iterators (:meth:`ds.iter_rows <ray.data.Dataset.iter_rows>`).
  This row view provides a dictionary interface and can be transparently used as a
  dictionary. See the :class:`TableRow API <ray.data.row.TableRow>` for more information
  on this row view object.

  Note that an ``ArrowRow`` is immmutable, so this row mapping cannot be updated
  in-place. If wanting to update the row, copy this zero-copy row view into a plain
  Python dictionary with :meth:`TableRow.as_pydict() <ray.data.row.TableRow.as_pydict>`
  and then mutate and return that dictionary.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_table_row_out_row_udfs_begin__
    :end-before: __writing_table_row_out_row_udfs_end__

.. tabbed:: np.ndarray

  Tensor dataset containing
  `pyarrow.Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  blocks, representing the tensor using our
  :ref:`tensor extension type <dataset-tensor-extension-api>` to embed the tensor in
  these tables under a single ``"__value__"`` column. Each such ``ndarray`` will be
  treated as a row in this column.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_numpy_out_row_udfs_begin__
    :end-before: __writing_numpy_out_row_udfs_end__

.. tabbed:: Any

  All other return row types will result in a simple dataset containing list blocks.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_simple_out_row_udfs_begin__
    :end-before: __writing_simple_out_row_udfs_end__

.. _transform_datasets_compute_strategy:

----------------
Compute Strategy
----------------

Datasets transformations are executed by either :ref:`Ray tasks <ray-remote-functions>`
or :ref:`Ray actors <actor-guide>` across a Ray cluster. By default, Ray tasks are
used (with ``compute="tasks"``). For transformations that require expensive setup,
it's preferrable to use Ray actors, which are stateful and allow setup to be reused
for efficiency. You can specify ``compute=ray.data.ActorPoolStrategy(min, max)`` and
Ray will use an autoscaling actor pool of ``min`` to ``max`` actors to execute your
transforms. For a fixed-size actor pool, just specify ``ActorPoolStrategy(n, n)``.

The following is an example of using the Ray tasks and actors compute strategy
for batch inference:

.. literalinclude:: ./doc_code/transforming_datasets.py
   :language: python
   :start-after: __dataset_compute_strategy_begin__
   :end-before: __dataset_compute_strategy_end__
