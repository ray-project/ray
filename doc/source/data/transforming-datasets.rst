.. _transforming_datasets:

=====================
Transforming Datasets
=====================

Datasets transformations take in datasets and produce new datasets.
For example, *map* is a transformation that applies a user-defined function (UDF)
on each dataset record and returns a new dataset as the result. Datasets
transformations can be composed to express a chain of computations.

.. _transform_datasets_transformations:

---------------
Transformations
---------------

There are two main types of transformations:

* One-to-one: each input block will contribute to only one output
  block, such as :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`.
* All-to-all: input blocks can contribute to multiple output blocks,
  such as :meth:`ds.random_shuffle() <ray.data.Dataset.random_shuffle>`.

Here is a table listing some common transformations supported by Ray Datasets.

.. list-table:: Common Ray Datasets transformations.
   :header-rows: 1

   * - Transformation
     - Type
     - Description
   * - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
     - One-to-one
     - Apply a given function to batches of records of this dataset. 
   * - :meth:`ds.add_column() <ray.data.Dataset.add_column>`
     - One-to-one
     - Apply a given function to batches of records to create a new column.
   * - :meth:`ds.drop_columns() <ray.data.Dataset.add_column>`
     - One-to-one
     - Drop the given columns from the dataset.
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

Here are the basics that you need to know about UDFs:

* A UDF can be either a function, or if using the :ref:`actor compute strategy <transform_datasets_compute_strategy>`, a :ref:`callable class <transform_datasets_callable_classes>`.
* Select the UDF input :ref:`batch format <transform_datasets_batch_formats>` using the ``batch_format`` argument.
* The UDF output type determines the Dataset schema of the transformation result.

.. _transform_datasets_callable_classes:

Callable Class UDFs
===================

When using the actor compute strategy, per-row and per-batch UDFs can also be
*callable classes*, i.e. classes that implement the ``__call__`` magic method. The
constructor of the class can be used for stateful setup, and will be only invoked once
per worker actor.

.. note::
  These transformation APIs take the uninstantiated callable class as an argument,
  not an instance of the class.

.. literalinclude:: ./doc_code/transforming_datasets.py
   :language: python
   :start-after: __writing_callable_classes_udfs_begin__
   :end-before: __writing_callable_classes_udfs_end__

.. _transform_datasets_batch_formats:

UDF Input Batch Format
======================

Choose the *batch format* of the data given to UDFs
by setting the ``batch_format`` option of :meth:`.map_batches() <ray.data.Dataset.map_batches>`.
Here is an overview of the available batch formats:

.. tabbed:: "native" (default)

  The "native" batch format presents data as follows for each Dataset type:

  * **Tabular Datasets**: Each batch will be a
    `pandas.DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__.
    This may incur a conversion cost if the underlying Dataset block is not
    zero-copy convertible from an Arrow table.

  * **Tensor Datasets** (single-column): Each batch will be a single
    `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__
    containing the single tensor column for this batch.

  * **Simple Datasets**: Each batch will be a Python list.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_native_udfs_begin__
    :end-before: __writing_native_udfs_end__

.. tabbed:: "pandas"

  The ``"pandas"`` batch format presents batches in
  `pandas.DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__
  format. If converting a simple dataset to Pandas DataFrame batches, a single-column
  dataframe with the column ``"__value__"`` will be created.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_pandas_udfs_begin__
    :end-before: __writing_pandas_udfs_end__

.. tabbed:: "pyarrow"

  The ``"pyarrow"`` batch format presents batches in
  `pyarrow.Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  format. If converting a simple dataset to Arrow Table batches, a single-column table
  with the column ``"__value__"`` will be created.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_arrow_udfs_begin__
    :end-before: __writing_arrow_udfs_end__

.. tabbed:: "numpy"

  The ``"numpy"`` batch format presents batches in
  `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__
  format as follows:

  * **Tabular Datasets**: Each batch will be a dictionary of NumPy
    ndarrays (``Dict[str, np.ndarray]``), with each key-value pair representing a column
    in the table.

  * **Tensor Datasets** (single-column): Each batch will be a single
    `numpy.ndarray <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.html>`__
    containing the single tensor column for this batch.

  * **Simple Datasets**: Each batch will be a single NumPy ndarray, where Datasets will
    attempt to convert each list-batch to an ndarray.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_numpy_udfs_begin__
    :end-before: __writing_numpy_udfs_end__

.. tip::

   Prefer using vectorized operations on the ``pandas.DataFrame``,
   ``pyarrow.Table``, and ``numpy.ndarray`` types for better performance. For
   example, suppose you want to compute the sum of a column in ``pandas.DataFrame``:
   instead of iterating over each row of a batch and summing up values of that column,
   use ``df_batch["col_foo"].sum()``.

.. _transform_datasets_batch_output_types:

Batch UDF Output Types
======================

The following output types are allowed for batch UDFs (e.g.,
:meth:`ds.map_batches() <ray.data.Dataset.map_batches>`). The following describes
how they are interpreted to create the transformation result:

.. tabbed:: pd.DataFrame

  Returning ``pd.DataFrame`` creates a Tabular dataset as the transformation result:

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_pandas_out_udfs_begin__
    :end-before: __writing_pandas_out_udfs_end__

.. tabbed:: pa.Table

  Returning ``pa.Table`` creates a Tabular dataset as the transformation result:

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_arrow_out_udfs_begin__
    :end-before: __writing_arrow_out_udfs_end__

.. tabbed:: np.ndarray

  Returning ``np.ndarray`` creates a single-column Tensor dataset as the transformation result:

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_numpy_out_udfs_begin__
    :end-before: __writing_numpy_out_udfs_end__

.. tabbed:: Dict[str, np.ndarray]

  Returning ``Dict[str, np.ndarray]`` creates a multi-column Tensor dataset as the transformation result.

  If a column tensor is 1-dimensional, then the native Arrow 1D list
  type is used; if a column tensor has 2 or more dimensions, then the Dataset
  :ref:`tensor extension type <dataset-tensor-extension-api>` to embed these
  n-dimensional tensors in the Arrow table.

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_numpy_dict_out_udfs_begin__
    :end-before: __writing_numpy_dict_out_udfs_end__

.. tabbed:: list

  Returning ``list`` creates a simple Python object dataset as the transformation result:

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_simple_out_udfs_begin__
    :end-before: __writing_simple_out_udfs_end__

.. _transform_datasets_row_output_types:

Row UDF Output Types
====================

The following output types are allowed for per-row UDFs (e.g.,
:meth:`ds.map() <ray.data.Dataset.map>`):

.. tabbed:: dict

  Returning a ``dict`` of Arrow-compatible data types creates a Tabular dataset
  as the transformation result. If any dict values are not Arrow-compatible, then
  a simple Python object dataset will be created:

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_dict_out_row_udfs_begin__
    :end-before: __writing_dict_out_row_udfs_end__

.. tabbed:: np.ndarray

  Returning ``np.ndarray`` creates a single-column Tensor dataset as the transformation result:

  .. literalinclude:: ./doc_code/transforming_datasets.py
    :language: python
    :start-after: __writing_numpy_out_row_udfs_begin__
    :end-before: __writing_numpy_out_row_udfs_end__

.. tabbed:: object

  Other return row types will create a simple Python object dataset as the transformation result:

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
transforms. For a fixed-size actor pool, specify ``ActorPoolStrategy(n, n)``.

The following is an example of using the Ray tasks and actors compute strategy
for batch inference:

.. literalinclude:: ./doc_code/transforming_datasets.py
   :language: python
   :start-after: __dataset_compute_strategy_begin__
   :end-before: __dataset_compute_strategy_end__
