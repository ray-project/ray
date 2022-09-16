.. _datasets_tensor_support:

ML Tensor Support
=================

Tensor (multi-dimensional array) data is ubiquitous in ML workloads. However, popular data formats such as Pandas, Parquet, and Arrow don't natively support tensor data types. To bridge this gap, Datasets provides a unified tensor data type that can be used to represent and store tensor data:

* For Pandas, Datasets will transparently convert ``List[np.ndarray]`` columns to and from the :class:`TensorDtype <ray.data.extensions.tensor_extension.TensorDtype>` extension type.
* For Parquet, the Datasets Arrow extension :class:`ArrowTensorType <ray.data.extensions.tensor_extension.ArrowTensorType>` allows Tensors to be loaded and stored in Parquet format.
* In addition, single-column Tensor datasets can be created from NumPy (.npy) files.

Datasets automatically converts between the extension types/arrays above. This means you can just think of "Tensors" as a single first-class data type in Datasets.

Creating Tensor Datasets
------------------------

This section shows how to create single and multi-column Tensor datasets.

.. tabbed:: Synthetic Data

  Create a synthetic tensor dataset from a range of integers.

  **Single-column only**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __create_range_begin__
    :end-before: __create_range_end__

.. tabbed:: Pandas UDF

  Create tensor datasets by returning ``List[np.ndarray]`` columns from a Pandas
  :ref:`user-defined function <transform_datasets_writing_udfs>`.

  **Single-column**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __create_pandas_begin__
    :end-before: __create_pandas_end__

  **Multi-column**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __create_pandas_2_begin__
    :end-before: __create_pandas_2_end__

.. tabbed:: NumPy

  Create from in-memory numpy data or from previously saved NumPy (.npy) files.

  **Single-column only**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __create_numpy_begin__
    :end-before: __create_numpy_end__

.. tabbed:: Parquet

  There are two ways to construct a parquet Tensor dataset: (1) loading a
  previously-saved Tensor dataset, or (2) casting non-Tensor parquet columns to Tensor
  type. When casting data, a tensor schema or deserialization
  :ref:`user-defined function <transform_datasets_writing_udfs>`  must be provided. The
  following are examples for each method.

  **Previously-saved Tensor datasets**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __create_parquet_1_begin__
    :end-before: __create_parquet_1_end__

  **Cast from data stored in C-contiguous format**:

  For tensors stored as raw NumPy ndarray bytes in C-contiguous order (e.g., via ``ndarray.tobytes()``), all you need to specify is the tensor column schema. The following is an end-to-end example:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __create_parquet_2_begin__
    :end-before: __create_parquet_2_end__

  **Cast from data stored in custom formats**:

  For tensors stored in other formats (e.g., pickled), you can specify a deserializer
  :ref:`user-defined function <transform_datasets_writing_udfs>` that returns
  TensorArray columns:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __create_parquet_3_begin__
    :end-before: __create_parquet_3_end__

.. tabbed:: Images (experimental)

  Load image data stored as individual files using :func:`~ray.data.read_images`:

  **Image and label columns**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __create_images_begin__
    :end-before: __create_images_end__

.. note::

  By convention, single-column Tensor datasets are represented with a single ``__value__`` column.
  This kind of dataset will be converted automatically to/from NumPy array format in all transformation and consumption APIs.

Transforming / Consuming Tensor Data
------------------------------------

Like any other Dataset, Datasets with tensor columns can be consumed / transformed in batches via the :meth:`ds.iter_batches(batch_format=\<format\>) <ray.data.Dataset.iter_batches>` and :meth:`ds.map_batches(fn, batch_format=\<format\>) <ray.data.Dataset.map_batches>` APIs. This section shows the available batch formats and their behavior:

.. tabbed:: "default"

  **Single-column**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __consume_native_begin__
    :end-before: __consume_native_end__

  **Multi-column**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __consume_native_2_begin__
    :end-before: __consume_native_2_end__

.. tabbed:: "pandas"

  **Single-column**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __consume_pandas_begin__
    :end-before: __consume_pandas_end__

  **Multi-column**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __consume_pandas_2_begin__
    :end-before: __consume_pandas_2_end__

.. tabbed:: "pyarrow"

  **Single-column**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __consume_pyarrow_begin__
    :end-before: __consume_pyarrow_end__

  **Multi-column**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __consume_pyarrow_2_begin__
    :end-before: __consume_pyarrow_2_end__

.. tabbed:: "numpy"

  **Single-column**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __consume_numpy_begin__
    :end-before: __consume_numpy_end__

  **Multi-column**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __consume_numpy_2_begin__
    :end-before: __consume_numpy_2_end__

Saving Tensor Datasets
----------------------

Because Tensor datasets rely on Datasets-specific extension types, they can only be saved in formats that preserve Arrow metadata (currently only Parquet). In addition, single-column Tensor datasets can be saved in NumPy format.

.. tabbed:: Parquet

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __write_1_begin_
    :end-before: __write_1_end__

.. tabbed:: NumPy

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __write_2_begin_
    :end-before: __write_2_end__

.. _disable_tensor_extension_casting:

Disabling Tensor Extension Casting
----------------------------------

To disable automatic casting of Pandas and Arrow arrays to
:class:`TensorArray <ray.data.extensions.tensor_extension.TensorArray>`, run the code
below.

.. code-block::

    from ray.data.context import DatasetContext

    ctx = DatasetContext.get_current()
    ctx.enable_tensor_extension_casting = False

Limitations
-----------

The following are current limitations of Tensor datasets.

* All tensors in a tensor column must have the same shape; see GitHub issue `#18316 <https://github.com/ray-project/ray/issues/18316>`__. An error will be raised in the ragged tensor case. Automatic casting can be disabled with ``ray.data.context.DatasetContext.get_current().enable_tensor_extension_cast = False`` in the ragged tensor scenario.
