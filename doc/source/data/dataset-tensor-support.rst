.. _datasets_tensor_support:

ML Tensor Support
=================

Tensor (multi-dimensional array) data is ubiquitous in ML workloads. However, popular data formats such as Pandas, Parquet, and Arrow don't natively support tensor data types. To bridge this gap, Datasets provides a unified tensor data type that can be used to represent, transform, and store tensor data:

* For Pandas, Datasets will transparently convert ``List[np.ndarray]`` columns to and from the :class:`TensorDtype <ray.data.extensions.tensor_extension.TensorDtype>` extension type.
* For Parquet, Datasets has an Arrow extension :class:`ArrowTensorType <ray.data.extensions.tensor_extension.ArrowTensorType>` that allows tensors to be loaded from and stored in the Parquet format.
* In addition, single-column tensor datasets can be created from NumPy (.npy) files.

Datasets automatically converts between the extension types/arrays above. This means you can think of a ``Tensor`` as a first-class data type in Datasets.

Creating Tensor Datasets
------------------------

This section shows how to create single and multi-column tensor datasets.

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

  Create from in-memory NumPy data or from previously saved NumPy (.npy) files.

  **Single-column only**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __create_numpy_begin__
    :end-before: __create_numpy_end__

.. tabbed:: Parquet

  There are two ways to construct a Parquet tensor dataset: (1) loading a
  previously-saved tensor dataset, or (2) casting non-tensor Parquet columns to tensor
  type. When casting data, a tensor schema or deserialization
  :ref:`user-defined function <transform_datasets_writing_udfs>`  must be provided. The
  following are examples for each method.

  **Previously-saved tensor datasets**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __create_parquet_1_begin__
    :end-before: __create_parquet_1_end__

  **Cast from data stored in C-contiguous format**:

  For tensors stored as raw NumPy ndarray bytes in C-contiguous order (e.g., via
  `ndarray.tobytes() <https://numpy.org/doc/stable/reference/generated/numpy.ndarray.tobytes.html>`__), all you need to specify is the tensor column schema. The following is an end-to-end example:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __create_parquet_2_begin__
    :end-before: __create_parquet_2_end__

  **Cast from data stored in custom formats**:

  For tensors stored in other formats (e.g., pickled), you can specify a deserializer
  :ref:`user-defined function <transform_datasets_writing_udfs>` that returns
  :class:`~ray.data.extensions.tensor_extension.TensorArray` columns:

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

  By convention, single-column tensor datasets are represented with a single ``__value__`` column.
  This kind of dataset will be converted automatically to/from NumPy ndarray format in all transformation and consumption APIs.

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

Because tensor datasets rely on Datasets-specific extension types, they can only be
saved in formats that preserve Arrow metadata (currently only Parquet). In addition,
single-column tensor datasets can be saved in NumPy format.

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

.. _ragged_tensor_support:

Ragged Tensor Support
---------------------

`Ragged tensors <https://www.tensorflow.org/guide/ragged_tensor>`__, i.e. tensors with non-uniform dimensions, pop up in NLP
(`textual sentences/documents of different lengths <https://towardsdatascience.com/using-tensorflow-ragged-tensors-2af07849a7bd>`__,
`N-grams <https://www.tensorflow.org/guide/ragged_tensor#example_use_case>`__),
computer vision (images of differing resolution,
`ssd300_vgg16 detection outputs <https://pytorch.org/vision/main/models/generated/torchvision.models.detection.ssd300_vgg16.html>`__),
and audio ML (differing durations). Datasets has basic support for ragged tensors,
namely tensors that are a collection (batch) of variably-shaped subtensors, e.g. a batch
of images of differing sizes or a batch of sentences of differing lengths.

.. literalinclude:: ./doc_code/tensor.py
  :language: python
  :start-after: __create_variable_shaped_tensors_begin__
  :end-before: __create_variable_shaped_tensors_end__

These variable-shaped tensors can be exchanged with popular training frameworks that support ragged tensors, such as `TensorFlow <https://www.tensorflow.org/guide/ragged_tensor#setup>`__.

.. literalinclude:: ./doc_code/tensor.py
  :language: python
  :start-after: __tf_variable_shaped_tensors_begin__
  :end-before: __tf_variable_shaped_tensors_end__

.. _disable_tensor_extension_casting:

Disabling Tensor Extension Casting
----------------------------------

To disable automatic casting of Pandas and Arrow arrays to
:class:`~ray.data.extensions.tensor_extension.TensorArray`, run the code
below.

.. code-block::

    from ray.data.context import DatasetContext

    ctx = DatasetContext.get_current()
    ctx.enable_tensor_extension_casting = False


Limitations
-----------

The following are current limitations of tensor datasets.

* Arbitrarily `nested/ragged tensors <https://www.tensorflow.org/guide/ragged_tensor>`__ are not supported. Only tensors with all uniform dimensions (i.e. a fully well-defined shape) and tensors representing a collection of variable-shaped tensor elements (e.g. a collection of images with different shapes) are supported; arbitrary raggedness and nested ragged tensors is not supported.
