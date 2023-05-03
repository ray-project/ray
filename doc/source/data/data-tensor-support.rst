.. _data_tensor_support:

ML Tensor Support
=================

Tensor (multi-dimensional array) data is ubiquitous in ML workloads. However, popular data formats such as Pandas, Parquet, and Arrow don't natively support tensor data types. To bridge this gap, Ray Data provides tensor extension types that integrate with Pandas and Arrow.

* For Pandas, Ray Data will transparently convert ``List[np.ndarray]`` columns to and from the :class:`TensorDtype <ray.data.extensions.tensor_extension.TensorDtype>` extension type.
* For Parquet, Ray Data has an Arrow extension :class:`ArrowTensorType <ray.data.extensions.tensor_extension.ArrowTensorType>` that allows tensors to be loaded from and stored in the Parquet format.

Ray Data automatically converts between the extension types/arrays above. This means you can think of a ``Tensor`` as a first-class data type in Ray Data.

Loading Tensor Data
-------------------

This section shows how to create datastreams that include tensor data.

.. tab-set::

    .. tab-item:: Synthetic Data

      Create synthetic tensor data from a range of integers.

      .. literalinclude:: ./doc_code/tensor.py
        :language: python
        :start-after: __create_range_begin__
        :end-before: __create_range_end__

    .. tab-item:: Images

      Load image data stored as individual files using :func:`~ray.data.read_images`:

      .. literalinclude:: ./doc_code/tensor.py
        :language: python
        :start-after: __create_images_begin__
        :end-before: __create_images_end__

    .. tab-item:: Pandas UDF

      Create tensor columns by returning ``List[np.ndarray]`` columns from a Pandas
      :ref:`user-defined function <transform_datastreams_writing_udfs>`.

      .. literalinclude:: ./doc_code/tensor.py
        :language: python
        :start-after: __create_pandas_2_begin__
        :end-before: __create_pandas_2_end__

    .. tab-item:: NumPy

      Create from in-memory NumPy data or previously saved NumPy (.npy) files.

      .. literalinclude:: ./doc_code/tensor.py
        :language: python
        :start-after: __create_numpy_begin__
        :end-before: __create_numpy_end__

    .. tab-item:: Parquet

      There are two ways to construct a Parquet tensor datastream: (1) loading a
      previously-saved tensor datastream, or (2) casting non-tensor Parquet columns to tensor
      type. When casting data, a tensor schema or deserialization
      :ref:`user-defined function <transform_datastreams_writing_udfs>`  must be provided. The
      following are examples for each method.

      **Previously-saved tensor datastreams**:

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
      :ref:`user-defined function <transform_datastreams_writing_udfs>` that returns
      :class:`~ray.data.extensions.tensor_extension.TensorArray` columns:

      .. literalinclude:: ./doc_code/tensor.py
        :language: python
        :start-after: __create_parquet_3_begin__
        :end-before: __create_parquet_3_end__

Processing Tensor Data
----------------------

Like any other Datastream, Datastreams with tensor columns can be processed in batches via :meth:`ds.iter_batches <ray.data.Datastream.iter_batches>` and :meth:`ds.map_batches <ray.data.Datastream.map_batches>` APIs. This section shows the available batch formats and their behavior:

.. tab-set::

    .. tab-item:: "numpy" (default)

      .. literalinclude:: ./doc_code/tensor.py
        :language: python
        :start-after: __consume_numpy_2_begin__
        :end-before: __consume_numpy_2_end__

    .. tab-item:: "pandas"

      .. literalinclude:: ./doc_code/tensor.py
        :language: python
        :start-after: __consume_pandas_2_begin__
        :end-before: __consume_pandas_2_end__

    .. tab-item:: "pyarrow"

      .. literalinclude:: ./doc_code/tensor.py
        :language: python
        :start-after: __consume_pyarrow_2_begin__
        :end-before: __consume_pyarrow_2_end__

Saving Tensor Data
------------------

Because tensor data relies on Datastream-specific extension types, they can only be
saved in formats that preserve Arrow metadata (currently only Parquet). In addition,
single-column tensor datastreams can be saved in NumPy format.

.. tab-set::

    .. tab-item:: Parquet

      .. literalinclude:: ./doc_code/tensor.py
        :language: python
        :start-after: __write_1_begin_
        :end-before: __write_1_end__

    .. tab-item:: NumPy

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
and audio ML (differing durations). Datastreams has basic support for ragged tensors,
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

    from ray.data import DataContext

    ctx = DataContext.get_current()
    ctx.enable_tensor_extension_casting = False


Limitations
-----------

The following are current limitations of tensor datastreams.

* Arbitrarily `nested/ragged tensors <https://www.tensorflow.org/guide/ragged_tensor>`__ are not supported. Only tensors with all uniform dimensions (i.e. a fully well-defined shape) and tensors representing a collection of variable-shaped tensor elements (e.g. a collection of images with different shapes) are supported; arbitrary raggedness and nested ragged tensors is not supported.
