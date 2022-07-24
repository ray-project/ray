.. _datasets_tensor_support:

Working with Tensors
====================

Tensor data (multi-dimensional arrays) are ubiquitous in ML workloads. However, popular data formats such as Pandas, Parquet, and Arrow don't natively support Tensor data types. To bridge this gap, Datasets provides a unified Tensor data type that can be used to represent and store Tensor data:

* For Pandas, the Datasets Pandas extension :class:`TensorDtype <ray.data.extensions.tensor_extension.TensorDtype>` and :class:`TensorArray <ray.data.extensions.tensor_extension.TensorArray>` enable Pandas-native manipulation of tensor data columns.
* For Parquet, the Datasets Arrow extension :class:`ArrowTensorType <ray.data.extensions.tensor_extension.ArrowTensorType>` and :class:`ArrowTensorArray <ray.data.extensions.tensor_extension.ArrowTensorArray>` allow Tensors to be loaded and stored in Parquet format.
* In addition, single-column Tensor datasets can be created from image and Numpy (.npy) files.

Datasets automatically converts between the Pandas and Arrow extension types/arrays above. This means you can just think of "Tensors" as a single first-class data type in Datasets.

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

  Create a tensor dataset by returning ``TensorArray`` columns from a Pandas UDF.

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

.. tabbed:: Numpy

  Create from in-memory numpy data or from previously saved Numpy (.npy) files.

  **Single-column only**:

  .. literalinclude:: ./doc_code/tensor.py
    :language: python
    :start-after: __create_numpy_begin__
    :end-before: __create_numpy_end__

.. tabbed:: Parquet

  There are two ways to construct a parquet Tensor dataset: (1) loading a previously-saved Tensor
  dataset, or (2) casting non-Tensor parquet columns to Tensor type. When casting data, a tensor
  schema and (optional) deserialization UDF must be provided. The following are examples for each method.

  **Previously-saved Tensor datasets**:

  .. code-block:: python

      import ray

      # Reading previously saved Tensor data works out of the box.
      ray.data.read_parquet("example://parquet_images_mini")
      # -> Dataset(num_blocks=3, num_rows=3, schema={image: TensorDtype, label: object})

      ds.take(1)
      # -> [{'image':
      #         array([[[ 92,  71,  57],
      #                 [107,  87,  72],
      #                 ...,
      #                 [141, 161, 185],
      #                 [139, 158, 184]],
      #                
      #                ...,
      #                
      #                [[135, 135, 109],
      #                 [135, 135, 108],
      #                 ...,
      #                 [167, 150,  89],
      #                 [165, 146,  90]]], dtype=uint8),
      #      'label': 'cat',
      #     }]

  **Cast from data stored in C-contiguous format**:

  For tensors stored as raw NumPy ndarray bytes in C-contiguous order (e.g., via ``ndarray.tobytes()``), all you need to specify is the tensor column schema. The following is an end-to-end example:

  .. code-block:: python

      import ray
      import numpy as np
      import pandas as pd

      path = "/tmp/some_path"

      # Create a DataFrame with a list of serialized ndarrays as a column.
      # Note that we do not cast it to a tensor array, so each element in the
      # column is an opaque blob of bytes.
      arr = np.arange(24).reshape((3, 2, 2, 2))
      df = pd.DataFrame({
          "one": [1, 2, 3],
          "two": [tensor.tobytes() for tensor in arr]})

      # Write the dataset to Parquet. The tensor column will be written as an
      # array of opaque byte blobs.
      ds = ray.data.from_pandas([df])
      ds.write_parquet(path)

      # Read the Parquet files into a new Dataset, with the serialized tensors
      # automatically cast to our tensor column extension type.
      ds = ray.data.read_parquet(
          path, tensor_column_schema={"two": (np.int, (2, 2, 2))})

      # The new column is represented with as a Tensor extension type.
      print(ds.schema())
      # -> one: int64
      #    two: extension<arrow.py_extension_type<ArrowTensorType>>

  **Cast from data stored in custom formats**:

  For tensors stored in other formats (e.g., pickled), you must specify both a deserializer UDF and the tensor column schema:

  .. code-block:: python

      import pickle
      import pyarrow as pa
      from ray.data.extensions import TensorArray

      # Create a DataFrame with a list of pickled ndarrays as a column.
      arr = np.arange(24).reshape((3, 2, 2, 2))
      df = pd.DataFrame({
          "one": [1, 2, 3],
          "two": [pickle.dumps(tensor) for tensor in arr]})

      # Write the dataset to Parquet. The tensor column will be written as an
      # array of opaque byte blobs.
      ds = ray.data.from_pandas([df])
      ds.write_parquet(path)

      # Manually deserialize the tensor pickle bytes and cast to our tensor
      # extension type. For the sake of efficiency, we directly construct a
      # TensorArray rather than .astype() casting on the mutated column with
      # TensorDtype.
      def cast_udf(block: pa.Table) -> pa.Table:
          block = block.to_pandas()
          block["two"] = TensorArray([pickle.loads(a) for a in block["two"]])
          return pa.Table.from_pandas(block)

      # Read the Parquet files into a new Dataset, applying the casting UDF
      # on-the-fly within the underlying read tasks.
      ds = ray.data.read_parquet(path, _block_udf=cast_udf)

      # The new column is represented with as a Tensor extension type.
      print(ds.schema())
      # -> one: int64
      #    two: extension<arrow.py_extension_type<ArrowTensorType>>

.. tabbed:: Images (experimental)

  Load image data stored as individual files using ``ImageFolderDatasource()``.

  **Image and label columns**:

  .. code-block:: python

      ray.data.read_datasource(ImageFolderDatasource(), paths=["example://image-folder"])
      # -> Dataset(num_blocks=3, num_rows=3, schema={image: TensorDtype, label: object})

      ds.take(1)
      # -> [{'image':
      #         array([[[ 92,  71,  57],
      #                 [107,  87,  72],
      #                 ...,
      #                 [141, 161, 185],
      #                 [139, 158, 184]],
      #                
      #                ...,
      #                
      #                [[135, 135, 109],
      #                 [135, 135, 108],
      #                 ...,
      #                 [167, 150,  89],
      #                 [165, 146,  90]]], dtype=uint8),
      #      'label': 'cat',
      #     }]

.. note::

  By convention, single-column Tensor datasets are represented with a single ``__value__`` column.
  This kind of dataset can be converted directly to/from Numpy array format.


Consuming Tensor Datasets
-------------------------

Like any other Dataset, Datasets with Tensor columns can be consumed / transformed in batches via the ``.iter_batches(batch_format=<format>)`` and ``.map_batches(batch_fn, batch_format=<format>)`` APIs. This section shows the available batch formats and their behavior:

.. tabbed:: "native" (default)

  **Single-column**:

  .. code-block:: python

    import ray

    # Read a single-column example dataset.
    ds = ray.data.read_numpy("example://mnist_subset.npy")
    # -> Dataset(num_blocks=1, num_rows=3,
    #            schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=uint8>})

    # This returns batches in numpy.ndarray format.
    next(ds.iter_batches())
    # -> array([[[0, 0, 0, ..., 0, 0, 0],
    #            [0, 0, 0, ..., 0, 0, 0],
    #            ...,
    #            [0, 0, 0, ..., 0, 0, 0],
    #            [0, 0, 0, ..., 0, 0, 0]],
    #
    #           ...,
    #
    #           [[0, 0, 0, ..., 0, 0, 0],
    #            [0, 0, 0, ..., 0, 0, 0],
    #            ...,
    #            [0, 0, 0, ..., 0, 0, 0],
    #            [0, 0, 0, ..., 0, 0, 0]]], dtype=uint8)

  **Multi-column**:

    Coming soon.

  ..
    #TODO(ekl) why does this crash with TensorDType not understood?

    # Read a multi-column example dataset.
    ray.data.read_parquet("example://parquet_images_mini")
    # -> Dataset(num_blocks=3, num_rows=3, schema={image: TensorDtype, label: object})

    next(ds.iter_batches())
    # -> TypeError: data type 'TensorDtype' not understood

.. tabbed:: "pandas"

  **Single-column**:

  .. code-block:: python

    import ray

    # Read a single-column example dataset.
    ds = ray.data.read_numpy("example://mnist_subset.npy")
    # -> Dataset(num_blocks=1, num_rows=3,
    #            schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=uint8>})

    # This returns batches in pandas.DataFrame format.
    next(ds.iter_batches(batch_format="pandas"))
    # ->                                            __value__
    # 0  [[  0,   0,   0,   0,   0,   0,   0,   0,   0,...
    # 1  [[  0,   0,   0,   0,   0,   0,   0,   0,   0,...
    # 2  [[  0,   0,   0,   0,   0,   0,   0,   0,   0,...

  **Multi-column**:

    Coming soon.

  ..
    #TODO(ekl) why does this crash with TensorDType not understood?

    # Read a multi-column example dataset.
    ray.data.read_parquet("example://parquet_images_mini")
    # -> Dataset(num_blocks=3, num_rows=3, schema={image: TensorDtype, label: object})

    next(ds.iter_batches(batch_format="pandas"))
    # -> TypeError: data type 'TensorDtype' not understood

.. tabbed:: "pyarrow"

  **Single-column**:

  .. code-block:: python

    import ray

    # Read a single-column example dataset.
    ds = ray.data.read_numpy("example://mnist_subset.npy")
    # -> Dataset(num_blocks=1, num_rows=3,
    #            schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=uint8>})

    # This returns batches in pyarrow.Table format.
    next(ds.iter_batches(batch_format="pyarrow"))
    # pyarrow.Table
    # __value__: extension<arrow.py_extension_type<ArrowTensorType>>
    # ----
    # __value__: [[[0,0,0,0,0,0,0,0,0,0,...],...,[0,0,0,0,0,0,0,0,0,0,...]]]

  **Multi-column**:

  .. code-block:: python

    # Read a multi-column example dataset.
    ray.data.read_parquet("example://parquet_images_mini")
    # -> Dataset(num_blocks=3, num_rows=3, schema={image: TensorDtype, label: object})

    # This returns batches in pyarrow.Table format.
    next(ds.iter_batches(batch_format="pyarrow"))
    # pyarrow.Table
    # image: extension<arrow.py_extension_type<ArrowTensorType>>
    # label: string
    # ----
    # image: [[[92,71,57,107,87,72,113,97,85,122,...,85,170,152,88,167,150,89,165,146,90]]]
    # label: [["cat"]]

.. tabbed:: "numpy"

  **Single-column**:

  .. code-block:: python

    import ray

    # Read a single-column example dataset.
    ds = ray.data.read_numpy("example://mnist_subset.npy")
    # -> Dataset(num_blocks=1, num_rows=3,
    #            schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=uint8>})

    # This returns batches in np.ndarray format.
    next(ds.iter_batches(batch_format="numpy"))
    # -> array([[[0, 0, 0, ..., 0, 0, 0],
    #            [0, 0, 0, ..., 0, 0, 0],
    #            ...,
    #            [0, 0, 0, ..., 0, 0, 0],
    #            [0, 0, 0, ..., 0, 0, 0]],
    #
    #           ...,
    #
    #           [[0, 0, 0, ..., 0, 0, 0],
    #            [0, 0, 0, ..., 0, 0, 0],
    #            ...,
    #            [0, 0, 0, ..., 0, 0, 0],
    #            [0, 0, 0, ..., 0, 0, 0]]], dtype=uint8)

  **Multi-column**:

  .. code-block:: python

    # Read a multi-column example dataset.
    ray.data.read_parquet("example://parquet_images_mini")
    # -> Dataset(num_blocks=3, num_rows=3, schema={image: TensorDtype, label: object})

    # This returns batches in Dict[str, np.ndarray] format.
    next(ds.iter_batches(batch_format="numpy"))
    # -> {'image': array([[[[ 92,  71,  57],
    #                       [107,  87,  72],
    #                       ...,
    #                       [141, 161, 185],
    #                       [139, 158, 184]],
    #
    #                      ...,
    #
    #                      [[135, 135, 109],
    #                       [135, 135, 108],
    #                       ...,
    #                       [167, 150,  89],
    #                       [165, 146,  90]]]], dtype=uint8),
    #     'label': array(['cat'], dtype=object)}

Saving Tensor Datasets
~~~~~~~~~~~~~~~~~~~~~~

Because Tensor datasets rely on Dataset-specific extension types, they can only be saved in formats that preserve Arrow metadata (currently only Parquet). In addition, single-column Tensor datasets can be saved in Numpy format.

.. tabbed:: Parquet

  .. code-block:: python

      # Read a multi-column example dataset.
      ds = ray.data.read_parquet("example://parquet_images_mini")
      # -> Dataset(num_blocks=3, num_rows=3, schema={image: TensorDtype, label: object})

      # You can write the dataset to Parquet.
      ds.write_parquet("/tmp/some_path")

      # And you can read it back.
      read_ds = ray.data.read_parquet("/tmp/some_path")
      print(read_ds.schema())
      # -> image: extension<arrow.py_extension_type<ArrowTensorType>>
      #    label: string

.. tabbed:: Numpy

  .. code-block:: python

      # Read a single-column example dataset.
      ds = ray.data.read_numpy("example://mnist_subset.npy")
      # -> Dataset(num_blocks=1, num_rows=3,
      #            schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=uint8>})

      # You can write the dataset to Parquet.
      ds.write_numpy("/tmp/some_path")

      # And you can read it back.
      read_ds = ray.data.read_numpy("/tmp/some_path")
      print(read_ds.schema())
      # -> __value__: extension<arrow.py_extension_type<ArrowTensorType>>

Example: Working with the Pandas extension type
-----------------------------------------------

This example shows how to work with the Pandas extension type directly.

.. code-block:: python

    from ray.data.extensions import TensorDtype

    # Create a DataFrame with a list of ndarrays as a column.
    df = pd.DataFrame({
        "one": [1, 2, 3],
        "two": list(np.arange(24).reshape((3, 2, 2, 2)))})
    # Note the opaque np.object dtype for this column.
    print(df.dtypes)
    # -> one     int64
    #    two    object
    #    dtype: object

    # Cast column to our TensorDtype Pandas extension type.
    df["two"] = df["two"].astype(TensorDtype())

    # Note that the column dtype is now TensorDtype instead of
    # np.object.
    print(df.dtypes)
    # -> one          int64
    #    two    TensorDtype
    #    dtype: object

    # Pandas is now aware of this tensor column, and we can do the
    # typical DataFrame operations on this column.
    col = 2 * df["two"]
    # The ndarrays underlying the tensor column will be manipulated,
    # but the column itself will continue to be a Pandas type.
    print(type(col))
    # -> pandas.core.series.Series
    print(col)
    # -> 0   [[[ 2  4]
    #          [ 6  8]]
    #         [[10 12]
    #           [14 16]]]
    #    1   [[[18 20]
    #          [22 24]]
    #         [[26 28]
    #          [30 32]]]
    #    2   [[[34 36]
    #          [38 40]]
    #         [[42 44]
    #          [46 48]]]
    #    Name: two, dtype: TensorDtype

    # Once you do an aggregation on that column that returns a single
    # row's value, you get back our TensorArrayElement type.
    tensor = col.mean()
    print(type(tensor))
    # -> ray.data.extensions.tensor_extension.TensorArrayElement
    print(tensor)
    # -> array([[[18., 20.],
    #            [22., 24.]],
    #           [[26., 28.],
    #            [30., 32.]]])

    # This is a light wrapper around a NumPy ndarray, and can easily
    # be converted to an ndarray.
    type(tensor.to_numpy())
    # -> numpy.ndarray

    # In addition to doing Pandas operations on the tensor column,
    # you can now put the DataFrame directly into a Dataset.
    ds = ray.data.from_pandas([df])
    # Internally, this column is represented with the corresponding
    # Arrow tensor extension type.
    print(ds.schema())
    # -> one: int64
    #    two: extension<arrow.py_extension_type<ArrowTensorType>>

Limitations
-----------

The following are current limitations of Tensor datasets.

 * All tensors in a tensor column must have the same shape; see GitHub issue `#18316 <https://github.com/ray-project/ray/issues/18316>`__.
