.. _datasets_tensor_support:

Working with Tensors
====================

Tensor data (multi-dimensional arrays) are ubiquitous in ML workloads. However, popular data formats such as Pandas, Parquet, and Arrow don't natively support Tensor data types. To bridge this gap, Datasets provides a unified Tensor data type that can be used to represent and store Tensor data:

* For Pandas, the Datasets Pandas extension :class:`TensorDtype <ray.data.extensions.tensor_extension.TensorDtype>` and :class:`TensorArray <ray.data.extensions.tensor_extension.TensorArray>` enable Pandas-native manipulation of tensor data columns.
* For Parquet, the Datasets Arrow extension :class:`ArrowTensorType <ray.data.extensions.tensor_extension.ArrowTensorType>` and :class:`ArrowTensorArray <ray.data.extensions.tensor_extension.ArrowTensorArray>` allow Tensors to be loaded and stored in Parquet format.
* In addition, single-column Tensor datasets can be created from image and Numpy (.npy) files.

Datasets automatically converts between the Pandas and Arrow extension types/arrays above. This means you can just think of "Tensors" as a single first-class data type in Datasets.

Creating Tensor Columns
-----------------------

This section shows how to create single and multi-column Tensor datasets.

.. tabbed:: Synthetic Data

  Create a synthetic tensor dataset from a range of integers.

  **Single-column only**:

  .. code-block:: python

      # Create a Dataset of tensors.
      ds = ray.data.range_tensor(100 * 64 * 64, shape=(64, 64))
      # -> Dataset(
      #       num_blocks=200,
      #       num_rows=409600,
      #       schema={value: <ArrowTensorType: shape=(64, 64), dtype=int64>}
      #    )
      
      ds.take(2)
      # -> [array([[0, 0, 0, ..., 0, 0, 0],
      #         [0, 0, 0, ..., 0, 0, 0],
      #         [0, 0, 0, ..., 0, 0, 0],
      #         ...,
      #         [0, 0, 0, ..., 0, 0, 0],
      #         [0, 0, 0, ..., 0, 0, 0],
      #         [0, 0, 0, ..., 0, 0, 0]]),
      #  array([[1, 1, 1, ..., 1, 1, 1],
      #         [1, 1, 1, ..., 1, 1, 1],
      #         [1, 1, 1, ..., 1, 1, 1],
      #         ...,
      #         [1, 1, 1, ..., 1, 1, 1],
      #         [1, 1, 1, ..., 1, 1, 1],
      #         [1, 1, 1, ..., 1, 1, 1]])]

.. tabbed:: Pandas UDF

  Create a tensor dataset by returning ``TensorArray`` columns from a Pandas UDF.

  **Single-column**:

  .. code-block:: python

      import ray
      from ray.data.extensions.tensor_extension import TensorArray

      import pandas as pd

      # Start with a tabular base dataset.
      ds = ray.data.range_table(1000)

      # Create a single TensorArray column.
      def single_col_udf(batch: pd.DataFrame) -> pd.DataFrame:
          bs = len(batch)
          arr = TensorArray(np.zeros((bs, 128, 128, 3), dtype=np.int64))
          return pd.DataFrame({"__value__": arr})

      ds.map_batches(single_col_udf)
      # -> Dataset(num_blocks=17, num_rows=1000, schema={__value__: TensorDtype})

  **Multi-column**:

  .. code-block:: python

      # Create multiple TensorArray columns.
      def multi_col_udf(batch: pd.DataFrame) -> pd.DataFrame:
          bs = len(batch)
          image = TensorArray(np.zeros((bs, 128, 128, 3), dtype=np.int64))
          embed = TensorArray(np.zeros((bs, 256,), dtype=np.uint8))
          return pd.DataFrame({"image": image, "embed": embed})

      ds.map_batches(multi_col_udf)
      # -> Dataset(num_blocks=17, num_rows=1000, schema={image: TensorDtype, embed: TensorDtype})

.. tabbed:: Numpy

  Create from in-memory numpy data or from previously saved Numpy (.npy) files.

  **Single-column only**:

  .. code-block:: python

    import ray

    # From in-memory numpy data.
    ray.data.from_numpy(np.zeros((1000, 128, 128, 3), dtype=np.int64))
    # -> Dataset(num_blocks=1, num_rows=1000,
    #            schema={__value__: <ArrowTensorType: shape=(128, 128, 3), dtype=int64>})

    # From saved numpy files.
    ray.data.read_numpy("example://mnist_subset.npy")
    # -> Dataset(num_blocks=1, num_rows=3,
    #            schema={__value__: <ArrowTensorType: shape=(28, 28), dtype=uint8>})

.. tabbed:: Images

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

.. tabbed:: Parquet

  Datasets provides methods for loading both previously-saved Tensor datasets, and constructing
  Tensor datasets by casting binary / other columns. When casting data, the schema and (optional)
  deserialization UDF must be provided. The following are examples for each method.

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

Consuming Tensor Columns
------------------------

Map batch formats

.. tabbed:: "native" (default)

  TODO

.. tabbed:: "pandas"

  TODO

.. tabbed:: "pyarrow"

  TODO

.. tabbed:: "numpy"

  TODO

Iterator formats

Saving Tensor Columns
~~~~~~~~~~~~~~~~~~~~~

Write to numpy

Write to parquet

.. 
  TODO: REWRITE ALL BELOW
  TODO: REWRITE ALL BELOW
  TODO: REWRITE ALL BELOW
  TODO: REWRITE ALL BELOW
  TODO: REWRITE ALL BELOW
  TODO: REWRITE ALL BELOW
  TODO: REWRITE ALL BELOW
  TODO: REWRITE ALL BELOW
  TODO: REWRITE ALL BELOW

Single-column tensor datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most basic case is when a dataset only has a single column, which is of tensor
type. This kind of dataset can be:

* created with :func:`range_tensor() <ray.data.range_tensor>`
  or :func:`from_numpy() <ray.data.from_numpy>`,
* transformed with NumPy UDFs via
  :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`,
* consumed with :meth:`ds.iter_rows() <ray.data.Dataset.iter_rows>` and
  :meth:`ds.iter_batches() <ray.data.Dataset.iter_batches>`, and
* can be read from and written to ``.npy`` files.

Here is an end-to-end example:

.. code-block:: python

    # Create a synthetic pure-tensor Dataset.
    ds = ray.data.range_tensor(10, shape=(3, 5))
    # -> Dataset(num_blocks=10, num_rows=10,
    #            schema={__value__: <ArrowTensorType: shape=(3, 5), dtype=int64>})

    # Create a pure-tensor Dataset from an existing NumPy ndarray.
    arr = np.arange(10 * 3 * 5).reshape((10, 3, 5))
    ds = ray.data.from_numpy(arr)
    # -> Dataset(num_blocks=1, num_rows=10,
    #            schema={__value__: <ArrowTensorType: shape=(3, 5), dtype=int64>})

    # Transform the tensors. Datasets will automatically unpack the single-column Arrow
    # table into a NumPy ndarray, provide that ndarray to your UDF, and then repack it
    # into a single-column Arrow table; this will be a zero-copy conversion in both
    # cases.
    ds = ds.map_batches(lambda arr: arr / arr.max())
    # -> Dataset(num_blocks=1, num_rows=10,
    #            schema={__value__: <ArrowTensorType: shape=(3, 5), dtype=double>})

    # Consume the tensor. This will yield the underlying (3, 5) ndarrays.
    for arr in ds.iter_rows():
        assert isinstance(arr, np.ndarray)
        assert arr.shape == (3, 5)

    # Consume the tensor in batches.
    for arr in ds.iter_batches(batch_size=2):
        assert isinstance(arr, np.ndarray)
        assert arr.shape == (2, 3, 5)

    # Save to storage. This will write out the blocks of the tensor column as NPY files.
    ds.write_numpy("/tmp/tensor_out")

    # Read back from storage.
    ray.data.read_numpy("/tmp/tensor_out")
    # -> Dataset(num_blocks=1, num_rows=?,
    #            schema={__value__: <ArrowTensorType: shape=(3, 5), dtype=double>})

Working with tensor column datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Now that the tensor column is properly typed and in a ``Dataset``, we can perform operations on the dataset as if it was a normal table:

.. code-block:: python

    # Arrow and Pandas is now aware of this tensor column, so we can do the
    # typical DataFrame operations on this column.
    ds = ds.map_batches(lambda x: 2 * (x + 1), batch_format="pandas")
    # -> Map Progress: 100%|████████████████████| 200/200 [00:00<00:00, 1123.54it/s]
    print(ds)
    # -> Dataset(
    #        num_blocks=1, num_rows=3,
    #        schema=<class 'int',
    #            class ray.data.extensions.tensor_extension.ArrowTensorType>)
    print([row["two"] for row in ds.take(5)])
    # -> [2, 4, 6, 8, 10]

Writing and reading tensor columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This dataset can then be written to Parquet files. The tensor column schema will be preserved via the Pandas and Arrow extension types and associated metadata, allowing us to later read the Parquet files into a Dataset without needing to specify a column casting schema. This Pandas --> Arrow --> Parquet --> Arrow --> Pandas conversion support makes working with tensor columns extremely easy when using Ray Datasets to both write and read data.

.. code-block:: python

    # You can write the dataset to Parquet.
    ds.write_parquet("/some/path")
    # And you can read it back.
    read_ds = ray.data.read_parquet("/some/path")
    print(read_ds.schema())
    # -> one: int64
    #    two: extension<arrow.py_extension_type<ArrowTensorType>>

.. _datasets_tensor_ml_exchange:

Example: End-to-end workflow with the Pandas extension type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If working with in-memory Pandas DataFrames that you want to analyze, manipulate, store, and eventually read, the Pandas/Arrow extension types/arrays make it easy to extend this end-to-end workflow to tensor columns.

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

    # You can write the dataset to Parquet.
    ds.write_parquet("/some/path")
    # And you can read it back.
    read_ds = ray.data.read_parquet("/some/path")
    print(read_ds.schema())
    # -> one: int64
    #    two: extension<arrow.py_extension_type<ArrowTensorType>>

    read_df = read_ds.to_pandas()
    print(read_df.dtypes)
    # -> one          int64
    #    two    TensorDtype
    #    dtype: object

    # The tensor extension type is preserved along the
    # Pandas --> Arrow --> Parquet --> Arrow --> Pandas
    # conversion chain.
    print(read_df.equals(df))
    # -> True

Limitations
~~~~~~~~~~~

This feature currently comes with a few known limitations that we are either actively working on addressing or have already implemented workarounds for.

 * All tensors in a tensor column currently must be the same shape. Please let us know if you require heterogeneous tensor shape for your tensor column! Tracking issue is `here <https://github.com/ray-project/ray/issues/18316>`__.
 * Automatic casting via specifying an override Arrow schema when reading Parquet is blocked by Arrow supporting custom ExtensionType casting kernels. See `issue <https://issues.apache.org/jira/browse/ARROW-5890>`__. An explicit ``tensor_column_schema`` parameter has been added for :func:`read_parquet() <ray.data.read_api.read_parquet>` as a stopgap solution.
