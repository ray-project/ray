.. _datasets_tensor_support:

Working with Tensors
====================

Tables with tensor columns
~~~~~~~~~~~~~~~~~~~~~~~~~~

Datasets supports tables with fixed-shape tensor columns, where each element in the column is a tensor (n-dimensional array) with the same shape. As an example, this allows you to use Pandas and Ray Datasets to read, write, and manipulate e.g., images. All conversions between Pandas, Arrow, and Parquet, and all application of aggregations/operations to the underlying image ndarrays are taken care of by Ray Datasets.

With our Pandas extension type, :class:`TensorDtype <ray.data.extensions.tensor_extension.TensorDtype>`, and extension array, :class:`TensorArray <ray.data.extensions.tensor_extension.TensorArray>`, you can do familiar aggregations and arithmetic, comparison, and logical operations on a DataFrame containing a tensor column and the operations will be applied to the underlying tensors as expected. With our Arrow extension type, :class:`ArrowTensorType <ray.data.extensions.tensor_extension.ArrowTensorType>`, and extension array, :class:`ArrowTensorArray <ray.data.extensions.tensor_extension.ArrowTensorArray>`, you'll be able to import that DataFrame into Ray Datasets and read/write the data from/to the Parquet format.

Automatic conversion between the Pandas and Arrow extension types/arrays keeps the details under-the-hood, so you only have to worry about casting the column to a tensor column using our Pandas extension type when first ingesting the table into a ``Dataset``, whether from storage or in-memory. All table operations downstream from that cast should work automatically.

Single-column tensor datasets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most basic case is when a dataset only has a single column, which is of tensor type. This kind of dataset can be created with ``.range_tensor()``, and can be read from and written to ``.npy`` files. Here are some examples:

.. code-block:: python

    # Create a Dataset of tensor-typed values.
    ds = ray.data.range_tensor(10000, shape=(3, 5))
    # -> Dataset(num_blocks=200, num_rows=10000,
    #            schema={value: <ArrowTensorType: shape=(3, 5), dtype=int64>})

    # Save to storage.
    ds.write_numpy("/tmp/tensor_out", column="value")

    # Read from storage.
    ray.data.read_numpy("/tmp/tensor_out")
    # -> Dataset(num_blocks=200, num_rows=?,
    #            schema={value: <ArrowTensorType: shape=(3, 5), dtype=int64>})

Reading existing serialized tensor columns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you already have a Parquet dataset with columns containing serialized tensors, you can have these tensor columns cast to our tensor extension type at read-time by giving a simple schema for the tensor columns. Note that these tensors must have been serialized as their raw NumPy ndarray bytes in C-contiguous order (e.g. serialized via ``ndarray.tobytes()``).

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
        path, _tensor_column_schema={"two": (np.int, (2, 2, 2))})

    # Internally, this column is represented with our Arrow tensor extension
    # type.
    print(ds.schema())
    # -> one: int64
    #    two: extension<arrow.py_extension_type<ArrowTensorType>>

If your serialized tensors don't fit the above constraints (e.g. they're stored in Fortran-contiguous order, or they're pickled), you can manually cast this tensor column to our tensor extension type via a read-time user-defined function. This UDF will be pushed down to Ray Datasets' IO layer and executed on each block in parallel, as it's read from storage.

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

    # Internally, this column is represented with our Arrow tensor extension
    # type.
    print(ds.schema())
    # -> one: int64
    #    two: extension<arrow.py_extension_type<ArrowTensorType>>

Please note that the ``_tensor_column_schema`` and ``_block_udf`` parameters are both experimental developer APIs and may break in future versions.

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

End-to-end workflow with our Pandas extension type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
 * Automatic casting via specifying an override Arrow schema when reading Parquet is blocked by Arrow supporting custom ExtensionType casting kernels. See `issue <https://issues.apache.org/jira/browse/ARROW-5890>`__. An explicit ``_tensor_column_schema`` parameter has been added for :func:`read_parquet() <ray.data.read_api.read_parquet>` as a stopgap solution.
 * Ingesting tables with tensor columns into pytorch via ``ds.to_torch()`` is blocked by pytorch supporting tensor creation from objects that implement the `__array__` interface. See `issue <https://github.com/pytorch/pytorch/issues/51156>`__. Workarounds are being `investigated <https://github.com/ray-project/ray/issues/18314>`__.
 * Ingesting tables with tensor columns into TensorFlow via ``ds.to_tf()`` is blocked by a Pandas fix for properly interpreting extension arrays in ``DataFrame.values`` being released. See `PR <https://github.com/pandas-dev/pandas/pull/43160>`__. Workarounds are being `investigated <https://github.com/ray-project/ray/issues/18315>`__.
