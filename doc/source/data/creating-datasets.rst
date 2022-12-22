.. _creating_datasets:

=================
Creating Datasets
=================

Ray :class:`Datasets <ray.data.Dataset>` can be created from:

* generated synthetic data,
* local and distributed in-memory data, and
* local and external storage systems (local disk, cloud storage, HDFS, etc.).

This guide surveys the many ways to create a ``Dataset``. If none of these meet your
needs, please reach out to us on `Discourse <https://discuss.ray.io/>`__ or open a feature
request on the `Ray GitHub repo <https://github.com/ray-project/ray>`__, and check out
our :ref:`guide for implementing a custom Datasets datasource <custom_datasources>`
if you're interested in rolling your own integration!

.. _dataset_generate_data:

-------------------------
Generating Synthetic Data
-------------------------

.. tabbed:: Int Range

  Create a ``Dataset`` from a range of integers.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __gen_synth_int_range_begin__
    :end-before: __gen_synth_int_range_end__

.. tabbed:: Tabular Range

  Create an Arrow (tabular) ``Dataset`` from a range of integers,
  with a single column containing this integer range.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __gen_synth_tabular_range_begin__
    :end-before: __gen_synth_tabular_range_end__

.. tabbed:: Tensor Range

  Create a tensor dataset from a range of integers, packing this integer range into
  tensors of the provided shape.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __gen_synth_tensor_range_begin__
    :end-before: __gen_synth_tensor_range_end__

.. _dataset_reading_from_storage:

--------------------------
Reading Files From Storage
--------------------------

Using the ``ray.data.read_*()`` APIs, Datasets can be created from files on local disk
or remote storage system such as S3, GCS, Azure Blob Storage, or HDFS. Any filesystem
`supported by pyarrow <http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__
can be used to specify file locations, and many common file formats are supported:
Parquet, CSV, JSON, NPY, text, binary.

Each of these APIs take a path or list of paths to files or directories. Any directories
provided will be walked in order to obtain concrete file paths, at which point all files
will be read in parallel.

.. _dataset_supported_file_formats:

Supported File Formats
======================

.. tabbed:: Parquet

  Read Parquet files into a tabular ``Dataset``. The Parquet data will be read into
  `Arrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  blocks. Although this simple example demonstrates reading a single file, note that
  Datasets can also read directories of Parquet files, with one tabular block created
  per file. For Parquet in particular, we also support reading partitioned Parquet
  datasets with partition column values pulled from the file paths.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_parquet_begin__
    :end-before: __read_parquet_end__

  Datasets' Parquet reader also supports projection and filter pushdown, allowing column
  selection and row filtering to be pushed down to the file scan. For column selection,
  unselected columns will never be read from the file.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_parquet_pushdown_begin__
    :end-before: __read_parquet_pushdown_end__

  See the API docs for :func:`read_parquet() <ray.data.read_parquet>`.

.. tabbed:: CSV

  Read CSV files into a tabular ``Dataset``. The CSV data will be read into
  `Arrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  blocks. Although this simple example demonstrates reading a single file, note that
  Datasets can also read directories of CSV files, with one tabular block created
  per file.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_csv_begin__
    :end-before: __read_csv_end__

  See the API docs for :func:`read_csv() <ray.data.read_csv>`.

.. tabbed:: JSON

  Read JSON files into a tabular ``Dataset``. The JSON data will be read into
  `Arrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  blocks. Although this simple example demonstrates reading a single file, note that
  Datasets can also read directories of JSON files, with one tabular block created
  per file.

  Currently, only newline-delimited JSON (NDJSON) is supported.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_json_begin__
    :end-before: __read_json_end__

  See the API docs for :func:`read_json() <ray.data.read_json>`.

.. tabbed:: NumPy

  Read NumPy files into a tensor ``Dataset``. The NumPy ndarray data will be read into
  single-column
  `Arrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  blocks using our
  :class:`tensor extension type <ray.data.extensions.tensor_extension.ArrowTensorType>`,
  treating the outermost ndarray dimension as the row dimension. See our
  :ref:`tensor data guide <datasets_tensor_support>` for more information on working
  with tensors in Datasets. Although this simple example demonstrates reading a single
  file, note that Datasets can also read directories of NumPy files, with one tensor
  block created per file.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_numpy_begin__
    :end-before: __read_numpy_end__

  See the API docs for :func:`read_numpy() <ray.data.read_numpy>`.

.. tabbed:: Text

  Read text files into a ``Dataset``. Each line in each text file will be treated as a
  row in the dataset, resulting in a list-of-strings block being created for each text
  file.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_text_begin__
    :end-before: __read_text_end__

  See the API docs for :func:`read_text() <ray.data.read_text>`.

.. tabbed:: Images (experimental)

  Call :func:`~ray.data.read_images` to read images into a :class:`~ray.data.Dataset`. 

  This function stores image data in single-column
  `Arrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__
  blocks using the 
  :class:`tensor extension type <ray.data.extensions.tensor_extension.ArrowTensorType>`.
  For more information on working with tensors in Datasets, read the 
  :ref:`tensor data guide <datasets_tensor_support>`.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_images_begin__
    :end-before: __read_images_end__

.. tabbed:: Binary

  Read binary files into a ``Dataset``. Each binary file will be treated as a single row
  of opaque bytes. These bytes can be decoded into tensor, tabular, text, or any other
  kind of data using :meth:`~ray.data.Dataset.map_batches` to apply a per-row decoding
  :ref:`user-defined function <transform_datasets_writing_udfs>`.

  Although this simple example demonstrates reading a single file, note that Datasets
  can also read directories of binary files, with one bytes block created per file.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_binary_begin__
    :end-before: __read_binary_end__

  See the API docs for :func:`read_binary_files() <ray.data.read_binary_files>`.

.. tabbed:: TFRecords

  Call :func:`~ray.data.read_tfrecords` to read TFRecord files into a tabular
  :class:`~ray.data.Dataset`.

  .. warning::
      Only `tf.train.Example <https://www.tensorflow.org/api_docs/python/tf/train/Example>`_
      records are supported.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_tfrecords_begin__
    :end-before: __read_tfrecords_end__

.. _dataset_reading_remote_storage:


Reading from Remote Storage
===========================

All of the file formats mentioned above can be read from remote storage, such as S3,
GCS, Azure Blob Storage, and HDFS. These storage systems are supported via Arrow's
filesystem APIs natively for S3 and HDFS, and as a wrapper around fsspec for GCS and
HDFS. All ``ray.data.read_*()`` APIs expose a ``filesystem`` argument that accepts both
`Arrow FileSystem <https://arrow.apache.org/docs/python/filesystems.html>`__ instances
and `fsspec FileSystem <https://filesystem-spec.readthedocs.io/en/latest/>`__ instances,
allowing you to configure this connection to the remote storage system, such as
authn/authz and buffer/block size.

For S3 and HDFS, the underlying `FileSystem
<https://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__
implementation will be inferred from the URL scheme (``"s3://"`` and ``"hdfs://"``); if
the default connection configuration suffices for your workload, you won't need to
specify a ``filesystem`` argument.

We use Parquet files for the below examples, but all of the aforementioned file formats
are supported for each of these storage systems.

.. tabbed:: S3

  The AWS S3 storage system is inferred from the URI scheme (``s3://``), with required connection
  configuration such as S3 credentials being pulled from the machine's environment
  (e.g. the ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` environment variables).

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_parquet_s3_begin__
    :end-before: __read_parquet_s3_end__

  If needing to customize this S3 storage system connection (credentials, region,
  endpoint override, etc.), you can pass in an
  `S3FileSystem <https://arrow.apache.org/docs/python/filesystems.html#s3>`__ instance
  to :func:`read_parquet() <ray.data.read_parquet>`.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_parquet_s3_with_fs_begin__
    :end-before: __read_parquet_s3_with_fs_end__

.. tabbed:: HDFS

  The HDFS storage system is inferred from the URI scheme (``hdfs://``), with required connection
  configuration such as the host and the port being derived from the URI.

  .. note::

    This example is not runnable as-is; you'll need to point it at your HDFS
    cluster/data.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_parquet_hdfs_begin__
    :end-before: __read_parquet_hdfs_end__

  If needing to customize this HDFS storage system connection (host, port, user, kerb
  ticket, etc.), you can pass in an `HDFSFileSystem
  <https://arrow.apache.org/docs/python/filesystems.html#hadoop-distributed-file-system-hdfs>`__
  instance to :func:`read_parquet() <ray.data.read_parquet>`.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_parquet_hdfs_with_fs_begin__
    :end-before: __read_parquet_hdfs_with_fs_end__

.. tabbed:: GCS

  Data can be read from Google Cloud Storage by providing a configured
  `gcsfs GCSFileSystem <https://gcsfs.readthedocs.io/en/latest/>`__, where the
  appropriate Google Cloud project and credentials can be specified.

  .. note::
    This example is not runnable as-is; you'll need to point it at your GCS bucket and
    configure your GCP project and credentials.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_parquet_gcs_begin__
    :end-before: __read_parquet_gcs_end__

.. tabbed:: ADL/ABS (Azure)

  Data can be read from Azure Blob Storage by providing a configured
  `adlfs AzureBlobFileSystem <https://github.com/fsspec/adlfs>`__, where the appropriate
  account name and account key can be specified.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __read_parquet_az_begin__
    :end-before: __read_parquet_az_end__

Reading from Local Storage
==========================

In Ray Datasets, users often read from remote storage systems as described above. In
some use cases, users may want to read from local storage. There are three ways to read
from a local filesystem:

* **Providing a local filesystem path**: For example, in ``ray.data.read_csv("my_file.csv")``,
  the given path will be resolved as a local filesystem path.

.. note::

  If the file exists only on the local node and you run this read operation in
  distributed cluster, this will fail as it cannot access the file from remote node.

* **Using ``local://`` custom URI scheme**: Similarly, this will be resolved to local
  filesystem, e.g. ``ray.data.read_csv("local://my_file.csv")`` will read the
  same file as the approach above. The difference is that this scheme will ensure
  all read tasks happen on the local node, so it's safe to run in a distributed
  cluster.
* **Using ``example://`` custom URI scheme**: The paths with this scheme will be resolved
  to ``ray/data/examples/data`` directory in the Ray package. This scheme is used
  only for testing or demoing examples.

.. _dataset_from_in_memory_data:

-------------------
From In-Memory Data
-------------------

Datasets can be constructed from existing in-memory data. In addition to being able to
construct a ``Dataset`` from plain Python objects, Datasets also interoperates with popular
single-node libraries (`Pandas <https://pandas.pydata.org/>`__,
`NumPy <https://numpy.org/>`__, `Arrow <https://arrow.apache.org/>`__) as well as
distributed frameworks (:ref:`Dask <dask-on-ray>`, :ref:`Spark <spark-on-ray>`,
:ref:`Modin <modin-on-ray>`, :ref:`Mars <mars-on-ray>`).

.. _dataset_from_in_memory_data_single_node:

From Single-Node Data Libraries
===============================

In this section, we demonstrate creating a ``Dataset`` from single-node in-memory data.

.. tabbed:: Pandas

  Create a ``Dataset`` from a Pandas DataFrame. This constructs a ``Dataset``
  backed by a single Pandas DataFrame block.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __from_pandas_begin__
    :end-before: __from_pandas_end__

  We can also build a ``Dataset`` from more than one Pandas DataFrame, where each said
  DataFrame will become a block in the ``Dataset``.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __from_pandas_mult_begin__
    :end-before: __from_pandas_mult_end__

.. tabbed:: NumPy

  Create a ``Dataset`` from a NumPy ndarray. This constructs a ``Dataset``
  backed by a single-column Arrow table block; the outer dimension of the ndarray
  will be treated as the row dimension, and the column will have name ``"__value__"``.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __from_numpy_begin__
    :end-before: __from_numpy_end__

  We can also build a ``Dataset`` from more than one NumPy ndarray, where each said
  ndarray will become a single-column Arrow table block in the ``Dataset``.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __from_numpy_mult_begin__
    :end-before: __from_numpy_mult_end__

.. tabbed:: Arrow

  Create a ``Dataset`` from an
  `Arrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__.
  This constructs a ``Dataset`` backed by a single Arrow ``Table`` block.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __from_arrow_begin__
    :end-before: __from_arrow_end__

  We can also build a ``Dataset`` from more than one Arrow Table, where each said
  ``Table`` will become a block in the ``Dataset``.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __from_arrow_mult_begin__
    :end-before: __from_arrow_mult_end__

.. tabbed:: Python Objects

  Create a ``Dataset`` from a list of Python objects; since each object in this
  particular list is a dictionary, Datasets will treat this list as a list of tabular
  records, and will construct an Arrow ``Dataset``.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __from_items_begin__
    :end-before: __from_items_end__

.. _dataset_from_in_memory_data_distributed:

From Distributed Data Processing Frameworks
===========================================

In addition to working with single-node in-memory data, Datasets can be constructed from
distributed (multi-node) in-memory data, interoperating with popular distributed
data processing frameworks such as :ref:`Dask <dask-on-ray>`, :ref:`Spark <spark-on-ray>`,
:ref:`Modin <modin-on-ray>`, and :ref:`Mars <mars-on-ray>`.

These conversions work by running Ray tasks converting each Dask/Spark/Modin/Mars
data partition to a block format supported by Datasets (copying data if needed), and using the
futures representing the return value of those conversion tasks as the ``Dataset`` block
futures.

.. note::

  These data processing frameworks must be running on Ray in order for these Datasets
  integrations to work. See how these frameworks can be run on Ray in our
  :ref:`data processing integrations docs <data_integrations>`.

.. tabbed:: Dask

  Create a ``Dataset`` from a
  `Dask DataFrame <https://docs.dask.org/en/stable/dataframe.html>`__. This constructs a
  ``Dataset`` backed by the distributed Pandas DataFrame partitions that underly the
  Dask DataFrame.

  This conversion has near-zero overhead, since Datasets simply reinterprets existing
  Dask-in-Ray partition objects as Dataset blocks.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __from_dask_begin__
    :end-before: __from_dask_end__

.. tabbed:: Spark

  Create a ``Dataset`` from a `Spark DataFrame
  <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html>`__.
  This constructs a ``Dataset`` backed by the distributed Spark DataFrame partitions
  that underly the Spark DataFrame. When this conversion happens, Spark-on-Ray (RayDP)
  will save the Spark DataFrame partitions to Ray's object store in the Arrow format,
  which Datasets will then interpret as its blocks.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __from_spark_begin__
    :end-before: __from_spark_end__

.. tabbed:: Modin

  Create a ``Dataset`` from a Modin DataFrame. This constructs a ``Dataset``
  backed by the distributed Pandas DataFrame partitions that underly the Modin DataFrame.

  This conversion has near-zero overhead, since Datasets simply reinterprets existing
  Modin partition objects as Dataset blocks.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __from_modin_begin__
    :end-before: __from_modin_end__

.. tabbed:: Mars

  Create a ``Dataset`` from a Mars DataFrame. This constructs a ``Dataset``
  backed by the distributed Pandas DataFrame partitions that underly the Mars DataFrame.

  This conversion has near-zero overhead, since Datasets simply reinterprets existing
  Mars partition objects as Dataset blocks.

  .. literalinclude:: ./doc_code/creating_datasets.py
    :language: python
    :start-after: __from_mars_begin__
    :end-before: __from_mars_end__

.. _dataset_from_torch_tf:

-------------------------
From Torch and TensorFlow
-------------------------

.. tabbed:: PyTorch

    If you already have a Torch dataset available, you can create a Ray Dataset using
    :class:`~ray.data.from_torch`.

    .. warning::
        :class:`~ray.data.from_torch` doesn't support parallel
        reads. You should only use this datasource for small datasets like MNIST or
        CIFAR.

    .. code-block:: python

        import ray
        import torchvision

        dataset = torchvision.datasets.MNIST("data", download=True)
        dataset = ray.data.from_torch(dataset)
        dataset.take(1)
        # (<PIL.Image.Image image mode=L size=28x28 at 0x1142CCA60>, 5)

.. tabbed:: TensorFlow

    If you already have a TensorFlow dataset available, you can create a Ray Dataset
    using :class:`~ray.data.from_tf`.

    .. warning::
        :class:`~ray.data.from_tf` doesn't support parallel reads. You
        should only use this function with small datasets like MNIST or CIFAR.

    .. code-block:: python

        import ray
        import tensorflow_datasets as tfds

        dataset, _ = tfds.load("cifar10", split=["train", "test"])
        dataset = ray.data.from_tf(dataset)

        dataset
        # -> Dataset(num_blocks=200, num_rows=50000, schema={id: binary, image: ArrowTensorType(shape=(32, 32, 3), dtype=uint8), label: int64})

.. _dataset_from_huggingface:

-------------------------------
From 🤗 (Hugging Face) Datasets
-------------------------------

You can convert 🤗 Datasets into Ray Datasets by using
:py:class:`~ray.data.from_huggingface`. This function accesses the underlying Arrow table and
converts it into a Ray Dataset directly.

.. warning::
    :py:class:`~ray.data.from_huggingface` doesn't support parallel
    reads. This will not usually be an issue with in-memory 🤗 Datasets,
    but may fail with large memory-mapped 🤗 Datasets. 🤗 ``IterableDataset``
    objects are not supported.

.. code-block:: python

    import ray.data
    from datasets import load_dataset

    hf_datasets = load_dataset("wikitext", "wikitext-2-raw-v1")
    ray_datasets = ray.data.from_huggingface(hf_datasets)
    ray_datasets["train"].take(2)
    # [{'text': ''}, {'text': ' = Valkyria Chronicles III = \n'}]

.. _datasets_custom_datasource:

------------------
Custom Datasources
------------------

Datasets can read and write in parallel to :ref:`custom datasources <data_source_api>` defined in Python.
Once you have implemented `YourCustomDataSource`, you can use it like any other source in Ray Data:

.. code-block:: python

    # Read from a custom datasource.
    ds = ray.data.read_datasource(YourCustomDatasource(), **read_args)

    # Write to a custom datasource.
    ds.write_datasource(YourCustomDatasource(), **write_args)

For more details, check out :ref:`guide for implementing a custom Datasets datasource <custom_datasources>`.

--------------------------
Performance Considerations
--------------------------

Read Parallelism
================

Datasets automatically selects the read ``parallelism`` according to the following procedure:

1. The number of available CPUs is estimated. If in a placement group, the number of CPUs in the cluster is scaled by the size of the placement group compared to the cluster size. If not in a placement group, this is the number of CPUs in the cluster.
2. The parallelism is set to the estimated number of CPUs multiplied by 2. If the parallelism is less than 8, it is set to 8.
3. The in-memory data size is estimated. If the parallelism would create in-memory blocks that are larger on average than the target block size (512MiB), the parallelism is increased until the blocks are < 512MiB in size.
4. The parallelism is truncated to ``min(num_files, parallelism)``.

To perform the read, ``parallelism`` parallel read tasks will be
launched, each reading one or more files and each creating a single block of data.
When reading from remote datasources, these parallel read tasks will be spread across
the nodes in your Ray cluster, creating the distributed collection of blocks that makes
up a distributed Ray Dataset.

.. image:: images/dataset-read.svg
   :width: 650px
   :align: center

This default parallelism can be overridden via the ``parallelism`` argument; see the
:ref:`performance guide <data_performance_tips>`  for tips on how to tune this read
parallelism.

.. _dataset_deferred_reading:

Deferred Read Task Execution
============================

Datasets created via the ``ray.data.read_*()`` APIs are semi-lazy: initially, only the
first read task will be executed. This avoids blocking Dataset creation on the reading
of all data files, enabling inspection functions like
:meth:`ds.schema() <ray.data.Dataset.schema>` and
:meth:`ds.show() <ray.data.Dataset.show>` to be used right away. Executing further
transformations on the Dataset will trigger execution of all read tasks, and execution
of all read tasks can be triggered manually using the
:meth:`ds.fully_executed() <ray.data.Dataset.fully_executed>` API.

