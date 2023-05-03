.. _loading_data:

====================
Loading Data
====================

:class:`Datastreams <ray.data.Datastream>` can be created from:

* generated synthetic data,
* local and distributed in-memory data, and
* local and external storage systems (local disk, cloud storage, HDFS, etc.).

.. _datastream_generate_data:

-------------------------
Generating Synthetic Data
-------------------------

.. tab-set::

    .. tab-item:: Int Range

      Create a ``Datastream`` from a range of integers, with a single column containing this integer range.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __gen_synth_tabular_range_begin__
        :end-before: __gen_synth_tabular_range_end__

    .. tab-item:: Tensor Range

      Create a datastream from a range of integers, packing this integer range into
      tensors of the provided shape.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __gen_synth_tensor_range_begin__
        :end-before: __gen_synth_tensor_range_end__

.. _datastream_reading_from_storage:

--------------------------
Reading Files From Storage
--------------------------

Using the ``ray.data.read_*()`` APIs, data can be loaded from files on local disk
or remote storage system such as S3, GCS, Azure Blob Storage, or HDFS. Any filesystem
`supported by pyarrow <http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__
can be used to specify file locations, and many common file formats are supported:
Parquet, CSV, JSON, NPY, text, binary.

Each of these APIs take a path or list of paths to files or directories. Any directories
provided will be walked in order to obtain concrete file paths, at which point all files
will be read in parallel.

.. _datastream_supported_file_formats:

Common File Formats
===================

.. tab-set::

    .. tab-item:: Parquet

      Read Parquet files and directories. Partitioned parquet read support is also available.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __read_parquet_begin__
        :end-before: __read_parquet_end__

      The Parquet reader also supports projection and filter pushdown, allowing column
      selection and row filtering to be pushed down to the file scan.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __read_parquet_pushdown_begin__
        :end-before: __read_parquet_pushdown_end__

      See the API docs for :func:`read_parquet() <ray.data.read_parquet>`.

    .. tab-item:: CSV

      Read CSV files and directories.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __read_csv_begin__
        :end-before: __read_csv_end__

      See the API docs for :func:`read_csv() <ray.data.read_csv>`.

    .. tab-item:: JSON

      Read JSON files and directories.

      Currently, only newline-delimited JSON (NDJSON) is supported.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __read_json_begin__
        :end-before: __read_json_end__

      See the API docs for :func:`read_json() <ray.data.read_json>`.

    .. tab-item:: NumPy

      Read NumPy files and directories. The NumPy data will be represented via the Ray Data
      :class:`tensor extension type <ray.data.extensions.tensor_extension.ArrowTensorType>`.
      Refer to the :ref:`tensor data guide <data_tensor_support>` for more information on working
      with tensors.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __read_numpy_begin__
        :end-before: __read_numpy_end__

      See the API docs for :func:`read_numpy() <ray.data.read_numpy>`.

    .. tab-item:: Text

      Read text files and directories. Each line in each text file will be treated as a row in the datastream.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __read_text_begin__
        :end-before: __read_text_end__

      See the API docs for :func:`read_text() <ray.data.read_text>`.

    .. tab-item:: Images

      Call :func:`~ray.data.read_images` to read images.

      This function represents image data using the Ray Data
      :class:`tensor extension type <ray.data.extensions.tensor_extension.ArrowTensorType>`.
      For more information on working with tensors, refer to the :ref:`tensor data guide <data_tensor_support>`.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __read_images_begin__
        :end-before: __read_images_end__

    .. tab-item:: Binary

      Read binary files and directories. Each binary file will be converted to a record
      containing opaque bytes. These bytes can be decoded into tensor, tabular, text, or any other
      kind of data using :meth:`~ray.data.Datastream.map_batches` to apply a per-row decoding
      :ref:`user-defined function <transform_datastreams_writing_udfs>`.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __read_binary_begin__
        :end-before: __read_binary_end__

      See the API docs for :func:`read_binary_files() <ray.data.read_binary_files>`.

    .. tab-item:: TFRecords

      Call :func:`~ray.data.read_tfrecords` to read TFRecord files into a
      :class:`~ray.data.Datastream`.

      .. warning::
          Only `tf.train.Example <https://www.tensorflow.org/api_docs/python/tf/train/Example>`_
          records are supported.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __read_tfrecords_begin__
        :end-before: __read_tfrecords_end__

.. _datastream_reading_remote_storage:


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

.. tab-set::

    .. tab-item:: S3

      The AWS S3 storage system is inferred from the URI scheme (``s3://``), with required connection
      configuration such as S3 credentials being pulled from the machine's environment
      (e.g. the ``AWS_ACCESS_KEY_ID`` and ``AWS_SECRET_ACCESS_KEY`` environment variables).

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __read_parquet_s3_begin__
        :end-before: __read_parquet_s3_end__

      If needing to customize this S3 storage system connection (credentials, region,
      endpoint override, etc.), you can pass in an
      `S3FileSystem <https://arrow.apache.org/docs/python/filesystems.html#s3>`__ instance
      to :func:`read_parquet() <ray.data.read_parquet>`.

      .. literalinclude:: ./doc_code/loading_data_untested.py
        :language: python
        :start-after: __read_parquet_s3_with_fs_begin__
        :end-before: __read_parquet_s3_with_fs_end__

    .. tab-item:: HDFS

      The HDFS storage system is inferred from the URI scheme (``hdfs://``), with required connection
      configuration such as the host and the port being derived from the URI.

      .. note::

        This example is not runnable as-is; you'll need to point it at your HDFS
        cluster/data.

      .. literalinclude:: ./doc_code/loading_data_untested.py
        :language: python
        :start-after: __read_parquet_hdfs_begin__
        :end-before: __read_parquet_hdfs_end__

      If needing to customize this HDFS storage system connection (host, port, user, kerb
      ticket, etc.), you can pass in an `HDFSFileSystem
      <https://arrow.apache.org/docs/python/filesystems.html#hadoop-distributed-file-system-hdfs>`__
      instance to :func:`read_parquet() <ray.data.read_parquet>`.

      .. literalinclude:: ./doc_code/loading_data_untested.py
        :language: python
        :start-after: __read_parquet_hdfs_with_fs_begin__
        :end-before: __read_parquet_hdfs_with_fs_end__

    .. tab-item:: GCS

      Data can be read from Google Cloud Storage by providing a configured
      `gcsfs GCSFileSystem <https://gcsfs.readthedocs.io/en/latest/>`__, where the
      appropriate Google Cloud project and credentials can be specified.

      .. note::
        This example is not runnable as-is; you'll need to point it at your GCS bucket and
        configure your GCP project and credentials.

      .. literalinclude:: ./doc_code/loading_data_untested.py
        :language: python
        :start-after: __read_parquet_gcs_begin__
        :end-before: __read_parquet_gcs_end__

      .. tip::
        To verify that your GCP project and credentials are set up, validate
        that the GCS `filesystem` has permissions to read the input `path`.

        .. literalinclude:: ./doc_code/loading_data_untested.py
          :language: python
          :start-after: __validate_parquet_gcs_begin__
          :end-before: __validate_parquet_gcs_end__

        For more examples, see the `GCSFS Documentation <https://gcsfs.readthedocs.io/en/latest/#examples>`__.

    .. tab-item:: ADL/ABS (Azure)

      Data can be read from Azure Blob Storage by providing a configured
      `adlfs AzureBlobFileSystem <https://github.com/fsspec/adlfs>`__, where the appropriate
      account name and account key can be specified.

      .. literalinclude:: ./doc_code/loading_data_untested.py
        :language: python
        :start-after: __read_parquet_az_begin__
        :end-before: __read_parquet_az_end__

Reading from Local Storage
==========================

In Ray Data, users often read from remote storage systems as described above. In
some use cases, users may want to read from local storage. There are three ways to read
from a local filesystem:

* **Providing a raw filesystem path**: For example, in ``ray.data.read_csv("my_file.csv")``,
  the given path will be resolved as a local filesystem path. If the file exists only on the
  local node and you run this read operation in distributed cluster, this will fail as it
  cannot access the file from remote nodes.
* **Using ``local://`` custom URI scheme**: Similarly, this will be resolved to local
  filesystem, e.g. ``ray.data.read_csv("local://my_file.csv")`` will read the
  same file as the approach above. The difference is that this scheme will ensure
  all read tasks happen on the local node, so it's safe to run in a distributed
  cluster.
* **Using ``example://`` custom URI scheme**: The paths with this scheme will be resolved
  to ``ray/data/examples/data`` directory in the Ray package. This scheme is used
  only for testing or demoing examples.

Reading Compressed Files
========================

Ray Data supports reading compressed files using the ``arrow_open_stream_args`` arg.
`Codecs supported by Arrow <https://arrow.apache.org/docs/python/generated/pyarrow.CompressedInputStream.html>`__
(bz2, brotli, gzip, lz4 or zstd) are compatible with Ray Data.
For example:

.. literalinclude:: ./doc_code/loading_data.py
  :language: python
  :start-after: __read_compressed_begin__
  :end-before: __read_compressed_end__

.. _datastream_from_in_memory_data:

-------------------
From In-Memory Data
-------------------

Datastreams can be constructed from existing in-memory data. In addition to being able to
construct a ``Datastream`` from plain Python objects, Datastreams also interoperates with popular
single-node libraries (`Pandas <https://pandas.pydata.org/>`__,
`NumPy <https://numpy.org/>`__, `Arrow <https://arrow.apache.org/>`__) as well as
distributed frameworks (:ref:`Dask <dask-on-ray>`, :ref:`Spark <spark-on-ray>`,
:ref:`Modin <modin-on-ray>`, :ref:`Mars <mars-on-ray>`).

.. _datastream_from_in_memory_data_single_node:

From Single-Node Data Libraries
===============================

In this section, we demonstrate creating a ``Datastream`` from single-node in-memory data.

.. tab-set::

    .. tab-item:: Pandas

      Create a ``Datastream`` from a Pandas DataFrame. This constructs a ``Datastream``
      backed by a single block.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __from_pandas_begin__
        :end-before: __from_pandas_end__

      We can also build a ``Datastream`` from more than one Pandas DataFrame, where each said
      DataFrame will become a block in the ``Datastream``.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __from_pandas_mult_begin__
        :end-before: __from_pandas_mult_end__

    .. tab-item:: NumPy

      Create a ``Datastream`` from a NumPy ndarray. This constructs a ``Datastream``
      backed by a single block; the outer dimension of the ndarray
      will be treated as the row dimension, and the column will have name ``"data"``.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __from_numpy_begin__
        :end-before: __from_numpy_end__

      We can also build a ``Datastream`` from more than one NumPy ndarray, where each said
      ndarray will become a block in the ``Datastream``.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __from_numpy_mult_begin__
        :end-before: __from_numpy_mult_end__

    .. tab-item:: Arrow

      Create a ``Datastream`` from an
      `Arrow Table <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`__.
      This constructs a ``Datastream`` backed by a single block.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __from_arrow_begin__
        :end-before: __from_arrow_end__

      We can also build a ``Datastream`` from more than one Arrow Table, where each said
      ``Table`` will become a block in the ``Datastream``.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __from_arrow_mult_begin__
        :end-before: __from_arrow_mult_end__

    .. tab-item:: Python Objects

      Create a ``Datastream`` from a list of Python objects; which are interpreted as dict records.
      If the object is not a dict, it will be wrapped as ``{"item": item}``.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __from_items_begin__
        :end-before: __from_items_end__

.. _datastream_from_in_memory_data_distributed:

From Distributed Data Processing Frameworks
===========================================

In addition to working with single-node in-memory data, Datastreams can be constructed from
distributed (multi-node) in-memory data, interoperating with popular distributed
data processing frameworks such as :ref:`Dask <dask-on-ray>`, :ref:`Spark <spark-on-ray>`,
:ref:`Modin <modin-on-ray>`, and :ref:`Mars <mars-on-ray>`.

Note that these data processing frameworks must be running on Ray in order for these
integrations to work. See how these frameworks can be run on Ray in our
:ref:`data processing integrations docs <data_integrations>`.

.. tab-set::

    .. tab-item:: Dask

      Create a ``MaterializedDatastream`` from a
      `Dask DataFrame <https://docs.dask.org/en/stable/dataframe.html>`__. This constructs a
      ``Datastream`` backed by the distributed Pandas DataFrame partitions that underly the
      Dask DataFrame.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __from_dask_begin__
        :end-before: __from_dask_end__

    .. tab-item:: Spark

      Create a ``MaterializedDatastream`` from a `Spark DataFrame
      <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html>`__.
      This constructs a ``Datastream`` backed by the distributed Spark DataFrame partitions
      that underly the Spark DataFrame. When this conversion happens, Spark-on-Ray (RayDP)
      will save the Spark DataFrame partitions to Ray's object store in the Arrow format,
      which Datastreams will then interpret as its blocks.

      .. literalinclude:: ./doc_code/loading_data_untested.py
        :language: python
        :start-after: __from_spark_begin__
        :end-before: __from_spark_end__

    .. tab-item:: Modin

      Create a ``MaterializedDatastream`` from a Modin DataFrame. This constructs a ``Datastream``
      backed by the distributed Pandas DataFrame partitions that underly the Modin DataFrame.

      .. literalinclude:: ./doc_code/loading_data.py
        :language: python
        :start-after: __from_modin_begin__
        :end-before: __from_modin_end__

    .. tab-item:: Mars

      Create a ``MaterializedDatastream`` from a Mars DataFrame. This constructs a ``Datastream``
      backed by the distributed Pandas DataFrame partitions that underly the Mars DataFrame.

      .. literalinclude:: ./doc_code/loading_data_untested.py
        :language: python
        :start-after: __from_mars_begin__
        :end-before: __from_mars_end__

.. _datastream_from_torch_tf:

-------------------------
From Torch and TensorFlow
-------------------------

.. tab-set::

    .. tab-item:: PyTorch

        If you already have a Torch dataset available, you can create a Datastream using
        :class:`~ray.data.from_torch`.

        .. warning::
            :class:`~ray.data.from_torch` doesn't support parallel
            reads. You should only use this datasource for small datastreams like MNIST or
            CIFAR.

        .. code-block:: python

            import ray
            import torchvision

            torch_ds = torchvision.datasets.MNIST("data", download=True)
            datastream = ray.data.from_torch(torch_ds)
            datastream.take(1)
            # {"item": (<PIL.Image.Image image mode=L size=28x28 at 0x1142CCA60>, 5)}

    .. tab-item:: TensorFlow

        If you already have a TensorFlow dataset available, you can create a Datastream
        using :class:`~ray.data.from_tf`.

        .. warning::
            :class:`~ray.data.from_tf` doesn't support parallel reads. You
            should only use this function with small datastreams like MNIST or CIFAR.

        .. code-block:: python

            import ray
            import tensorflow_datasets as tfds

            tf_ds, _ = tfds.load("cifar10", split=["train", "test"])
            datastream = ray.data.from_tf(tf_ds)

            datastream
            # -> MaterializedDatastream(num_blocks=200, num_rows=50000, schema={id: binary, image: numpy.ndarray(shape=(32, 32, 3), dtype=uint8), label: int64})

.. _datastream_from_huggingface:

-------------------------------
From 🤗 (Hugging Face) Datasets
-------------------------------

You can convert 🤗 Datasets into Ray Data by using
:py:class:`~ray.data.from_huggingface`. This function accesses the underlying Arrow table and
converts it into a Datastream directly.

.. warning::
    :py:class:`~ray.data.from_huggingface` doesn't support parallel
    reads. This will not usually be an issue with in-memory 🤗 Datasets,
    but may fail with large memory-mapped 🤗 Datasets. 🤗 ``IterableDatastream``
    objects are not supported.

.. code-block:: python

    import ray.data
    from datasets import load_dataset

    hf_ds = load_dataset("wikitext", "wikitext-2-raw-v1")
    ray_ds = ray.data.from_huggingface(hf_ds)
    ray_ds["train"].take(2)
    # [{'text': ''}, {'text': ' = Valkyria Chronicles III = \n'}]

.. _datastream_mongo_db:

------------
From MongoDB
------------

A Datastream can also be created from `MongoDB <https://www.mongodb.com/>`__ with
:py:class:`~ray.data.read_mongo`.
This interacts with MongoDB similar to external filesystems, except here you will
need to specify the MongoDB source by its `uri <https://www.mongodb.com/docs/manual/reference/connection-string/>`__,
`database and collection <https://www.mongodb.com/docs/manual/core/databases-and-collections/>`__,
and specify a `pipeline <https://www.mongodb.com/docs/manual/core/aggregation-pipeline/>`__ to run against
the collection. The execution results are then used to create a Datastream.

.. note::

  This example is not runnable as-is; you'll need to point it at your MongoDB
  instance.

.. code-block:: python

    import ray

    # Read a local MongoDB.
    ds = ray.data.read_mongo(
        uri="mongodb://localhost:27017",
        database="my_db",
        collection="my_collection",
        pipeline=[{"$match": {"col": {"$gte": 0, "$lt": 10}}}, {"$sort": "sort_col"}],
    )

    # Reading a remote MongoDB is the same.
    ds = ray.data.read_mongo(
        uri="mongodb://username:password@mongodb0.example.com:27017/?authSource=admin",
        database="my_db",
        collection="my_collection",
        pipeline=[{"$match": {"col": {"$gte": 0, "$lt": 10}}}, {"$sort": "sort_col"}],
    )

    # Write back to MongoDB.
    ds.write_mongo(
        MongoDatasource(),
        uri="mongodb://username:password@mongodb0.example.com:27017/?authSource=admin",
        database="my_db",
        collection="my_collection",
    )

.. _datastreams_sql_databases:

--------------------------
Reading From SQL Databases
--------------------------

Call :func:`~ray.data.read_sql` to read data from a database that provides a 
`Python DB API2-compliant <https://peps.python.org/pep-0249/>`_ connector.

.. tab-set::

    .. tab-item:: MySQL

        To read from MySQL, install
        `MySQL Connector/Python <https://dev.mysql.com/doc/connector-python/en/>`_. It's the
        first-party MySQL database connector.

        .. code-block:: console

            pip install mysql-connector-python

        Then, define your connection login and query the database.

        .. code-block:: python

            import mysql.connector

            import ray

            def create_connection():
                return mysql.connector.connect(
                    user="admin",
                    password=...,
                    host="example-mysql-database.c2c2k1yfll7o.us-west-2.rds.amazonaws.com",
                    connection_timeout=30,
                    database="example",
                )

            # Get all movies
            datastream = ray.data.read_sql("SELECT * FROM movie", create_connection)
            # Get movies after the year 1980
            datastream = ray.data.read_sql(
                "SELECT title, score FROM movie WHERE year >= 1980", create_connection
            )
            # Get the number of movies per year
            datastream = ray.data.read_sql(
                "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
            )


    .. tab-item:: PostgreSQL

        To read from PostgreSQL, install `Psycopg 2 <https://www.psycopg.org/docs>`_. It's
        the most popular PostgreSQL database connector.

        .. code-block:: console

            pip install psycopg2-binary

        Then, define your connection login and query the database.

        .. code-block:: python

            import psycopg2

            import ray

            def create_connection():
                return psycopg2.connect(
                    user="postgres",
                    password=...,
                    host="example-postgres-database.c2c2k1yfll7o.us-west-2.rds.amazonaws.com",
                    dbname="example",
                )

            # Get all movies
            datastream = ray.data.read_sql("SELECT * FROM movie", create_connection)
            # Get movies after the year 1980
            datastream = ray.data.read_sql(
                "SELECT title, score FROM movie WHERE year >= 1980", create_connection
            )
            # Get the number of movies per year
            datastream = ray.data.read_sql(
                "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
            )

    .. tab-item:: Snowflake

        To read from Snowflake, install the
        `Snowflake Connector for Python <https://docs.snowflake.com/en/user-guide/python-connector>`_.

        .. code-block:: console

            pip install snowflake-connector-python

        Then, define your connection login and query the database.

        .. code-block:: python

            import snowflake.connector

            import ray

            def create_connection():
                return snowflake.connector.connect(
                    user=...,
                    password=...
                    account="ZZKXUVH-IPB52023",
                    database="example",
                )

            # Get all movies
            datastream = ray.data.read_sql("SELECT * FROM movie", create_connection)
            # Get movies after the year 1980
            datastream = ray.data.read_sql(
                "SELECT title, score FROM movie WHERE year >= 1980", create_connection
            )
            # Get the number of movies per year
            datastream = ray.data.read_sql(
                "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
            )


    .. tab-item:: Databricks

        To read from Databricks, install the
        `Databricks SQL Connector for Python <https://docs.databricks.com/dev-tools/python-sql-connector.html>`_.

        .. code-block:: console

            pip install databricks-sql-connector


        Then, define your connection logic and read from the Databricks SQL warehouse.

        .. code-block:: python

            from databricks import sql

            import ray

            def create_connection():
                return sql.connect(
                    server_hostname="dbc-1016e3a4-d292.cloud.databricks.com",
                    http_path="/sql/1.0/warehouses/a918da1fc0b7fed0",
                    access_token=...,


            # Get all movies
            datastream = ray.data.read_sql("SELECT * FROM movie", create_connection)
            # Get movies after the year 1980
            datastream = ray.data.read_sql(
                "SELECT title, score FROM movie WHERE year >= 1980", create_connection
            )
            # Get the number of movies per year
            datastream = ray.data.read_sql(
                "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
            )

    .. tab-item:: BigQuery

        To read from BigQuery, install the
        `Python Client for Google BigQuery <https://cloud.google.com/python/docs/reference/bigquery/latest>`_.
        This package includes a DB API2-compliant database connector.

        .. code-block:: console

            pip install google-cloud-bigquery

        Then, define your connection login and query the dataset.

        .. code-block:: python

            from google.cloud import bigquery
            from google.cloud.bigquery import dbapi

            import ray

            def create_connection():
                client = bigquery.Client(...)
                return dbapi.Connection(client)

            # Get all movies
            datastream = ray.data.read_sql("SELECT * FROM movie", create_connection)
            # Get movies after the year 1980
            datastream = ray.data.read_sql(
                "SELECT title, score FROM movie WHERE year >= 1980", create_connection
            )
            # Get the number of movies per year
            datastream = ray.data.read_sql(
                "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
            )


.. _data_custom_datasource:

------------------
Custom Datasources
------------------

Datastreams can read and write in parallel to :ref:`custom datasources <data_source_api>` defined in Python.
Once you have implemented `YourCustomDataSource`, you can use it like any other source in Ray Data:

.. code-block:: python

    # Read from a custom datasource.
    ds = ray.data.read_datasource(YourCustomDatasource(), **read_args)

    # Write to a custom datasource.
    ds.write_datasource(YourCustomDatasource(), **write_args)

For more details, check out :ref:`guide for implementing a custom datasource <custom_datasources>`.

--------------------------
Performance Considerations
--------------------------

The datastream ``parallelism`` determines the number of blocks the base data will be split into for parallel reads. Ray Data will decide internally how many read tasks to run concurrently to best utilize the cluster, ranging from ``1...parallelism`` tasks. In other words, the higher the parallelism, the smaller the data blocks in the Datastream and hence the more opportunity for parallel execution.

.. image:: images/datastream-read.svg
   :width: 650px
   :align: center

This default parallelism can be overridden via the ``parallelism`` argument; see the
:ref:`performance guide <data_performance_tips>`  for more information on how to tune this read parallelism.
