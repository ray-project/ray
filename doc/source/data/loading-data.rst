.. _loading_data:

============
Loading Data
============

Ray Data loads data from various sources. This guide shows you how to:

* `Read files <#reading-files>`_ like images
* `Load in-memory data <#loading-data-from-other-libraries>`_ like pandas DataFrames
* `Read databases <#reading-databases>`_ like MySQL

.. _reading-files:

Reading files
=============

Ray Data reads files from local disk or cloud storage in a variety of file formats.
To view the full list of supported file formats, see the
:ref:`Input/Output reference <input-output>`.

.. tab-set::

    .. tab-item:: Parquet

        To read Parquet files, call :func:`~ray.data.read_parquet`.

        .. testcode::

            import ray

            ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")

            print(ds.schema())

        .. testoutput::

            Column        Type
            ------        ----
            sepal.length  double
            sepal.width   double
            petal.length  double
            petal.width   double
            variety       string

        .. tip::

            When reading parquet files, you can take advantage of column pruning to
            efficiently filter columns at the file scan level. See
            :ref:`Parquet column pruning <parquet_column_pruning>` for more details
            on the projection pushdown feature.

    .. tab-item:: Images

        To read raw images, call :func:`~ray.data.read_images`. Ray Data represents
        images as NumPy ndarrays.

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/batoidea/JPEGImages/")

            print(ds.schema())

        .. testoutput::

            Column  Type
            ------  ----
            image   numpy.ndarray(shape=(32, 32, 3), dtype=uint8)

    .. tab-item:: Text

        To read lines of text, call :func:`~ray.data.read_text`.

        .. testcode::

            import ray

            ds = ray.data.read_text("s3://anonymous@ray-example-data/this.txt")

            print(ds.schema())

        .. testoutput::

            Column  Type
            ------  ----
            text    string

    .. tab-item:: CSV

        To read CSV files, call :func:`~ray.data.read_csv`.

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

            print(ds.schema())

        .. testoutput::

            Column             Type
            ------             ----
            sepal length (cm)  double
            sepal width (cm)   double
            petal length (cm)  double
            petal width (cm)   double
            target             int64

    .. tab-item:: Binary

        To read raw binary files, call :func:`~ray.data.read_binary_files`.

        .. testcode::

            import ray

            ds = ray.data.read_binary_files("s3://anonymous@ray-example-data/documents")

            print(ds.schema())

        .. testoutput::

            Column  Type
            ------  ----
            bytes   binary

    .. tab-item:: TFRecords

        To read TFRecords files, call :func:`~ray.data.read_tfrecords`.

        .. testcode::

            import ray

            ds = ray.data.read_tfrecords("s3://anonymous@ray-example-data/iris.tfrecords")

            print(ds.schema())

        .. testoutput::
            :options: +MOCK

            Column        Type
            ------        ----
            label         binary
            petal.length  float
            sepal.width   float
            petal.width   float
            sepal.length  float


Reading files from local disk
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To read files from local disk, call a function like :func:`~ray.data.read_parquet` and
specify paths with the ``local://`` schema. Paths can point to files or directories.

To read formats other than Parquet, see the :ref:`Input/Output reference <input-output>`.

.. tip::

    If your files are accessible on every node, exclude ``local://`` to parallelize the
    read tasks across the cluster.

.. testcode::
    :skipif: True

    import ray

    ds = ray.data.read_parquet("local:///tmp/iris.parquet")

    print(ds.schema())

.. testoutput::

    Column        Type
    ------        ----
    sepal.length  double
    sepal.width   double
    petal.length  double
    petal.width   double
    variety       string

Reading files from cloud storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To read files in cloud storage, authenticate all nodes with your cloud service provider.
Then, call a method like :func:`~ray.data.read_parquet` and specify URIs with the
appropriate schema. URIs can point to buckets, folders, or objects.

To read formats other than Parquet, see the :ref:`Input/Output reference <input-output>`.

.. tab-set::

    .. tab-item:: S3

        To read files from Amazon S3, specify URIs with the ``s3://`` scheme.

        .. testcode::

            import ray

            ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")

            print(ds.schema())

        .. testoutput::

            Column        Type
            ------        ----
            sepal.length  double
            sepal.width   double
            petal.length  double
            petal.width   double
            variety       string

        Ray Data relies on PyArrow for authentication with Amazon S3. For more on how to configure
        your credentials to be compatible with PyArrow, see their
        `S3 Filesystem docs <https://arrow.apache.org/docs/python/filesystems.html#s3>`_.

    .. tab-item:: GCS

        To read files from Google Cloud Storage, install the
        `Filesystem interface to Google Cloud Storage <https://gcsfs.readthedocs.io/en/latest/>`_

        .. code-block:: console

            pip install gcsfs

        Then, create a ``GCSFileSystem`` and specify URIs with the ``gs://`` scheme.

        .. testcode::
            :skipif: True

            import ray

            filesystem = gcsfs.GCSFileSystem(project="my-google-project")
            ds = ray.data.read_parquet(
                "gs://...",
                filesystem=filesystem
            )

            print(ds.schema())

        .. testoutput::

            Column        Type
            ------        ----
            sepal.length  double
            sepal.width   double
            petal.length  double
            petal.width   double
            variety       string

        Ray Data relies on PyArrow for authentication with Google Cloud Storage. For more on how
        to configure your credentials to be compatible with PyArrow, see their
        `GCS Filesystem docs <https://arrow.apache.org/docs/python/filesystems.html#google-cloud-storage-file-system>`_.

    .. tab-item:: ABS

        To read files from Azure Blob Storage, install the
        `Filesystem interface to Azure-Datalake Gen1 and Gen2 Storage <https://pypi.org/project/adlfs/>`_

        .. code-block:: console

            pip install adlfs

        Then, create a ``AzureBlobFileSystem`` and specify URIs with the `az://` scheme.

        .. testcode::
            :skipif: True

            import adlfs
            import ray

            ds = ray.data.read_parquet(
                "az://ray-example-data/iris.parquet",
                adlfs.AzureBlobFileSystem(account_name="azureopendatastorage")
            )

            print(ds.schema())

        .. testoutput::

            Column        Type
            ------        ----
            sepal.length  double
            sepal.width   double
            petal.length  double
            petal.width   double
            variety       string

        Ray Data relies on PyArrow for authentication with Azure Blob Storage. For more on how
        to configure your credentials to be compatible with PyArrow, see their
        `fsspec-compatible filesystems docs <https://arrow.apache.org/docs/python/filesystems.html#using-fsspec-compatible-filesystems-with-arrow>`_.

Reading files from NFS
~~~~~~~~~~~~~~~~~~~~~~

To read files from NFS filesystems, call a function like :func:`~ray.data.read_parquet`
and specify files on the mounted filesystem. Paths can point to files or directories.

To read formats other than Parquet, see the :ref:`Input/Output reference <input-output>`.

.. testcode::
    :skipif: True

    import ray

    ds = ray.data.read_parquet("/mnt/cluster_storage/iris.parquet")

    print(ds.schema())

.. testoutput::

    Column        Type
    ------        ----
    sepal.length  double
    sepal.width   double
    petal.length  double
    petal.width   double
    variety       string

Handling compressed files
~~~~~~~~~~~~~~~~~~~~~~~~~

To read a compressed file, specify ``compression`` in ``arrow_open_stream_args``.
You can use any `codec supported by Arrow <https://arrow.apache.org/docs/python/generated/pyarrow.CompressedInputStream.html>`__.

.. testcode::

    import ray

    ds = ray.data.read_csv(
        "s3://anonymous@ray-example-data/iris.csv.gz",
        arrow_open_stream_args={"compression": "gzip"},
    )

Loading data from other libraries
=================================

Loading data from single-node data libraries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Data interoperates with libraries like pandas, NumPy, and Arrow.

.. tab-set::

    .. tab-item:: Python objects

        To create a :class:`~ray.data.dataset.Dataset` from Python objects, call
        :func:`~ray.data.from_items` and pass in a list of ``Dict``. Ray Data treats
        each ``Dict`` as a row.

        .. testcode::

            import ray

            ds = ray.data.from_items([
                {"food": "spam", "price": 9.34},
                {"food": "ham", "price": 5.37},
                {"food": "eggs", "price": 0.94}
            ])

            print(ds)

        .. testoutput::

            MaterializedDataset(
               num_blocks=3,
               num_rows=3,
               schema={food: string, price: double}
            )

        You can also create a :class:`~ray.data.dataset.Dataset` from a list of regular
        Python objects.

        .. testcode::

            import ray

            ds = ray.data.from_items([1, 2, 3, 4, 5])

            print(ds)

        .. testoutput::

            MaterializedDataset(num_blocks=5, num_rows=5, schema={item: int64})

    .. tab-item:: NumPy

        To create a :class:`~ray.data.dataset.Dataset` from a NumPy array, call
        :func:`~ray.data.from_numpy`. Ray Data treats the outer axis as the row
        dimension.

        .. testcode::

            import numpy as np
            import ray

            array = np.ones((3, 2, 2))
            ds = ray.data.from_numpy(array)

            print(ds)

        .. testoutput::

            MaterializedDataset(
               num_blocks=1,
               num_rows=3,
               schema={data: numpy.ndarray(shape=(2, 2), dtype=double)}
            )

    .. tab-item:: pandas

        To create a :class:`~ray.data.dataset.Dataset` from a pandas DataFrame, call
        :func:`~ray.data.from_pandas`.

        .. testcode::

            import pandas as pd
            import ray

            df = pd.DataFrame({
                "food": ["spam", "ham", "eggs"],
                "price": [9.34, 5.37, 0.94]
            })
            ds = ray.data.from_pandas(df)

            print(ds)

        .. testoutput::

            MaterializedDataset(
               num_blocks=1,
               num_rows=3,
               schema={food: object, price: float64}
            )

    .. tab-item:: PyArrow

        To create a :class:`~ray.data.dataset.Dataset` from an Arrow table, call
        :func:`~ray.data.from_arrow`.

        .. testcode::

            import pyarrow as pa

            table = pa.table({
                "food": ["spam", "ham", "eggs"],
                "price": [9.34, 5.37, 0.94]
            })
            ds = ray.data.from_arrow(table)

            print(ds)

        .. testoutput::

            MaterializedDataset(
               num_blocks=1,
               num_rows=3,
               schema={food: string, price: double}
            )

.. _loading_datasets_from_distributed_df:

Loading data from distributed DataFrame libraries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Data interoperates with distributed data processing frameworks like
:ref:`Dask <dask-on-ray>`, :ref:`Spark <spark-on-ray>`, :ref:`Modin <modin-on-ray>`, and
:ref:`Mars <mars-on-ray>`.

.. note::

    The Ray Community provides these operations but may not actively maintain them. If you run into issues,
    create a GitHub issue `here <https://github.com/ray-project/ray/issues>`__.

.. tab-set::

    .. tab-item:: Dask

        To create a :class:`~ray.data.dataset.Dataset` from a
        `Dask DataFrame <https://docs.dask.org/en/stable/dataframe.html>`__, call
        :func:`~ray.data.from_dask`. This function constructs a
        ``Dataset`` backed by the distributed Pandas DataFrame partitions that underly
        the Dask DataFrame.

        .. testcode::

            import dask.dataframe as dd
            import pandas as pd
            import ray

            df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
            ddf = dd.from_pandas(df, npartitions=4)
            # Create a Dataset from a Dask DataFrame.
            ds = ray.data.from_dask(ddf)

            ds.show(3)

        .. testoutput::

            {'col1': 0, 'col2': '0'}
            {'col1': 1, 'col2': '1'}
            {'col1': 2, 'col2': '2'}

    .. tab-item:: Spark

        To create a :class:`~ray.data.dataset.Dataset` from a `Spark DataFrame
        <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html>`__,
        call :func:`~ray.data.from_spark`. This function creates a ``Dataset`` backed by
        the distributed Spark DataFrame partitions that underly the Spark DataFrame.

        ..
            TODO: This code snippet might not work correctly. We should test it.

        .. testcode::
            :skipif: True

            import ray
            import raydp

            spark = raydp.init_spark(app_name="Spark -> Datasets Example",
                                    num_executors=2,
                                    executor_cores=2,
                                    executor_memory="500MB")
            df = spark.createDataFrame([(i, str(i)) for i in range(10000)], ["col1", "col2"])
            ds = ray.data.from_spark(df)

            ds.show(3)

        .. testoutput::

            {'col1': 0, 'col2': '0'}
            {'col1': 1, 'col2': '1'}
            {'col1': 2, 'col2': '2'}

    .. tab-item:: Iceberg

        To create a :class:`~ray.data.dataset.Dataset` from an `Iceberg Table
        <https://iceberg.apache.org>`__,
        call :func:`~ray.data.read_iceberg`. This function creates a ``Dataset`` backed by
        the distributed files that underlie the Iceberg table.

        ..

        .. testcode::
            :skipif: True

            >>> import ray
            >>> from pyiceberg.expressions import EqualTo
            >>> ds = ray.data.read_iceberg(
            ...     table_identifier="db_name.table_name",
            ...     row_filter=EqualTo("column_name", "literal_value"),
            ...     catalog_kwargs={"name": "default", "type": "glue"}
            ... )


        .. testoutput::

            {'col1': 0, 'col2': '0'}
            {'col1': 1, 'col2': '1'}
            {'col1': 2, 'col2': '2'}

    .. tab-item:: Modin

        To create a :class:`~ray.data.dataset.Dataset` from a Modin DataFrame, call
        :func:`~ray.data.from_modin`. This function constructs a ``Dataset`` backed by
        the distributed Pandas DataFrame partitions that underly the Modin DataFrame.

        .. testcode::

            import modin.pandas as md
            import pandas as pd
            import ray

            df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
            mdf = md.DataFrame(df)
            # Create a Dataset from a Modin DataFrame.
            ds = ray.data.from_modin(mdf)

            ds.show(3)

        .. testoutput::

            {'col1': 0, 'col2': '0'}
            {'col1': 1, 'col2': '1'}
            {'col1': 2, 'col2': '2'}

    .. tab-item:: Mars

        To create a :class:`~ray.data.dataset.Dataset` from a Mars DataFrame, call
        :func:`~ray.data.from_mars`. This function constructs a ``Dataset``
        backed by the distributed Pandas DataFrame partitions that underly the Mars
        DataFrame.

        .. testcode::

            import mars
            import mars.dataframe as md
            import pandas as pd
            import ray

            cluster = mars.new_cluster_in_ray(worker_num=2, worker_cpu=1)

            df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
            mdf = md.DataFrame(df, num_partitions=8)
            # Create a tabular Dataset from a Mars DataFrame.
            ds = ray.data.from_mars(mdf)

            ds.show(3)

        .. testoutput::

            {'col1': 0, 'col2': '0'}
            {'col1': 1, 'col2': '1'}
            {'col1': 2, 'col2': '2'}

.. _loading_datasets_from_ml_libraries:

Loading data from ML libraries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Data interoperates with HuggingFace, PyTorch, and TensorFlow datasets.

.. tab-set::

    .. tab-item:: HuggingFace

        To convert a HuggingFace Dataset to a Ray Datasets, call
        :func:`~ray.data.from_huggingface`. This function accesses the underlying Arrow
        table and converts it to a Dataset directly.

        .. warning::
            :class:`~ray.data.from_huggingface` only supports parallel reads in certain
            instances, namely for untransformed public HuggingFace Datasets. For those datasets,
            Ray Data uses `hosted parquet files <https://huggingface.co/docs/datasets-server/parquet#list-parquet-files>`_
            to perform a distributed read; otherwise, Ray Data uses a single node read.
            This behavior shouldn't be an issue with in-memory HuggingFace Datasets, but may cause a failure with
            large memory-mapped HuggingFace Datasets. Additionally, HuggingFace `DatasetDict <https://huggingface.co/docs/datasets/en/package_reference/main_classes#datasets.DatasetDict>`_ and
            `IterableDatasetDict <https://huggingface.co/docs/datasets/en/package_reference/main_classes#datasets.IterableDatasetDict>`_
            objects aren't supported.

        .. testcode::

            import ray.data
            from datasets import load_dataset

            hf_ds = load_dataset("wikitext", "wikitext-2-raw-v1")
            ray_ds = ray.data.from_huggingface(hf_ds["train"])
            ray_ds.take(2)

        .. testoutput::
            :options: +MOCK

            [{'text': ''}, {'text': ' = Valkyria Chronicles III = \n'}]

    .. tab-item:: PyTorch

        To convert a PyTorch dataset to a Ray Dataset, call :func:`~ray.data.from_torch`.

        .. testcode::

            import ray
            from torch.utils.data import Dataset
            from torchvision import datasets
            from torchvision.transforms import ToTensor

            tds = datasets.CIFAR10(root="data", train=True, download=True, transform=ToTensor())
            ds = ray.data.from_torch(tds)

            print(ds)

        .. testoutput::
            :options: +MOCK

            Downloading https://www.cs.toronto.edu/~kriz/cifar-10-python.tar.gz to data/cifar-10-python.tar.gz
            100%|███████████████████████| 170498071/170498071 [00:07<00:00, 23494838.54it/s]
            Extracting data/cifar-10-python.tar.gz to data
            Dataset(num_rows=50000, schema={item: object})


    .. tab-item:: TensorFlow

        To convert a TensorFlow dataset to a Ray Dataset, call :func:`~ray.data.from_tf`.

        .. warning::
            :class:`~ray.data.from_tf` doesn't support parallel reads. Only use this
            function with small datasets like MNIST or CIFAR.

        .. testcode::

            import ray
            import tensorflow_datasets as tfds

            tf_ds, _ = tfds.load("cifar10", split=["train", "test"])
            ds = ray.data.from_tf(tf_ds)

            print(ds)

        ..
            The following `testoutput` is mocked to avoid illustrating download logs like
            "Downloading and preparing dataset 162.17 MiB".

        .. testoutput::
            :options: +MOCK

            MaterializedDataset(
               num_blocks=...,
               num_rows=50000,
               schema={
                  id: binary,
                  image: numpy.ndarray(shape=(32, 32, 3), dtype=uint8),
                  label: int64
               }
            )

Reading databases
=================

Ray Data reads from databases like MySQL, PostgreSQL, MongoDB, and BigQuery.

.. _reading_sql:

Reading SQL databases
~~~~~~~~~~~~~~~~~~~~~

Call :func:`~ray.data.read_sql` to read data from a database that provides a
`Python DB API2-compliant <https://peps.python.org/pep-0249/>`_ connector.

.. tab-set::

    .. tab-item:: MySQL

        To read from MySQL, install
        `MySQL Connector/Python <https://dev.mysql.com/doc/connector-python/en/>`_. It's the
        first-party MySQL database connector.

        .. code-block:: console

            pip install mysql-connector-python

        Then, define your connection logic and query the database.

        .. testcode::
            :skipif: True

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
            dataset = ray.data.read_sql("SELECT * FROM movie", create_connection)
            # Get movies after the year 1980
            dataset = ray.data.read_sql(
                "SELECT title, score FROM movie WHERE year >= 1980", create_connection
            )
            # Get the number of movies per year
            dataset = ray.data.read_sql(
                "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
            )


    .. tab-item:: PostgreSQL

        To read from PostgreSQL, install `Psycopg 2 <https://www.psycopg.org/docs>`_. It's
        the most popular PostgreSQL database connector.

        .. code-block:: console

            pip install psycopg2-binary

        Then, define your connection logic and query the database.

        .. testcode::
            :skipif: True

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
            dataset = ray.data.read_sql("SELECT * FROM movie", create_connection)
            # Get movies after the year 1980
            dataset = ray.data.read_sql(
                "SELECT title, score FROM movie WHERE year >= 1980", create_connection
            )
            # Get the number of movies per year
            dataset = ray.data.read_sql(
                "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
            )

    .. tab-item:: Snowflake

        To read from Snowflake, install the
        `Snowflake Connector for Python <https://docs.snowflake.com/en/user-guide/python-connector>`_.

        .. code-block:: console

            pip install snowflake-connector-python

        Then, define your connection logic and query the database.

        .. testcode::
            :skipif: True

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
            dataset = ray.data.read_sql("SELECT * FROM movie", create_connection)
            # Get movies after the year 1980
            dataset = ray.data.read_sql(
                "SELECT title, score FROM movie WHERE year >= 1980", create_connection
            )
            # Get the number of movies per year
            dataset = ray.data.read_sql(
                "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
            )


    .. tab-item:: Databricks

        To read from Databricks, set the ``DATABRICKS_TOKEN`` environment variable to
        your Databricks warehouse access token.

        .. code-block:: console

            export DATABRICKS_TOKEN=...

        If you're not running your program on the Databricks runtime, also set the
        ``DATABRICKS_HOST`` environment variable.

        .. code-block:: console

            export DATABRICKS_HOST=adb-<workspace-id>.<random-number>.azuredatabricks.net

        Then, call :func:`ray.data.read_databricks_tables` to read from the Databricks
        SQL warehouse.

        .. testcode::
            :skipif: True

            import ray

            dataset = ray.data.read_databricks_tables(
                warehouse_id='...',  # Databricks SQL warehouse ID
                catalog='catalog_1',  # Unity catalog name
                schema='db_1',  # Schema name
                query="SELECT title, score FROM movie WHERE year >= 1980",
            )

    .. tab-item:: BigQuery

        To read from BigQuery, install the
        `Python Client for Google BigQuery <https://cloud.google.com/python/docs/reference/bigquery/latest>`_ and the `Python Client for Google BigQueryStorage <https://cloud.google.com/python/docs/reference/bigquerystorage/latest>`_.

        .. code-block:: console

            pip install google-cloud-bigquery
            pip install google-cloud-bigquery-storage

        To read data from BigQuery, call :func:`~ray.data.read_bigquery` and specify the project id, dataset, and query (if applicable).

        .. testcode::
            :skipif: True

            import ray

            # Read the entire dataset. Do not specify query.
            ds = ray.data.read_bigquery(
                project_id="my_gcloud_project_id",
                dataset="bigquery-public-data.ml_datasets.iris",
            )

            # Read from a SQL query of the dataset. Do not specify dataset.
            ds = ray.data.read_bigquery(
                project_id="my_gcloud_project_id",
                query = "SELECT * FROM `bigquery-public-data.ml_datasets.iris` LIMIT 50",
            )

            # Write back to BigQuery
            ds.write_bigquery(
                project_id="my_gcloud_project_id",
                dataset="destination_dataset.destination_table",
                overwrite_table=True,
            )

.. _reading_mongodb:

Reading MongoDB
~~~~~~~~~~~~~~~

To read data from MongoDB, call :func:`~ray.data.read_mongo` and specify
the source URI, database, and collection. You also need to specify a pipeline to
run against the collection.

.. testcode::
    :skipif: True

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

Creating synthetic data
=======================

Synthetic datasets can be useful for testing and benchmarking.

.. tab-set::

    .. tab-item:: Int Range

        To create a synthetic :class:`~ray.data.Dataset` from a range of integers, call
        :func:`~ray.data.range`. Ray Data stores the integer range in a single column.

        .. testcode::

            import ray

            ds = ray.data.range(10000)

            print(ds.schema())

        .. testoutput::

            Column  Type
            ------  ----
            id      int64

    .. tab-item:: Tensor Range

        To create a synthetic :class:`~ray.data.Dataset` containing arrays, call
        :func:`~ray.data.range_tensor`. Ray Data packs an integer range into ndarrays of
        the provided shape.

        .. testcode::

            import ray

            ds = ray.data.range_tensor(10, shape=(64, 64))

            print(ds.schema())

        .. testoutput::

            Column  Type
            ------  ----
            data    numpy.ndarray(shape=(64, 64), dtype=int64)

Loading other datasources
==========================

If Ray Data can't load your data, subclass
:class:`~ray.data.Datasource`. Then, construct an instance of your custom
datasource and pass it to :func:`~ray.data.read_datasource`. To write results, you might
also need to subclass :class:`ray.data.Datasink`. Then, create an instance of your custom
datasink and pass it to :func:`~ray.data.Dataset.write_datasink`. For more details, see
:ref:`Advanced: Read and Write Custom File Types <custom_datasource>`.

.. testcode::
    :skipif: True

    # Read from a custom datasource.
    ds = ray.data.read_datasource(YourCustomDatasource(), **read_args)

    # Write to a custom datasink.
    ds.write_datasink(YourCustomDatasink())

Performance considerations
==========================

By default, the number of output blocks from all read tasks is dynamically decided
based on input data size and available resources. It should work well in most cases.
However, you can also override the default value by setting the ``override_num_blocks``
argument. Ray Data decides internally how many read tasks to run concurrently to best
utilize the cluster, ranging from ``1...override_num_blocks`` tasks. In other words,
the higher the ``override_num_blocks``, the smaller the data blocks in the Dataset and
hence more opportunities for parallel execution.

For more information on how to tune the number of output blocks and other suggestions
for optimizing read performance, see `Optimizing reads <performance-tips.html#optimizing-reads>`__.
