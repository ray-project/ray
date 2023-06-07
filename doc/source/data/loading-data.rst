.. _loading_data:

============
Loading Data
============

Ray Data loads data from various sources. This guide shows you how to:

* `Read files <#reading-files>`_ like images
* `Load in-memory data <#loading-in-memory-data>`_ like pandas DataFrames
* `Read databases <#reading-databases>`_ like MySQL

To view the full list of supported sources, see the :ref:`Input/Output reference <input-output>`.

Reading files
=============

Ray Data reads files from disk or cloud storage.

Reading files from disk
~~~~~~~~~~~~~~~~~~~~~~~

To read files from disk, call a function like ``ray.data.read_*`` and specify paths with
the ``local://`` schema. Paths can point to files or directories.

.. tip::

    If your files are accessible on every node, exclude ``local://`` to optimize
    reads.

.. tab-set::

    .. tab-item:: Images

        To read images, call :func:`~ray.data.read_images`. Ray Data represents images
        as NumPy ndarrays.

        .. testcode::

            import ray

            ds = ray.data.read_images("/tmp/batoidea/JPEGImages")

            print(ds.schema())

        .. testoutput::

            Column  Type
            ------  ----
            image   numpy.ndarray(shape=(32, 32, 3), dtype=uint8)

        To read other file formats, see the :ref:`Input/Output reference <input-output>`.

    .. tab-item:: Text

        To read lines of text, call :func:`~ray.data.read_text`.

        .. testcode::

            import ray

            ds = ray.data.read_text("/tmp/this.txt")

            print(ds.schema())

        .. testoutput::

            Column  Type
            ------  ----
            text    string

        To read other file formats, see the :ref:`Input/Output reference <input-output>`.

    .. tab-item:: Parquet

        To read Parquet files, call :func:`~ray.data.read_parquet`.

        .. testcode::

            import ray

            ds = ray.data.read_parquet("/tmp/iris.parquet")

            print(ds.schema())

        .. testoutput::

            Column        Type
            ------        ----
            sepal.length  double
            sepal.width   double
            petal.length  double
            petal.width   double
            variety       string

        To read other file formats, see the :ref:`Input/Output reference <input-output>`.

    .. tab-item:: CSV

        To read Parquet files, call :func:`~ray.data.read_csv`.

        .. testcode::

            import ray

            ds = ray.data.read_csv("/tmp/iris.csv")

            print(ds.schema())

        .. testoutput::

            Column             Type
            ------             ----
            sepal length (cm)  double
            sepal width (cm)   double
            petal length (cm)  double
            petal width (cm)   double
            target             int64

        To read other file formats, see the :ref:`Input/Output reference <input-output>`.

Reading files from S3
~~~~~~~~~~~~~~~~~~~~~

To read files in S3, ensure all nodes are authenticated with AWS. Then, call a function
like ``ray.data.read_*`` and specify S3 URIs. URIs can point to objects, buckets, or
folders.

.. tab-set::

    .. tab-item:: Images

        To read images, call :func:`~ray.data.read_images`. Ray Data represents images
        as NumPy ndarrays.

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/batoidea/JPEGImages/")

            print(ds.schema())

        .. testoutput::

            Column  Type
            ------  ----
            image   numpy.ndarray(shape=(32, 32, 3), dtype=uint8)

        To read other file formats, see the :ref:`Input/Output reference <input-output>`.

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

        To read other file formats, see the :ref:`Input/Output reference <input-output>`.

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

        To read other file formats, see the :ref:`Input/Output reference <input-output>`.

    .. tab-item:: CSV

        To read Parquet files, call :func:`~ray.data.read_csv`.

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

        To read other file formats, see the :ref:`Input/Output reference <input-output>`.

Handling compressed files
~~~~~~~~~~~~~~~~~~~~~~~~~

To read a compressed file, specify ``compression`` in ``arrow_open_stream_args``.
You can use any `Codec supported by Arrow <https://arrow.apache.org/docs/python/generated/pyarrow.CompressedInputStream.html>`__.

.. testcode::

    import ray

    ds = ray.data.read_csv(
        "s3://anonymous@ray-example-data/iris.csv.gz",
        arrow_open_stream_args={"compression": "gzip"},
    )

Loading in-memory data
======================

Loading data from common libraries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Loading data from distributed DataFrame libraries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Data interoperates with distributed data processing frameworks like
:ref:`Dask <dask-on-ray>`, :ref:`Spark <spark-on-ray>`, :ref:`Modin <modin-on-ray>`, and
:ref:`Mars <mars-on-ray>`.

.. tab-set::

    .. tab-item:: Dask

        To create a :class:`~ray.data.dataset.Dataset` from a
        `Dask DataFrame <https://docs.dask.org/en/stable/dataframe.html>`__, call
        :func:`~ray.data.from_dask`. This function constructs a
        ``Dataset`` backed by the distributed Pandas DataFrame partitions that underly
        the Dask DataFrame.

        .. testcode::
            :skipif: True

            import dask.dataframe as dd
            import pandas as pd
            import ray

            df = pd.DataFrame({"col1": list(range(10000)), "col2": list(map(str, range(10000)))})
            ddf = dd.from_pandas(df, npartitions=4)
            # Create a Dataset from a Dask DataFrame.
            ds = ray.data.from_dask(ddf)

            ds.show(3)

        .. testoutput::

            {'string': 'spam', 'number': 0}
            {'string': 'ham', 'number': 1}
            {'string': 'eggs', 'number': 2}

    .. tab-item:: Spark

        To create a :class:`~ray.data.dataset.Dataset` from a `Spark DataFrame
        <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html>`__,
        call :func:`~ray.data.from_spark`. This function creates a ``Dataset`` backed by
        the distributed Spark DataFrame partitions that underly the Spark DataFrame.

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

    .. tab-item:: Modin

        To create a :class:`~ray.data.dataset.Dataset` from a Modin DataFrame, call
        :func:`~ray.data.from_modin`. This function constructs a ``Dataset`` backed by
        the distributed Pandas DataFrame partitions that underly the Modin DataFrame.

        .. testcode::
            :skipif: True

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
            :skipif: True

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

Loading data from ML libraries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Data interoperates with HuggingFace and TensorFlow datasets.

.. tab-set::

    .. tab-item:: HuggingFace

        To convert a 🤗 Dataset to a Ray Datasets, call
        :func:`~ray.data.from_huggingface`. This function accesses the underlying Arrow
        table and converts it to a Dataset directly.

        .. warning::
            :class:`~ray.data.from_huggingface` doesn't support parallel
            reads. This isn't an issue with in-memory 🤗 Datasets, but may fail with
            large memory-mapped 🤗 Datasets. Also, 🤗 ``IterableDataset`` objects aren't
            supported.

        .. testcode::

            import ray.data
            from datasets import load_dataset

            hf_ds = load_dataset("wikitext", "wikitext-2-raw-v1")
            ray_ds = ray.data.from_huggingface(hf_ds)
            ray_ds["train"].take(2)

        .. testoutput::
            :options: +MOCK

            [{'text': ''}, {'text': ' = Valkyria Chronicles III = \n'}]

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

        .. testoutput::

            MaterializedDataset(num_blocks=200, num_rows=50000, schema={id: binary, image: numpy.ndarray(shape=(32, 32, 3), dtype=uint8), label: int64})

Reading databases
=================

Ray Data reads from databases like MySQL, Postgres, and MongoDB.

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
            dataset = ray.data.read_sql("SELECT * FROM movie", create_connection)
            # Get movies after the year 1980
            dataset = ray.data.read_sql(
                "SELECT title, score FROM movie WHERE year >= 1980", create_connection
            )
            # Get the number of movies per year
            dataset = ray.data.read_sql(
                "SELECT year, COUNT(*) FROM movie GROUP BY year", create_connection
            )

    .. tab-item:: BigQuery

        To read from BigQuery, install the
        `Python Client for Google BigQuery <https://cloud.google.com/python/docs/reference/bigquery/latest>`_.
        This package includes a DB API2-compliant database connector.

        .. code-block:: console

            pip install google-cloud-bigquery

        Then, define your connection logic and query the dataset.

        .. code-block:: python

            from google.cloud import bigquery
            from google.cloud.bigquery import dbapi

            import ray

            def create_connection():
                client = bigquery.Client(...)
                return dbapi.Connection(client)

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

Reading NoSQL databases
~~~~~~~~~~~~~~~~~~~~~~~

Ray Data reads from MongoDB.

.. tab-set::

    .. tab-item:: MongoDB

        To read data from MongoDB, call :func:`~ray.data.read_mongo` and specify the
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

Loading unsupported data
========================

If Ray Data can't load your data, subclass
:class:`~ray.data.datasource.Datasource`. Then, construct an instance of your custom
datasource and pass it to :func:`~ray.data.read_datasource`.

.. testcode::
    :skipif: True

    # Read from a custom datasource.
    ds = ray.data.read_datasource(YourCustomDatasource(), **read_args)

    # Write to a custom datasource.
    ds.write_datasource(YourCustomDatasource(), **write_args)

For an example, see :ref:`Implementing a Custom Datasource <custom_datasources>`.
