.. _loading-data:

============
Loading data
============

Reading files
=============

Reading files from local storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. testcode::

    import ray

    ds = ray.data.read_images("s3://anonymous@air-example-data/AnimalDetection")

.. warning::

    If you're running Ray on a multi-node cluster and you downloaded your data to the head
    node, specify the `local://` scheme.

Reading files from cloud storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tab-set::

    .. tab-item:: AWS

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@air-example-data/AnimalDetection")

    .. tab-item:: GCP

        .. code-block:: python

            import gcsfs

            # Create a tabular Dataset by reading a Parquet file from GCS, passing the configured
            # GCSFileSystem.
            # NOTE: This example is not runnable as-is; you need to point it at your GCS bucket
            # and configure your GCP project and credentials.
            path = "gs://path/to/file.parquet"
            filesystem = gcsfs.GCSFileSystem(project="my-google-project")
            ds = ray.data.read_parquet(path, filesystem=filesystem)

    .. tab-item:: Azure

        .. code-block:: python

            import adlfs

            # Create a tabular Dataset by reading a Parquet file from Azure Blob Storage, passing
            # the configured AzureBlobFileSystem.
            path = (
                "az://nyctlc/yellow/puYear=2009/puMonth=1/"
                "part-00019-tid-8898858832658823408-a1de80bd-eed3-4d11-b9d4-fa74bfbd47bc-426333-4"
                ".c000.snappy.parquet"
            )
            ds = ray.data.read_parquet(
                path,
                filesystem=adlfs.AzureBlobFileSystem(account_name="azureopendatastorage")
            )

Reading files from HDFS
~~~~~~~~~~~~~~~~~~~~~~~

.. testcode::

    import pyarrow as pa

    # Create a tabular Dataset by reading a Parquet file from HDFS, manually specifying a
    # configured HDFS connection via a Pyarrow HDFSFileSystem instance.
    # NOTE: This example is not runnable as-is; you'll need to point it at your HDFS
    # cluster/data.
    ds = ray.data.read_parquet(
        "hdfs://path/to/file.parquet",
        filesystem=pa.fs.HDFSFileSystem(host="localhost", port=9000, user="bob"),
    )

Handling compressed files
~~~~~~~~~~~~~~~~~~~~~~~~~

.. testcode::

    # Read a gzip-compressed CSV file from S3.
    ds = ray.data.read_csv(
        "s3://anonymous@air-example-data/gzip_compressed.csv",
        arrow_open_stream_args={"compression": "gzip"},
    )

Loading in-memory data
======================

Common libraries
~~~~~~~~~~~~~~~~

.. tab-set::

    .. tab-item:: Python objects

        .. testcode::

            import ray

            ds = ray.data.from_items([
                {"food": "spam", "number": 0},
                {"food": "ham", "number": 1},
                {"food": "eggs", "number": 2}
            ])
            print(ds)

        .. testoutput::

            TODO

    .. tab-item:: NumPy



    .. tab-item:: pandas

        ham

    .. tab-item:: PyArrow

        ham

Distributed DataFrame libraries
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. tab-set::

    .. tab-item:: Dask

        spam

    .. tab-item:: Modin

        ham

    .. tab-item:: Spark

        ham

    .. tab-item:: Mars

        ham

ML-related libraries
~~~~~~~~~~~~~~~~~~~~

.. tab-set::

    .. tab-item:: HuggingFace

        You can convert 🤗 Datasets into Ray Datasets by using from_huggingface. This
        function accesses the underlying Arrow table and converts it into a Ray Dataset
        directly.



        ham

    .. tab-item:: TensorFlow

        ham

    .. tab-item:: Torch

        ham

.. _datasets-databases:

Reading databases
=================

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

        Then, define your connection login and query the dataset.

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

Reading MongoDB
~~~~~~~~~~~~~~~

A Dataset can also be created from MongoDB with read_mongo. This interacts with MongoDB
similar to external filesystems, except here you will need to specify the MongoDB source
by its uri, database and collection, and specify a pipeline to run against the
collection. The execution results are then used to create a Dataset.

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

Loading unsupported data
========================
