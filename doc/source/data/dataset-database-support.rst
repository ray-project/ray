.. _dataset_database_support:

.. _Python DB API 2: https://peps.python.org/pep-0249/

======================================================
Working with databases, data warehouses and data lakes
======================================================

Ray :class:`Datasets <ray.data.Dataset>` can be read from:
* Any table or query from a database with a `Python DB API 2`_ library
* Databricks tables or SQL queries
* Snowflake tables or SQL queries
* MongoDB documents or document queries

Ray :class:`Datasets <ray.data.Dataset>` can be written to:
* Any table in a database with a compliant `Python DB API 2`_  library
* Delta Lake tables
* Snowflake tables
* MongoDB documents

.. note::
    This guide surveys the current database integrations. If none of these meet your
    needs, please reach out to us on `Discourse <https://discuss.ray.io/>`__ or open a feature
    request on the `Ray GitHub repo <https://github.com/ray-project/ray>`__, and check out
    our :ref:`guide for implementing a custom Datasets datasource <custom_datasources>`
    if you're interested in rolling your own integration!

.. _dataset_db_api2:

----------------------------------
From an DB API 2 compliant library
----------------------------------
Any database that provides a `Python DB API 2`_ compliant library can be read from and written 
to using Ray Datasets with the :py:class:`~ray.data.read_dbapi2` 
and :py:class:`~ray.data.Dataset.write_dbapi2` methods.

Connection properties
==========================
Connection properties need to be provided to these methods. The 
required properties depend on the underlying databases connection properties, 
but typically ``user`` and ``password`` are required. 

Below are examples of creating connect properties in code, loading them with yaml or 
reading them from the environment.

.. tabbed:: Set in code
  .. warning:: 
      For security, avoid putting password and other sensitive properties 
      directly in the code.

  .. code-block:: python
      # set directly in code
      connect_props = dict(
        user = 'MY_USER',
        password = 'MY_PASSWORD',
        database = 'MY_DATABASE'
      )

.. tabbed:: Load from yaml file
  .. code-block:: python
      import yaml

      # load from yaml file
      with open("connect_props.yaml", 'r') as stream:
        connect_props = yaml.safe_load(stream)

.. tabbed:: Load from environment
  .. code-block:: python

      import os
      prefix = 'DB_CONNECT_'

      # load all properties that are prefixed
      connect_props = {
          key.replace(prefix,'').lower(): value \
          for key,value in os.environ.items() if prefix in key
      }

Reading and writing
===================
A dataset can be read from a database using the :py:class:`~ray.data.read_dbapi2` method,
and written to a database using the :py:class:`~ray.data.Dataset.write_dbapi2` method.
Both methods require that connection properties and the native DB API 2 connect function 
for the database be provided. 

The below are exmaples of how to import various database specific connect functions.

.. tabbed:: SQLite
    .. note::
        SQLite is part of the standard Python packages, and does not need to be installed.

    .. warning::
        For parallel reads to work with SQLite on a multi-instance cluster, 
        the database needs to be located in a shared storage location accessible 
        to all nodes in the cluster. This could be a storage device mounted to 
        all nodes in the cluster with NFS. 

    .. code-block:: python
        from sqlite3 import connect as connect_fn

.. tabbed:: Postgres
    .. note::
        In order to read and write to Postgress, a DBA API 2 compliant Postgres library 
        must be installed onto all nodes of the cluster.

    .. code-block:: python
        from psycopg2 import connect as connect_fn

.. tabbed:: MySQL
    .. note::
        In order to read and write to MySql, a DBA API 2 compliant MySql library must be installed 
        onto all nodes of the cluster.

    .. code-block:: python
        from mysql.connector import connect as connect_fn

The example below shows how to read and write from and to a database. 
For reading from the database, a table name or a full query can be specified. For writing, 
the name of a table that has already been created in the database must be provided.

.. code-block:: python
    from ray.data import read_dbapi2

    # read all columns and rows from a table
    dataset = ray.data.read_dbapi2(
      connect_fn, connect_props, 
      table='my_src_table'
    )
    # read with a query, specifying two columns 
    # and filtering rows with a where clause
    dataset = ray.data.read_dbapi2(
      connect_fn, connect_props, 
      query='SELECT col1, col2 FROM my_src_table WHERE col1 > 2'
    )

    # write the dataset back to another table
    dataset.write_dbapi2(
      connect_fn, connect_props, 
      table='my_dest_table'
    )

Controlling parallelism
=======================
Ray datasets are read and written to and from the cluster in parallel to the database. 
For reading, a query is created using standard SQL LIMIT and OFFSET semantics, 
and each of these queries is then issued by a seperate Ray task according to the parallism specified.

For writing, a write task is created for each partition of the dataset. To control
the number of parallel write operations the dataset can be repartitioned prior to writing.

If needed, a dataset can be written to staging tables, prior to being copied into 
a single destination table. To enable this, you need to specify ``stage`` mode when writing. 
This will cause each partition in the dataset to be written to an individual stage table. 
Stage tables are named according to the destination table name and a suffix of 
`_stage_<partition number>`.  The staging tables are created prior to writing. After all partitions
are written to a satge table, the stages are then copied to the destination table. After the destination
table has been copied to from all stages, the staging tables are then dropped.

The example below shows how to specify parallelism when reading and writing, and 
how to turn on staging for writes.

.. code-block:: python

  # read from an entire table with 100 read tasks
  dataset = ray.data.read_dbapi2(
    connect_fn, connect_props, parallelism=100
    table='my_src_table'
  )

  # write to a table with 20 write tasks
  dataset.repartition(20).write_dbapi2(
    connect_fn, connect_props, 
    table='my_dest_table'
  )

  # write to staging tables prior to final copy to destination table
  dataset.write_dbapi2(
    connect_fn, connect_props, 
    table='my_dest_table',
    mode='stage'
  )

.. _datasets_snowflake:

For more control over staging table creation and cleanup, you can use 
the :py:class:`~ray.data.datasource.database.DBAPI2Datasource` and the 
:py:class:`~ray.data.read_datasource` method and :py:class:`~ray.data.Dataset.write_datasource` method
and specify custom queries. See the :py:class:`~ray.data.datasource.database.DBAPI2Datasource` 
API documentation for more details.

---------------
From Databricks
---------------
To take advantage of optimisations to read from and write to Databricks, use the 
:py:class:`~ray.data.read_databricks` and :py:class:`~ray.data.Dataset.write_databricks` methods.

Connection properties
=====================
The minimal required connection properties for Databricks are `user`, `password`, `account` and 
`warehouse`. To use API keys instead of `password`, functionality to load Snowflake API keys is 
also provided. API keys can be loaded from a file specified by the `private_key_file` 
property, or can be passed directly via the `private_key` property. 
If the key is password protected, the password can be given via the `pk_password` property.  
Optional properties like database and schema can also be provided at construction or be included 
in the fully specified table name of format `db.schema.table` when calling read or write methods.

.. code-block:: python
    import os
    # read login properties from environment
    prefix = 'DATABRICKS_'
    login_props = {
        key.replace(prefix,'').lower(): value 
        for key,value in os.environ.items() if prefix in key
    }

Reading
===================================
Ray data uses the `Databricks Connect API`_ `execute`_ method to parallelize 
loading the results of queries across the cluster using `LIMIT` and `OFFSET`.

.. image:: images/snowflake_read_table.png
  :width: 200

.. warning::
  The `get_result_batches`_ has no way to specify the number of batches returned. Setting parallism 
  during :py:class:`~ray.data.read_snowflake` will have no affect on the number of read tasks.

The code below will read in a sample customer table from the Snowflake sample database.

.. code-block:: python
  from ray.data import read_snowflake

  # read entire customer table
  ds = read_snowflake(connect_props, table='customer`)

  # query specific columns with a weher clause
  ds = read_snowflake(
    connect_props, 
    query='SELECT c_acctbal, c_mktsegment FROM customer WHERE c_acctbal < 0`
  )

Additional read parameters
==========================
The native `Snowflake Python API`_  arguments are also available when reading. 
The `timeout` and `params` arguments may be used in the 
 method.

.. code-block:: python
    ds = read_snowflake(
      connect_props, 
      query='SELECT c_acctbal, c_mktsegment FROM customer WHERE c_acctbal < ?`,
      params = [0],
      timeout = 100
    )

Writing
=======
Ray data uses the `Snowflake Python API`_  `write_pandas`_ method to write Ray datasets to 
Snowflake tables. Each partition in the Ray dataset will call this method in parallel. 
`write_pandas`_ method will write data to a Snowflake stage, and then upon successful 
write to the stage, copy the data into the destination table.

.. image:: images/snowflake_write_table.png)
  :width: 200

.. code-block:: python
    ds.write_snowflake(connect_props, table='my_db.my_schema.my_table')

Additional write parameters
===========================
The native `Snowflake Python API`_  arguments are also available from the `write_pandas`_ method.
- ``auto_create_table``: When true, will automatically create a table with corresponding columns for each column in the passed in DataFrame. The table will not be created if it already exists
- ``overwrite``: When true, and if auto_create_table is true, then it drops the table. Otherwise, it truncates the table. In both cases it will replace the existing contents of the table with that of the passed in Pandas DataFrame.
- ``table_type``: The table type of to-be-created table. 
The supported table types include ``temp``/``temporary`` and ``transient``. 
Empty means permanent table as per SQL convention.

In the example below, we use the `auto_create_table` parameter to create the output table before writing.

.. code-block:: python
    ds.write_datasource(table='my_db.my_schema.my_table', auto_create_table=True)

--------------
From Snowflake
--------------
The `Snowflake Python API`_ includes extra methods to read and write in 
parallel more eficiently. To take advantage of these optimisations, use the 
:py:class:`~ray.data.read_snowflake` and :py:class:`~ray.data.Dataset.write_snowflake` methods.

.. _Snowflake Python API: https://docs.snowflake.com/en/user-guide/python-connector.html
.. _get_result_batches: https://docs.snowflake.com/en/user-guide/python-connector-api.html#get_result_batches
.. _write_pandas: https://docs.snowflake.com/en/user-guide/python-connector-api.html#write_pandas

Connection properties
=====================
The minimal required connection properties for Snowflake are `user`, `password`, `account` and 
`warehouse`. To use API keys instead of `password`, functionality to load Snowflake API keys is 
also provided. API keys can be loaded from a file specified by the `private_key_file` 
property, or can be passed directly via the `private_key` property. 
If the key is password protected, the password can be given via the `pk_password` property.  
Optional properties like database and schema can also be provided at construction or be included 
in the fully specified table name of format `db.schema.table` when calling read or write methods.

.. code-block:: python
    import os
    # read login properties from environment
    prefix = 'SNOWFLAKE_'
    login_props = {
        key.replace(prefix,'').lower(): value 
        for key,value in os.environ.items() if prefix in key
    }

    # use the snowflake sample db/schema and default warehouse
    connect_props = dict (
      database = 'snowflake_sample_data'
      schema = 'tpch_sf1'
      warehouse='compute_wh',
       **login_props 
    )

Reading
===================================
Ray data uses the `Snowflake Python API`_ `get_result_batches`_ method to parallelize 
loading the results of queries across the cluster.

.. image:: images/snowflake_read_table.png
  :width: 200

.. warning::
  The `get_result_batches`_ has no way to specify the number of batches returned. Setting parallism 
  during :py:class:`~ray.data.read_snowflake` will have no affect on the number of read tasks.

The code below will read in a sample customer table from the Snowflake sample database.

.. code-block:: python
  from ray.data import read_snowflake

  # read entire customer table
  ds = read_snowflake(connect_props, table='customer`)

  # query specific columns with a weher clause
  ds = read_snowflake(
    connect_props, 
    query='SELECT c_acctbal, c_mktsegment FROM customer WHERE c_acctbal < 0`
  )

Additional read parameters
==========================
The native `Snowflake Python API`_  arguments are also available when reading. 
The `timeout` and `params` arguments may be used in the 
 method.

.. code-block:: python
    ds = read_snowflake(
      connect_props, 
      query='SELECT c_acctbal, c_mktsegment FROM customer WHERE c_acctbal < ?`,
      params = [0],
      timeout = 100
    )

Writing
=======
Ray data uses the `Snowflake Python API`_  `write_pandas`_ method to write Ray datasets to 
Snowflake tables. Each partition in the Ray dataset will call this method in parallel. 
`write_pandas`_ method will write data to a Snowflake stage, and then upon successful 
write to the stage, copy the data into the destination table.

.. image:: images/snowflake_write_table.png)
  :width: 200

.. code-block:: python
    ds.write_snowflake(connect_props, table='my_db.my_schema.my_table')

Additional write parameters
===========================
The native `Snowflake Python API`_  arguments are also available from the `write_pandas`_ method.
- ``auto_create_table``: When true, will automatically create a table with corresponding columns for each column in the passed in DataFrame. The table will not be created if it already exists
- ``overwrite``: When true, and if auto_create_table is true, then it drops the table. Otherwise, it truncates the table. In both cases it will replace the existing contents of the table with that of the passed in Pandas DataFrame.
- ``table_type``: The table type of to-be-created table. 
The supported table types include ``temp``/``temporary`` and ``transient``. 
Empty means permanent table as per SQL convention.

In the example below, we use the `auto_create_table` parameter to create the output table before writing.

.. code-block:: python
    ds.write_datasource(table='my_db.my_schema.my_table', auto_create_table=True)