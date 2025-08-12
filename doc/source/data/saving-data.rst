.. _saving-data:

===========
Saving Data
===========

Ray Data lets you save data in files or other Python objects.

This guide shows you how to:

* `Write data to files <#writing-data-to-files>`_
* `Convert Datasets to other Python libraries <#converting-datasets-to-other-python-libraries>`_

Writing data to files
=====================

Ray Data writes to local disk and cloud storage.

Writing data to local disk
~~~~~~~~~~~~~~~~~~~~~~~~~~

To save your :class:`~ray.data.dataset.Dataset` to local disk, call a method
like :meth:`Dataset.write_parquet <ray.data.Dataset.write_parquet>`  and specify a local
directory with the `local://` scheme.

.. warning::

    If your cluster contains multiple nodes and you don't use `local://`, Ray Data
    writes different partitions of data to different nodes.

.. testcode::

    import ray

    ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

    ds.write_parquet("local:///tmp/iris/")

To write data to formats other than Parquet, read the
:ref:`Input/Output reference <input-output>`.

Writing data to cloud storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To save your :class:`~ray.data.dataset.Dataset` to cloud storage, authenticate all nodes
with your cloud service provider. Then, call a method like
:meth:`Dataset.write_parquet <ray.data.Dataset.write_parquet>` and specify a URI with
the appropriate scheme. URI can point to buckets or folders.

To write data to formats other than Parquet, read the :ref:`Input/Output reference <input-output>`.

.. tab-set::

    .. tab-item:: S3

        To save data to Amazon S3, specify a URI with the ``s3://`` scheme.

        .. testcode::
            :skipif: True

            import ray

            ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

            ds.write_parquet("s3://my-bucket/my-folder")

        Ray Data relies on PyArrow to authenticate with Amazon S3. For more on how to configure
        your credentials to be compatible with PyArrow, see their
        `S3 Filesystem docs <https://arrow.apache.org/docs/python/filesystems.html#s3>`_.

    .. tab-item:: GCS

        To save data to Google Cloud Storage, install the
        `Filesystem interface to Google Cloud Storage <https://gcsfs.readthedocs.io/en/latest/>`_

        .. code-block:: console

            pip install gcsfs

        Then, create a ``GCSFileSystem`` and specify a URI with the ``gcs://`` scheme.

        .. testcode::
            :skipif: True

            import ray

            ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

            filesystem = gcsfs.GCSFileSystem(project="my-google-project")
            ds.write_parquet("gcs://my-bucket/my-folder", filesystem=filesystem)

        Ray Data relies on PyArrow for authentication with Google Cloud Storage. For more on how
        to configure your credentials to be compatible with PyArrow, see their
        `GCS Filesystem docs <https://arrow.apache.org/docs/python/filesystems.html#google-cloud-storage-file-system>`_.

    .. tab-item:: ABS

        To save data to Azure Blob Storage, install the
        `Filesystem interface to Azure-Datalake Gen1 and Gen2 Storage <https://pypi.org/project/adlfs/>`_

        .. code-block:: console

            pip install adlfs

        Then, create a ``AzureBlobFileSystem`` and specify a URI with the ``az://`` scheme.

        .. testcode::
            :skipif: True

            import ray

            ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

            filesystem = adlfs.AzureBlobFileSystem(account_name="azureopendatastorage")
            ds.write_parquet("az://my-bucket/my-folder", filesystem=filesystem)

        Ray Data relies on PyArrow for authentication with Azure Blob Storage. For more on how
        to configure your credentials to be compatible with PyArrow, see their
        `fsspec-compatible filesystems docs <https://arrow.apache.org/docs/python/filesystems.html#using-fsspec-compatible-filesystems-with-arrow>`_.

Writing data to NFS
~~~~~~~~~~~~~~~~~~~

To save your :class:`~ray.data.dataset.Dataset` to NFS file systems, call a method
like :meth:`Dataset.write_parquet <ray.data.Dataset.write_parquet>` and specify a
mounted directory.

.. testcode::
    :skipif: True

    import ray

    ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

    ds.write_parquet("/mnt/cluster_storage/iris")

To write data to formats other than Parquet, read the
:ref:`Input/Output reference <input-output>`.

.. _changing-number-output-files:

Changing the number of output files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you call a write method, Ray Data writes your data to several files. To control the
number of output files, configure ``min_rows_per_file``.

.. note::

    ``min_rows_per_file`` is a hint, not a strict limit. Ray Data might write more or
    fewer rows to each file. Under the hood, if the number of rows per block is
    larger than the specified value, Ray Data writes
    the number of rows per block to each file.


.. testcode::

    import os
    import ray

    ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
    ds.write_csv("/tmp/few_files/", min_rows_per_file=75)

    print(os.listdir("/tmp/few_files/"))

.. testoutput::
    :options: +MOCK

    ['0_000001_000000.csv', '0_000000_000000.csv', '0_000002_000000.csv']


Writing into Partitioned Dataset
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When writing partitioned dataset (using Hive-style, folder-based partitioning) it's recommended to repartition the dataset by the partition columns prior to writing into it. 
This allows you to *have the control over the file-sizes and their number*. When the dataset is repartitioned by the partition columns every block should contain all of the rows corresponding to particular partition, 
meaning that the number of files created should be controlled based on the configuration provided to, 
for example, `write_parquet` method (such as `min_rows_per_file`, `max_rows_per_file`). 
Since every block is written out independently, when writing the dataset without prior 
repartitioning you could potentially get an N number of files per partition 
(where N is the number of blocks in your dataset) with very limited ability to control the 
number of files & their sizes (since every block could potentially carry the rows corresponding to any partition).

.. testcode::
    import ray
    import pandas as pd
    from ray.data import DataContext
    from ray.data.context import ShuffleStrategy

    def print_directory_tree(start_path: str) -> None:
        """
        Prints the directory tree structure starting from the given path.
        """
        for root, dirs, files in os.walk(start_path):
            level = root.replace(start_path, '').count(os.sep)
            indent = ' ' * 4 * (level)
            print(f'{indent}{os.path.basename(root)}/')
            subindent = ' ' * 4 * (level + 1)
            for f in files:
                print(f'{subindent}{f}')

    # Sample dataset that we’ll partition by ``city`` and ``year``.
    df = pd.DataFrame(
        {
            "city": ["SF", "SF", "NYC", "NYC", "SF", "NYC", "SF", "NYC"],
            "year": [2023, 2024, 2023, 2024, 2023, 2023, 2024, 2024],
            "sales": [100, 120, 90, 115, 105, 95, 130, 110],
        }
    )

    ds = ray.data.from_pandas(df)
    DataContext.shuffle_strategy=ShuffleStrategy.HASH_SHUFFLE

    # ── Partitioned write ──────────────────────────────────────────────────────
    # 1. Repartition so all rows with the same (city, year) land in the same
    #    block – this minimises shuffling during the write.
    # 2. Pass the same columns to ``partition_cols`` so Ray creates a
    #    Hive-style directory layout:  city=<value>/year=<value>/....
    # 3. Use ``min_rows_per_file`` / ``max_rows_per_file`` to control how many
    #    rows Ray puts in each Parquet file.
    ds.repartition(keys=["city", "year"], num_blocks=4).write_parquet(
        "/tmp/sales_partitioned",
        partition_cols=["city", "year"],
        min_rows_per_file=2,     # At least 2 rows in each file …
        max_rows_per_file=3,     # … but never more than 3.
    )

    print_directory_tree("/tmp/sales_partitioned")

.. testoutput::
    :options: +MOCK

    sales_partitioned/
        city=NYC/
            year=2024/
                1_a2b8b82cd2904a368ec39f42ae3cf830_000000_000000-0.parquet
            year=2023/
                1_a2b8b82cd2904a368ec39f42ae3cf830_000001_000000-0.parquet
        city=SF/
            year=2024/
                1_a2b8b82cd2904a368ec39f42ae3cf830_000000_000000-0.parquet
            year=2023/
                1_a2b8b82cd2904a368ec39f42ae3cf830_000001_000000-0.parquet


Converting Datasets to other Python libraries
=============================================

Converting Datasets to pandas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To convert a :class:`~ray.data.dataset.Dataset` to a pandas DataFrame, call
:meth:`Dataset.to_pandas() <ray.data.Dataset.to_pandas>`. Your data must fit in memory
on the head node.

.. testcode::

    import ray

    ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

    df = ds.to_pandas()
    print(df)

.. testoutput::
    :options: +NORMALIZE_WHITESPACE

         sepal length (cm)  sepal width (cm)  ...  petal width (cm)  target
    0                  5.1               3.5  ...               0.2       0
    1                  4.9               3.0  ...               0.2       0
    2                  4.7               3.2  ...               0.2       0
    3                  4.6               3.1  ...               0.2       0
    4                  5.0               3.6  ...               0.2       0
    ..                 ...               ...  ...               ...     ...
    145                6.7               3.0  ...               2.3       2
    146                6.3               2.5  ...               1.9       2
    147                6.5               3.0  ...               2.0       2
    148                6.2               3.4  ...               2.3       2
    149                5.9               3.0  ...               1.8       2
    <BLANKLINE>
    [150 rows x 5 columns]

Converting Datasets to distributed DataFrames
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Data interoperates with distributed data processing frameworks like `Daft <https://www.getdaft.io>`_,
:ref:`Dask <dask-on-ray>`, :ref:`Spark <spark-on-ray>`, :ref:`Modin <modin-on-ray>`, and
:ref:`Mars <mars-on-ray>`.

.. tab-set::

    .. tab-item:: Daft

        To convert a :class:`~ray.data.dataset.Dataset` to a `Daft Dataframe <https://docs.getdaft.io/en/stable/api/dataframe/>`_, call
        :meth:`Dataset.to_daft() <ray.data.Dataset.to_daft>`.

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

            df = ds.to_daft()
            print(df)

        .. testoutput::
            :options: +MOCK

            ╭───────────────────┬──────────────────┬───────────────────┬──────────────────┬────────╮
            │ sepal length (cm) ┆ sepal width (cm) ┆ petal length (cm) ┆ petal width (cm) ┆ target │
            │ ---               ┆ ---              ┆ ---               ┆ ---              ┆ ---    │
            │ Float64           ┆ Float64          ┆ Float64           ┆ Float64          ┆ Int64  │
            ╞═══════════════════╪══════════════════╪═══════════════════╪══════════════════╪════════╡
            │ 5.1               ┆ 3.5              ┆ 1.4               ┆ 0.2              ┆ 0      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 4.9               ┆ 3                ┆ 1.4               ┆ 0.2              ┆ 0      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 4.7               ┆ 3.2              ┆ 1.3               ┆ 0.2              ┆ 0      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 4.6               ┆ 3.1              ┆ 1.5               ┆ 0.2              ┆ 0      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 5                 ┆ 3.6              ┆ 1.4               ┆ 0.2              ┆ 0      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 5.4               ┆ 3.9              ┆ 1.7               ┆ 0.4              ┆ 0      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 4.6               ┆ 3.4              ┆ 1.4               ┆ 0.3              ┆ 0      │
            ├╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌╌┼╌╌╌╌╌╌╌╌┤
            │ 5                 ┆ 3.4              ┆ 1.5               ┆ 0.2              ┆ 0      │
            ╰───────────────────┴──────────────────┴───────────────────┴──────────────────┴────────╯

            (Showing first 8 of 150 rows)


    .. tab-item:: Dask

        To convert a :class:`~ray.data.dataset.Dataset` to a
        `Dask DataFrame <https://docs.dask.org/en/stable/dataframe.html>`__, call
        :meth:`Dataset.to_dask() <ray.data.Dataset.to_dask>`.

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

            df = ds.to_dask()

    .. tab-item:: Spark

        To convert a :class:`~ray.data.dataset.Dataset` to a `Spark DataFrame
        <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html>`__,
        call :meth:`Dataset.to_spark() <ray.data.Dataset.to_spark>`.

        .. testcode::
            :skipif: True

            import ray
            import raydp

            spark = raydp.init_spark(
                app_name = "example",
                num_executors = 1,
                executor_cores = 4,
                executor_memory = "512M"
            )

            ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")
            df = ds.to_spark(spark)

        .. testcode::
            :skipif: True
            :hide:

            raydp.stop_spark()

    .. tab-item:: Modin

        To convert a :class:`~ray.data.dataset.Dataset` to a Modin DataFrame, call
        :meth:`Dataset.to_modin() <ray.data.Dataset.to_modin>`.

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

            mdf = ds.to_modin()

    .. tab-item:: Mars

        To convert a :class:`~ray.data.dataset.Dataset` from a Mars DataFrame, call
        :meth:`Dataset.to_mars() <ray.data.Dataset.to_mars>`.

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@ray-example-data/iris.csv")

            mdf = ds.to_mars()
