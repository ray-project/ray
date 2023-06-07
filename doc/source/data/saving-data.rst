.. _saving-data:

===========
Saving data
===========

Writing data to files
=====================

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

    ds = ray.data.read_csv("example://iris.csv")

    ds.write_parquet("local:///tmp/iris/")

To write data to formats other than Parquet, read the
:ref:`Input/Output reference <input-output>`.

Writing data to cloud storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To save your :class:`~ray.data.dataset.Dataset` to cloud storage, authenticate all nodes
with your cloud service provider. Then, call a method like
:meth:`Dataset.write_parquet <ray.data.Dataset.write_parquet>` and specify a bucket or
folder URI.

.. tab-set::

    .. tab-item:: S3

        .. testcode::
            :skipif: True

            import ray

            ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

            ds.write_parquet("s3://my-bucket/my-folder")

    .. tab-item:: GCS

        .. testcode::
            :skipif: True

            import ray

            ds = ray.data.read_csv("gcs://anonymous@air-example-data/iris.csv")

            ds.write_parquet("gcs://my-bucket/my-folder")

To write data to formats other than Parquet, read the
:ref:`Input/Output reference <input-output>`.

Writing data to NFS
~~~~~~~~~~~~~~~~~~~

To save your :class:`~ray.data.dataset.Dataset` to NFS file systems, call a method
like :meth:`Dataset.write_parquet <ray.data.Dataset.write_parquet>` and specify a
mounted directory.

.. testcode::

    import ray

    ds = ray.data.read_csv("example://iris.csv")

    ds.write_parquet("/mnt/cluster_storage/iris")

To write data to formats other than Parquet, read the
:ref:`Input/Output reference <input-output>`.

Converting Datasets to other Python libraries
=============================================

Converting Datasets to pandas
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To convert a :class:`~ray.data.dataset.Dataset` to a pandas DataFrame, call
:meth:`Dataset.to_pandas() <ray.data.Dataset.to_pandas>`.

.. testcode::

    import ray

    ds = ray.data.read_csv("example://iris.csv")

    df = ds.to_pandas()

.. testoutput::

         sepal.length  sepal.width  petal.length  petal.width    variety
    0             5.1          3.5           1.4          0.2     Setosa
    1             4.9          3.0           1.4          0.2     Setosa
    ...
    149           5.9          3.0           5.1          1.8  Virginica


Converting Datasets to distributed DataFrames
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Data interoperates with distributed data processing frameworks like
:ref:`Dask <dask-on-ray>`, :ref:`Spark <spark-on-ray>`, :ref:`Modin <modin-on-ray>`, and
:ref:`Mars <mars-on-ray>`.

.. tab-set::

    .. tab-item:: Dask

        To convert a :class:`~ray.data.dataset.Dataset` to a
        `Dask DataFrame <https://docs.dask.org/en/stable/dataframe.html>`__, call
        :meth:`Dataset.to_dask() <ray.data.Dataset.to_dask>`.

        .. testcode::
            :skipif: True

            import ray

            ds = ray.data.read_csv("example://iris.csv")

            df = ds.to_dask()

    .. tab-item:: Spark

        To convert a :class:`~ray.data.dataset.Dataset` to a `Spark DataFrame
        <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html>`__,
        call :meth:`Dataset.to_spark() <ray.data.Dataset.to_spark>`.

        .. testcode::
            :skipif: True

            import ray

            ds = ray.data.read_csv("example://iris.csv")

            df = ds.to_spark()

    .. tab-item:: Modin

        To convert a :class:`~ray.data.dataset.Dataset` to a Modin DataFrame, call
        :meth:`Dataset.to_modin() <ray.data.Dataset.to_modin>`.

        .. testcode::
            :skipif: True

            import ray

            ds = ray.data.read_csv("example://iris.csv")

            mdf = ds.to_modin()

    .. tab-item:: Mars

        To convert a :class:`~ray.data.dataset.Dataset` from a Mars DataFrame, call
        :meth:`Dataset.to_mars() <ray.data.Dataset.to_mars>`.

        .. testcode::
            :skipif: True

            import ray

            ds = ray.data.read_csv("example://iris.csv")

            mdf = ds.to_mars()

Changing the number of output files
===================================

When you call a write method, Ray Data writes your data to one file per :term:`block`.
To change the number of blocks, call :meth:`~ray.data.Dataset.repartition`.

.. testcode::

    import os
    import ray

    ds = ray.data.read_images("s3://anonymous@air-example-data/iris.csv")
    ds.repartition(2).write_csv("/tmp/two_files/")

    print(os.listdir("/tmp/two_files/"))

.. testoutput::
    :options: +MOCK

    ['26b07dba90824a03bb67f90a1360e104_000003.csv', '26b07dba90824a03bb67f90a1360e104_000002.csv']
