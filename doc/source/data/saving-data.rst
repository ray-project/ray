.. _saving-data:

===========
Saving data
===========

Save data to local files or files in cloud storage.

Ray Data supports file formats such as Parquet, CSV, and NumPy. To view all file
formats, read the Input/Output reference.

Writing data to files
=====================

Writing data to local storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Call a method such as :meth:`~ray.data.write_csv` and specify a local directory with the
`local://` scheme.

.. warning::

    If you don't `local://`, TK.

.. testcode::

    import ray

    ds = ray.data.read_csv("example://iris.csv")

    ds.write_csv("local:///tmp/iris/")

To write data to formats other than CSV, read the :ref:`Input/Output reference <input-output>`.

Writing data to cloud storage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray Data supports writing data to Amazon S3, Google Cloud Storage, and Azure Blob
Storage.

.. tab-set::

    .. tab-item:: AWS

        Call a method such as :meth:`~ray.data.write_csv` and specify an S3 directory.

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

            ds.write_csv("/tmp/iris/")


    .. tab-item:: GCP

        ham


To write data to formats other than CSV, read the :ref:`Input/Output reference <input-output>`.

Writing data to NFS
~~~~~~~~~~~~~~~~~~~

Call a method such as :meth:`~ray.data.write_csv` and specify a mounted directory.

.. testcode::

    import ray

    ds = ray.data.read_csv("example://iris.csv")

    ds.write_csv("/mnt/cluster_storage/iris")

To write data to formats other than CSV, read the :ref:`Input/Output reference <input-output>`.

Changing the number of output files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you call a write method, Ray Data writes your data to one or more files at the
specified path. To change the number of output files, call :meth:`~ray.data.Dataset.repartition`.

.. testcode::

    import os
    import ray

    ds = ray.data.read_images("s3://anonymous@air-example-data/iris.csv")
    ds.repartition(2).write_csv("/tmp/two_files/")

    print(os.listdir("/tmp/two_files/"))

.. testoutput::

    ['26b07dba90824a03bb67f90a1360e104_000003.csv', '26b07dba90824a03bb67f90a1360e104_000002.csv']
