.. _saving-data:

===========
Saving data
===========

Writing data to local storage
=============================

To save your :class:`~ray.data.dataset.Dataset` to local filesystems, call a method
like :meth:`~ray.data.write_csv` and specify a local directory with the `local://`
scheme.

.. warning::

    If your cluster contains multiple nodes and you don't use `local://`, Ray Data
    writes your data to multiple nodes.

.. testcode::

    import ray

    ds = ray.data.read_csv("example://iris.csv")

    ds.write_csv("local:///tmp/iris/")

To write data to formats other than CSV, read the :ref:`Input/Output reference <input-output>`.

Writing data to S3
==================

To save your :class:`~ray.data.dataset.Dataset` to S3, call a method like
:meth:`~ray.data.write_csv` and specify a bucket or folder.

.. testcode::

    import ray

    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

    ds.write_csv("s3://my-bucket/my-folder")


To write data to formats other than CSV, read the :ref:`Input/Output reference <input-output>`.

Writing data to NFS
===================

To save your :class:`~ray.data.dataset.Dataset` to NFS file systems, call a method
like :meth:`~ray.data.write_csv` and specify a mounted directory.

.. testcode::

    import ray

    ds = ray.data.read_csv("example://iris.csv")

    ds.write_csv("/mnt/cluster_storage/iris")

To write data to formats other than CSV, read the :ref:`Input/Output reference <input-output>`.

Changing the number of output files
===================================

When you call a write method, Ray Data writes your data to one file per *block*. To
change the number of blocks, call :meth:`~ray.data.Dataset.repartition`.

.. testcode::

    import os
    import ray

    ds = ray.data.read_images("s3://anonymous@air-example-data/iris.csv")
    ds.repartition(2).write_csv("/tmp/two_files/")

    print(os.listdir("/tmp/two_files/"))

.. testoutput::
    :options: +MOCK

    ['26b07dba90824a03bb67f90a1360e104_000003.csv', '26b07dba90824a03bb67f90a1360e104_000002.csv']
