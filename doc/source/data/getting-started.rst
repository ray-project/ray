.. _datasets_getting_started:

Dataset Quick Start
===================

Ray Dataset is an abstraction over a list of Ray object references to *blocks*, with APIs for distributed data loading and processing. Each block holds a set of items and can be in format of either an `Arrow table <https://arrow.apache.org/docs/python/data.html#tables>`__
, a `Pandas DataFrame <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`__
, or a Python list.

In this tutorial you will learn how to:

- Create and save a Ray ``Dataset``.
- Transform a ``Dataset``.
- Pass a ``Dataset`` to Ray tasks/actors and access the data inside.

.. tip::

   Run ``pip install "ray[data]"`` to get started!

Creating and Saving Datasets
----------------------------

You can create a Dataset from Python objects. These objects can be held inside Dataset as the plain Python objects (where the schema is a Python type), or as Arrow records (in which case their schema is Arrow).

.. literalinclude:: ./doc_code/quick_start.py
   :language: python
   :start-after: __create_from_python_begin__`
   :end-before: __create_from_python_end__

Datasets can also be created from files on local disk or remote datasources such as S3.
Any filesystem `supported by pyarrow <http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__
can be used to specify file locations:

.. literalinclude:: ./doc_code/quick_start.py
   :language: python
   :start-after: __create_from_files_begin__`
   :end-before: __create_from_files_end__

Once you have a Dataset, you can save it to local or remote storage in desired format, using ``.write_csv()``, ``.write_json()``, and ``.write_parquet()``.

.. literalinclude:: ./doc_code/quick_start.py
   :language: python
   :start-after: __save_dataset_begin__
   :end-before: __save_dataset_end__


Transforming Datasets
---------------------

Once you have a Dataset, you can transform it by applying a user-defined function and produce another Dataset.
Under the hood the transformation is executed in parrallel for performance at scale.

.. literalinclude:: ./doc_code/quick_start.py
   :language: python
   :start-after: __data_transform_begin__
   :end-before: __data_transform_end__

.. tip::

    Datasets also provides the convenience transformation methods ``map``, ``flat_map``, and ``filter``, which are not vectorized (slower than ``map_batches``), but may be useful for development.

The transformation is composable. You can further apply transformations on the output Dataset, forming
a chain of transformations. Ray Dataset will optimize the execution by fusing transformations when appropriate.

By default, transformations are executed using Ray tasks.
For transformations that require setup, you may want to use Ray actors by specifying ``compute=ray.data.ActorPoolStrategy(min, max)`` and Ray will use an autoscaling actor pool of ``min`` to ``max`` actors to execute your transforms.

Passing and accessing datasets
------------------------------

Datasets can be passed to Ray tasks or actors and accessed with ``.iter_batches()`` or ``.iter_rows()``.
This does not incur a copy, since the blocks of the Dataset are passed by reference as Ray objects:

.. literalinclude:: ./doc_code/quick_start.py
   :language: python
   :start-after: __data_access_begin__
   :end-before: __data_access_end__

Datasets can be split up into disjoint sub-datasets.
Locality-aware splitting is supported if you pass in a list of actor handles to the ``split()`` function along with the number of desired splits.
This is a common pattern useful for loading and splitting data between distributed training actors:

.. literalinclude:: ./doc_code/quick_start.py
   :language: python
   :start-after: __dataset_split_begin__
   :end-before: __dataset_split_end__
