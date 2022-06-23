.. _datasets_getting_started:

Getting Started
===============

A Ray :class:`Dataset <ray.data.Dataset>` is a distributed data collection. It holds
a list of Ray object references pointing to distributed data *blocks*, and has APIs
for distributed data loading and processing. Each block holds an ordered collection
of items in either an `Arrow table <https://arrow.apache.org/docs/python/data.html#tables>`__
(when creating from or transforming to tabular or tensor data) or a Python list (for non-tabular Python objects).

In this tutorial you will learn how to:

- Create and save a Ray ``Dataset``.
- Transform a ``Dataset``.
- Pass a ``Dataset`` to Ray tasks/actors and access the data inside.

.. tip::

   Run ``pip install "ray[data]"`` to get started!

Creating and Saving Datasets
----------------------------

You can create a Dataset from Python objects. These objects can be held inside
Dataset as the plain Python objects (where the schema is a Python type), or as
Arrow records (in which case their schema is Arrow).

.. literalinclude:: ./doc_code/quick_start.py
   :language: python
   :start-after: __create_from_python_begin__
   :end-before: __create_from_python_end__

Datasets can also be created from files on local disk or remote datasources such as S3.
Any filesystem `supported by pyarrow <http://arrow.apache.org/docs/python/generated/pyarrow.fs.FileSystem.html>`__
can be used to specify file locations. See more at :ref:`Creating Datasets <creating_datasets>`.

.. literalinclude:: ./doc_code/quick_start.py
   :language: python
   :start-after: __create_from_files_begin__
   :end-before: __create_from_files_end__

Once you have a Dataset (potentially after transformation), you can save it to local
or remote storage in desired format, using ``.write_csv()``, ``.write_json()``, and
``.write_parquet()``. See more at :ref:`Saving Datasets <saving_datasets>`.

.. literalinclude:: ./doc_code/quick_start.py
   :language: python
   :start-after: __save_dataset_begin__
   :end-before: __save_dataset_end__

See the :ref:`Creating Datasets <creating_datasets>` and :ref:`Saving Datasets
<saving_datasets>` guides for more details on how to create and save datasets.


Transforming Datasets
---------------------

Once you have a ``Dataset``, you can transform it by applying a user-defined function,
which produces another ``Dataset``.
Under the hood, the transformation is executed in parallel for performance at scale.

.. literalinclude:: ./doc_code/quick_start.py
   :language: python
   :start-after: __data_transform_begin__
   :end-before: __data_transform_end__

.. tip::

    Datasets also provides the convenience transformation methods :meth:`ds.map() <ray.data.Dataset.map>`, :meth:`ds.flat_map() <ray.data.Dataset.flat_map>`, and :meth:`ds.filter() <ray.data.Dataset.filter>`, which are not vectorized (slower than :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`), but may be useful for development.

These transformations are composable. You can further apply transformations on the
output Dataset, forming a chain of transformations to express more complex logic.

By default, transformations are executed using Ray tasks.
For transformations that require setup, you may want to use Ray actors by specifying
``compute=ray.data.ActorPoolStrategy(min, max)`` and Ray will use an autoscaling
actor pool of ``min`` to ``max`` actors to execute your transforms. This will cache
the stateful setup at the actor creation time, which is particularly useful if the
setup is expensive.

See the :ref:`Transforming Datasets guide <transforming_datasets>` for an in-depth guide
on transforming datasets.

Passing and accessing datasets
------------------------------

Datasets can be passed to Ray tasks or actors and accessed with
:meth:`.iter_batches() <ray.data.Dataset.iter_batches>` or
:meth:`.iter_rows() <ray.data.Dataset.iter_rows>`.
This does not incur a copy, since the blocks of the Dataset are passed by reference
as Ray objects:

.. literalinclude:: ./doc_code/quick_start.py
   :language: python
   :start-after: __data_access_begin__
   :end-before: __data_access_end__

Datasets can be split up into disjoint sub-datasets.
Locality-aware splitting is supported if you pass in a list of actor handles to the
:meth:`split() <ray.data.Dataset.split>` function along with the number of desired
splits.
This is a common pattern useful for loading and splitting data between distributed
training actors:

.. literalinclude:: ./doc_code/quick_start.py
   :language: python
   :start-after: __dataset_split_begin__
   :end-before: __dataset_split_end__

See the :ref:`Accessing Datasets guide <accessing_datasets>` for an in-depth guide
on accessing and exchanging datasets.
