.. _transforming_datasets:

=====================
Transforming Datasets
=====================

The Ray Datasets transformations take in datasets and produce new datasets.
For example, *map* is a transformation that applies a user-defined function (UDF)
on each row of input dataset and returns a new dataset as result. The Datasets
transformations are **composable**. Operations can be further applied on the result
dataset, forming a chain of transformations to express more complex computations.
Transformations are the core for expressing business logic in Datasets.

Transformations
---------------

In general, we have two types of transformations:

* **One-to-one transformations:** each input block will contribute to only one output
  block, such as :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`. In other
  systems this may be called narrow transformations.
* **All-to-all transformations:** input blocks can contribute to multiple output blocks,
  such as :meth:`ds.random_shuffle() <ray.data.Dataset.random_shuffle>`. In other
  systems this may be called wide transformations.

Here is a table listing some common transformations supported by Ray Datasets.

.. list-table:: Common Ray Datasets transformations.
   :header-rows: 1

   * - Transformation
     - Type
     - Description
   * - :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`
     - One-to-one
     - Apply a given function to batches of records of this dataset. 
   * - :meth:`ds.split() <ray.data.Dataset.split>`
     - One-to-one
     - | Split the dataset into N disjoint pieces.
   * - :meth:`ds.repartition(shuffle=False) <ray.data.Dataset.repartition>`
     - One-to-one
     - | Repartition the dataset into N blocks, without shuffling the data.
   * - :meth:`ds.repartition(shuffle=True) <ray.data.Dataset.repartition>`
     - All-to-all
     - | Repartition the dataset into N blocks, shuffling the data during repartition.
   * - :meth:`ds.random_shuffle() <ray.data.Dataset.random_shuffle>`
     - All-to-all
     - | Randomly shuffle the elements of this dataset.
   * -  :meth:`ds.sort() <ray.data.Dataset.sort>`
     - All-to-all
     - | Sort the dataset by a sortkey.
   * -  :meth:`ds.groupby() <ray.data.Dataset.groupby>`
     - All-to-all
     - | Group the dataset by a groupkey.

.. tip::

    Datasets also provides the convenience transformation methods :meth:`ds.map() <ray.data.Dataset.map>`,
    :meth:`ds.flat_map() <ray.data.Dataset.flat_map>`, and :meth:`ds.filter() <ray.data.Dataset.filter>`,
    which are not vectorized (slower than :meth:`ds.map_batches() <ray.data.Dataset.map_batches>`), but
    may be useful for development.

The following is an example to make use of those transformation APIs for processing
the Iris dataset.

.. literalinclude:: ./doc_code/transforming_datasets.py
   :language: python
   :start-after: __dataset_transformation_begin__
   :end-before: __dataset_transformation_end__

Writing UDFs
------------

User-defined functions (UDFs) are routines that apply on one row (e.g.
:meth:`.map() <ray.data.Dataset.map>`) or a batch of rows (e.g.
:meth:`.map_batches() <ray.data.Dataset.map_batches>`) of a dataset. UDFs let you
express your customized business logic in transformations. Here we will focus on
:meth:`.map_batches() <ray.data.Dataset.map_batches>` as it's the primary mapping
API in Datasets.

A UDF can be a function or a callable class, which has the following input/output:

* **Input type**: a ``pandas.DataFrame``, ``pyarrow.Table`` or a Python list. You can
  control the input type fed to your UDF by specifying the ``batch_format`` parameter in
  :meth:`.map_batches() <ray.data.Dataset.map_batches>`. By default, the ``batch_format``
  is "native", which will feed ``pandas.DataFrame`` to UDF regardless of the underlying
  batch type.
* **Output type**: a ``pandas.DataFrame``, ``pyarrow.Table`` or a Python list. Note
  the output type doesn't need to be the same as input type.

The following are some UDF examples.

.. literalinclude:: ./doc_code/transforming_datasets.py
   :language: python
   :start-after: __writing_udfs_begin__
   :end-before: __writing_udfs_end__

You may reference the `pyarrow.Table APIs <https://arrow.apache.org/docs/python/generated/pyarrow.Table.html>`
or the `pandas.DataFrame APIs <https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html>`
in writing UDFs.

.. tip::

   Write your UDFs to leverage built-in vectorized operations in ``pandas.DataFrame``
   or ``pyarrow.Table`` for better performance. For example, suppose you want to compute
   the sum of a column in ``pandas.DataFrame``, instead of iterating each row of a batch
   and sum up values of that column, you may want to use ``df_batch["col_foo"].sum()``.

Compute Strategy
----------------

Datasets transformations are executed by either :ref:`Ray tasks <ray-remote-functions>`
or :ref:`Ray actors <actor-guide>` across a Ray cluster. By default, Ray tasks are
used (with ``compute="tasks"``). For transformations that require expensive setup,
it's preferrable to use Ray actors, which are stateful and allow setup to be reused
for efficiency. You can specify ``compute=ray.data.ActorPoolStrategy(min, max)`` and
Ray will use an autoscaling actor pool of ``min`` to ``max`` actors to execute your
transforms. For a fixed-size actor pool, just specify ``ActorPoolStrategy(n, n)``.

The following is an example of using the Ray tasks and actors compute strategy
for batch inference:

.. literalinclude:: ./doc_code/transforming_datasets.py
   :language: python
   :start-after: __dataset_compute_strategy_begin__
   :end-before: __dataset_compute_strategy_end__
