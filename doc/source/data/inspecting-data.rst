.. _inspecting-data:

===============
Inspecting Data
===============

Inspect :class:`Datasets <ray.data.Dataset>` to better understand your data.

This guide shows you how to:

* `Describe datasets <#describing-datasets>`_
* `Inspect rows <#inspecting-rows>`_
* `Inspect batches <#inspecting-batches>`_
* `Inspect execution statistics <#inspecting-stats>`_

.. _describing-datasets:

Describing datasets
===================

:class:`Datasets <ray.data.Dataset>` are tabular. To view a dataset's column names and
types, call :meth:`Dataset.schema() <ray.data.Dataset.schema>`.

.. testcode::

    import ray

    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

    print(ds.schema())

.. testoutput::

    Column             Type
    ------             ----
    sepal length (cm)  double
    sepal width (cm)   double
    petal length (cm)  double
    petal width (cm)   double
    target             int64

For more information like the number of rows, print the Dataset.

.. testcode::

    import ray

    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

    print(ds)

.. testoutput::

    Dataset(
       num_blocks=...,
       num_rows=150,
       schema={
          sepal length (cm): double,
          sepal width (cm): double,
          petal length (cm): double,
          petal width (cm): double,
          target: int64
       }
    )

.. _inspecting-rows:

Inspecting rows
===============

To get a list of rows, call :meth:`Dataset.take() <ray.data.Dataset.take>` or
:meth:`Dataset.take_all() <ray.data.Dataset.take_all>`. Ray Data represents each row as
a dictionary.

.. testcode::

    import ray

    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

    rows = ds.take(1)
    print(rows)

.. testoutput::

    [{'sepal length (cm)': 5.1, 'sepal width (cm)': 3.5, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}]


For more information on working with rows, see
:ref:`Transforming rows <transforming_rows>` and
:ref:`Iterating over rows <iterating-over-rows>`.

.. _inspecting-batches:

Inspecting batches
==================

A batch contains data from multiple rows. To inspect batches, call
`Dataset.take_batch() <ray.data.Dataset.take_batch>`.

By default, Ray Data represents batches as dicts of NumPy ndarrays. To change the type
of the returned batch, set ``batch_format``.

.. tab-set::

    .. tab-item:: NumPy

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

            batch = ds.take_batch(batch_size=2, batch_format="numpy")
            print("Batch:", batch)
            print("Image shape", batch["image"].shape)

        .. testoutput::
            :options: +MOCK

            Batch: {'image': array([[[[...]]]], dtype=uint8)}
            Image shape: (2, 32, 32, 3)

    .. tab-item:: pandas

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

            batch = ds.take_batch(batch_size=2, batch_format="pandas")
            print(batch)

        .. testoutput::
            :options: +NORMALIZE_WHITESPACE

               sepal length (cm)  sepal width (cm)  ...  petal width (cm)  target
            0                5.1               3.5  ...               0.2       0
            1                4.9               3.0  ...               0.2       0
            <BLANKLINE>
            [2 rows x 5 columns]

For more information on working with batches, see
:ref:`Transforming batches <transforming_batches>` and
:ref:`Iterating over batches <iterating-over-batches>`.


Inspecting execution statistics
===============================

Ray Data calculates statistics during execution like the wall clock time and memory usage for the different stages.

To view stats about your :class:`Datasets <ray.data.Dataset>`, call :meth:`Dataset.stats() <ray.data.Dataset.stats>` on an executed dataset. The stats are also persisted under `/tmp/ray/session_*/logs/ray-data.log`.

.. testcode::
    import ray
    import time

    def pause(x):
        time.sleep(.0001)
        return x

    ds = (
        ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
        .map(lambda x: x)
        .map(pause)
    )

    for batch in ds.iter_batches():
        pass

    print(ds.stats())

.. testoutput::
    :options: +MOCK

    Stage 1 ReadCSV->Map(<lambda>)->Map(pause): 1/1 blocks executed in 0.23s
    * Remote wall time: 222.1ms min, 222.1ms max, 222.1ms mean, 222.1ms total
    * Remote cpu time: 15.6ms min, 15.6ms max, 15.6ms mean, 15.6ms total
    * Peak heap memory usage (MiB): 157953.12 min, 157953.12 max, 157953 mean
    * Output num rows: 150 min, 150 max, 150 mean, 150 total
    * Output size bytes: 6000 min, 6000 max, 6000 mean, 6000 total
    * Tasks per node: 1 min, 1 max, 1 mean; 1 nodes used
    * Extra metrics: {'obj_store_mem_alloc': 6000, 'obj_store_mem_freed': 5761, 'obj_store_mem_peak': 6000}

    Dataset iterator time breakdown:
    * Total time user code is blocked: 5.68ms
    * Total time in user code: 0.96us
    * Total time overall: 238.93ms
    * Num blocks local: 0
    * Num blocks remote: 0
    * Num blocks unknown location: 1
    * Batch iteration time breakdown (summed across prefetch threads):
        * In ray.get(): 2.16ms min, 2.16ms max, 2.16ms avg, 2.16ms total
        * In batch creation: 897.67us min, 897.67us max, 897.67us avg, 897.67us total
        * In batch formatting: 836.87us min, 836.87us max, 836.87us avg, 836.87us total
