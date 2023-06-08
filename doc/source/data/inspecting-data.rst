.. _inspecting-data:

===============
Inspecting Data
===============

Inspect :class:`Datasets <ray.data.Dataset>` to better understand your data.

This guide shows you how to:

* `Describe datasets <#describing-datasets>`_
* `Inspect rows <#inspecting-rows>`_
* `Inspect batches <#inspecting-batches>`_

.. _describing-datasets:

Describing datasets
===================

:class:`Datasets <ray.data.Dataset>` are tabular. To view a Dataset's column names and
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
:ref:`Transforming rows <transforming-rows>` and
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

            ds = ray.data.read_images("example://image-datasets/simple")

            batch = ds.take_batch(batch_size=2, batch_format="numpy")
            print("Batch:", batch)
            print("Image shape", batch["image"].shape)

        .. testoutput::
            :options: +SKIP

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
:ref:`Transforming batches <transforming-batches>` and
:ref:`Iterating over batches <iterating-over-batches>`.
