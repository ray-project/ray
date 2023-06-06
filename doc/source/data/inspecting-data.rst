.. _inspecting-data:

===============
Inspecting data
===============

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

For more information such as the number of rows, print the Dataset.

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

To inspect rows, call `Dataset.take() <ray.data.Dataset.take>`. Ray Data represents rows
as dictionaries.

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
`Dataset.take_batch() <ray.data.Dataset.take_batch>`. By default, Ray Data represents
batches as ndarrays.

.. tab-set::

    .. tab-item:: NumPy

        .. testcode::

            import ray

            ds = ray.data.read_images("example://image-datasets/simple")

            batch = ds.take_batch(batch_size=2, batch_format="numpy")
            print(batch)

        .. testoutput::

            {'image': array([[[[...]]]], dtype=uint8)}

    .. tab-item:: pandas

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

            batch = ds.take_batch(batch_size=2, batch_format="pandas")
            print(batch)

        .. testoutput::
            :options: +NORMALIZE_WHITESPACE

               sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
            0                5.1               3.5                1.4               0.2       0
            1                4.9               3.0                1.4               0.2       0

For more information on working with batches, see
:ref:`Transforming batches <transforming-batches>` and
:ref:`Iterating over batches <iterating-over-batches>`.
