.. _iterating-over-data:

===================
Iterating over data
===================

.. _iterating-over-rows:

Iterating over rows
===================

To iterate over rows, call :meth:`Dataset.iter_rows() <ray.data.Dataset.iter_rows>`. Ray
Data represents rows as dictionaries.

.. testcode::

    import ray

    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

    for row in ds.iter_rows():
        print(row)

.. testoutput::

    {'sepal length (cm)': 5.1, 'sepal width (cm)': 3.5, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}
    {'sepal length (cm)': 4.9, 'sepal width (cm)': 3.0, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}
    ...
    {'sepal length (cm)': 5.9, 'sepal width (cm)': 3.0, 'petal length (cm)': 5.1, 'petal width (cm)': 1.8, 'target': 2}


For more information on working with rows, see
:ref:`Transforming rows <transforming-rows>` and
:ref:`Inspecting rows <inspecting-rows>`.

.. _iterating-over-batches:

Iterating over batches
======================

A batch contains data from multiple rows. To iterate over batches, call one of the
following methods:

* `Dataset.iter_batches() <ray.data.Dataset.iter_batches>`
* `Dataset.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`
* `Dataset.to_tf() <ray.data.Dataset.to_tf>`

.. tab-set::

    .. tab-item:: NumPy
        :sync: NumPy

        .. testcode::

            import ray

            ds = ray.data.read_images("example://image-datasets/simple")

            for batch in ds.iter_batches(batch_size=2, batch_format="numpy"):
                print(batch)

        .. testoutput::

            {'image': array([[[[...]]]], dtype=uint8)}
            ...
            {'image': array([[[[...]]]], dtype=uint8)}

    .. tab-item:: pandas
        :sync: pandas

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

            for batch in ds.iter_batches(batch_size=2, batch_format="pandas"):
                print(batch)

        .. testoutput::
            :options:+NORMALIZE_WHITESPACE

               sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
            0                5.1               3.5                1.4               0.2       0
            1                4.9               3.0                1.4               0.2       0
            ...
               sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
            0                5.1               3.5                1.4               0.2       0
            1                4.9               3.0                1.4               0.2       0

    .. tab-item:: Torch
        :sync: Torch

        .. testcode::

            import ray

            ds = ray.data.read_images("example://image-datasets/simple")

            for batch in ds.iter_torch_batches(batch_size=2):
                print(batch)

        .. testoutput::

            {'image': tensor([[[[...]]]], dtype=torch.uint8)}
            ...
            {'image': tensor([[[[...]]]], dtype=torch.uint8)}

    .. tab-item:: TensorFlow
        :sync: TensorFlow

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

            tf_dataset = ds.to_tf(
                feature_columns="sepal length (cm)",
                label_columns="target",
                batch_size=2
            )
            for features, labels in tf_dataset:
                print(features, labels)

        .. testoutput:

            tf.Tensor([5.1 4.9], shape=(2,), dtype=float64) tf.Tensor([0 0], shape=(2,), dtype=int64)
            ...
            tf.Tensor([5.1 4.9], shape=(2,), dtype=float64) tf.Tensor([0 0], shape=(2,), dtype=int64)

For more information on working with batches, see
:ref:`Transforming batches <transforming-batches>` and
:ref:`Inspecting batches <inspecting-batches>`.

.. _iterating-over-batches-with-shuffling:

Iterating over batches with shuffling
=====================================

:class:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>` is slow. For better
performance, shuffle data while iterating.

To iterate over batches with shuffling, specify ``local_shuffle_buffer_size``.

.. tip::

    To configure ``local_shuffle_buffer_size``, choose the largest value that fits
    in memory. For most workloads, 10x the batch size is reasonable.

.. tab-set::

    .. tab-item:: NumPy
        :sync: NumPy

        .. testcode::

            import ray

            ds = ray.data.read_images("example://image-datasets/simple")

            for batch in ds.iter_batches(
                batch_size=2,
                batch_format="numpy",
                local_shuffle_buffer_size=250,
            ):
                print(batch)


        .. testoutput::

            {'image': array([[[[...]]]], dtype=uint8)}
            ...
            {'image': array([[[[...]]]], dtype=uint8)}

    .. tab-item:: pandas
        :sync: pandas

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

            for batch in ds.iter_batches(
                batch_size=2,
                batch_format="pandas",
                local_shuffle_buffer_size=250,
            ):
                print(batch)

        .. testoutput::
            :options: +NORMALIZE_WHITESPACE

               sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
            0                6.1               2.9                4.7               1.4       1
            1                6.3               2.8                5.1               1.5       2
            ...
               sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
            0                6.1               2.9                4.7               1.4       1
            1                6.3               2.8                5.1               1.5       2
    .. tab-item:: Torch
        :sync: Torch

        .. testcode::

            import ray

            ds = ray.data.read_images("example://image-datasets/simple")
            for batch in ds.iter_torch_batches(
                batch_size=2,
                local_shuffle_buffer_size=250,
            ):
                print(batch)

        .. testoutput::

            {'image': tensor([[[[...]]]], dtype=torch.uint8)}
            ...
            {'image': tensor([[[[...]]]], dtype=torch.uint8)}

    .. tab-item:: TensorFlow
        :sync: TensorFlow

        .. testcode::

            import ray

            ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

            tf_dataset = ds.to_tf(
                feature_columns="sepal length (cm)",
                label_columns="target",
                batch_size=2,
                local_shuffle_buffer_size=250,
            )
            for features, labels in tf_dataset:
                print(features, labels)

        .. testoutput::

            tf.Tensor([6.1 6.3], shape=(2,), dtype=float64) tf.Tensor([1 2], shape=(2,), dtype=int64)
            ...
            tf.Tensor([6.1 6.3], shape=(2,), dtype=float64) tf.Tensor([1 2], shape=(2,), dtype=int64)
