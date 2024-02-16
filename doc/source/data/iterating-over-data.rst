.. _iterating-over-data:

===================
Iterating over Data
===================

Ray Data lets you iterate over rows or batches of data.

This guide shows you how to:

* `Iterate over rows <#iterating-over-rows>`_
* `Iterate over batches <#iterating-over-batches>`_
* `Iterate over batches with shuffling <#iterating-over-batches-with-shuffling>`_
* `Split datasets for distributed parallel training <#splitting-datasets-for-distributed-parallel-training>`_

.. _iterating-over-rows:

Iterating over rows
===================

To iterate over the rows of your dataset, call
:meth:`Dataset.iter_rows() <ray.data.Dataset.iter_rows>`. Ray Data represents each row
as a dictionary.

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
:ref:`Transforming rows <transforming_rows>` and
:ref:`Inspecting rows <inspecting-rows>`.

.. _iterating-over-batches:

Iterating over batches
======================

A batch contains data from multiple rows. Iterate over batches of dataset in different
formats by calling one of the following methods:

* `Dataset.iter_batches() <ray.data.Dataset.iter_batches>`
* `Dataset.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`
* `Dataset.to_tf() <ray.data.Dataset.to_tf>`

.. tab-set::

    .. tab-item:: NumPy
        :sync: NumPy

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

            for batch in ds.iter_batches(batch_size=2, batch_format="numpy"):
                print(batch)

        .. testoutput::
            :options: +MOCK

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
            :options: +MOCK

               sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
            0                5.1               3.5                1.4               0.2       0
            1                4.9               3.0                1.4               0.2       0
            ...
               sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
            0                6.2               3.4                5.4               2.3       2
            1                5.9               3.0                5.1               1.8       2

    .. tab-item:: Torch
        :sync: Torch

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

            for batch in ds.iter_torch_batches(batch_size=2):
                print(batch)

        .. testoutput::
            :options: +MOCK

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

        .. testoutput::

            tf.Tensor([5.1 4.9], shape=(2,), dtype=float64) tf.Tensor([0 0], shape=(2,), dtype=int64)
            ...
            tf.Tensor([6.2 5.9], shape=(2,), dtype=float64) tf.Tensor([2 2], shape=(2,), dtype=int64)

For more information on working with batches, see
:ref:`Transforming batches <transforming_batches>` and
:ref:`Inspecting batches <inspecting-batches>`.

.. _iterating-over-batches-with-shuffling:

Iterating over batches with shuffling
=====================================

:class:`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>` is slow because it
shuffles all rows. If a full global shuffle isn't required, you can shuffle a subset of
rows up to a provided buffer size during iteration by specifying
``local_shuffle_buffer_size``. While this isn't a true global shuffle like
``random_shuffle``, it's more performant because it doesn't require excessive data
movement.

.. tip::

    To configure ``local_shuffle_buffer_size``, choose the smallest value that achieves
    sufficient randomness. Higher values result in more randomness at the cost of slower
    iteration. See :ref:`Local shuffle when iterating over batches <local_shuffle_buffer>`
    on how to diagnose slowdowns.

.. tab-set::

    .. tab-item:: NumPy
        :sync: NumPy

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

            for batch in ds.iter_batches(
                batch_size=2,
                batch_format="numpy",
                local_shuffle_buffer_size=250,
            ):
                print(batch)


        .. testoutput::
            :options: +MOCK

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
            :options: +MOCK

               sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
            0                6.3               2.9                5.6               1.8       2
            1                5.7               4.4                1.5               0.4       0
            ...
               sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
            0                5.6               2.7                4.2               1.3       1
            1                4.8               3.0                1.4               0.1       0

    .. tab-item:: Torch
        :sync: Torch

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
            for batch in ds.iter_torch_batches(
                batch_size=2,
                local_shuffle_buffer_size=250,
            ):
                print(batch)

        .. testoutput::
            :options: +MOCK

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
            :options: +MOCK

            tf.Tensor([5.2 6.3], shape=(2,), dtype=float64) tf.Tensor([1 2], shape=(2,), dtype=int64)
            ...
            tf.Tensor([5.  5.8], shape=(2,), dtype=float64) tf.Tensor([0 0], shape=(2,), dtype=int64)

Splitting datasets for distributed parallel training
====================================================

If you're performing distributed data parallel training, call
:meth:`Dataset.streaming_split <ray.data.Dataset.streaming_split>` to split your dataset
into disjoint shards.

.. note::

  If you're using :ref:`Ray Train <train-docs>`, you don't need to split the dataset.
  Ray Train automatically splits your dataset for you. To learn more, see
  :ref:`Data Loading for ML Training guide <data-ingest-torch>`.

.. testcode::

    import ray

    @ray.remote
    class Worker:

        def train(self, data_iterator):
            for batch in data_iterator.iter_batches(batch_size=8):
                pass

    ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")
    workers = [Worker.remote() for _ in range(4)]
    shards = ds.streaming_split(n=4, equal=True)
    ray.get([w.train.remote(s) for w, s in zip(workers, shards)])
