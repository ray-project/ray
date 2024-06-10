.. _working_with_tensors:

Working with Tensors / NumPy
============================

N-dimensional arrays (in other words, tensors) are ubiquitous in ML workloads. This guide
describes the limitations and best practices of working with such data.

Tensor data representation
--------------------------

Ray Data represents tensors as
`NumPy ndarrays <https://numpy.org/doc/stable/reference/arrays.ndarray.html>`__.

.. testcode::

    import ray

    ds = ray.data.read_images("s3://anonymous@air-example-data/digits")
    print(ds)

.. testoutput::

    Dataset(
       num_rows=100,
       schema={image: numpy.ndarray(shape=(28, 28), dtype=uint8)}
    )

Batches of fixed-shape tensors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your tensors have a fixed shape, Ray Data represents batches as regular ndarrays.

.. doctest::

    >>> import ray
    >>> ds = ray.data.read_images("s3://anonymous@air-example-data/digits")
    >>> batch = ds.take_batch(batch_size=32)
    >>> batch["image"].shape
    (32, 28, 28)
    >>> batch["image"].dtype
    dtype('uint8')

Batches of variable-shape tensors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your tensors vary in shape, Ray Data represents batches as arrays of object dtype.

.. doctest::

    >>> import ray
    >>> ds = ray.data.read_images("s3://anonymous@air-example-data/AnimalDetection")
    >>> batch = ds.take_batch(batch_size=32)
    >>> batch["image"].shape
    (32,)
    >>> batch["image"].dtype
    dtype('O')

The individual elements of these object arrays are regular ndarrays.

.. doctest::

    >>> batch["image"][0].dtype
    dtype('uint8')
    >>> batch["image"][0].shape  # doctest: +SKIP
    (375, 500, 3)
    >>> batch["image"][3].shape  # doctest: +SKIP
    (333, 465, 3)

.. _transforming_tensors:

Transforming tensor data
------------------------

Call :meth:`~ray.data.Dataset.map` or :meth:`~ray.data.Dataset.map_batches` to transform tensor data.

.. testcode::

    from typing import Any, Dict

    import ray
    import numpy as np

    ds = ray.data.read_images("s3://anonymous@air-example-data/AnimalDetection")

    def increase_brightness(row: Dict[str, Any]) -> Dict[str, Any]:
        row["image"] = np.clip(row["image"] + 4, 0, 255)
        return row

    # Increase the brightness, record at a time.
    ds.map(increase_brightness)

    def batch_increase_brightness(batch: Dict[str, np.ndarray]) -> Dict:
        batch["image"] = np.clip(batch["image"] + 4, 0, 255)
        return batch

    # Increase the brightness, batch at a time.
    ds.map_batches(batch_increase_brightness)

In addition to NumPy ndarrays, Ray Data also treats returned lists of NumPy ndarrays and
objects implementing ``__array__`` (for example, ``torch.Tensor``) as tensor data.

For more information on transforming data, read
:ref:`Transforming data <transforming_data>`.


Saving tensor data
------------------

Save tensor data with formats like Parquet, NumPy, and JSON. To view all supported
formats, see the :ref:`Input/Output reference <input-output>`.

.. tab-set::

    .. tab-item:: Parquet

        Call :meth:`~ray.data.Dataset.write_parquet` to save data in Parquet files.

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
            ds.write_parquet("/tmp/simple")


    .. tab-item:: NumPy

        Call :meth:`~ray.data.Dataset.write_numpy` to save an ndarray column in NumPy
        files.

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
            ds.write_numpy("/tmp/simple", column="image")

    .. tab-item:: JSON

        To save images in a JSON file, call :meth:`~ray.data.Dataset.write_json`.

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
            ds.write_json("/tmp/simple")

For more information on saving data, read :ref:`Saving data <saving-data>`.
