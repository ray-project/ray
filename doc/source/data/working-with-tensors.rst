.. _working_with_tensors:

Working with tensor data
========================

N-dimensional arrays (i.e., tensors) are ubiquitous in ML workloads. This guide
describes the limitations and best practices of working with such data.

Tensor data representation
--------------------------

Ray Data represents tensors as
`NumPy ndarrays <https://numpy.org/doc/stable/reference/arrays.ndarray.html>`__.

.. testcode::

    import ray

    ds = ray.data.read_images("example://image-datasets/simple")
    print(ds)

.. testoutput::

    Datastream(
      num_blocks=3,
      num_rows=3,
      schema={image: numpy.ndarray(shape=(32, 32, 3), dtype=uint8)}
    )

Batches of fixed-shape tensors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your tensors have a fixed shape, Ray Data represents batches with regular ndarrays.

.. doctest::

  >>> import ray
  >>> ds = ray.data.read_images("s3://anonymous@air-example-data2/imagenet-sample-images")
  >>> batch = ds.take_batch(batch_size=32)
  >>> batch["image"].shape
  (32, 28, 28, 3)
  >>> batch["image"].dtype
  dtype('uint8')

Batches of variable-shape tensors
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If your tensors vary in shape, Ray Data represents batches with ragged arrays.

.. doctest::

  >>> import ray
  >>> ds = ray.data.read_images("s3://anonymous@air-example-data/AnimalDetection")
  >>> batch = ds.take_batch(batch_size=32)
  >>> batch["image"].shape
  (32,)
  >>> batch["image"].dtype
  dtype('O')

Elements of these ragged arrays are regular ndarrays.

.. doctest::

  >>> batch["image"][0].dtype
  dtype('uint8')
  >>> batch["image"][0].shape
  (375, 500, 3)
  >>> batch["image"][3].shape
  (333, 500, 3)


Saving tensor data
------------------

Save tensor data in Parquet or Numpy files. Other formats aren't supported.

.. tab-set::

  .. tab-item:: Parquet

    Call :meth:`~ray.data.Dataset.write_parquet` to save data in Parquet files.

    .. testcode::

      import ray

      ds = ray.data.read_images("example://image-datasets/simple")
      ds.write_parquet("data")


  .. tab-item:: NumPy

    Call :meth:`~ray.data.Dataset.write_numpy` to save an ndarray column in a NumPy
    file.

    .. testcode::

      import ray

      ds = ray.data.read_images("example://image-datasets/simple")
      ds.write_numpy("simple.npy", column="image")

For more information on saving data, read :ref:`Saving data <loading_data>`.

Transforming variable-shape tensor data
---------------------------------------

Call :meth:`~ray.data.Dataset.map` to transform variable-shape tensor data. Avoid using
:meth:`~ray.data.Dataset.map_batches`.

.. testcode::

  import ray
  import numpy as np

  ds = ray.data.read_images("s3://anonymous@air-example-data/AnimalDetection")

  def increase_brightness(row: Dict[str, Any]) -> Dict[str, Any]:
    row["image"] = np.clip(row["image"] + 4, 0, 255)
    return row

  ds.map(increase_brightness)

For more information on transforming data, read
:ref:`Transforming data <transforming_data>`.
