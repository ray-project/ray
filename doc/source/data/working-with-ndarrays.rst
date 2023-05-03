.. _working_with_ndarrays:

Working with array-like data
============================

Array-like data is ubiquitous in ML workloads. This guide describes the limitations
and best practices of working such data.

Data representation
-------------------

Ray Data represents array-like data as NumPy ndarrays.

.. testcode::

    import ray

    ds = ray.data.read_images("s3://anonymous@air-example-data/AnimalDetection")
    print(ds)

.. testoutput::

    Datastream(
      num_blocks=3,
      num_rows=3,
      schema={image: numpy.ndarray(shape=(32, 32, 3), dtype=uint8)}
    )


To inspect batches of array-like data, call :meth:`~ray.data.Dataset.iter_batches`.

.. testcode::

  batch = next(iter(ds.iter_batches(batch_size=4)))
  print(batch["image"].shape)
  print(batch["image"].dtype)

.. testoutput::

  (3, 32, 32, 3)
  dtype('uint8')

Saving array-like data
----------------------

Save array-like data in Parquet or Numpy files. Other formats aren't supported.

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

About ragged arrays
-------------------

A ragged array is a nested array where arrays vary in size.

.. testcode::

  from ray.air.util.tensor_extensions.utils import create_ragged_ndarray

  values = [np.zeros((3, 1)), np.zeros((3, 2))]
  ragged_array = create_ragged_ndarray(values)
  print(ragged_array)

.. testoutput::

  [array([[0.],
        [0.],
        [0.]]) array([[0., 0.],
                      [0., 0.],
                      [0., 0.]])]

Ray Data represents ragged arrays as single-dimensional arrays of object dtype.

.. doctest::

  >>> import ray
  >>> ds = ray.data.read_images("s3://anonymous@air-example-data/AnimalDetection")
  >>> batch = ds.take_batch(batch_size=32)
  >>> batch["image"].shape
  (32,)
  >>> batch["image"].dtype
  dtype('O')

In contrast to the outer array, member arrays can contain one or more dimensions. They
can also vary in size and type.

.. doctest::

  >>> batch["image"][0].dtype
  dtype('uint8')
  >>> batch["image"][0].shape
  (375, 500, 3)
  >>> batch["image"][3].shape
  (333, 500, 3)

Transforming ragged arrays
--------------------------

Call :meth:`~ray.data.Dataset.map` to transform ragged arrays. Avoid using
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
