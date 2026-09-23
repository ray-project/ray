---
myst:
  html_meta:
    description: "Work with n-dimensional array data in Ray Data, covering fixed- and variable-shape tensor batches, transformations, and saving."
---

(working_with_tensors)=
(working-with-tensors--numpy)=

# Working with tensors and NumPy

Tensors, or n-dimensional arrays, are ubiquitous in machine learning workloads. This guide describes the limitations of and best practices for working with tensor data in Ray Data.

(tensor-data-representation)=

## How does Ray Data represent tensors?

Ray Data represents tensors as [NumPy ndarrays](https://numpy.org/doc/stable/reference/arrays.ndarray.html).

```{testcode}
import ray

ds = ray.data.read_images("s3://anonymous@air-example-data/digits")
print(ds)
```

```{testoutput}
Dataset(num_rows=100, schema=...)
```

(batches-of-fixed-shape-tensors)=

### How does Ray Data batch fixed-shape tensors?

If your tensors have a fixed shape, Ray Data represents batches as regular ndarrays.

```{doctest}
>>> import ray
>>> ds = ray.data.read_images("s3://anonymous@air-example-data/digits")
>>> batch = ds.take_batch(batch_size=32)
>>> batch["image"].shape
(32, 28, 28)
>>> batch["image"].dtype
dtype('uint8')

```

(batches-of-variable-shape-tensors)=

### How does Ray Data batch variable-shape tensors?

If your tensors vary in shape, Ray Data represents batches as arrays of object dtype.

```{doctest}
>>> import ray
>>> ds = ray.data.read_images("s3://anonymous@air-example-data/AnimalDetection")
>>> batch = ds.take_batch(batch_size=32)
>>> batch["image"].shape
(32,)
>>> batch["image"].dtype
dtype('O')

```

Each element of these object arrays is a regular ndarray.

```{doctest}
>>> batch["image"][0].dtype
dtype('uint8')
>>> batch["image"][0].shape  # doctest: +SKIP
(375, 500, 3)
>>> batch["image"][3].shape  # doctest: +SKIP
(333, 465, 3)

```

(transforming_tensors)=
(transforming-tensor-data)=

## Transform tensor data

Call {meth}`~ray.data.Dataset.map` or {meth}`~ray.data.Dataset.map_batches` to transform tensor data.

```{testcode}
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
ds.map_batches(batch_increase_brightness, batch_size="auto")
```

Set `batch_size="auto"` to have Ray Data pick a batch size based on the size of your data.

Besides NumPy ndarrays, Ray Data also treats returned lists of NumPy ndarrays as tensor data. The same goes for returned objects that implement `__array__`, such as `torch.Tensor`.

For more information on transforming data, see {ref}`Transforming data <transforming_data>`.

(saving-tensor-data)=

## Save tensor data

Save tensor data in formats such as Parquet, NumPy, and JSON. For all supported formats, see the {ref}`Saving Data API <saving-data-api>`.

::::{tab-set}

:::{tab-item} Parquet

Call {meth}`~ray.data.Dataset.write_parquet` to save data in Parquet files.

```{testcode}
import ray

ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
ds.write_parquet("/tmp/simple")
```

:::

:::{tab-item} NumPy

Call {meth}`~ray.data.Dataset.write_numpy` to save an ndarray column in NumPy files.

```{testcode}
import ray

ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
ds.write_numpy("/tmp/simple", column="image")
```

:::

:::{tab-item} JSON

Call {meth}`~ray.data.Dataset.write_json` to save images in a JSON file.

```{testcode}
import ray

ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
ds.write_json("/tmp/simple")
```

:::

::::

For more information on saving data, see {ref}`Saving data <saving-data>`.
