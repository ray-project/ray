---
myst:
  html_meta:
    description: "Iterate over a Ray Data Dataset by rows or batches, including the framework-specific batch formats used in training loops."
---

(iterating-over-data)=

# Iterating over data

With Ray Data, you can iterate over rows or batches of data.

This guide shows you how to do the following:

* [Iterate over rows](#iterating-over-rows)
* [Iterate over batches](#iterating-over-batches)
* [Iterate over batches with shuffling](#iterating-over-batches-with-shuffling)
* [Split datasets for distributed parallel training](#splitting-datasets-for-distributed-parallel-training)

(iterating-over-rows)=

## Iterate over rows

To iterate over the rows of your dataset, call {meth}`Dataset.iter_rows() <ray.data.Dataset.iter_rows>`. Ray Data represents each row as a dictionary.

```{testcode}
import ray

ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

for row in ds.iter_rows():
    print(row)
```

```{testoutput}
{'sepal length (cm)': 5.1, 'sepal width (cm)': 3.5, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}
{'sepal length (cm)': 4.9, 'sepal width (cm)': 3.0, 'petal length (cm)': 1.4, 'petal width (cm)': 0.2, 'target': 0}
...
{'sepal length (cm)': 5.9, 'sepal width (cm)': 3.0, 'petal length (cm)': 5.1, 'petal width (cm)': 1.8, 'target': 2}
```

For more information on working with rows, see {ref}`Transforming rows <transforming_rows>` and {ref}`Inspect rows <inspecting-rows>`.

(iterating-over-batches)=

## Iterate over batches

A batch contains data from multiple rows. To iterate over batches in different formats, call one of the following methods:

* {meth}`Dataset.iter_batches() <ray.data.Dataset.iter_batches>`
* {meth}`Dataset.iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`
* {meth}`Dataset.to_tf() <ray.data.Dataset.to_tf>`

::::{tab-set}

:::{tab-item} NumPy
:sync: NumPy

```{testcode}
import ray

ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

for batch in ds.iter_batches(batch_size=2, batch_format="numpy"):
    print(batch)
```

```{testoutput}
:options: +MOCK

{'image': array([[[[...]]]], dtype=uint8)}
...
{'image': array([[[[...]]]], dtype=uint8)}
```

:::

:::{tab-item} pandas
:sync: pandas

```{testcode}
import ray

ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

for batch in ds.iter_batches(batch_size=2, batch_format="pandas"):
    print(batch)
```

```{testoutput}
:options: +MOCK

   sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
0                5.1               3.5                1.4               0.2       0
1                4.9               3.0                1.4               0.2       0
...
   sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
0                6.2               3.4                5.4               2.3       2
1                5.9               3.0                5.1               1.8       2
```

:::

:::{tab-item} Torch
:sync: Torch

```{testcode}
import ray

ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

for batch in ds.iter_torch_batches(batch_size=2):
    print(batch)
```

```{testoutput}
:options: +MOCK

{'image': tensor([[[[...]]]], dtype=torch.uint8)}
...
{'image': tensor([[[[...]]]], dtype=torch.uint8)}
```

:::

:::{tab-item} TensorFlow
:sync: TensorFlow

```{testcode}
import ray

ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

tf_dataset = ds.to_tf(
    feature_columns="sepal length (cm)",
    label_columns="target",
    batch_size=2
)
for features, labels in tf_dataset:
    print(features, labels)
```

```{testoutput}
tf.Tensor([5.1 4.9], shape=(2,), dtype=float64) tf.Tensor([0 0], shape=(2,), dtype=int64)
...
tf.Tensor([6.2 5.9], shape=(2,), dtype=float64) tf.Tensor([2 2], shape=(2,), dtype=int64)
```

:::

::::

For more information on working with batches, see {ref}`Transforming batches <transforming_batches>` and {ref}`Inspect batches <inspecting-batches>`.

(iterating-over-batches-with-shuffling)=

## Iterate over batches with shuffling

{class}`Dataset.random_shuffle <ray.data.Dataset.random_shuffle>` is slow because it shuffles all rows. If you don't need a full global shuffle, specify `local_shuffle_buffer_size` to shuffle a subset of rows, up to the buffer size, during iteration. This local shuffle isn't a true global shuffle like `random_shuffle`, but it performs better because it avoids excessive data movement. For details on these options, see {doc}`Shuffling data <shuffling-data>`.

:::{tip}
Set `local_shuffle_buffer_size` to the smallest value that achieves sufficient randomness. Higher values increase randomness but slow down iteration. To diagnose slowdowns, see {ref}`Shuffle rows with a local buffer <local_shuffle_buffer>`.
:::

::::{tab-set}

:::{tab-item} NumPy
:sync: NumPy

```{testcode}
import ray

ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")

for batch in ds.iter_batches(
    batch_size=2,
    batch_format="numpy",
    local_shuffle_buffer_size=250,
):
    print(batch)
```

```{testoutput}
:options: +MOCK

{'image': array([[[[...]]]], dtype=uint8)}
...
{'image': array([[[[...]]]], dtype=uint8)}
```

:::

:::{tab-item} pandas
:sync: pandas

```{testcode}
import ray

ds = ray.data.read_csv("s3://anonymous@air-example-data/iris.csv")

for batch in ds.iter_batches(
    batch_size=2,
    batch_format="pandas",
    local_shuffle_buffer_size=250,
):
    print(batch)
```

```{testoutput}
:options: +MOCK

   sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
0                6.3               2.9                5.6               1.8       2
1                5.7               4.4                1.5               0.4       0
...
   sepal length (cm)  sepal width (cm)  petal length (cm)  petal width (cm)  target
0                5.6               2.7                4.2               1.3       1
1                4.8               3.0                1.4               0.1       0
```

:::

:::{tab-item} Torch
:sync: Torch

```{testcode}
import ray

ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
for batch in ds.iter_torch_batches(
    batch_size=2,
    local_shuffle_buffer_size=250,
):
    print(batch)
```

```{testoutput}
:options: +MOCK

{'image': tensor([[[[...]]]], dtype=torch.uint8)}
...
{'image': tensor([[[[...]]]], dtype=torch.uint8)}
```

:::

:::{tab-item} TensorFlow
:sync: TensorFlow

```{testcode}
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
```

```{testoutput}
:options: +MOCK

tf.Tensor([5.2 6.3], shape=(2,), dtype=float64) tf.Tensor([1 2], shape=(2,), dtype=int64)
...
tf.Tensor([5.  5.8], shape=(2,), dtype=float64) tf.Tensor([0 0], shape=(2,), dtype=int64)
```

:::

::::

(splitting-datasets-for-distributed-parallel-training)=

## Split datasets for distributed parallel training

For distributed data parallel training, call {meth}`Dataset.streaming_split <ray.data.Dataset.streaming_split>` to split your dataset into disjoint shards.

:::{note}
If you're using {ref}`Ray Train <train-docs>`, you don't need to split the dataset, because Ray Train splits it automatically. To learn more, see the {ref}`data loading and preprocessing guide <data-ingest-torch>`.
:::

```{testcode}
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
```
