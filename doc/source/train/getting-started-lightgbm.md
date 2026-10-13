---
myst:
  html_meta:
    description: "Distribute LightGBM training with Ray Train: build the training function, shard data with Ray Data, and configure scale, GPUs, and storage."
---

(train-lightgbm)=

# Get started with distributed training using LightGBM

This tutorial shows how to convert an existing LightGBM script to use Ray Train.

Learn how to do the following:

1. Configure a {ref}`training function <train-overview-training-function>` to report metrics and save checkpoints.
1. Configure {ref}`scaling <train-overview-scaling-config>` and CPU or GPU resource requirements for a training job.
1. Launch a distributed training job with a {class}`~ray.train.lightgbm.LightGBMTrainer`.

## Quickstart

The finished code looks similar to the following example:

```{testcode}
:skipif: True

import ray.train
from ray.train.lightgbm import LightGBMTrainer

def train_func():
    # Your LightGBM training code here.
    ...

scaling_config = ray.train.ScalingConfig(num_workers=2, resources_per_worker={"CPU": 4})
trainer = LightGBMTrainer(train_func, scaling_config=scaling_config)
result = trainer.fit()
```

1. `train_func` is the Python code that executes on each distributed training worker.
1. {class}`~ray.train.ScalingConfig` defines the number of distributed training workers and whether to use GPUs.
1. {class}`~ray.train.lightgbm.LightGBMTrainer` launches the distributed training job.

Compare a LightGBM training script with and without Ray Train.

::::{tab-set}
:::{tab-item} LightGBM with Ray Train
```{literalinclude} ./doc_code/lightgbm_quickstart.py
:language: python
:start-after: __lightgbm_ray_start__
:end-before: __lightgbm_ray_end__
```
:::

:::{tab-item} LightGBM
```{literalinclude} ./doc_code/lightgbm_quickstart.py
:language: python
:start-after: __lightgbm_start__
:end-before: __lightgbm_end__
```
:::
::::

## Set up a training function

To support distributed training, first wrap your [native](https://lightgbm.readthedocs.io/en/latest/Python-Intro.html) or [scikit-learn estimator](https://lightgbm.readthedocs.io/en/latest/Python-API.html#scikit-learn-api) LightGBM training code in a {ref}`training function <train-overview-training-function>`:

```{testcode}
:skipif: True

def train_func():
    # Your native LightGBM training code here.
    train_set = ...
    lightgbm.train(...)
```

Each distributed training worker executes this function.

You can also pass a dictionary to `train_func` as its input argument through the trainer's `train_loop_config` parameter, as the following example shows:

```{testcode} python
:skipif: True

def train_func(config):
    label_column = config["label_column"]
    num_boost_round = config["num_boost_round"]
    ...

config = {"label_column": "target", "num_boost_round": 100}
trainer = ray.train.lightgbm.LightGBMTrainer(train_func, train_loop_config=config, ...)
```

:::{warning}
To reduce serialization and deserialization overhead, don't pass large data objects through `train_loop_config`. Instead, initialize large objects, such as datasets and models, directly in `train_func`.

```diff
 def load_dataset():
     # Return a large in-memory dataset
     ...

 def load_model():
     # Return a large in-memory model instance
     ...

-config = {"data": load_dataset(), "model": load_model()}

 def train_func(config):
-    data = config["data"]
-    model = config["model"]

+    data = load_dataset()
+    model = load_model()
     ...

 trainer = ray.train.lightgbm.LightGBMTrainer(train_func, train_loop_config=config, ...)
```
:::


### Configure distributed training parameters

For distributed LightGBM training, add network communication parameters to your training configuration with {func}`ray.train.lightgbm.get_network_params`. This function automatically configures the network settings that workers use to communicate:

```diff
 def train_func():
     ...
     params = {
         # Your LightGBM training parameters here
         ...
+        "tree_learner": "data_parallel",
+        "pre_partition": True,
+        **ray.train.lightgbm.get_network_params(),
     }

     model = lightgbm.train(
         params,
         ...
     )
     ...
```

:::{note}
Set `tree_learner` to enable distributed training. For details, see the [LightGBM documentation](https://lightgbm.readthedocs.io/en/latest/Parallel-Learning-Guide.html#tree-learner). If you use Ray Data to load and shard your dataset, also set `pre_partition=True`, as the quickstart example shows.
:::

### Report metrics and save checkpoints

To persist your checkpoints and monitor training progress, add a {class}`ray.train.lightgbm.RayTrainReportCallback` utility callback to your `lightgbm.train` call:


```{testcode} python
:skipif: True

import lightgbm
from ray.train.lightgbm import RayTrainReportCallback

def train_func():
    ...
    bst = lightgbm.train(
        ...,
        callbacks=[
            RayTrainReportCallback(
                metrics=["eval-multi_logloss"], frequency=1
            )
        ],
    )
    ...
```


Report metrics and checkpoints to Ray Train to use {ref}`fault-tolerant training <train-fault-tolerance>` and the Ray Tune integration.

## Load data

For distributed LightGBM training, give each worker a different shard of the dataset.


```{testcode} python
:skipif: True

def get_train_dataset(world_rank: int) -> lightgbm.Dataset:
    # Define logic to get the Dataset shard for this worker rank
    ...

def get_eval_dataset(world_rank: int) -> lightgbm.Dataset:
    # Define logic to get the Dataset for each worker
    ...

def train_func():
    rank = ray.train.get_world_rank()
    train_set = get_train_dataset(rank)
    eval_set = get_eval_dataset(rank)
    ...
```

One common approach is to pre-shard the dataset and assign each worker a different set of files to read.

Pre-sharding doesn't adapt well to changes in the number of workers, because some workers might receive more data than others. For more flexibility, use Ray Data to shard the dataset at runtime.

### Use Ray Data to shard the dataset

{ref}`Ray Data <data>` is a distributed data processing library that you can use to shard and distribute your data across multiple workers.

First, load the entire dataset, not a pre-sharded subset of it, as a Ray Data Dataset. See the {ref}`data_quickstart` for details on how to load and preprocess data from different sources.

```{testcode} python
:skipif: True

train_dataset = ray.data.read_parquet("s3://path/to/entire/train/dataset/dir")
eval_dataset = ray.data.read_parquet("s3://path/to/entire/eval/dataset/dir")
```

In the training function, access this worker's dataset shards with {meth}`ray.train.get_dataset_shard` and convert each one into a native [`lightgbm.Dataset`](https://lightgbm.readthedocs.io/en/latest/Python-Intro.html#dataset).


```{testcode} python
:skipif: True

from ray.train.lightgbm import normalize_pandas_for_lightgbm

def get_dataset(dataset_name: str) -> lightgbm.Dataset:
    shard = ray.train.get_dataset_shard(dataset_name)
    df = normalize_pandas_for_lightgbm(shard.materialize().to_pandas())
    X, y = df.drop("target", axis=1), df["target"]
    return lightgbm.Dataset(X, label=y)

def train_func():
    train_set = get_dataset("train")
    eval_set = get_dataset("eval")
    ...
```

:::{note}
Starting in Ray 2.56, Ray Data preserves Arrow-backed pandas dtypes, such as `int64[pyarrow]`, when it converts Arrow blocks to pandas. LightGBM's pandas input validation rejects these dtypes, so normalize a pandas DataFrame from Ray Data before you pass it to `lightgbm.Dataset`.

{func}`ray.train.lightgbm.normalize_pandas_for_lightgbm` maps Arrow-backed numeric and Boolean columns to nullable NumPy equivalents and leaves all other columns unchanged. Use it instead of `df.convert_dtypes(dtype_backend="numpy_nullable")`, which scans every value in every column and also rewrites NumPy-backed columns as nullable equivalents, even when no Arrow dtypes are present.
:::


Finally, pass the datasets to the trainer. Ray Train then automatically shards them across the workers. The keys in the `datasets` dictionary must match the keys you pass to `get_dataset_shard` in the training function.


```{testcode} python
:skipif: True

trainer = LightGBMTrainer(..., datasets={"train": train_dataset, "eval": eval_dataset})
trainer.fit()
```


For details, see {ref}`data-ingest-torch`.

## Configure scale and GPUs

Outside your training function, create a {class}`~ray.train.ScalingConfig` object to configure the following:

1. {class}`num_workers <ray.train.ScalingConfig>`: The number of distributed training worker processes.
1. {class}`use_gpu <ray.train.ScalingConfig>`: Whether each worker uses a GPU or a CPU.
1. {class}`resources_per_worker <ray.train.ScalingConfig>`: The number of CPUs or GPUs per worker.

```{testcode}
from ray.train import ScalingConfig

# 4 nodes with 8 CPUs each.
scaling_config = ScalingConfig(num_workers=4, resources_per_worker={"CPU": 8})
```

:::{note}
When you use Ray Data with Ray Train, don't request every available CPU in your cluster with the `resources_per_worker` parameter. Ray Data needs CPUs to run data preprocessing operations in parallel. If you allocate all CPUs to training workers, Ray Data operations might become a bottleneck and reduce performance. Leave some CPUs available for Ray Data operations.

For example, if your cluster has 8 CPUs per node, you might allocate 6 CPUs to training workers and leave 2 CPUs for Ray Data:

```{testcode}
# Allocate 6 CPUs per worker, leaving resources for Ray Data operations
scaling_config = ScalingConfig(num_workers=4, resources_per_worker={"CPU": 6})
```
:::


To use GPUs, set the `use_gpu` parameter to `True` in your {class}`~ray.train.ScalingConfig` object. This setting requests and assigns one GPU per worker.

```{testcode}
# 1 node with 8 CPUs and 4 GPUs each.
scaling_config = ScalingConfig(num_workers=4, use_gpu=True)

# 4 nodes with 8 CPUs and 4 GPUs each.
scaling_config = ScalingConfig(num_workers=16, use_gpu=True)
```

When you use GPUs, also update your training function to use the assigned GPU by setting the `"device"` parameter to `"gpu"`. For details on LightGBM's GPU support, see the [LightGBM GPU documentation](https://lightgbm.readthedocs.io/en/latest/GPU-Tutorial.html).

```diff
  def train_func():
      ...

      params = {
          ...,
+         "device": "gpu",
      }

      bst = lightgbm.train(
          params,
          ...
      )
```


## Configure persistent storage

Create a {class}`~ray.train.RunConfig` object to specify the path where Ray Train saves results, including checkpoints and artifacts.

```{testcode}
from ray.train import RunConfig

# Local path (/some/local/path/unique_run_name)
run_config = RunConfig(storage_path="/some/local/path", name="unique_run_name")

# Shared cloud storage URI (s3://bucket/unique_run_name)
run_config = RunConfig(storage_path="s3://bucket", name="unique_run_name")

# Shared NFS path (/mnt/nfs/unique_run_name)
run_config = RunConfig(storage_path="/mnt/nfs", name="unique_run_name")
```


:::{warning}
A *shared storage location*, such as cloud storage or NFS, is optional for single-node clusters, but it's required for multi-node clusters. On a multi-node cluster, a local path {ref}`raises an error <multinode-local-storage-warning>` during checkpointing.
:::


For details, see {ref}`persistent-storage-guide`.

## Launch a training job

Put it all together and launch a distributed training job with a {class}`~ray.train.lightgbm.LightGBMTrainer`.

```{testcode}
:hide:

from ray.train import ScalingConfig

train_func = lambda: None
scaling_config = ScalingConfig(num_workers=1)
run_config = None
```

```{testcode}
from ray.train.lightgbm import LightGBMTrainer

trainer = LightGBMTrainer(
    train_func, scaling_config=scaling_config, run_config=run_config
)
result = trainer.fit()
```

## Access training results

After training completes, `trainer.fit()` returns a {class}`~ray.train.Result` object that contains information about the training run, including the metrics and checkpoints reported during training.

```{testcode}
result.metrics     # The metrics reported during training.
result.checkpoint  # The latest checkpoint reported during training.
result.path        # The path where logs are stored.
result.error       # The exception that was raised, if training failed.
```

For more usage examples, see {ref}`train-inspect-results`.

## Next steps

After you convert your LightGBM training script to use Ray Train, explore the following resources:

* See the {ref}`user guides <train-user-guides>` to learn how to perform specific tasks.
* Browse the {doc}`examples <examples>` for end-to-end examples of how to use Ray Train.
* See the {ref}`API reference <train-api>` for details on the classes and methods in this tutorial.
