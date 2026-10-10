---
myst:
  html_meta:
    description: "Convert an XGBoost script to distributed training with Ray Train using XGBoostTrainer, with checkpointing and a CPU or GPU ScalingConfig."
---

(train-xgboost)=

# Get started with distributed training using XGBoost

This tutorial shows how to convert an existing XGBoost script to use Ray Train.

In this tutorial, you learn how to do the following:

1. Configure a {ref}`training function <train-overview-training-function>` to report metrics and save checkpoints.
1. Configure {ref}`scaling <train-overview-scaling-config>` and CPU or GPU resource requirements for a training job.
1. Launch a distributed training job with a {class}`~ray.train.xgboost.XGBoostTrainer`.

## Quickstart

For reference, the final code looks similar to the following:

```{testcode}
:skipif: True

import ray.train
from ray.train.xgboost import XGBoostTrainer

def train_func():
    # Your XGBoost training code here.
    ...

scaling_config = ray.train.ScalingConfig(num_workers=2, resources_per_worker={"CPU": 4})
trainer = XGBoostTrainer(train_func, scaling_config=scaling_config)
result = trainer.fit()
```

1. `train_func` is the Python code that executes on each distributed training worker.
1. {class}`~ray.train.ScalingConfig` defines the number of distributed training workers and whether to use GPUs.
1. {class}`~ray.train.xgboost.XGBoostTrainer` launches the distributed training job.

Compare an XGBoost training script with and without Ray Train.

::::{tab-set}
:::{tab-item} XGBoost with Ray Train
```{literalinclude} ./doc_code/xgboost_quickstart.py
:emphasize-lines: 3-4, 7-8, 11, 15-16, 19-20, 48, 53, 56-64
:language: python
:start-after: __xgboost_ray_start__
:end-before: __xgboost_ray_end__
```
:::

:::{tab-item} XGBoost
```{literalinclude} ./doc_code/xgboost_quickstart.py
:language: python
:start-after: __xgboost_start__
:end-before: __xgboost_end__
```
:::
::::


## Set up a training function

First, update your training code to support distributed training. Wrap your [native](https://xgboost.readthedocs.io/en/latest/python/python_intro.html) or [scikit-learn estimator](https://xgboost.readthedocs.io/en/latest/python/sklearn_estimator.html) XGBoost training code in a {ref}`training function <train-overview-training-function>`:

```{testcode}
:skipif: True

def train_func():
    # Your native XGBoost training code here.
    dmatrix = ...
    xgboost.train(...)
```

Each distributed training worker executes this function.

You can also pass the input argument for `train_func` as a dictionary through the trainer's `train_loop_config` argument, as in the following example:

```{testcode} python
:skipif: True

def train_func(config):
    label_column = config["label_column"]
    num_boost_round = config["num_boost_round"]
    ...

config = {"label_column": "y", "num_boost_round": 10}
trainer = ray.train.xgboost.XGBoostTrainer(train_func, train_loop_config=config, ...)
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

 trainer = ray.train.xgboost.XGBoostTrainer(train_func, train_loop_config=config, ...)
```
:::

Ray Train automatically sets up the worker communication that distributed XGBoost training needs.

### Report metrics and save checkpoints

To persist your checkpoints and monitor training progress, add a {class}`ray.train.xgboost.RayTrainReportCallback` utility callback to your `xgboost.train` call:


```{testcode} python
:skipif: True

import xgboost
from ray.train.xgboost import RayTrainReportCallback

def train_func():
    ...
    bst = xgboost.train(
        ...,
        callbacks=[
            RayTrainReportCallback(
                metrics=["eval-logloss"], frequency=1
            )
        ],
    )
    ...
```


Report metrics and checkpoints to Ray Train to use {ref}`fault-tolerant training <train-fault-tolerance>` and the Ray Tune integration.

## Load data

In distributed XGBoost training, give each worker a different shard of the dataset.


```{testcode} python
:skipif: True

def get_train_dataset(world_rank: int) -> xgboost.DMatrix:
    # Define logic to get the DMatrix shard for this worker rank
    ...

def get_eval_dataset(world_rank: int) -> xgboost.DMatrix:
    # Define logic to get the DMatrix for each worker
    ...

def train_func():
    rank = ray.train.get_world_rank()
    dtrain = get_train_dataset(rank)
    deval = get_eval_dataset(rank)
    ...
```

A common approach is to pre-shard the dataset and assign each worker a different set of files to read.

Pre-sharding doesn't adapt well to changes in the number of workers, because some workers might end up with more data than others. For more flexibility, Ray Data can shard the dataset at runtime.

### Use Ray Data to shard the dataset

{ref}`Ray Data <data>` is a distributed data processing library. Use it to shard and distribute your data across multiple workers.

First, load your entire dataset, rather than a shard of it, as a Ray Data `Dataset`. For details on how to load and preprocess data from different sources, see {ref}`data_quickstart`.

```{testcode} python
:skipif: True

train_dataset = ray.data.read_parquet("s3://path/to/entire/train/dataset/dir")
eval_dataset = ray.data.read_parquet("s3://path/to/entire/eval/dataset/dir")
```

In the training function, access this worker's dataset shards with {meth}`ray.train.get_dataset_shard`. Convert each shard into a native [`xgboost.DMatrix`](https://xgboost.readthedocs.io/en/stable/python/python_api.html#xgboost.DMatrix).


```{testcode} python
:skipif: True

def get_dmatrix(dataset_name: str) -> xgboost.DMatrix:
    shard = ray.train.get_dataset_shard(dataset_name)
    df = shard.materialize().to_pandas()
    X, y = df.drop("target", axis=1), df["target"]
    return xgboost.DMatrix(X, label=y)

def train_func():
    dtrain = get_dmatrix("train")
    deval = get_dmatrix("eval")
    ...
```


Finally, pass the datasets to the trainer, which automatically shards them across the workers. The keys in the `datasets` dictionary must match the keys you pass to `get_dataset_shard` in the training function.


```{testcode} python
:skipif: True

trainer = XGBoostTrainer(..., datasets={"train": train_dataset, "eval": eval_dataset})
trainer.fit()
```


For details, see {ref}`data-ingest-torch`.

## Configure scale and GPUs

Outside your training function, create a {class}`~ray.train.ScalingConfig` object to configure the following three settings:

- {class}`num_workers <ray.train.ScalingConfig>` sets the number of distributed training worker processes.
- {class}`use_gpu <ray.train.ScalingConfig>` sets whether each worker uses a GPU or a CPU.
- {class}`resources_per_worker <ray.train.ScalingConfig>` sets the number of CPUs or GPUs per worker.

```{testcode}
from ray.train import ScalingConfig

# 4 nodes with 8 CPUs each.
scaling_config = ScalingConfig(num_workers=4, resources_per_worker={"CPU": 8})
```

:::{note}
When you use Ray Data with Ray Train, don't request all available CPUs in your cluster with the `resources_per_worker` parameter. Ray Data needs CPU resources to run data preprocessing operations in parallel. If you allocate all CPUs to training workers, Ray Data operations might bottleneck and reduce performance. Leave some CPU resources available for Ray Data operations.

For example, if your cluster has 8 CPUs per node, you might allocate 6 CPUs to training workers and leave 2 CPUs for Ray Data:

```{testcode}
# Allocate 6 CPUs per worker, leaving resources for Ray Data operations
scaling_config = ScalingConfig(num_workers=4, resources_per_worker={"CPU": 6})
```
:::


To use GPUs, set the `use_gpu` parameter to `True` in your {class}`~ray.train.ScalingConfig` object. This setting requests and assigns a single GPU per worker.

```{testcode}
# 1 node with 8 CPUs and 4 GPUs each.
scaling_config = ScalingConfig(num_workers=4, use_gpu=True)

# 4 nodes with 8 CPUs and 4 GPUs each.
scaling_config = ScalingConfig(num_workers=16, use_gpu=True)
```

When you use GPUs, also update your training function to use the assigned GPU by setting the `"device"` parameter to `"cuda"`. For details on XGBoost's GPU support, see the [XGBoost GPU documentation](https://xgboost.readthedocs.io/en/stable/gpu/index.html).

```diff
  def train_func():
      ...

      params = {
          ...,
+         "device": "cuda",
      }

      bst = xgboost.train(
          params,
          ...
      )
```


## Configure persistent storage

Create a {class}`~ray.train.RunConfig` object to specify where to save results, including checkpoints and artifacts.

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
A *shared storage location*, such as cloud storage or NFS, is optional for single-node clusters but required for multi-node clusters. On a multi-node cluster, a local path {ref}`raises an error <multinode-local-storage-warning>` during checkpointing.
:::


For details, see {ref}`persistent-storage-guide`.


## Launch a training job

To tie it all together, launch a distributed training job with an {class}`~ray.train.xgboost.XGBoostTrainer`.

```{testcode}
:hide:

from ray.train import ScalingConfig

train_func = lambda: None
scaling_config = ScalingConfig(num_workers=1)
run_config = None
```

```{testcode}
from ray.train.xgboost import XGBoostTrainer

trainer = XGBoostTrainer(
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

After you convert your XGBoost training script to use Ray Train, explore the following resources:

* See the {ref}`user guides <train-user-guides>` to learn how to perform specific tasks.
* Browse the {doc}`examples <examples>` for end-to-end examples of how to use Ray Train.
* See the {ref}`API reference <train-api>` for details on the classes and methods from this tutorial.
