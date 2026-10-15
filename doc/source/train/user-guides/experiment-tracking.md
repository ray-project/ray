---
myst:
  html_meta:
    description: "Use experiment tracking libraries such as MLflow and Weights & Biases with distributed Ray Train runs, covering credentials and per-worker logging."
---

(train-experiment-tracking-native)=

# Experiment tracking

Most experiment tracking libraries work out of the box with Ray Train. This guide shows how to set up your code so that your experiment tracking library works for distributed training with Ray Train. The end of the guide covers common errors to help you debug the setup.

The following code skeleton shows how to call a native experiment tracking library inside Ray Train:

```{testcode}
:skipif: True

from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig

def train_func():
    # Training code and native experiment tracking library calls go here.

scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
trainer = TorchTrainer(train_func, scaling_config=scaling_config)
result = trainer.fit()
```

To use a native experiment tracking library with Ray Train, put your tracking logic inside the {ref}`train_func<train-overview-training-function>` training function. This way, you can port your experiment tracking logic to Ray Train with minimal changes.

## Get started

The following examples use Weights & Biases (W&B) and MLflow, but you can adapt them to other frameworks.

::::{tab-set}
:::{tab-item} W&B
```{testcode}
:skipif: True

import ray
from ray import train
import wandb

# Step 1
# This ensures that all ray worker processes have `WANDB_API_KEY` set.
ray.init(runtime_env={"env_vars": {"WANDB_API_KEY": "your_api_key"}})

def train_func():
    # Step 1 and 2
    if train.get_context().get_world_rank() == 0:
        wandb.init(
            name=...,
            project=...,
            # ...
        )

    # ...
    loss = optimize()
    metrics = {"loss": loss}

    # Step 3
    if train.get_context().get_world_rank() == 0:
        # Only report the results from the rank 0 worker to W&B to avoid duplication.
        wandb.log(metrics)

    # ...

    # Step 4
    # Make sure that all loggings are uploaded to the W&B backend.
    if train.get_context().get_world_rank() == 0:
        wandb.finish()
```
:::

:::{tab-item} MLflow
```{testcode}
:skipif: True

from ray import train
import mlflow

# Run the following on the head node:
# $ databricks configure --token
# mv ~/.databrickscfg YOUR_SHARED_STORAGE_PATH
# This function assumes `databricks_config_file` is specified in the Trainer's `train_loop_config`.
def train_func(config):
    # Step 1 and 2
    os.environ["DATABRICKS_CONFIG_FILE"] = config["databricks_config_file"]
    mlflow.set_tracking_uri("databricks")
    mlflow.set_experiment_id(...)
    mlflow.start_run()

    # ...

    loss = optimize()

    metrics = {"loss": loss}

    # Step 3
    if train.get_context().get_world_rank() == 0:
        # Only report the results from the rank 0 worker to MLflow to avoid duplication.
        mlflow.log_metrics(metrics)
```
:::
::::

:::{tip}
A major difference between distributed and non-distributed training is that in distributed training, multiple processes run in parallel, and in some setups they produce the same results. If all of them report results to the tracking backend, you might get duplicated results. To avoid this, apply logging logic to only the rank 0 worker with {meth}`ray.train.get_context().get_world_rank() <ray.train.context.TrainContext.get_world_rank>`.

```{testcode}
:skipif: True

from ray import train
def train_func():
    ...
    if train.get_context().get_world_rank() == 0:
        # Add your logging logic only for rank0 worker.
    ...
```
:::

The interaction with the experiment tracking backend within the {ref}`train_func<train-overview-training-function>` has four logical steps:

1. Set up the connection to a tracking backend.
1. Configure and launch a run.
1. Log metrics.
1. Finish the run.

The following sections describe each step.

### Step 1: Connect to your tracking backend

First, decide which tracking backend to use, such as W&B, MLflow, TensorBoard, or Comet. If applicable, make sure that you set up credentials on each training worker.

::::{tab-set}
:::{tab-item} W&B
W&B offers both *online* and *offline* modes.

In *online* mode, you log to W&B's tracking service, so set the credentials inside {ref}`train_func<train-overview-training-function>`. For details, see {ref}`Set up credentials<set-up-credentials>`.

```{testcode}
:skipif: True

# This is equivalent to `os.environ["WANDB_API_KEY"] = "your_api_key"`
wandb.login(key="your_api_key")
```

In *offline* mode, you log to a local file system, so point the offline directory to a shared storage path that all nodes can write to. For details, see {ref}`Set up a shared file system<set-up-shared-file-system>`.

```{testcode}
:skipif: True

os.environ["WANDB_MODE"] = "offline"
wandb.init(dir="some_shared_storage_path/wandb")
```
:::

:::{tab-item} MLflow
MLflow offers both *local* and *remote* modes. In remote mode, you can log to a hosted service such as Databricks' MLflow service.

In *local* mode, you log to a local file system, so point the tracking URI to a shared storage path that all nodes can write to. For details, see {ref}`Set up a shared file system<set-up-shared-file-system>`.

```{testcode}
:skipif: True

mlflow.set_tracking_uri(uri="file://some_shared_storage_path/mlruns")
mlflow.start_run()
```

In *remote* mode with Databricks hosting, make sure that all nodes can access the Databricks configuration file. For details, see {ref}`Set up credentials<set-up-credentials>`.

```{testcode}
:skipif: True

# The MLflow client looks for a Databricks config file
# at the location specified by `os.environ["DATABRICKS_CONFIG_FILE"]`.
os.environ["DATABRICKS_CONFIG_FILE"] = config["databricks_config_file"]
mlflow.set_tracking_uri("databricks")
mlflow.start_run()
```
:::
::::

(set-up-credentials)=

#### Set up credentials

See each tracking library's API documentation for how to set up credentials. This step usually involves setting an environment variable or accessing a configuration file.

The simplest way to pass an environment variable credential to training workers is through {ref}`runtime environments <runtime-environments>`. Initialize Ray with the following code:

```{testcode}
:skipif: True

import ray
# This makes sure that training workers have the same env var set
ray.init(runtime_env={"env_vars": {"SOME_API_KEY": "your_api_key"}})
```

To use a configuration file, make sure that all nodes can access it. You can set up shared storage, or save a copy of the file on each node.

(set-up-shared-file-system)=

#### Set up a shared file system

Set up a network file system that all nodes in the cluster can access, such as Amazon Elastic File System or Google Cloud Filestore.

### Step 2: Configure and start the run

This step usually involves picking an identifier for the run and associating it with a project. See the tracking library's documentation for the semantics.

<!-- To conveniently link back to Ray Train run, you may want to log the persistent storage path -->
<!-- of the run as a config. -->

<!-- 
    .. testcode::

       def train_func():
            if ray.train.get_context().get_world_rank() == 0:
                   wandb.init(..., config={"ray_train_persistent_storage_path": "TODO: fill in when API stabilizes"}) -->

:::{tip}
When you run fault-tolerant training with auto-restoration, use a consistent ID to configure all tracking runs that logically belong to the same training run.
:::


### Step 3: Log metrics

Inside {ref}`train_func<train-overview-training-function>`, log parameters, metrics, models, or media content the same way you would in a non-distributed training script. You can also use a tracking framework's native integrations with specific training frameworks, such as `mlflow.pytorch.autolog()` or `lightning.pytorch.loggers.MLFlowLogger`.

### Step 4: Finish the run

This step ensures that the tracking library syncs all logs to the tracking service. Depending on their implementation, some tracking libraries first cache logs locally and sync them to the tracking service asynchronously. Finishing the run makes sure that the library syncs all logs before the training workers exit.

::::{tab-set}
:::{tab-item} W&B
```{testcode}
:skipif: True

# https://docs.wandb.ai/ref/python/finish
wandb.finish()
```
:::

:::{tab-item} MLflow
```{testcode}
:skipif: True

# https://mlflow.org/docs/1.2.0/python_api/mlflow.html
mlflow.end_run()
```
:::

:::{tab-item} Comet
```{testcode}
:skipif: True

# https://www.comet.com/docs/v2/api-and-sdk/python-sdk/reference/Experiment/#experimentend
Experiment.end()
```
:::
::::

## Examples

The following are runnable examples for PyTorch and PyTorch Lightning.

### PyTorch

:::{dropdown} Log to W&B
```{literalinclude} ../../../../python/ray/train/examples/experiment_tracking//torch_exp_tracking_wandb.py
:emphasize-lines: 16, 19-21, 59-60, 62-63
:language: python
:start-after: __start__
```
:::

:::{dropdown} Log to file-based MLflow
```{literalinclude} ../../../../python/ray/train/examples/experiment_tracking/torch_exp_tracking_mlflow.py
:emphasize-lines: 22-25, 58-59, 61-62, 68
:language: python
:start-after: __start__
:end-before: __end__
```
:::

### PyTorch Lightning

You can use the native logger integrations in PyTorch Lightning for W&B, CometML, MLflow, and TensorBoard with Ray Train's `TorchTrainer`.

The following runnable examples walk you through the process.

:::{dropdown} W&B
```{literalinclude} ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_model_dl.py
:language: python
:start-after: __model_dl_start__
```

```{literalinclude} ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_wandb.py
:language: python
:start-after: __lightning_experiment_tracking_wandb_start__
```
:::

:::{dropdown} MLflow
```{literalinclude} ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_model_dl.py
:language: python
:start-after: __model_dl_start__
```

```{literalinclude} ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_mlflow.py
:language: python
:start-after: __lightning_experiment_tracking_mlflow_start__
:end-before: __lightning_experiment_tracking_mlflow_end__
```
:::

:::{dropdown} Comet
```{literalinclude} ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_model_dl.py
:language: python
:start-after: __model_dl_start__
```

```{literalinclude} ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_comet.py
:language: python
:start-after: __lightning_experiment_tracking_comet_start__
```
:::

:::{dropdown} TensorBoard
```{literalinclude} ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_model_dl.py
:language: python
:start-after: __model_dl_start__
```

```{literalinclude} ../../../../python/ray/train/examples/experiment_tracking/lightning_exp_tracking_tensorboard.py
:language: python
:start-after: __lightning_experiment_tracking_tensorboard_start__
:end-before: __lightning_experiment_tracking_tensorboard_end__
```
:::

## Common errors

The following sections describe common setup errors and how to fix them.

### Missing credentials

You might get the following error even after you run the `wandb login` CLI command:

```none
wandb: ERROR api_key not configured (no-tty). call wandb.login(key=[your_api_key]).
```

This error likely means that the W&B credentials aren't set up correctly on the worker nodes. Make sure that you run `wandb.login` or pass `WANDB_API_KEY` to each training function. For details, see {ref}`Set up credentials <set-up-credentials>`.

### Missing configurations

You might get the following error even after you run `databricks configure`:

```none
databricks_cli.utils.InvalidConfigurationError: You haven't configured the CLI yet!
```

This error usually occurs because running `databricks configure` generates `~/.databrickscfg` only on the head node. Move this file to a shared location or copy it to each node. For details, see {ref}`Set up credentials <set-up-credentials>`.
