---
myst:
  html_meta:
    description: "Core Ray Train concepts: the training function, worker processes, ScalingConfig for CPU and GPU resources, and the trainer."
---

(train-key-concepts)=

(train-overview)=

# Ray Train overview

To use Ray Train effectively, understand the following four main concepts:

* {ref}`Training function <train-overview-training-function>`: A Python function that contains your model training logic.
* {ref}`Worker <train-overview-worker>`: A process that runs the training function.
* {ref}`Scaling configuration <train-overview-scaling-config>`: A configuration of the number of workers and the compute resources, such as CPUs or GPUs.
* {ref}`Trainer <train-overview-trainers>`: A Python class that ties together the training function, workers, and scaling configuration to run a distributed training job.

```{figure} images/overview.png
:align: center
```

(train-overview-training-function)=

## Training function

The training function is a Python function that you define, and it contains the end-to-end model training loop. When you launch a distributed training job, each worker runs this training function.

Ray Train documentation uses the following two conventions:

* `train_func` is a function that you define, and it contains the training code.
* You pass `train_func` to the trainer's `train_loop_per_worker` parameter.

```{testcode}
def train_func():
    """User-defined training function that runs on each distributed worker process.

    This function typically contains logic for loading the model,
    loading the dataset, training the model, saving checkpoints,
    and logging metrics.
    """
    ...
```

(train-overview-worker)=

## Worker

Ray Train distributes model training compute to individual worker processes across the cluster. Each worker is a process that runs `train_func`. The number of workers determines the parallelism of the training job, and you set it in the {class}`~ray.train.ScalingConfig`.

(train-overview-scaling-config)=

## Scaling configuration

The {class}`~ray.train.ScalingConfig` defines the scale of the training job. Specify the following two basic parameters for worker parallelism and compute resources:

* {class}`num_workers <ray.train.ScalingConfig>`: The number of workers to launch for a distributed training job.
* {class}`use_gpu <ray.train.ScalingConfig>`: Whether each worker uses a GPU.

```{testcode}
from ray.train import ScalingConfig

# Single worker with a CPU
scaling_config = ScalingConfig(num_workers=1, use_gpu=False)

# Single worker with a GPU
scaling_config = ScalingConfig(num_workers=1, use_gpu=True)

# Multiple workers, each with a GPU
scaling_config = ScalingConfig(num_workers=4, use_gpu=True)
```

(train-overview-trainers)=

## Trainer

The trainer ties the previous three concepts together to launch distributed training jobs. Ray Train provides {ref}`trainer classes <train-api>` for different frameworks. When you call the {meth}`fit() <ray.train.trainer.BaseTrainer.fit>` method, the trainer does the following to run the training job:

1. Launches workers according to the {ref}`scaling_config <train-overview-scaling-config>`.
1. Sets up the framework's distributed environment on all workers.
1. Runs `train_func` on all workers.

```{testcode}
:hide:

def train_func():
    pass

scaling_config = ScalingConfig(num_workers=1, use_gpu=False)
```

```{testcode}
from ray.train.torch import TorchTrainer

trainer = TorchTrainer(train_func, scaling_config=scaling_config)
trainer.fit()
```
