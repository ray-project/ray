---
myst:
  html_meta:
    description: "Distribute training with Horovod on Ray Train by adapting your training function and configuring HorovodTrainer."
---

(train-horovod)=

# Get started with distributed training using Horovod

Ray Train configures the Horovod environment and rendezvous server for you, so you can run your `DistributedOptimizer` training script. For more information, see the [Horovod documentation](https://horovod.readthedocs.io/en/stable/index.html).

## Quickstart

```{literalinclude} ./doc_code/hvd_trainer.py
:language: python
```

## Update your training function

First, update your {ref}`training function <train-overview-training-function>` to support distributed training.

If your training function already runs with the [Horovod Ray Executor](https://horovod.readthedocs.io/en/stable/ray_include.html#horovod-ray-executor), you shouldn't need to change it.

If you're new to Horovod, see the [Horovod guide](https://horovod.readthedocs.io/en/stable/index.html#get-started).

## Create a `HorovodTrainer`

A trainer is the primary Ray Train class for managing state and running training. For Horovod, set up a {class}`~ray.train.horovod.HorovodTrainer` as in the following example:

```{testcode}
:hide:

train_func = lambda: None
```

```{testcode}
from ray.train import ScalingConfig
from ray.train.horovod import HorovodTrainer
# For GPU Training, set `use_gpu` to True.
use_gpu = False
trainer = HorovodTrainer(
    train_func,
    scaling_config=ScalingConfig(use_gpu=use_gpu, num_workers=2)
)
```

Always use a `HorovodTrainer` when you train with Horovod, regardless of the training framework, such as PyTorch or TensorFlow.

To customize the backend setup, pass a {class}`~ray.train.horovod.HorovodConfig`:

```{testcode}
:skipif: True

from ray.train import ScalingConfig
from ray.train.horovod import HorovodTrainer, HorovodConfig

trainer = HorovodTrainer(
    train_func,
    tensorflow_backend=HorovodConfig(...),
    scaling_config=ScalingConfig(num_workers=2),
)
```

For more configuration options, see the {py:class}`~ray.train.data_parallel_trainer.DataParallelTrainer` API.

## Run a training function

After you have a distributed training function and a trainer, call `trainer.fit()` to start training:

```{testcode}
:skipif: True

trainer.fit()
```

## Further reading

The Ray Train {class}`~ray.train.horovod.HorovodTrainer` replaces the distributed communication backend of the native libraries with its own implementation, so the remaining integration points stay the same. If you use Horovod with {ref}`PyTorch <train-pytorch>` or {ref}`TensorFlow <train-tensorflow-overview>`, see the corresponding guide for configuration details.

If you implement your own Horovod-based training routine without any of the training libraries, read the {ref}`Ray Train user guides <train-user-guides>`. You can adapt much of their content to generic use cases.
