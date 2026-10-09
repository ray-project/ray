---
myst:
  html_meta:
    description: "Convert a PyTorch script to distributed training with Ray Train: TorchTrainer, sharded data loaders, metrics, checkpoints, and GPU scaling."
---

(train-pytorch)=

# Get started with distributed training using PyTorch

This tutorial shows you how to convert an existing PyTorch script to use Ray Train.

Learn how to do the following:

1. Configure a model to run distributed and on the correct CPU or GPU device.
1. Configure a data loader to shard data across the {ref}`workers <train-overview-worker>` and place data on the correct CPU or GPU device.
1. Configure a {ref}`training function <train-overview-training-function>` to report metrics and save checkpoints.
1. Configure {ref}`scaling <train-overview-scaling-config>` and CPU or GPU resource requirements for a training job.
1. Launch a distributed training job with {class}`~ray.train.torch.TorchTrainer`.

## Quickstart

For reference, the final code looks similar to the following:

```{testcode}
:skipif: True

from ray.train.torch import TorchTrainer
from ray.train import ScalingConfig

def train_func():
    # Your PyTorch training code here.
    ...

scaling_config = ScalingConfig(num_workers=2, use_gpu=True)
trainer = TorchTrainer(train_func, scaling_config=scaling_config)
result = trainer.fit()
```

1. `train_func` is the Python code that runs on each distributed training worker.
1. {class}`~ray.train.ScalingConfig` defines the number of distributed training workers and whether to use GPUs.
1. {class}`~ray.train.torch.TorchTrainer` launches the distributed training job.

Compare a PyTorch training script with and without Ray Train.

::::{tab-set}
:::{tab-item} PyTorch with Ray Train
```{code-block} python
:emphasize-lines: 12, 14, 21, 32, 36-37, 55-58, 59, 63, 66-73

import os
import tempfile

import torch
from torch.nn import CrossEntropyLoss
from torch.optim import Adam
from torch.utils.data import DataLoader
from torchvision.models import resnet18
from torchvision.datasets import FashionMNIST
from torchvision.transforms import ToTensor, Normalize, Compose

import ray.train.torch

def train_func():
    # Model, Loss, Optimizer
    model = resnet18(num_classes=10)
    model.conv1 = torch.nn.Conv2d(
        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
    )
    # [1] Prepare model.
    model = ray.train.torch.prepare_model(model)
    # model.to("cuda")  # This is done by `prepare_model`
    criterion = CrossEntropyLoss()
    optimizer = Adam(model.parameters(), lr=0.001)

    # Data
    transform = Compose([ToTensor(), Normalize((0.28604,), (0.32025,))])
    data_dir = os.path.join(tempfile.gettempdir(), "data")
    train_data = FashionMNIST(root=data_dir, train=True, download=True, transform=transform)
    train_loader = DataLoader(train_data, batch_size=128, shuffle=True)
    # [2] Prepare dataloader.
    train_loader = ray.train.torch.prepare_data_loader(train_loader)

    # Training
    for epoch in range(10):
        if ray.train.get_context().get_world_size() > 1:
            train_loader.sampler.set_epoch(epoch)

        for images, labels in train_loader:
            # This is done by `prepare_data_loader`!
            # images, labels = images.to("cuda"), labels.to("cuda")
            outputs = model(images)
            loss = criterion(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        # [3] Report metrics and checkpoint.
        metrics = {"loss": loss.item(), "epoch": epoch}
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            torch.save(
                model.module.state_dict(),
                os.path.join(temp_checkpoint_dir, "model.pt")
            )
            ray.train.report(
                metrics,
                checkpoint=ray.train.Checkpoint.from_directory(temp_checkpoint_dir),
            )
        if ray.train.get_context().get_world_rank() == 0:
            print(metrics)

# [4] Configure scaling and resource requirements.
scaling_config = ray.train.ScalingConfig(num_workers=2, use_gpu=True)

# [5] Launch distributed training job.
trainer = ray.train.torch.TorchTrainer(
    train_func,
    scaling_config=scaling_config,
    # [5a] If running in a multi-node cluster, this is where you
    # should configure the run's persistent storage that is accessible
    # across all worker nodes.
    # run_config=ray.train.RunConfig(storage_path="s3://..."),
)
result = trainer.fit()

# [6] Load the trained model.
with result.checkpoint.as_directory() as checkpoint_dir:
    model_state_dict = torch.load(os.path.join(checkpoint_dir, "model.pt"))
    model = resnet18(num_classes=10)
    model.conv1 = torch.nn.Conv2d(
        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
    )
    model.load_state_dict(model_state_dict)
```
:::

:::{tab-item} PyTorch
<!-- This snippet isn't tested because it doesn't use any Ray code. -->

```{testcode}
:skipif: True

import os
import tempfile

import torch
from torch.nn import CrossEntropyLoss
from torch.optim import Adam
from torch.utils.data import DataLoader
from torchvision.models import resnet18
from torchvision.datasets import FashionMNIST
from torchvision.transforms import ToTensor, Normalize, Compose

# Model, Loss, Optimizer
model = resnet18(num_classes=10)
model.conv1 = torch.nn.Conv2d(
    1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
)
model.to("cuda")
criterion = CrossEntropyLoss()
optimizer = Adam(model.parameters(), lr=0.001)

# Data
transform = Compose([ToTensor(), Normalize((0.28604,), (0.32025,))])
train_data = FashionMNIST(root='./data', train=True, download=True, transform=transform)
train_loader = DataLoader(train_data, batch_size=128, shuffle=True)

# Training
for epoch in range(10):
    for images, labels in train_loader:
        images, labels = images.to("cuda"), labels.to("cuda")
        outputs = model(images)
        loss = criterion(outputs, labels)
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

    metrics = {"loss": loss.item(), "epoch": epoch}
    checkpoint_dir = tempfile.mkdtemp()
    checkpoint_path = os.path.join(checkpoint_dir, "model.pt")
    torch.save(model.state_dict(), checkpoint_path)
    print(metrics)
```
:::
::::


## Set up a training function

```{include} common/torch-configure-train_func.md
```

### Set up a model

Use the {func}`ray.train.torch.prepare_model` utility function to do the following:

1. Move your model to the correct device.
1. Wrap it in `DistributedDataParallel`.

```diff
-from torch.nn.parallel import DistributedDataParallel
+import ray.train.torch

 def train_func():

     ...

     # Create model.
     model = ...

     # Set up distributed training and device placement.
-    device_id = ... # Your logic to get the right device.
-    model = model.to(device_id or "cpu")
-    model = DistributedDataParallel(model, device_ids=[device_id])
+    model = ray.train.torch.prepare_model(model)

     ...
```

### Set up a dataset

<!-- TODO: Update this to use Ray Data. -->

Use the {func}`ray.train.torch.prepare_data_loader` utility function, which does the following:

1. Adds a {class}`~torch.utils.data.distributed.DistributedSampler` to your {class}`~torch.utils.data.DataLoader`.
1. Moves the batches to the right device.

You don't need this step if you pass a Ray Data dataset to your trainer. See {ref}`data-ingest-torch`.

```diff
 from torch.utils.data import DataLoader
+import ray.train.torch

 def train_func():

     ...

     dataset = ...

     data_loader = DataLoader(dataset, batch_size=worker_batch_size, shuffle=True)
+    data_loader = ray.train.torch.prepare_data_loader(data_loader)

     for epoch in range(10):
+        if ray.train.get_context().get_world_size() > 1:
+            data_loader.sampler.set_epoch(epoch)

         for X, y in data_loader:
-            X = X.to_device(device)
-            y = y.to_device(device)

     ...
```

:::{tip}
The `batch_size` that `DataLoader` takes is the batch size for each worker. To convert between the worker batch size and the global batch size, use the following equation:

```{testcode}
:skipif: True

global_batch_size = worker_batch_size * ray.train.get_context().get_world_size()
```
:::

:::{note}
If you already set up your `DataLoader` with a `DistributedSampler`, {meth}`~ray.train.torch.prepare_data_loader` doesn't add another one and respects the configuration of the existing sampler.
:::

:::{note}
{class}`~torch.utils.data.distributed.DistributedSampler` doesn't work with a `DataLoader` that wraps {class}`~torch.utils.data.IterableDataset`. To work with a dataset iterator, use {ref}`Ray Data <data>` instead of a PyTorch `DataLoader`. Ray Data provides streaming data ingestion for large-scale datasets.

For details, see {ref}`data-ingest-torch`.
:::

### Report checkpoints and metrics

To monitor progress, report intermediate metrics and checkpoints with the {func}`ray.train.report` utility function.

```diff
+import os
+import tempfile

+import ray.train

 def train_func():

     ...

     with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        torch.save(
            model.state_dict(), os.path.join(temp_checkpoint_dir, "model.pt")
        )

+       metrics = {"loss": loss.item()}  # Training/validation metrics.

        # Build a Ray Train checkpoint from a directory
+       checkpoint = ray.train.Checkpoint.from_directory(temp_checkpoint_dir)

        # Ray Train will automatically save the checkpoint to persistent storage,
        # so the local `temp_checkpoint_dir` can be safely cleaned up after.
+       ray.train.report(metrics=metrics, checkpoint=checkpoint)

     ...
```

For details, see {ref}`train-monitoring-and-logging` and {ref}`train-checkpointing`.


```{include} common/torch-configure-run.md
:heading-offset: 1
```


## Next steps

After you convert your PyTorch training script to use Ray Train, explore the following resources:

* See the {ref}`user guides <train-user-guides>` to learn how to perform specific tasks.
* Browse the {doc}`examples <examples>` for end-to-end examples of Ray Train.
* See the {ref}`API reference <train-api>` for details on the classes and methods that this tutorial uses.
