---
myst:
  html_meta:
    description: "Run a Ray Train training function without launching worker actors, for faster iteration, testing, and single-node multi-GPU runs."
---

(train-local-mode)=

# Local mode

:::{important}
This user guide shows how to use local mode with Ray Train V2 only. To migrate from Ray Train V1 to V2, see the [Train V2 migration guide](https://github.com/ray-project/ray/issues/49454).
:::

(what-is-local-mode)=

## What's local mode?

Local mode in Ray Train runs your training function directly in the current process instead of launching Ray Train worker actors and distributing your training code across them. This gives you a simplified debugging environment where you can iterate quickly on your training logic.

Local mode supports two execution modes:

* **Single-process mode**: Runs your training function in a single process. Use it for rapid iteration and debugging.
* **Multi-process mode with `torchrun`**: Launches multiple processes for multi-GPU training. Use it to debug distributed training logic with familiar tools.

(how-to-enable-local-mode)=

## Enable local mode

To enable local mode, set `num_workers=0` in your {class}`~ray.train.ScalingConfig`:

```{testcode}
:skipif: True

from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

def train_func(config):
    # Your training logic
    pass

trainer = TorchTrainer(
    train_loop_per_worker=train_func,
    scaling_config=ScalingConfig(num_workers=0),
)
result = trainer.fit()
```

Local mode provides the same `ray.train` APIs you use in distributed training, so your training code runs without other changes. You can verify your training logic locally before you scale to distributed training.

## When to use local mode

Use single-process local mode for the following tasks:

* **Develop and iterate quickly**: Test changes to your training function locally.
* **Write unit tests**: Verify that your training logic works correctly in a simplified environment.
* **Debug training logic**: Step through your training code with standard Python debugging tools to find issues.

Use multi-process local mode with `torchrun` for the following tasks:

* **Test multi-GPU logic**: Verify that your distributed training code works correctly across multiple GPUs with familiar `torchrun` commands.
* **Migrate existing code**: Bring existing `torchrun`-based training scripts into Ray Train and keep your development workflow.
* **Debug distributed behavior**: Isolate issues in your distributed training logic with `torchrun` process management.

:::{note}
In local mode, Ray Train doesn't launch worker actors. Your training code can still launch Ray actors or use other Ray features, such as Ray Data in single-process mode.
:::

## Single-process local mode

The following example shows how to use single-process local mode with PyTorch:

```{testcode}
:skipif: True

import torch
from torch import nn
import ray
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

def train_func(config):
    model = nn.Linear(10, 1)
    optimizer = torch.optim.SGD(model.parameters(), lr=config["lr"])

    for epoch in range(config["epochs"]):
        # Training loop
        loss = model(torch.randn(32, 10)).sum()
        loss.backward()
        optimizer.step()

        # Report metrics
        ray.train.report({"loss": loss.item()})

trainer = TorchTrainer(
    train_loop_per_worker=train_func,
    train_loop_config={"lr": 0.01, "epochs": 3},
    scaling_config=ScalingConfig(num_workers=0),
)
result = trainer.fit()
print(f"Final loss: {result.metrics['loss']}")
```

:::{note}
Local mode works with all Ray Train framework integrations, including PyTorch Lightning, Hugging Face Transformers, LightGBM, XGBoost, and TensorFlow.
:::

(testing-with-local-mode)=

### Test with local mode

The following example shows how to write a unit test with local mode:

```{testcode}
:skipif: True

import pytest
import ray
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

def test_training_runs():
    def train_func(config):
        # Report minimal training result
        ray.train.report({"loss": 0.5})

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        scaling_config=ScalingConfig(num_workers=0),
    )
    result = trainer.fit()

    assert result.error is None
    assert result.metrics["loss"] == 0.5
```

(using-local-mode-with-ray-data)=

### Use local mode with Ray Data

Single-process local mode works with Ray Data for data loading and preprocessing. Ray Data processes your data and returns it to your training function in the local process.

The following example shows how to use Ray Data with single-process local mode:

```{testcode}
:skipif: True

import ray
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

def train_func(config):
    # Get the dataset shard
    train_dataset = ray.train.get_dataset_shard("train")

    # Iterate over batches
    for batch in train_dataset.iter_batches(batch_size=32):
        # Training logic
        pass

# Create a Ray Dataset
dataset = ray.data.read_csv("s3://bucket/data.csv")

trainer = TorchTrainer(
    train_loop_per_worker=train_func,
    scaling_config=ScalingConfig(num_workers=0),
    datasets={"train": dataset},
)
result = trainer.fit()
```

:::{warning}
Ray Data isn't supported when you use `torchrun` for multi-process training in local mode. For multi-process training, use standard PyTorch data loading, such as `DataLoader` with `DistributedSampler`.
:::

## Multi-process local mode with `torchrun`

Local mode supports multi-GPU training through `torchrun`, so you can develop and debug with `torchrun` process management.

### Single-node multi-GPU training

The following example shows how to use `torchrun` with local mode for multi-GPU training on a single node. Use this approach to migrate existing PyTorch training code or to debug distributed training logic with the familiar `torchrun` process management. The example loads data with the standard PyTorch `DataLoader`, so you can adapt your existing PyTorch training code to it.

First, create your training script, `train_script.py`:

```{testcode}
:skipif: True

import os
import tempfile
import torch
import torch.distributed as dist
from torch import nn
from torch.utils.data import DataLoader
from torchvision.datasets import FashionMNIST
from torchvision.transforms import ToTensor, Normalize, Compose
from filelock import FileLock
import ray
from ray.train import Checkpoint, ScalingConfig, get_context
from ray.train.torch import TorchTrainer

def train_func(config):
    # Load dataset with file locking to avoid multiple downloads
    transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
    data_dir = "./data"
    # Only local rank 0 downloads the dataset
    local_rank = get_context().get_local_rank()
    if local_rank == 0:
        with FileLock(os.path.join(data_dir, "fashionmnist.lock")):
            train_dataset = FashionMNIST(
                root=data_dir, train=True, download=True, transform=transform
            )

    # Wait for rank 0 to finish downloading
    dist.barrier()

    # Now all ranks can safely load the dataset
    train_dataset = FashionMNIST(
        root=data_dir, train=True, download=False, transform=transform
    )
    train_loader = DataLoader(
        train_dataset, batch_size=config["batch_size"], shuffle=True
    )

    # Prepare dataloader for distributed training
    train_loader = ray.train.torch.prepare_data_loader(train_loader)

    # Prepare model for distributed training
    model = nn.Sequential(
        nn.Flatten(),
        nn.Linear(28 * 28, 128),
        nn.ReLU(),
        nn.Linear(128, 10)
    )
    model = ray.train.torch.prepare_model(model)

    criterion = nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=config["lr"])

    # Training loop
    for epoch in range(config["epochs"]):
        # Set epoch for distributed sampler
        if ray.train.get_context().get_world_size() > 1:
            train_loader.sampler.set_epoch(epoch)

        epoch_loss = 0.0
        for batch_idx, (images, labels) in enumerate(train_loader):
            outputs = model(images)
            loss = criterion(outputs, labels)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            epoch_loss += loss.item()

        avg_loss = epoch_loss / len(train_loader)

        # Report metrics and checkpoint
        with tempfile.TemporaryDirectory() as temp_dir:
            torch.save(model.state_dict(), os.path.join(temp_dir, "model.pt"))
            ray.train.report(
                {"loss": avg_loss, "epoch": epoch},
                checkpoint=Checkpoint.from_directory(temp_dir)
            )

# Configure trainer for local mode
trainer = TorchTrainer(
    train_loop_per_worker=train_func,
    train_loop_config={"lr": 0.001, "epochs": 10, "batch_size": 32},
    scaling_config=ScalingConfig(num_workers=0, use_gpu=True),
)
result = trainer.fit()
```


Then, launch training with `torchrun`:

```bash
# Train on 4 GPUs on a single node
torchrun --nproc-per-node=4 train_script.py
```

Ray Train automatically detects the `torchrun` environment variables and configures distributed training accordingly. To access distributed training information, use {func}`ray.train.get_context()`:

```{testcode}
:skipif: True

from ray.train import get_context

context = get_context()
print(f"World size: {context.get_world_size()}")
print(f"World rank: {context.get_world_rank()}")
print(f"Local rank: {context.get_local_rank()}")
```

:::{warning}
Ray Data isn't supported when you use `torchrun` for multi-process training in local mode. For multi-process training, use standard PyTorch data loading, such as `DataLoader` with `DistributedSampler`.
:::

### Multi-node multi-GPU training

You can also use `torchrun` to launch multi-node training with local mode. The following example shows how to launch training across two nodes with four GPUs each.

On the master node at `192.168.1.1`, run the following command:

```bash
RAY_TRAIN_V2_ENABLED=1 torchrun \
    --nnodes=2 \
    --nproc-per-node=4 \
    --node_rank=0 \
    --rdzv_backend=c10d \
    --rdzv_endpoint=192.168.1.1:29500 \
    --rdzv_id=job_id \
    train_script.py
```

On the worker node, run the following command:

```bash
RAY_TRAIN_V2_ENABLED=1 torchrun \
    --nnodes=2 \
    --nproc-per-node=4 \
    --node_rank=1 \
    --rdzv_backend=c10d \
    --rdzv_endpoint=192.168.1.1:29500 \
    --rdzv_id=job_id \
    train_script.py
```

(transitioning-from-local-mode-to-distributed-training)=

## Transition from local mode to distributed training

When you're ready to scale from local mode to distributed training, change `num_workers` to a value greater than 0:

```diff
 trainer = TorchTrainer(
     train_loop_per_worker=train_func,
     train_loop_config=config,
-    scaling_config=ScalingConfig(num_workers=0),
+    scaling_config=ScalingConfig(num_workers=4, use_gpu=True),
 )
```

Your training function stays the same, and Ray Train handles distributed coordination automatically.

## Limitations and API differences

Local mode provides simplified implementations of Ray Train APIs for rapid debugging without distributed orchestration. As a result, some features behave differently or aren't available.

### Features not available in local mode

The following Ray Train features aren't available in local mode:

* **Worker-level fault tolerance**: Ray Train's automatic fault tolerance features, such as restarting workers on failure, aren't available. If you configure {class}`~ray.train.FailureConfig`, its settings don't apply in local mode.
* **Callbacks**: Local mode doesn't invoke callbacks that you specify in {class}`~ray.train.RunConfig`.
* **Ray Data with multi-process training**: Ray Data isn't supported when you use `torchrun` with local mode for multi-process training. Use standard PyTorch data loading instead.

### API behavior differences

The following table summarizes how `ray.train` APIs behave differently in local mode:

```{list-table}
:header-rows: 1
:widths: 30 70

* - API
  - Behavior in local mode
* - {func}`ray.train.report`
  - Stores checkpoints in memory only and doesn't persist them to storage. Ignores the `checkpoint_upload_mode`, `checkpoint_upload_fn`, `validation`, and `delete_local_checkpoint_after_upload` parameters. Logs metrics locally instead of through the reporting pipeline. Doesn't invoke a synchronization barrier across workers.
* - {func}`ray.train.get_checkpoint`
  - Returns the last checkpoint from memory. Doesn't load checkpoints from persistent storage.
* - {func}`ray.train.get_all_reported_checkpoints`
  - Always returns an empty list. Doesn't track checkpoint history.
* - {func}`ray.train.collective.barrier`
  - No-op.
* - {func}`ray.train.collective.broadcast_from_rank_zero`
  - Returns data as-is.
* - {meth}`ray.train.get_context().get_storage() <ray.train.TrainContext.get_storage>`
  - Raises `NotImplementedError`.
```
