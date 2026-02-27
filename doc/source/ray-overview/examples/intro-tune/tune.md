# Introduction to Ray Tune

This template provides a hands-on introduction to **Ray Tune** — a scalable hyperparameter tuning library built on [Ray](https://docs.ray.io/en/latest/). You will learn what Ray Tune is, why it matters, and how to use its core APIs to efficiently search for the best hyperparameters for your models.

In the first half, we'll walk through the core Ray Tune workflow — from a baseline training loop to a fully tuned model with smart search and early stopping. The second half covers production concerns and advanced patterns you can explore as your workloads grow.

**Here is the roadmap for this template:**

**Core:**
- **Part 1:** Baseline PyTorch Training
- **Part 2:** Your First Tune Experiment
- **Part 3:** Smarter Search and Early Stopping

**Advanced topics:**
- **Part 4:** Checkpointing and Fault Tolerance
- **Part 5:** Integrating with Ray Train
- **Summary and Next Steps**

```python
import os
import tempfile
from typing import Any

import numpy as np
import torch
from torchvision.datasets import MNIST
from torchvision.transforms import Compose, ToTensor, Normalize
from torchvision.models import resnet18
from torch.utils.data import DataLoader
from torch.optim import Adam
from torch.nn import CrossEntropyLoss

import ray
from ray import tune
from ray.tune import Checkpoint, Stopper
from ray.tune.search.optuna import OptunaSearch
from ray.tune.schedulers import ASHAScheduler
```

### Note on Storage

Throughout this tutorial, we use `/mnt/cluster_storage` to represent a shared storage location. In a multi-node cluster, Ray workers on different nodes cannot access the head node's local file system. Use a [shared storage solution](https://docs.anyscale.com/configuration/storage#shared) accessible from every node.

---

## Part 1: Baseline PyTorch Training

We begin with a standard PyTorch training loop to establish a baseline. Our running example throughout this template is:

- **Objective**: Classify handwritten digits (0-9)
- **Model**: ResNet18 adapted for single-channel MNIST images
- **Evaluation Metric**: CrossEntropy Loss
- **Dataset**: MNIST (60,000 training images, 28x28 grayscale)

```python
# Helper to build a DataLoader for MNIST.
def build_data_loader(batch_size: int) -> DataLoader:
    transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
    train_data = MNIST(root="./data", train=True, download=True, transform=transform)
    return DataLoader(train_data, batch_size=batch_size, shuffle=True, drop_last=True)
```

Here is our baseline training function with predefined hyperparameters:

```python
# Baseline PyTorch training loop with hardcoded hyperparameters.
def train_loop_torch(num_epochs: int = 2, batch_size: int = 128, lr: float = 1e-3):
    criterion = CrossEntropyLoss()

    model = resnet18()
    model.conv1 = torch.nn.Conv2d(
        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
    )
    model.to("cuda")

    data_loader = build_data_loader(batch_size)
    optimizer = Adam(model.parameters(), lr=lr)

    for epoch in range(num_epochs):
        for images, labels in data_loader:
            images, labels = images.to("cuda"), labels.to("cuda")
            outputs = model(images)
            loss = criterion(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        print(f"Epoch {epoch}, Loss: {loss:.4f}")
```

We schedule this on a GPU worker node using `@ray.remote` (GPU-intensive work shouldn't run directly on the head node):

```python
# Initialize Ray (or connect to an existing cluster).
ray.init(ignore_reinit_error=True)
```

```python
# Run the baseline training on a GPU worker node.
@ray.remote(num_gpus=1, resources={"accelerator_type:T4": 0.001})
def run_baseline():
    train_loop_torch(num_epochs=2)

ray.get(run_baseline.remote())
```

**Can we do better?** The model has several hyperparameters — learning rate, batch size, number of epochs — that we chose somewhat arbitrarily. Tuning them systematically could improve performance, but searching over combinations is expensive and slow when done sequentially.

This is exactly what [Ray Tune](https://docs.ray.io/en/latest/tune/) solves — it's a distributed hyperparameter tuning library that runs many trials in parallel across your cluster:

| Challenge | **Ray Tune Solution** |
| --- | --- |
| **Scale tuning** | Distributes trials across cluster CPUs/GPUs for massive parallelism. |
| **Sophisticated search** | Wraps complex algorithms (Bayesian optimization, PBT) and runs them distributed — no custom parallelization code needed. |
| **Hardware utilization** | Gang-schedules trials via [placement groups](https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html) so each trial gets exactly the resources it needs. |
| **Early stopping** | Schedulers such as **ASHA** and **PBT** terminate underperformers early, freeing resources for promising trials. |
| **Ecosystem integration** | Plugs into Optuna, HyperOpt, Ax, and experiment tracking tools. |
| **Fault tolerance** | Trials checkpoint automatically; experiments can be resumed end-to-end. |

Let's apply Ray Tune to our model.

---

## Part 2: Your First Tune Experiment

We'll tune our ResNet18/MNIST model in four steps: **define** a training function, **configure** a Tuner, **run** the experiment, and **inspect** the results.

### Step 1: Define the training function

A Tune training function accepts a `config` dictionary containing hyperparameters and reports metrics back to Tune using `tune.report()`:

```python
# Tune-compatible PyTorch training function.
def train_pytorch(config):
    criterion = CrossEntropyLoss()

    model = resnet18()
    model.conv1 = torch.nn.Conv2d(
        1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False
    )
    model.to("cuda")

    optimizer = Adam(model.parameters(), lr=config["lr"])
    data_loader = build_data_loader(config["batch_size"])

    for epoch in range(config["num_epochs"]):
        for images, labels in data_loader:
            images, labels = images.to("cuda"), labels.to("cuda")
            outputs = model(images)
            loss = criterion(outputs, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        # Report metrics at the end of each epoch
        tune.report({"loss": loss.item()})
```

### Step 2: Configure the Tuner

Each trial runs as a separate process. Use `tune.with_resources` to specify what each trial needs — here, 1 GPU per trial:

```python
# Allocate 1 T4 GPU per trial and search over learning rate.
tuner = tune.Tuner(
    trainable=tune.with_resources(
        train_pytorch, {"gpu": 1, "accelerator_type:T4": 0.001}
    ),
    param_space={
        "num_epochs": 3,
        "batch_size": 128,
        "lr": tune.loguniform(1e-4, 1e-1),
    },
    tune_config=tune.TuneConfig(
        mode="min",
        metric="loss",
        num_samples=4,
    ),
)
```

A few things to note about the configuration:

- **`param_space`** defines the hyperparameters to explore. `tune.loguniform(1e-4, 1e-1)` samples the learning rate on a log scale. Ray Tune provides other primitives such as `tune.choice`, `tune.uniform`, and `tune.randint` — see the [full Search Space API reference](https://docs.ray.io/en/latest/tune/api/search_space.html).
- **`tune_config`** tells Tune which metric to optimize (`"loss"`), whether to minimize or maximize (`"min"`), and how many trials to run (`num_samples=4`).
- By default, Tune uses random search (`BasicVariantGenerator`) to pick hyperparameters. We'll plug in a smarter search algorithm in Part 3.

Here's how the Tuner, search algorithm, and scheduler fit together:

<img src="https://docs.ray.io/en/latest/_images/tune_flow.png" width="800" />

To learn more about these concepts, visit the [Ray Tune Key Concepts documentation](https://docs.ray.io/en/latest/tune/key-concepts.html).

### Step 3: Run the Tuner

```python
results = tuner.fit()
```

### Step 4: Inspect the results

```python
# Print the best configuration and loss.
best_result = results.get_best_result()
print(f"Best config: {best_result.config}")
print(f"Best loss: {best_result.metrics['loss']:.4f}")
```

```python
# View results for all trials.
results.get_dataframe()[["loss", "config/lr"]]
```

### Recap

To summarize, a `tune.Tuner` accepts:
- **`trainable`** — a training function (or class) to be tuned
- **`param_space`** — a dictionary defining the hyperparameter search space
- **`tune_config`** — configuration for the metric to optimize (`metric`, `mode`) and how many trials to run (`num_samples`)

`tuner.fit()` runs multiple trials in parallel, each with a different set of hyperparameters, and returns a `ResultGrid` from which you can retrieve the best configuration.

---

## Part 3: Smarter Search and Early Stopping

Now that we've seen the basic Tune workflow, let's make it smarter — with a better search algorithm and a scheduler that stops underperformers early, combined in a single experiment.

In Part 2, Tune used random search and ran every trial to completion. We can do better on both fronts:

- **Search algorithm** — Instead of random search, use **Optuna** for Bayesian optimization. It learns from previous trial results to make smarter choices about which hyperparameters to try next.
- **Scheduler** — Instead of the default `FIFOScheduler` (which runs all trials to completion), use the **ASHAScheduler** (Asynchronous Successive Halving). It terminates underperforming trials early, freeing resources for more promising ones.

You can combine both in a single Tuner:

```python
# Combine Optuna search with ASHA early stopping.
tuner = tune.Tuner(
    trainable=tune.with_resources(
        train_pytorch, {"gpu": 1, "accelerator_type:T4": 0.001}
    ),
    param_space={
        "num_epochs": 8,
        "batch_size": 128,
        "lr": tune.loguniform(1e-4, 1e-1),
    },
    tune_config=tune.TuneConfig(
        metric="loss",
        mode="min",
        num_samples=4,
        search_alg=OptunaSearch(),
        scheduler=ASHAScheduler(
            max_t=10,        # Max training iterations
            grace_period=2,  # Min iterations before stopping is allowed
        ),
    ),
)
```

```python
results = tuner.fit()
```

Notice from the output that some trials were terminated early before reaching epoch 8 — ASHA stopped them because they weren't competitive.

```python
print(f"Best config: {results.get_best_result().config}")
results.get_dataframe()[["loss", "training_iteration", "config/lr"]].sort_values("loss")
```

Ray Tune integrates with many search libraries and schedulers:

| **Library** | **Search Algorithm** | **Best For** |
|------------|---------------------|--------------|
| Built-in | `BasicVariantGenerator` | Simple random/grid search |
| Optuna | `OptunaSearch` | Bayesian optimization with pruning |
| HyperOpt | `HyperOptSearch` | Tree-structured Parzen Estimators |
| Ax| `AxSearch` | Bayesian optimization |

See the full list in the [Search Algorithm API docs](https://docs.ray.io/en/latest/tune/api/suggestion.html) and [Scheduler API docs](https://docs.ray.io/en/latest/tune/api/schedulers.html).

---

You've seen the core Ray Tune workflow — from a baseline training loop to smart search with Optuna and early stopping with ASHA. The following sections cover production concerns and advanced patterns.

---

## Part 4: Checkpointing and Fault Tolerance

For production-grade experiments, you need persistent storage, checkpointing, and fault tolerance.

### Persistent Storage

On a distributed cluster, Ray Tune needs a persistent storage location accessible from all nodes to save checkpoints and experiment state. Configure it via `tune.RunConfig(storage_path="/mnt/cluster_storage")`.

<img src="https://docs.ray.io/en/latest/_images/checkpoint_lifecycle.png" alt="Checkpoint Lifecycle" width="700"/>

The checkpoint lifecycle: saved locally, then uploaded to persistent storage via `tune.report()`.

### Checkpointing Trials

To make trials resumable, save model state as a `Checkpoint` inside `tune.report()`. Here is the pattern for PyTorch:

```python
# Training function with checkpointing for fault tolerance.
def train_pytorch_with_checkpoints(config):
    model = resnet18()
    model.conv1 = torch.nn.Conv2d(1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False)
    model.to("cuda")
    optimizer = Adam(model.parameters(), lr=config["lr"])
    criterion = CrossEntropyLoss()
    data_loader = build_data_loader(config["batch_size"])
    start_epoch = 0

    # Resume from checkpoint if available
    checkpoint = tune.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as ckpt_dir:
            state = torch.load(os.path.join(ckpt_dir, "model.pt"), weights_only=False)
            model.load_state_dict(state["model"])
            optimizer.load_state_dict(state["optimizer"])
            start_epoch = state["epoch"] + 1

    for epoch in range(start_epoch, config["num_epochs"]):
        for images, labels in data_loader:
            images, labels = images.to("cuda"), labels.to("cuda")
            loss = criterion(model(images), labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        # Save checkpoint with each metric report
        with tempfile.TemporaryDirectory() as tmp_dir:
            torch.save(
                {"model": model.state_dict(), "optimizer": optimizer.state_dict(), "epoch": epoch},
                os.path.join(tmp_dir, "model.pt"),
            )
            tune.report(
                {"loss": loss.item()},
                checkpoint=Checkpoint.from_directory(tmp_dir),
            )
```

### Fault Tolerance

Ray Tune provides two mechanisms for handling failures:

**1. Automatic trial retries** — Configure `FailureConfig` to retry failed trials automatically. For example, `tune.FailureConfig(max_failures=3)` retries each trial up to 3 times.

**2. Experiment recovery** — If the entire experiment fails (e.g., driver crash), you can resume it with `tune.Tuner.restore(path=..., trainable=..., restart_errored=True)`. This picks up where the experiment left off, skipping completed trials and restarting errored ones.

Here is a complete example combining checkpointing and fault tolerance:

```python
# Combine checkpointing with fault tolerance and persistent storage.
tuner = tune.Tuner(
    trainable=tune.with_resources(
        train_pytorch_with_checkpoints, {"gpu": 1, "accelerator_type:T4": 0.001}
    ),
    param_space={
        "num_epochs": 2,
        "batch_size": 128,
        "lr": tune.loguniform(1e-4, 1e-1),
    },
    tune_config=tune.TuneConfig(
        metric="loss",
        mode="min",
        num_samples=4,
    ),
    run_config=tune.RunConfig(
        storage_path="/mnt/cluster_storage",
        name="resnet18_fault_tolerant",
        failure_config=tune.FailureConfig(max_failures=2),
    ),
)
```

```python
results = tuner.fit()
```

```python
# Inspect the fault-tolerant experiment results.
best_result = results.get_best_result()
print(f"Best config: {best_result.config}")
print(f"Best loss: {best_result.metrics['loss']:.4f}")
print(f"Best checkpoint: {best_result.checkpoint}")
```

### Stopping Criteria

Beyond ASHA, Ray Tune offers additional ways to stop trials and experiments:

**Metric-based stopping** — Define a custom `Stopper` to stop individual trials or the entire experiment based on metric thresholds:

```python
class CustomStopper(Stopper):
    def __init__(self):
        self.should_stop = False

    def __call__(self, trial_id: str, result: dict) -> bool:
        if result["loss"] > 1.0 and result["training_iteration"] > 5:
            return True  # Stop this underperforming trial
        if result["loss"] <= 0.05:
            self.should_stop = True  # Found a great result
        return False

    def stop_all(self) -> bool:
        return self.should_stop
```

Pass the stopper via `RunConfig(stop=CustomStopper())`.

**Time-based stopping** — Stop trials after a certain duration with `RunConfig(stop={"time_total_s": 120})`, or cap the full experiment time with `TuneConfig(time_budget_s=600.0)`.

### Resource Management

When running many concurrent trials, OOM errors can occur. Mitigate this by:
- **Setting memory resources:** `tune.with_resources(trainable, {"gpu": 1, "memory": 6 * 1024**3})`
- **Limiting concurrency:** `tune.TuneConfig(max_concurrent_trials=4)`

---

## Part 5: Integrating with Ray Train

By default, each Ray Tune trial runs as a single-worker process — training happens on one machine with one GPU. For large models that require distributed (multi-GPU or multi-node) training, you need the Ray Train integration. Wrapping a Ray Train `Trainer` inside a Tune trial lets each trial run a full distributed training job — giving you distributed hyperparameter search *and* distributed training at the same time.

To set this up, wrap your Ray Train `Trainer` creation in a driver function that Tune calls with different hyperparameter configurations. Each Tune trial launches a full Ray Train distributed training run.

```python
def train_loop_per_worker(config):
    """Adapted from train_pytorch for Ray Train"""
    import ray.train

    criterion = CrossEntropyLoss()
    model = resnet18()
    model.conv1 = torch.nn.Conv2d(1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False)
    model = ray.train.torch.prepare_model(model)  # added for distributed

    optimizer = Adam(model.parameters(), lr=config["lr"])
    data_loader = build_data_loader(config["batch_size"])

    for epoch in range(config["num_epochs"]):
        for images, labels in data_loader:
            images, labels = images.to("cuda"), labels.to("cuda")
            loss = criterion(model(images), labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
        ray.train.report({"loss": loss.item()})  # changed from tune.report
```

```python
# Define a driver function that creates a Ray Train Trainer per Tune trial.
from ray.train.torch import TorchTrainer
from ray.tune.integration.ray_train import TuneReportCallback

def train_driver_fn(config):
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config=config["train_loop_config"],
        # Use 2 gpus per trial
        scaling_config=ray.train.ScalingConfig(num_workers=2, use_gpu=True),
        run_config=ray.train.RunConfig(
            name=f"train-trial_id={tune.get_context().get_trial_id()}",
            callbacks=[TuneReportCallback()],
        ),
    )
    trainer.fit()
```

```python
# Launch multi-GPU Tune trials with Ray Train.
tuner = tune.Tuner(
    train_driver_fn,
    param_space={"train_loop_config": {"lr": tune.loguniform(1e-4, 1e-1), "batch_size": 128, "num_epochs": 3}},
    tune_config=tune.TuneConfig(num_samples=2, max_concurrent_trials=2),
)
results = tuner.fit()
```

Key details:
- **`TuneReportCallback`** propagates metrics reported by Ray Train workers back to Tune, so the `Tuner` can track and compare trial results.
- **`tune.get_context().get_trial_id()`** ensures each Train run gets a unique name tied to the Tune trial, which is required for proper fault tolerance.
- **`max_concurrent_trials`** limits how many Train runs compete for cluster resources at once. Set this based on your GPU budget (e.g., `total_gpus // gpus_per_trial`).

See the [Ray Train + Tune guide](https://docs.ray.io/en/latest/train/user-guides/hyperparameter-optimization.html) for full details.

---

## Summary and Next Steps

In this template, you learned:

- **What** Ray Tune is — a scalable, distributed hyperparameter tuning library
- **Why** to use it — parallel trial execution, smart search algorithms, early stopping, fault tolerance, and ecosystem integration
- **How** to use it — defining trainable functions with `tune.report()`, configuring `tune.Tuner` with search spaces and `TuneConfig`, running experiments with `tuner.fit()`, and retrieving best results
- **Core concepts** — resources (`tune.with_resources`), search algorithms (random, Optuna), schedulers (FIFO, ASHA)
- **Production features** — checkpointing, persistent storage, fault tolerance, experiment recovery

### Next Steps

1. **[Ray Tune User Guide](https://docs.ray.io/en/latest/tune/getting-started.html)** — Complete guide to Ray Tune
2. **[Search Algorithm Reference](https://docs.ray.io/en/latest/tune/api/suggestion.html)** — All supported search algorithms
3. **[Scheduler Reference](https://docs.ray.io/en/latest/tune/api/schedulers.html)** — All supported schedulers including ASHA and PBT
4. **[Ray Train + Tune Integration](https://docs.ray.io/en/latest/train/user-guides/hyperparameter-optimization.html)** — Combining distributed training with HPO
5. **[Tune Examples Gallery](https://docs.ray.io/en/latest/tune/examples/index.html)** — End-to-end examples with popular frameworks
