# Introduction to Ray Tune

This template provides a hands-on introduction to **Ray Tune** — a scalable hyperparameter tuning library built on [Ray](https://docs.ray.io/en/latest/). You will learn what Ray Tune is, why it matters, and how to use its core APIs to efficiently search for the best hyperparameters for your models.

**Assumed knowledge:** You should be comfortable with PyTorch, standard training loops, and common hyperparameters (learning rate, batch size, etc.).

**Here is the roadmap for this template:**

- **Part 1:** Baseline PyTorch Training
- **Part 2:** Why Ray Tune?
- **Part 3:** Getting Started — Your First Tune Job
- **Part 4:** Ray Tune Core Concepts
- **Part 5:** Tuning the PyTorch Model with Ray Tune
- **Part 6:** Search Algorithms and Stopping Criteria
- **Part 7:** Checkpointing, Storage, and Fault Tolerance
- **Part 8:** Monitoring with the Ray Dashboard
- **Part 9:** Advanced Patterns
- **Part 10:** Ray Tune in Production
- **Summary and Next Steps**

## Imports

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
from ray import tune, train
from ray.tune.search.optuna import OptunaSearch
from ray.tune.schedulers import ASHAScheduler
from ray.tune import Stopper
```

### Note on Storage

Throughout this tutorial, we use `/mnt/cluster_storage` to represent a shared storage location. In a multi-node cluster, Ray workers on different nodes cannot access the head node's local file system. Use a [shared storage solution](https://docs.anyscale.com/configuration/storage#shared) accessible from every node.

## Part 1: Baseline PyTorch Training

We begin with a standard PyTorch training loop to establish a baseline. Our running example throughout this template is:

- **Objective**: Classify handwritten digits (0-9)
- **Model**: ResNet18 adapted for single-channel MNIST images
- **Evaluation Metric**: CrossEntropy Loss
- **Dataset**: MNIST (60,000 training images, 28×28 grayscale)

```python
def build_data_loader(batch_size: int) -> DataLoader:
    transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
    train_data = MNIST(root="./data", train=True, download=True, transform=transform)
    return DataLoader(train_data, batch_size=batch_size, shuffle=True, drop_last=True)
```

Here is our baseline training function with hardcoded hyperparameters:

```python
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

We schedule this on a GPU worker node using `@ray.remote` (GPU-intensive work should not run directly on the head node):

```python
ray.init(ignore_reinit_error=True)

@ray.remote(num_gpus=1, resources={"accelerator_type:T4": 0.0001})
def run_baseline():
    train_loop_torch(num_epochs=2)

ray.get(run_baseline.remote())
```

**Can we do better?** The model has several hyperparameters — learning rate, batch size, number of epochs — that we chose somewhat arbitrarily. Tuning them systematically could improve performance, but searching over combinations is expensive and slow when done sequentially.

[Ray Tune](https://docs.ray.io/en/latest/tune/) is a distributed hyperparameter tuning library that can run many trials in parallel across your cluster, dramatically speeding up the search.

## Part 2: Why Ray Tune?

Here is a summary of the key challenges Ray Tune solves:

| Challenge | Detail | **Ray Tune Solution** |
| --- | --- | --- |
| **Scale hyperparameter tuning** | Large search spaces and GPU-hungry models take days serially. | Distributes trials across all CPUs and GPUs in your cluster, so hundreds of experiments run in parallel. |
| **Optimise hardware utilisation** | Naive scheduling may let trials hoard GPUs or leave nodes idle. | Uses gang/atomic scheduling via [placement groups](https://docs.ray.io/en/latest/ray-core/scheduling/placement-group.html) to grant each trial exactly the resources it needs. |
| **Early-terminate bad trials** | Running every trial to completion wastes time and money. | Plug in schedulers like **ASHA** or **Population-Based Training (PBT)** to stop under-performers early. |
| **Ecosystem integration** | Teams already use Optuna, HyperOpt, etc.; rewriting is non-trivial. | Integrates with existing search libraries (Optuna, HyperOpt, Ax) and experiment tracking tools. |
| **Observability** | Hard to debug and profile many distributed trials. | The **Ray Dashboard** provides visibility into running trials, resource usage, and logs. |
| **Fault tolerance** | Long experiments may be interrupted by pre-emptions or node failures. | Trials checkpoint automatically, and experiments can be resumed end-to-end. |

Now let's see Ray Tune in action, starting with a simple example before applying it to our PyTorch model.

## Part 3: Getting Started — Your First Tune Job

We start with a minimal toy example to learn the core Ray Tune API.

### Step 1: Define the training function

A Ray Tune training function must accept a `config` dictionary and report metrics back using `train.report()`.

> **API Note:** Use `train.report()` from `ray.train` to report metrics. The older `tune.report()` API is deprecated.

```python
def my_simple_model(distance: np.ndarray, a: float) -> np.ndarray:
    return distance * a

def train_my_simple_model(config: dict[str, Any]) -> None:
    distances = np.array([0.1, 0.2, 0.3, 0.4, 0.5])
    total_amts = distances * 10

    a = config["a"]
    predictions = my_simple_model(distances, a)
    rmse = np.sqrt(np.mean((total_amts - predictions) ** 2))

    train.report({"rmse": rmse})  # Report metrics to Ray Tune
```

### Step 2: Set up the Tuner

A `tune.Tuner` takes three key arguments: the `trainable` function, a `param_space` defining the search space, and a `tune_config` specifying the metric and number of trials.

> **API Note:** Always use `tune.Tuner` + `tuner.fit()`. The older `tune.run()` API is deprecated.

```python
tuner = tune.Tuner(
    trainable=train_my_simple_model,
    param_space={
        "a": tune.randint(0, 20),
    },
    tune_config=tune.TuneConfig(
        metric="rmse",
        mode="min",
        num_samples=5,
    ),
)
```

### Step 3: Run the Tuner

```python
results = tuner.fit()
```

### Step 4: Inspect the results

```python
best_result = results.get_best_result()
print(f"Best hyperparameters: {best_result.config}")
print(f"Best RMSE: {best_result.metrics['rmse']:.4f}")
```

Let's also look at the full results table:

```python
results.get_dataframe()[["rmse", "config/a"]]
```

### Recap

A `tune.Tuner` accepts:
- **`trainable`** — a training function (or class) to be tuned
- **`param_space`** — a dictionary defining the hyperparameter search space
- **`tune_config`** — configuration for the metric to optimize (`metric`, `mode`) and how many trials to run (`num_samples`)

`tuner.fit()` runs multiple trials in parallel, each with a different set of hyperparameters, and returns a `ResultGrid` from which you can retrieve the best configuration.

## Part 4: Ray Tune Core Concepts

You might be wondering:
- How does the tuner allocate resources to each trial?
- How does it decide which hyperparameters to try next?
- How does it decide when to stop a trial early?

Ray Tune has three key configurable dimensions:

**1. Resource allocation** — Each trial runs as a separate process. By default, each consumes 1 CPU. Specify resources explicitly with `tune.with_resources()`.

**2. Search algorithm** — Determines how the next trial's hyperparameters are chosen. Default: `BasicVariantGenerator` (random/grid search). Alternatives: Optuna, Bayesian Optimization, HyperOpt.

**3. Scheduler** — Controls whether to stop, pause, or prioritize trials based on intermediate results. Default: `FIFOScheduler` (no early stopping). Alternatives: ASHA, PBT.

Here is the same toy example with all defaults explicitly specified:

```python
tuner = tune.Tuner(
    trainable=tune.with_resources(train_my_simple_model, {"cpu": 1}),
    param_space={"a": tune.randint(0, 20)},
    tune_config=tune.TuneConfig(
        mode="min",
        metric="rmse",
        num_samples=5,
        search_alg=tune.search.BasicVariantGenerator(),
        scheduler=tune.schedulers.FIFOScheduler(),
    ),
)
results = tuner.fit()
```

Let's inspect the results to see how different trial configurations performed:

```python
results.get_dataframe()[["rmse", "config/a"]]
```

Below is a diagram showing the relationship between these Ray Tune components:

<img src="https://docs.ray.io/en/latest/_images/tune_flow.png" width="800" />

To learn more about these concepts, visit the [Ray Tune Key Concepts documentation](https://docs.ray.io/en/latest/tune/key-concepts.html).

With the fundamentals in place, let's now apply Ray Tune to our actual PyTorch model.

## Part 5: Tuning the PyTorch Model with Ray Tune

We follow the same four steps, now applied to our ResNet18/MNIST model.

### Step 1: Refactor the training function

We move the PyTorch code into a Tune-compatible function that accepts `config` and reports metrics via `train.report()`:

```python
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
        train.report({"loss": loss.item()})
```

### Step 2: Set up the Tuner

We allocate 1 GPU per trial and search over the learning rate:

```python
tuner = tune.Tuner(
    trainable=tune.with_resources(train_pytorch, {"gpu": 1}),
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

### Step 3: Run the Tuner

```python
results = tuner.fit()
```

### Step 4: Inspect the results

```python
best_result = results.get_best_result()
print(f"Best config: {best_result.config}")
print(f"Best loss: {best_result.metrics['loss']:.4f}")
```

```python
results.get_dataframe()[["loss", "config/lr"]]
```

## Part 6: Search Algorithms and Stopping Criteria

Now that we've seen the basic Tune workflow, let's explore how to make it smarter — with better search algorithms and stopping strategies.

### Search Space Primitives

Ray Tune provides a rich set of search space primitives:

| **Primitive** | **Example** | **Use Case** |
|--------------|-------------|-------------|
| `tune.randint(lower, upper)` | `tune.randint(1, 10)` | Integer hyperparameters (depth, layers) |
| `tune.uniform(lower, upper)` | `tune.uniform(0.0, 1.0)` | Continuous range (dropout rate) |
| `tune.loguniform(lower, upper)` | `tune.loguniform(1e-5, 1e-1)` | Log-scale search (learning rate) |
| `tune.choice(list)` | `tune.choice(["relu", "gelu"])` | Categorical choices |
| `tune.grid_search(list)` | `tune.grid_search([32, 64, 128])` | Exhaustive grid over values |

### Advanced Search Algorithms

Beyond random/grid search, you can plug in sophisticated search algorithms. Here's an example using Optuna for Bayesian optimization:

```python
tuner = tune.Tuner(
    trainable=tune.with_resources(train_pytorch, {"gpu": 1}),
    param_space={
        "num_epochs": 3,
        "batch_size": 128,
        "lr": tune.loguniform(1e-4, 1e-1),
    },
    tune_config=tune.TuneConfig(
        num_samples=4,
        metric="loss",
        mode="min",
        search_alg=OptunaSearch(),
    ),
)
results = tuner.fit()
```

```python
print(f"Best config (Optuna): {results.get_best_result().config}")
results.get_dataframe()[["loss", "config/lr"]]
```

Ray Tune integrates with many search libraries:

| **Library** | **Search Algorithm** | **Best For** |
|------------|---------------------|--------------|
| Built-in | `BasicVariantGenerator` | Simple random/grid search |
| [Optuna](https://optuna.org/) | `OptunaSearch` | Bayesian optimization with pruning |
| [HyperOpt](http://hyperopt.github.io/hyperopt/) | `HyperOptSearch` | Tree-structured Parzen Estimators |
| [Ax](https://ax.dev/) | `AxSearch` | Bayesian optimization |

See the full list in the [Search Algorithm API docs](https://docs.ray.io/en/latest/tune/api/suggestion.html).

### Stopping Criteria

Ray Tune offers several ways to stop trials and experiments early:

**1. Metric-based stopping** — Define a custom `Stopper` to stop individual trials or the entire experiment based on metric thresholds. The `__call__` method returns `True` to stop a specific trial, and `stop_all()` returns `True` to stop the entire experiment.

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

**2. Time-based stopping** — You can stop trials after a certain duration with `RunConfig(stop={"time_total_s": 120})`, or cap the full experiment time with `TuneConfig(time_budget_s=600.0)`.

**3. Early stopping with schedulers** — The `ASHAScheduler` (Asynchronous Successive Halving) is the most commonly used early stopping scheduler. It terminates underperforming trials early, freeing resources for more promising ones:

```python
tuner = tune.Tuner(
    trainable=tune.with_resources(train_pytorch, {"gpu": 1}),
    param_space={
        "num_epochs": 10,
        "batch_size": 128,
        "lr": tune.loguniform(1e-4, 1e-1),
    },
    tune_config=tune.TuneConfig(
        metric="loss",
        mode="min",
        num_samples=8,
        scheduler=ASHAScheduler(
            max_t=10,        # Max training iterations
            grace_period=2,  # Min iterations before stopping is allowed
        ),
    ),
)
results = tuner.fit()
```

```python
print(f"Best config (ASHA): {results.get_best_result().config}")
results.get_dataframe()[["loss", "training_iteration", "config/lr"]].sort_values("loss")
```

See the full list of schedulers in the [Scheduler API docs](https://docs.ray.io/en/latest/tune/api/schedulers.html).

## Part 7: Checkpointing, Storage, and Fault Tolerance

For production-grade experiments, you need persistent storage, checkpointing, and fault tolerance. This section covers these operational concerns at a high level.

### Persistent Storage

On a distributed cluster, Ray Tune needs a persistent storage location accessible from all nodes to save checkpoints and experiment state. Configure it via `train.RunConfig(storage_path="/mnt/cluster_storage")`.

<img src="https://docs.ray.io/en/latest/_images/checkpoint_lifecycle.png" alt="Checkpoint Lifecycle" width="700"/>

The checkpoint lifecycle: saved locally → uploaded to persistent storage via `train.report()`.

### Checkpointing Trials

To make trials resumable, save model state as a `Checkpoint` inside `train.report()`. Here is the pattern for PyTorch:

```python
def train_pytorch_with_checkpoints(config):
    model = resnet18()
    model.conv1 = torch.nn.Conv2d(1, 64, kernel_size=(7, 7), stride=(2, 2), padding=(3, 3), bias=False)
    model.to("cuda")
    optimizer = Adam(model.parameters(), lr=config["lr"])
    criterion = CrossEntropyLoss()
    data_loader = build_data_loader(config["batch_size"])
    start_epoch = 0

    # Resume from checkpoint if available
    checkpoint = train.get_checkpoint()
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
            train.report(
                {"loss": loss.item()},
                checkpoint=train.Checkpoint.from_directory(tmp_dir),
            )
```

### Fault Tolerance

Ray Tune provides two mechanisms for handling failures:

**1. Automatic trial retries** — Configure `FailureConfig` to retry failed trials automatically. For example, `train.FailureConfig(max_failures=3)` will retry each trial up to 3 times.

**2. Experiment recovery** — If the entire experiment fails (e.g., driver crash), you can resume it with `tune.Tuner.restore(path=..., trainable=..., restart_errored=True)`. This picks up where the experiment left off, skipping completed trials and restarting errored ones.

Here is a complete example combining checkpointing and fault tolerance:

```python
tuner = tune.Tuner(
    trainable=tune.with_resources(train_pytorch_with_checkpoints, {"gpu": 1}),
    param_space={
        "num_epochs": 5,
        "batch_size": 128,
        "lr": tune.loguniform(1e-4, 1e-1),
    },
    tune_config=tune.TuneConfig(
        metric="loss",
        mode="min",
        num_samples=4,
    ),
    run_config=train.RunConfig(
        storage_path="/mnt/cluster_storage",
        name="resnet18_fault_tolerant",
        failure_config=train.FailureConfig(max_failures=2),
    ),
)
results = tuner.fit()
```

```python
print(f"Best config: {results.get_best_result().config}")
print(f"Best loss: {results.get_best_result().metrics['loss']:.4f}")
```

### Resource Management

When running many concurrent trials, OOM errors can occur. Mitigate this by:
- **Setting memory resources:** `tune.with_resources(trainable, {"gpu": 1, "memory": 6 * 1024**3})`
- **Limiting concurrency:** `tune.TuneConfig(max_concurrent_trials=4)`

## Part 8: Monitoring with the Ray Dashboard

The Ray Dashboard provides observability into your Tune experiments.

**Jobs tab** — View running Tune jobs and click into individual experiments.

> **TODO (Human Review Needed):** Add screenshot of Ray Dashboard showing a Tune job.
> - Action needed: Run a Tune experiment and capture a screenshot of the Jobs tab.

**Task-level view** — Each trial runs as a `trainable` actor with its own task history. You can inspect:
- Which trials are running vs. waiting for resources
- CPU and memory usage per trial
- `bundle_reservation_check_func` tasks that reserve resources via placement groups

**Resource monitoring** — Track GPU utilization, memory, and CPU usage across the cluster. On Anyscale, use the **Metrics tab** for GPU utilization, memory, network I/O, and disk I/O.

For more details, see the [Ray Dashboard documentation](https://docs.ray.io/en/latest/ray-observability/getting-started.html) and the [Anyscale monitoring and debugging guide](https://docs.anyscale.com/monitoring).

## Part 9: Advanced Patterns

### Passing Data to Trials

By default, each trial loads its own copy of the data, which is wasteful. You can place data in the Ray object store once and share it across trials using `tune.with_parameters()`. Call a `@ray.remote` function to load data into the object store, then pass the reference to your trainable:

```
trainable_with_data = tune.with_parameters(train_func, data=data_ref)
```

See the [Tune FAQ on data passing](https://docs.ray.io/en/latest/tune/faq.html) for complete examples.

### Integrating with Ray Train

For distributed multi-GPU training combined with hyperparameter tuning, pass a Ray Train `Trainer` directly to `tune.Tuner`. This allows each trial to use multiple GPUs (via `ScalingConfig`), while Tune manages the hyperparameter search across trials:

```
trainer = TorchTrainer(
    train_loop_per_worker=...,
    scaling_config=train.ScalingConfig(num_workers=2, use_gpu=True),
)

tuner = tune.Tuner(
    trainer,
    param_space={"train_loop_config": {"lr": tune.loguniform(1e-4, 1e-1)}},
    tune_config=tune.TuneConfig(num_samples=4, metric="loss", mode="min"),
)
results = tuner.fit()
```

See the [Ray Train + Tune guide](https://docs.ray.io/en/latest/train/getting-started-pytorch.html) for full details.

## Part 10: Ray Tune in Production

Ray Tune is used in production at leading companies:

- **Uber** — Built an internal autotune service on Ray Tune for their generative AI workloads. [Read their blog post](https://www.uber.com/blog/from-predictive-to-generative-ai/).
- **Spotify** — Uses Ray Tune for hyperparameter tuning at scale across their ML platform. [Read their engineering blog](https://engineering.atspotify.com/2023/02/unleashing-ml-innovation-at-spotify-with-ray/).

## Summary and Next Steps

In this template, you learned:

- **What** Ray Tune is — a scalable, distributed hyperparameter tuning library
- **Why** to use it — parallel trial execution, smart search algorithms, early stopping, fault tolerance, and ecosystem integration
- **How** to use it — defining trainable functions with `train.report()`, configuring `tune.Tuner` with search spaces and `TuneConfig`, running experiments with `tuner.fit()`, and retrieving best results
- **Core concepts** — resources (`tune.with_resources`), search algorithms (random, grid, Optuna), schedulers (FIFO, ASHA), stopping criteria
- **Operational features** — checkpointing, persistent storage, fault tolerance, experiment recovery, monitoring

### Deprecated APIs to Avoid

| **Deprecated** | **Use Instead** |
|---------------|----------------|
| `tune.report()` | `train.report()` from `ray.train` |
| `tune.run()` | `tune.Tuner` + `tuner.fit()` |

### Next Steps

1. **[Ray Tune User Guide](https://docs.ray.io/en/latest/tune/getting-started.html)** — Complete guide to Ray Tune
2. **[Search Algorithm Reference](https://docs.ray.io/en/latest/tune/api/suggestion.html)** — All supported search algorithms
3. **[Scheduler Reference](https://docs.ray.io/en/latest/tune/api/schedulers.html)** — All supported schedulers including ASHA and PBT
4. **[Ray Train + Tune Integration](https://docs.ray.io/en/latest/train/getting-started-pytorch.html)** — Combining distributed training with HPO
5. **[Tune Examples Gallery](https://docs.ray.io/en/latest/tune/examples/index.html)** — End-to-end examples with popular frameworks

## Cleanup

```python
ray.shutdown()
```
