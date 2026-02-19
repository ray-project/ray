# Introduction to Ray Train

This guide introduces distributed training with Ray Train. It demonstrates how to scale a PyTorch training loop from a single GPU to a cluster of GPUs, using Ray Data for efficient, scalable data ingestion.

For a conceptual overview of Ray Train, see the [Ray Train overview](https://docs.ray.io/en/latest/train/overview.html) and the main [Ray Train documentation](https://docs.ray.io/en/latest/train/train.html). For a broader set of topics and how-to guides, refer to the [Ray Train user guides](https://docs.ray.io/en/latest/train/user-guides.html).

**Roadmap**

1. **Single GPU PyTorch**: A baseline implementation.
2. **Migrating to Ray Train**:
   - Model preparation
   - Data ingestion with Ray Data
   - Metrics and Checkpointing
   - Updating the training loop
3. **Launching the Job**: Configuring and running the distributed training.
4. **Inspecting Results**: Accessing metrics and checkpoints.
5. **Observability**: Monitoring your training.
6. **Fault Tolerance**: Automatic retries, elastic training, and mid-epoch resumption.
7. **Troubleshooting**: Diagnosing common issues.

## Imports

```python
import os
import tempfile
import csv
import datetime

import torch
import torch.nn as nn
import torch.optim as optim
from torchvision.models import resnet18
from torchvision.datasets import MNIST
from torchvision.transforms import ToTensor, Normalize, Compose
from torch.utils.data import DataLoader

import ray
import ray.train
import ray.data
from ray.train import ScalingConfig, RunConfig, Checkpoint, FailureConfig
from ray.train.torch import TorchTrainer
import pandas as pd
import numpy as np
from PIL import Image
import matplotlib.pyplot as plt
```

### Note on Storage

Throughout this tutorial, we use `/mnt/cluster_storage` to represent a shared storage location. This is because in a multi-node cluster, Ray workers running on different nodes cannot access the head node's local file system. To ensure all workers can read datasets and write checkpoints, you must use a [shared storage solution](https://docs.anyscale.com/configuration/storage#shared) (such as NFS or cloud object storage like S3) that is accessible from every node.

---

## 1. Single GPU PyTorch

First, let's look at a standard PyTorch training setup for the MNIST dataset on a single GPU.

<div align="center"><img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/ray-ai-libraries/diagrams/single_gpu_pytorch_v3.png" width="90%"></div>

### Model and Data

Download and preview the dataset:

```python
dataset = MNIST(root="/mnt/cluster_storage/data", train=True, download=True)

fig, axs = plt.subplots(1, 10, figsize=(20, 2))
for i in range(10):
    img, label = dataset[i]
    axs[i].imshow(img, cmap="gray")
    axs[i].axis("off")
    axs[i].set_title(label)
plt.show()
```

### Standard Training Loop

Build a ResNet18 model adapted for grayscale MNIST images (1 channel):

```python
def build_model() -> nn.Module:
    model = resnet18(num_classes=10)
    model.conv1 = nn.Conv2d(
        in_channels=1,
        out_channels=64,
        kernel_size=(7, 7),
        stride=(2, 2),
        padding=(3, 3),
        bias=False,
    )
    return model
```

Create a DataLoader for the MNIST dataset with normalization:

```python
def get_data_loader(batch_size: int = 128) -> DataLoader:
    transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
    dataset = MNIST(root="/mnt/cluster_storage/data", train=True, download=True, transform=transform)
    return DataLoader(dataset, batch_size=batch_size, shuffle=True, drop_last=True)
```

Log and print training metrics:

```python
def report_metrics_torch(loss: torch.Tensor, epoch: int) -> dict:
    metrics = {"loss": loss.item(), "epoch": epoch}
    print(metrics)
    return metrics
```

Save training metrics to a CSV file and the model state to a checkpoint:

```python
def save_checkpoint_and_metrics_torch(metrics: dict, model: nn.Module, local_path: str) -> None:
    os.makedirs(local_path, exist_ok=True)

    # Save metrics to CSV
    with open(os.path.join(local_path, "metrics.csv"), "a") as f:
        writer = csv.writer(f)
        writer.writerow(metrics.values())

    # Save model checkpoint
    torch.save(model.state_dict(), os.path.join(local_path, "model.pt"))
```

Execute the training loop on a single GPU (or CPU) for the specified number of epochs:

```python
def train_func_single_gpu(num_epochs: int = 2, local_path: str = "/mnt/cluster_storage/single_gpu_mnist") -> None:
    # Setup
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = build_model().to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=1e-5)
    dataloader = get_data_loader()

    # Training loop
    model.train()
    for epoch in range(num_epochs):
        for images, labels in dataloader:
            images, labels = images.to(device), labels.to(device)

            outputs = model(images)
            loss = criterion(outputs, labels)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        # Report and save
        metrics = report_metrics_torch(loss, epoch)
        save_checkpoint_and_metrics_torch(metrics, model, local_path)
```

To run this training function on a worker node, we can use a Ray task:

```python
@ray.remote(num_gpus=1, resources={"accelerator_type:T4": 0.0001})
def run_single_gpu_job() -> None:
    train_func_single_gpu()

ray.init(ignore_reinit_error=True)  # Initialize Ray; no-op if already initialized
ray.get(run_single_gpu_job.remote())
```

### Inspecting Single GPU Results

After the training job finishes, we can inspect the metrics and verify the model's performance:

```python
# List the output files
output_dir = "/mnt/cluster_storage/single_gpu_mnist"
if os.path.exists(output_dir):
    print(f"Training output contents: {os.listdir(output_dir)}")

# Read and display metrics
metrics_path = os.path.join(output_dir, "metrics.csv")
if os.path.exists(metrics_path):
    metrics_df = pd.read_csv(metrics_path, names=["loss", "epoch"])
    print(metrics_df.head())
```

Load model and run inference:

```python
model_path = os.path.join(output_dir, "model.pt")
if os.path.exists(model_path):
    # Load the trained model
    loaded_model = build_model()
    loaded_model.load_state_dict(torch.load(model_path, map_location="cpu"))
    loaded_model.eval()

    # Prepare test data
    test_dataset = MNIST(root="/mnt/cluster_storage/data", train=False, download=True)
    transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])

    # Visualize predictions
    fig, axs = plt.subplots(1, 10, figsize=(20, 2))
    for i in range(10):
        img, label = test_dataset[i]
        axs[i].imshow(img, cmap="gray")
        axs[i].axis("off")

        with torch.no_grad():
            img_tensor = transform(img).unsqueeze(0)
            pred = loaded_model(img_tensor).argmax().item()

        axs[i].set_title(f"Pred: {pred}\nTrue: {label}")
    plt.show()
```

---

## 2. Migrating to Ray Train

Ray Train solves common challenges in scaling deep learning:

- **Scale**: Move from single GPU to multiple GPUs/nodes with minimal code changes.
- **Infrastructure**: Abstracts away cluster management and resource provisioning.
- **Observability**: Provides built-in dashboards for monitoring metrics, logs, and resource usage.
- **Reliability**: Features automatic fault tolerance to recover from worker or node failures.

To migrate our PyTorch code to Ray Train, we need to adapt the model preparation, data loading, and the training loop.

The goal is to scale the single-GPU setup to a distributed data-parallel architecture:

<div align="center"><img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/ray-ai-libraries/diagrams/multi_gpu_pytorch_v4.png" width="90%"></div>

At a high level, Ray Train uses a controller (trainer) process to coordinate a group of training worker processes. The [Ray Train overview](https://docs.ray.io/en/latest/train/overview.html) introduces the core concepts: training function, workers, scaling configuration, and trainer.

<div align="center"><img src="https://docs.ray.io/en/latest/_images/overview.png" width="80%"></div>

### 2.1 Migrating the Model

Use [`ray.train.torch.prepare_model`](https://docs.ray.io/en/latest/train/api/doc/ray.train.torch.prepare_model.html) to automatically wrap your model in `DistributedDataParallel` and move it to the correct device:

```python
def build_model() -> nn.Module:
    model = resnet18(num_classes=10)
    model.conv1 = nn.Conv2d(
        in_channels=1,
        out_channels=64,
        kernel_size=(7, 7),
        stride=(2, 2),
        padding=(3, 3),
        bias=False,
    )
    return model

def train_loop_per_worker(config: dict) -> None:
    # 1. Prepare Model
    model = build_model()
    model = ray.train.torch.prepare_model(model)  # Instead of model = model.to("cuda")

    # ... rest of the loop
```

### 2.2 Migrating Data Ingestion (Ray Data)

Instead of a PyTorch `DataLoader`, we will use Ray Data. With a few modifications, you can scale data preprocessing and training separately — for example, doing the former with a pool of CPU workers and the latter with a pool of GPU workers.

Ray Data addresses common data pipeline needs:

- **Consistent data loading**: Standardize ingestion across formats (Parquet, CSV, images) and sources.
- **Scalable preprocessing**: Run on-the-fly transformations (augmentations, tokenization) on a separate pool of CPU workers so your training GPUs don't stall.

For efficient data loading in distributed settings, Ray Data handles sharding, streaming, and preprocessing data across the cluster, preventing training from being bottlenecked by data ingestion. The architecture below shows how Ray Train (controller + workers) integrates with Ray Data and your storage layer:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/ray-train-deep-dive/ray_train_v2_architecture.png" width="100%">

#### Preparing the Dataset

First, we download the MNIST dataset and save the raw data to a local Parquet file. This simulates saving raw data to shared storage, allowing us to perform random transformations (like augmentations) on-the-fly during training:

```python
# Download MNIST
dataset = MNIST(root="/mnt/cluster_storage/data", train=True, download=True)
df = pd.DataFrame({
    "image": dataset.data.numpy().tolist(),
    "label": dataset.targets.numpy()
})
ds = ray.data.from_pandas(df)
mnist_path = os.path.abspath("/mnt/cluster_storage/train_data")  # must be shared storage in multi-node clusters
ds.write_parquet(mnist_path)
```

#### Reading and Transforming the Dataset

We create a Ray Dataset and define preprocessing using standard torchvision transforms:

```python
def get_ray_dataset(path: str) -> ray.data.Dataset:
    ds = ray.data.read_parquet(path, file_extensions=["parquet"])

    def transform_images(row: dict) -> dict:
        transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
        image_arr = np.array(row["image"], dtype=np.uint8)
        row["image"] = transform(Image.fromarray(image_arr))
        return row

    # Apply the transform on-the-fly
    return ds.map(transform_images)
```

For more details and performance tips, see the [Data loading and preprocessing](https://docs.ray.io/en/latest/train/user-guides/data-loading-preprocessing.html) user guide.

### 2.3 Reporting Metrics and Checkpoints

Ray Train uses `ray.train.report()` to log metrics and report checkpoints to the Ray Train driver.

- **Metrics**: Dictionaries of values (e.g., loss, accuracy) passed to `report()` are logged. By default, Ray Train only reports metrics from the rank 0 worker.
- **Checkpoints**: Model states saved to a directory and passed as a `ray.train.Checkpoint`.

**Key Behaviors**:

1. **Synchronization**: `ray.train.report()` acts as a global barrier. All workers must call it to ensure training stays in sync.
2. **Efficient Checkpointing**: To avoid redundant uploads in standard DDP, only the rank 0 worker should save the checkpoint to disk. Ray Train then automatically persists it to your configured storage.

The following diagram shows this checkpoint lifecycle:

<div align="center"><img src="https://docs.ray.io/en/latest/_images/checkpoint_lifecycle.png" width="95%"></div>

```python
def save_checkpoint_and_report_metrics(
    model: torch.nn.Module, metrics: dict[str, float]
) -> None:
    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        checkpoint = None

        # Checkpoint only from rank 0 worker
        if ray.train.get_context().get_world_rank() == 0:
            # Access the original model via `model.module` when wrapped in DistributedDataParallel
            torch.save(
                model.module.state_dict(), os.path.join(temp_checkpoint_dir, "model.pt")
            )
            checkpoint = ray.train.Checkpoint.from_directory(temp_checkpoint_dir)

        # All workers must call report to synchronize
        ray.train.report(
            metrics,
            checkpoint=checkpoint,
        )
```

For an in-depth guide on saving checkpoints and metrics, see the [Saving and Loading Checkpoints guide](https://docs.ray.io/en/latest/train/user-guides/checkpoints.html).

### 2.4 Updating the Training Loop

The following diagram shows how Ray Train and Ray Data work together: `TorchTrainer` launches a set of workers, and each worker reads its own shard of the training dataset.

<div align="center"><img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/multi_gpu_pytorch_annotated.png" width="90%"></div>

In code, each worker calls `ray.train.get_dataset_shard("train")` to fetch its shard (from the `datasets` passed to `TorchTrainer`), then iterates over it with `iter_torch_batches`:

```python
def train_loop_per_worker(config: dict) -> None:
    # 1. Setup Model
    model = build_model()
    model = ray.train.torch.prepare_model(model)  # Instead of model = model.to("cuda")

    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=config["lr"])

    # 2. Calculate Batch Size
    global_batch_size = config["batch_size"]
    world_size = ray.train.get_context().get_world_size()
    per_worker_batch_size = global_batch_size // world_size

    # 3. Setup Data (Ray Data)
    # Get the data shard for this worker and create an iterator
    dataset_shard = ray.train.get_dataset_shard("train")
    dataloader = dataset_shard.iter_torch_batches(
        batch_size=per_worker_batch_size,
        dtypes={"image": torch.float32, "label": torch.long},
        device=ray.train.torch.get_device()  # Auto-move to GPU
    )

    for epoch in range(config["epochs"]):
        for batch in dataloader:
            # Note: Batches are dictionaries (from Ray Data), not tuples
            inputs, labels = batch["image"], batch["label"]

            outputs = model(inputs)
            loss = criterion(outputs, labels)

            optimizer.zero_grad()
            loss.backward()  # gradients are accumulated across workers
            optimizer.step()

        # 4. Report Metrics & Checkpoint
        metrics = {"loss": loss.item(), "epoch": epoch}
        save_checkpoint_and_report_metrics(model, metrics)
```

---

## 3. Launching the Distributed Job

To launch the distributed training job, we need to configure:

1. [**Scaling Configuration**](https://docs.ray.io/en/latest/train/api/doc/ray.train.ScalingConfig.html): Defines the number of workers and compute resources (GPUs/CPUs) per worker.
2. [**Run Configuration**](https://docs.ray.io/en/latest/train/api/doc/ray.train.RunConfig.html): Specifies the storage location for checkpoints and experiment results.

Create the dataset:

```python
train_ds = get_ray_dataset(mnist_path)
```

Configure the `ScalingConfig`, `RunConfig`, and `TorchTrainer`:

```python
scaling_config = ScalingConfig(
    num_workers=4,
    use_gpu=True,
    resources_per_worker={"accelerator_type:T4": 0.0001}
)

run_config = RunConfig(
    name=f"mnist_ray_train_demo_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}",
    storage_path="/mnt/cluster_storage/distributed_training",
)

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={"lr": 1e-5, "batch_size": 128, "epochs": 2},
    scaling_config=scaling_config,
    run_config=run_config,
    datasets={"train": train_ds},
)
```

Start training:

```python
result = trainer.fit()
```

---

## 4. Inspecting Results

The `trainer.fit()` call returns a `Result` object containing metrics and checkpoint information. We can use this to load the trained model and generate predictions:

```python
print(f"Training finished. Result: {result}")

if result.checkpoint:
    with result.checkpoint.as_directory() as ckpt_dir:
        model_path = os.path.join(ckpt_dir, "model.pt")
        print(f"Checkpoint saved at: {model_path}")

        # Load the model state dict
        loaded_model = build_model()
        state_dict = torch.load(model_path, map_location="cpu")
        loaded_model.load_state_dict(state_dict)
        loaded_model.eval()
```

Generate predictions:

```python
dataset = MNIST(root="/mnt/cluster_storage/data", train=False, download=True)
transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])

fig, axs = plt.subplots(1, 10, figsize=(20, 2))
for i in range(10):
    img, label = dataset[i]
    axs[i].imshow(img, cmap="gray")
    axs[i].axis("off")

    with torch.no_grad():
        img_tensor = transform(img).unsqueeze(0)
        pred = loaded_model(img_tensor).argmax().item()

    axs[i].set_title(f"Pred: {pred}\nTrue: {label}")

plt.show()
```

For more details on inspecting results, see [Inspecting training results](https://docs.ray.io/en/latest/train/user-guides/results.html).

---

## 5. Observability

Ray provides built-in tools to monitor your training run and debug issues. See [Monitoring and Logging](https://docs.ray.io/en/latest/train/user-guides/monitoring-logging.html) for more details.

### Monitor a Ray Train run in an Anyscale Workspace

- **Ray Train Workloads**: View worker status, inspect per-worker logs, and track training progress.

  <img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/ray-dashboard.png" width="100%">

- **Metrics**: Monitor time-series charts for GPU utilization, GPU memory, network I/O, and disk I/O.

  <div align="center"><img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/apple/gpu_util_and_disk.png" width="60%"></div>

- **Ray Dashboard**: Debug the cluster (nodes, actors, tasks) when you need deeper system-level visibility.

---

## 6. Fault Tolerance

Ray Train provides built-in fault tolerance to recover from worker failures (e.g., hardware failures, network failures, preemption).

### Automatic Retries and Manual Restoration

Ray Train can automatically restart failed workers and resume training from the latest checkpoint.

<div align="center"><img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/ray-train-deep-dive/fault_tolerance_train_v2.png" width="90%"></div>

Two things are required: enabling retries via `max_failures`, and implementing checkpoint loading in your training loop so restarts resume from saved progress rather than from scratch (see [Handling failures and node preemption](https://docs.ray.io/en/latest/train/user-guides/fault-tolerance.html)).

```python
run_config = RunConfig(
    name="mnist_ray_train_demo",
    ...,
    failure_config=ray.train.FailureConfig(max_failures=3),
)
```

If the job is interrupted beyond `max_failures` (e.g., a driver crash), resume manually by re-executing the script with the same `RunConfig` (same `name` and `storage_path`).

### Elastic Training

Standard fault tolerance restarts with the same fixed worker count — which may not be possible if nodes were permanently lost or preempted. Elastic training lets the job continue with fewer workers and expand back when capacity returns, enabling spot instances and cutting cloud costs by up to 60%.

```python
elastic_scaling_config = ScalingConfig(
    use_gpu=True,
    num_workers=(2, 8),  # (min_workers, max_workers) — tuple instead of fixed count
)
```

Ray Train requests `max_workers` at startup, falls back in steps to `min_workers` if needed, restarts with surviving workers on node loss, and scales back up as capacity returns. Your training loop must implement [checkpoint loading](https://docs.ray.io/en/latest/train/user-guides/fault-tolerance.html) so restarts resume from progress rather than from scratch. Use gradient accumulation to keep the effective global batch size consistent across varying worker counts.

For the full walkthrough see the [Elastic Training guide](https://docs.anyscale.com/runtime/train).

### Mid-Epoch Resumption

When a job restarts after a mid-epoch failure, batches already processed in that epoch are replayed — some samples are seen twice, others not at all. Mid-epoch resumption checkpoints the dataset iterator position alongside model weights so training resumes from the exact batch where it stopped, ensuring each sample is seen exactly once per epoch.

```python
# Save iterator state alongside the model checkpoint
iterator_state = dataloader.state_dict()
torch.save(iterator_state, os.path.join(ckpt_dir, "iterator_state.pt"))

# On resumption, restore the iterator to the saved position
dataset_shard = ray.train.get_dataset_shard("train", state_dict=iterator_state)
```

Datasets must include a unique row identifier and use only map-based transformations. Iterator state is written asynchronously with minimal overhead.

For setup requirements and the full implementation guide, see the [Mid-Epoch Resumption docs](https://docs.anyscale.com/runtime/mid-epoch-resumption).

---

## 7. Troubleshooting

### Diagnosis

1. **Persisted Logs**: When a failure occurs, check the error output in your terminal, or open the persisted worker application logs under the **Logs** tab in a Workspace, then find the traceback frame that points to your file and line number — this is usually the user-code line that caused the error:

   ```text
   File "/home/ray/default/my_trainer.py", line 34, in train_func
   ```

2. **Ray observability**: Use Ray's built-in observability tools to spot resource issues and hangs:
   - **Ray Dashboard**: Monitor GPU utilization and GPU memory.
   - **Ray Train workload UI**: If GPU and CPU utilization stay near zero while you expect training to run, it often means one or more workers are hung.

   <div align="center"><img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/low-utilization-hang.png" width="95%"></div>

3. **Ray Data Issues**: If you're using Ray Data with Ray Train, performance bottlenecks or OOM errors can originate in the data pipeline. See **Introduction to Ray Data** for data-related diagnosis steps.

### Common Issues

#### 1. Training Hangs

Training often hangs due to **rank-specific logic** or synchronization mismatches. Ensure that:

- `ray.train.report` is called by **all** workers at the same step.
- `ds.iter_torch_batches` is iterated by **all** workers simultaneously.

If your code is hanging, you might see this warning in the driver logs:

```text
StreamSplitDataIterator(epoch=1, split=1) blocked waiting on other clients for more than 30s.
```

Or a synchronization timeout from Ray Train:

```text
`ray.train.report` has not been called by all 4 workers in the group.
The workers have been waiting for 60.10 s for the following ranks to join the `ray.train.report` call: [1, 2, 3].
```

To diagnose a Train worker hang, inspect the worker **stack trace** for `RayTrainWorker` under the Ray Train workload UI while the job is still running:

<div align="center"><img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/stacktrace_from_worker.png" width="95%"></div>

#### 2. Wrong CUDA Device

Hardcoding `torch.device("cuda:0")` can cause errors when multiple workers share a node. Always use `ray.train.torch.get_device()` to assign the correct GPU.

#### Other Tools

For deeper debugging, use the [**Ray Distributed Debugger**](https://docs.ray.io/en/latest/ray-observability/ray-distributed-debugger.html) to break into remote tasks and inspect failures post-mortem.

---

## 8. Conclusion

Ray Train coupled with Ray Data provides a powerful stack for scaling deep learning:

- **Simplicity**: Minimal code changes to migrate from single GPU.
- **Scalability**: Seamlessly scale to many GPUs and nodes.
- **Efficiency**: Ray Data ensures your GPUs are fed efficiently.
- **Observability**: Built-in tools to monitor and debug distributed runs.

### Next Steps

- Explore more [Ray Train examples](https://docs.ray.io/en/latest/train/examples.html) for different frameworks and workloads.
- Combine Ray Train with Ray Tune for hyperparameter optimization using the [Hyperparameter optimization](https://docs.ray.io/en/latest/train/user-guides/hyperparameter-optimization.html) guide.

