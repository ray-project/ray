# Introduction to Ray Train and Ray Data

This guide introduces distributed training with Ray Train. It demonstrates how to scale a PyTorch training loop from a single GPU to a cluster of GPUs, using Ray Data for efficient, scalable data ingestion.

For a conceptual overview of Ray Train, see the [Ray Train overview](https://docs.ray.io/en/latest/train/overview.html) and the main [Ray Train documentation](https://docs.ray.io/en/latest/train/train.html). For a broader set of topics and how-to guides, refer to the [Ray Train user guides](https://docs.ray.io/en/latest/train/user-guides.html).

**Roadmap**
1.  **Single GPU PyTorch**: A baseline implementation.
2.  **Migrating to Ray Train**:
    *   Model preparation
    *   Data ingestion with Ray Data
    *   Metrics and Checkpointing
    *   Updating the training loop
3.  **Launching the Job**: Configuring and running the distributed training.
4.  **Inspecting Results**: Accessing metrics and checkpoints.
5.  **Observability**: Monitoring your training.
6.  **Fault Tolerance**: Configuring automatic retries.
7.  **Troubleshooting**: Diagnosing common issues.

## Imports

```python
import os
import tempfile
import csv
import datetime
import time

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

## 1. Single GPU PyTorch

First, let's look at a standard PyTorch training setup for the MNIST dataset on a single GPU.

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/ray-ai-libraries/diagrams/single_gpu_pytorch_v3.png" width="800" loading="lazy">

### Model and Data

```python
def build_model():
    model = resnet18(num_classes=10)
    # Adjust first layer for grayscale MNIST (1 channel)
    model.conv1 = nn.Conv2d(
        in_channels=1, 
        out_channels=64,
        kernel_size=(7, 7),
        stride=(2, 2),
        padding=(3, 3),
        bias=False,
    )
    return model

def get_data_loader(batch_size=128):
    transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
    dataset = MNIST(root="./data", train=True, download=True, transform=transform)
    return DataLoader(dataset, batch_size=batch_size, shuffle=True, drop_last=True)
```

### Standard Training Loop

```python
def report_metrics_torch(loss: torch.Tensor, epoch: int):
    metrics = {"loss": loss.item(), "epoch": epoch}
    print(metrics)
    return metrics

def save_checkpoint_and_metrics_torch(metrics, model, local_path):
    os.makedirs(local_path, exist_ok=True)
    
    # Save metrics to CSV
    with open(os.path.join(local_path, "metrics.csv"), "a") as f:
        writer = csv.writer(f)
        writer.writerow(metrics.values())

    # Save model checkpoint
    torch.save(model.state_dict(), os.path.join(local_path, "model.pt"))

def train_func_single_gpu(num_epochs=2, local_path="/mnt/cluster_storage/single_gpu_mnist"):
    # Setup
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    model = build_model().to(device)
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=1e-5)
    dataloader = get_data_loader()
    
    # Training Loop
    model.train()
    for epoch in range(num_epochs):
        for images, labels in dataloader:
            images, labels = images.to(device), labels.to(device)
            
            outputs = model(images)
            loss = criterion(outputs, labels)
            
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            
        # Report and Save
        metrics = report_metrics_torch(loss, epoch)
        save_checkpoint_and_metrics_torch(metrics, model, local_path)

# To run this training function on a worker node, we can use a Ray task.
# To request a specific GPU (e.g. T4), use resources={"accelerator_type:T4": 0.0001}
@ray.remote(num_gpus=1, resources={"accelerator_type:T4": 0.0001})
def run_single_gpu_job():
    train_func_single_gpu()

ray.init() # Ensure Ray is initialized
ray.get(run_single_gpu_job.remote())
```

## 2. Migrating to Ray Train

Ray Train solves common challenges in scaling deep learning:
*   **Scale**: Move from single GPU to multiple GPUs/nodes with minimal code changes.
*   **Infrastructure**: Abstracts away cluster management and resource provisioning.
*   **Observability**: Provides built-in dashboards for monitoring metrics, logs, and resource usage.
*   **Reliability**: Features automatic fault tolerance to recover from worker or node failures.

To migrate our PyTorch code to Ray Train, we need to adapt the model preparation, data loading, and the training loop.

The goal is to scale the single-GPU setup to a distributed data-parallel architecture:

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/ray-ai-libraries/diagrams/multi_gpu_pytorch_v4.png" width="800" loading="lazy">

At a high level, Ray Train uses a controller (trainer) process to coordinate a group of training worker processes. The [Ray Train overview](https://docs.ray.io/en/latest/train/overview.html) introduces the core concepts: training function, workers, scaling configuration, and trainer.

<img src="https://docs.ray.io/en/latest/_images/overview.png" width="700" loading="lazy">

### 2.1 Migrating the Model

Use [`ray.train.torch.prepare_model`](https://docs.ray.io/en/latest/train/api/doc/ray.train.torch.prepare_model.html) to automatically wrap your model in `DistributedDataParallel` and move it to the correct device.

```python
def train_loop_per_worker(config):
    # 1. Prepare Model
    model = build_model()
    model = ray.train.torch.prepare_model(model) # Instead of model = model.to("cuda")
    
    # ... rest of the loop
```

### 2.2 Migrating Data Ingestion (Ray Data)

Instead of a PyTorch `DataLoader`, we will use Ray Data. With a few modifications, you can scale data preprocessing and training separately. For example, you can do the former with a pool of CPU workers and the latter with a pool of GPU workers.

Use Ray Data when you face one of the following challenges:

*   **Consistent Data Loading**: Standardize data ingestion across various formats (Parquet, CSV, images) and sources.
*   **Scalable Preprocessing**: Perform on-the-fly transformations (augmentations, tokenization) on a separate pool of CPU workers to avoid stalling training GPUs.

For efficient data loading in distributed settings, Ray Data handles sharding, streaming, and preprocessing data across the cluster, preventing the training from being bottlenecked by data ingestion. The architecture below shows how Ray Train (controller + workers) integrates with Ray Data and your storage layer:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/ray-train-deep-dive/ray_train_v2_architecture.png" width="800" loading="lazy">


#### Preparing the Dataset

First, we download the MNIST dataset and save the raw data to a local Parquet file. This simulates saving raw data to shared storage, allowing us to perform random transformations (like augmentations) on-the-fly during training.

```python
# Download MNIST
dataset = MNIST(root="./data", train=True, download=True)
df = pd.DataFrame({
    "image": dataset.data.numpy().tolist(),
    "label": dataset.targets.numpy()
})
ds = ray.data.from_pandas(df)
# NOTE: In a multi-node cluster, this path must be a shared storage location (e.g., S3, NFS) accessible by all workers.
mnist_path = os.path.abspath("/mnt/cluster_storage/raw_data")
ds.write_parquet(mnist_path)
```

#### Reading and Transforming the Dataset

We create a Ray Dataset and define preprocessing using standard torchvision transforms.

```python
def get_ray_dataset(path):
    ds = ray.data.read_parquet(path)
    
    def transform_images(row: dict):
        # Define the torchvision transform.
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

*   **Metrics**: Dictionaries of values (e.g., loss, accuracy) passed to `report()` are aggregated and logged.
*   **Checkpoints**: Model states saved to a directory and passed as a `ray.train.Checkpoint`.

**Key Behaviors**:
1.  **Synchronization**: `ray.train.report()` acts as a global barrier. All workers must call it to ensure training stays in sync.
2.  **Efficient Checkpointing**: To avoid redundant uploads in standard DDP, only the rank 0 worker should save the checkpoint to disk. Ray Train then automatically persists it to your configured storage.

The following diagram shows this checkpoint lifecycle:

<img src="https://docs.ray.io/en/latest/_images/checkpoint_lifecycle.png" width="800" loading="lazy">

```python
def save_checkpoint_and_report_metrics(model, metrics):
    checkpoint = None
    # Only the rank 0 worker saves the checkpoint to storage
    if ray.train.get_context().get_world_rank() == 0:
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            # Access the original model via `model.module` when wrapped in DistributedDataParallel
            torch.save(model.module.state_dict(), os.path.join(temp_checkpoint_dir, "model.pt"))
            checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)
    
    # All workers must call report to synchronize
    ray.train.report(metrics, checkpoint=checkpoint)
```

For an in-depth guide on saving checkpoints and metrics, see the [Saving and Loading Checkpoints guide](https://docs.ray.io/en/latest/train/user-guides/checkpoints.html).

### 2.4 Updating the Training Loop

In the training loop, `ray.train.get_dataset_shard("train")` automatically retrieves the data shard assigned to this worker (from the `datasets` passed to `TorchTrainer`). No manual sharding logic is required. We then iterate over the shard using `iter_torch_batches`.

This pattern is also shown in the [Data loading and preprocessing](https://docs.ray.io/en/latest/train/user-guides/data-loading-preprocessing.html) guide.

```python
def train_loop_per_worker(config):
    # 1. Setup Model
    model = build_model()
    model = ray.train.torch.prepare_model(model) # Instead of model = model.to("cuda")
    
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=config["lr"])

    # 2. Setup Data (Ray Data)
    # Get the data shard for this worker
    dataset_shard = ray.train.get_dataset_shard("train")
    
    # 3. Calculate Batch Size
    global_batch_size = config["batch_size"]
    world_size = ray.train.get_context().get_world_size()
    per_worker_batch_size = global_batch_size // world_size

    for epoch in range(config["epochs"]):
        # Create an iterator for this epoch
        dataloader = dataset_shard.iter_torch_batches(
            batch_size=per_worker_batch_size,
            dtypes={"image": torch.float32, "label": torch.long},
            device=ray.train.torch.get_device() # Auto-move to GPU
        )

        # No longer need to ensure data is on the correct device
        # dataloader.sampler.set_epoch(epoch)

        for batch in dataloader:

            # Note: Batches are dictionaries (from Ray Data), not tuples
            inputs, labels = batch["image"], batch["label"]
            
            outputs = model(inputs)
            loss = criterion(outputs, labels)
            
            optimizer.zero_grad()
            loss.backward() # gradients are now accumulated across the workers
            optimizer.step()

        # 4. Report Metrics & Checkpoint
        metrics = {"loss": loss.item(), "epoch": epoch}
        
        save_checkpoint_and_report_metrics(model, metrics)

```

## 3. Launching the Distributed Job

The following diagram illustrates the distributed training workflow. The `TorchTrainer` launches a set of workers, and each worker calls `ray.train.get_dataset_shard()` to receive its portion of the data stream from Ray Data.

<img src="https://anyscale-public-materials.s3.us-west-2.amazonaws.com/intro-ai-libraries/multi_gpu_pytorch_annotated.png" width="900" loading="lazy">

To launch the distributed training job, we need to configure:
1.  **Scaling Configuration**: Defines the number of workers and compute resources (GPUs/CPUs) per worker.
2.  **Run Configuration**: Specifies the storage location for checkpoints and experiment results.

```python
# Create the dataset
train_ds = get_ray_dataset(mnist_path)

# Configure Scale (2 Workers)
scaling_config = ScalingConfig(
    num_workers=2,
    use_gpu=True, # Implicitly requests 1 GPU per worker
    resources_per_worker={"CPU": 1, "accelerator_type:T4": 0.0001} # Explicitly requests 1 CPU per worker and a specific GPU type
)

# Configure Run (Storage path)
run_config = RunConfig(
    name="mnist_ray_train_demo",
    storage_path="/mnt/cluster_storage/distributed_training" # must be a shared storage location in a multi-node cluster
)

# Initialize Trainer
trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={"lr": 1e-5, "batch_size": 128, "epochs": 2},
    scaling_config=scaling_config,
    run_config=run_config,
    datasets={"train": train_ds}, # Inject Ray Data
)

# Start Training
result = trainer.fit()
```


## 4. Inspecting Results

The `trainer.fit()` call returns a `Result` object containing metrics and checkpoint information. We can use this to load the trained model and generate predictions.

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

# Generate predictions

dataset = MNIST(root="./data", train=False, download=True)
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

## 5. Observability

Ray provides comprehensive monitoring tools to understand your training performance. See [Monitoring and Logging](https://docs.ray.io/en/latest/train/user-guides/monitoring-logging.html) for more details.

### Ray Dashboard
The Ray Dashboard is accessible via your browser (default `http://localhost:8265`). Key views include:

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/apple/train_dashboard.png" width="800" loading="lazy">

* **Cluster View**: Monitor CPU/GPU utilization across nodes.
* **Jobs View**: See the status of your `TorchTrainer` job.
* **Actors View**: Inspect individual `RayTrainWorker` actors (stack traces, logs).
* **Metrics**: View time-series graphs of system metrics (GPU memory, network I/O).

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/apple/gpu_util_and_disk.png" width="400" loading="lazy">

The above metrics include GPU utilization (training activity) and disk I/O (data download to worker nodes).

## 6. Fault Tolerance

Ray Train provides built-in fault tolerance to recover from worker failures (e.g., preemption).

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/ray-train-deep-dive/fault_tolerance_train_v2.png" width="800" loading="lazy">

### Automatic Retries

Ray Train can automatically restart failed workers and resume training. To enable this, set `max_failures` in `RunConfig`:

```python
run_config = RunConfig(
    failure_config=ray.train.FailureConfig(max_failures=3)
)
```

### Manual Restoration

If the job is interrupted (e.g., driver crash) or `max_failures` is exceeded, you can resume training manually by re-executing the script with the same `RunConfig` (same `name` and `storage_path`).

**Prerequisite**: For either mechanism to resume progress instead of restarting from scratch, your training loop must implement logic to load the latest checkpoint upon startup. See [Handling failures and node preemption](https://docs.ray.io/en/latest/train/user-guides/fault-tolerance.html) for the implementation guide.

## 7. Troubleshooting

Common issues in distributed training and how to diagnose them:

### 1. Trainer Hangs
* **Symptoms**: The job is running but logs stop updating; GPU utilization drops to 0.
* **Causes**:
  * **Collective Ops Mismatch**: `ray.train.report` must be called on *all* workers. If one worker skips it (e.g., inside an `if` block), the others will wait indefinitely.
  * **Data Iterator Sync**: `iter_torch_batches` acts as a synchronization barrier. All workers must iterate the dataset shard in sync.
* **Diagnosis**: Use the Ray Dashboard to view the **Stack Trace** of the `RayTrainWorker` actors from the **Actors** or **Jobs** views.

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/apple/train_dashboard_stack_trace.png" width="700" loading="lazy">

### 2. Data Bottlenecks
* **Symptoms**: GPU utilization is low or oscillating.
* **Diagnosis**: Check **Iteration Blocked Time** and dataset metrics. High blocked time means the GPU is waiting for data.

<img src="https://anyscale-materials.s3.us-west-2.amazonaws.com/pinterest/train_deep_dive/increasing-iteration-blocked-time.png" width="500" loading="lazy">

* **Fixes**:
  * Increase `prefetch_batches` in `iter_torch_batches`.
  * Scale up data processing by adding more CPU resources or optimizing the `map_batches` function.

### 3. OOM (Out of Memory)
* **Symptoms**: Worker crashes with CUDA OOM.
* **Fixes**:
  * Reduce `batch_size` (note: `global_batch_size` vs per-worker batch size).
  * Ensure you are not accumulating history (tensors with gradients) in lists/dicts over epochs.
  * If checkpoints are large, consider lighter checkpointing strategies described in [Saving and loading checkpoints](https://docs.ray.io/en/latest/train/user-guides/checkpoints.html).

## 8. Conclusion

Ray Train coupled with Ray Data provides a powerful stack for scaling deep learning:

* **Simplicity**: Minimal code changes to migrate from single GPU.
* **Scalability**: Seamlessly scale to many GPUs and nodes.
* **Efficiency**: Ray Data ensures your GPUs are fed efficiently.
* **Observability**: Built-in tools to monitor and debug distributed runs.

Next steps:
* Explore more [Ray Train examples](https://docs.ray.io/en/latest/train/examples.html) for different frameworks and workloads.

* Combine Ray Train with Ray Tune for hyperparameter optimization using the [Hyperparameter optimization](https://docs.ray.io/en/latest/train/user-guides/hyperparameter-optimization.html) guide.