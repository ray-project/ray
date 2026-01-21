# 00. Runtime setup — install same deps as build.sh and set env vars
import os
import sys
import subprocess

# Non-secret env var
os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

# Install Python dependencies (
subprocess.check_call(
    [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--no-cache-dir",
        "torch==2.8.0",
        "torchvision==0.23.0",
        "matplotlib==3.10.6",
        "pyarrow==14.0.2",
    ]
)

# 01. Imports

# --- Standard library: file IO, paths, timestamps, temp dirs, cleanup ---
import csv  # Simple CSV logging for metrics in single-GPU section
import datetime  # Timestamps for run directories / filenames
import os  # Filesystem utilities (paths, env vars)
import tempfile  # Ephemeral dirs for checkpoint staging with ray.train.report()
import shutil  # Cleanup of artifacts (later cells)
import gc  # Manual garbage collection to cleanup after inference

from pathlib import Path  # Convenient, cross-platform path handling

# --- Visualization & data wrangling ---
import matplotlib.pyplot as plt  # Plot sample digits and metrics curves
from PIL import Image  # Image utilities for inspection/debug
import numpy as np  # Numeric helpers (random sampling, arrays)
import pandas as pd  # Read metrics.csv into a DataFrame

# --- PyTorch & TorchVision (model + dataset) ---
import torch
from torch.nn import CrossEntropyLoss  # Classification loss for MNIST
from torch.optim import Adam  # Optimizer
from torchvision.models import (
    resnet18,
)  # Baseline CNN (we’ll adapt for 1-channel input)
from torchvision.datasets import MNIST  # Dataset
from torchvision.transforms import (
    ToTensor,
    Normalize,
    Compose,
)  # Preprocessing pipeline

# --- Ray Train (distributed orchestration) ---
import ray
from ray.train import ScalingConfig, RunConfig  # Configure scale and storage
from ray.train.torch import TorchTrainer  # Multi-GPU PyTorch trainer (DDP/FSDP)

# 02. Download MNIST Dataset

dataset = MNIST(root="/mnt/cluster_storage/data", train=True, download=True)

# 03. Visualize Sample Digits

# Create a square figure for plotting 9 samples (3x3 grid)
figure = plt.figure(figsize=(8, 8))
cols, rows = 3, 3

# Loop through grid slots and plot a random digit each time
for i in range(1, cols * rows + 1):
    # Randomly select an index from the dataset
    sample_idx = np.random.randint(0, len(dataset.data))
    img, label = dataset[sample_idx]  # image (PIL) and its digit label

    # Add subplot to the figure
    figure.add_subplot(rows, cols, i)
    plt.title(label)  # show the digit label above each subplot
    plt.axis("off")  # remove axes for cleaner visualization
    plt.imshow(img, cmap="gray")  # display as grayscale image

# 04. Define ResNet-18 Model for MNIST


def build_resnet18():
    # Start with a torchvision ResNet-18 backbone
    # Set num_classes=10 since MNIST has digits 0–9
    model = resnet18(num_classes=10)

    # Override the first convolution layer:
    # - Default expects 3 channels (RGB images)
    # - MNIST is grayscale → only 1 channel
    # - Keep kernel size/stride/padding consistent with original ResNet
    model.conv1 = torch.nn.Conv2d(
        in_channels=1,  # input = grayscale
        out_channels=64,  # number of filters remains the same as original ResNet
        kernel_size=(7, 7),
        stride=(2, 2),
        padding=(3, 3),
        bias=False,
    )

    # Return the customized ResNet-18
    return model


# 05. Define the Ray Train per-worker training loop


def train_loop_ray_train(config: dict):  # pass in hyperparameters in config
    # config holds hyperparameters passed from TorchTrainer (e.g. num_epochs, global_batch_size)

    # Define loss function for MNIST classification
    criterion = CrossEntropyLoss()

    # Build and prepare the model for distributed training.
    # load_model_ray_train() calls ray.train.torch.prepare_model()
    # → moves model to GPU and wraps it in DistributedDataParallel (DDP).
    model = load_model_ray_train()

    # Standard optimizer (learning rate fixed for demo)
    optimizer = Adam(model.parameters(), lr=1e-5)

    # Calculate the batch size for each worker
    global_batch_size = config["global_batch_size"]
    world_size = (
        ray.train.get_context().get_world_size()
    )  # total # of workers in the job
    batch_size = global_batch_size // world_size  # split global batch evenly
    print(f"{world_size=}\n{batch_size=}")

    # Wrap DataLoader with prepare_data_loader()
    # → applies DistributedSampler (shards data across workers)
    # → ensures batches are automatically moved to correct device
    data_loader = build_data_loader_ray_train(batch_size=batch_size)

    # ----------------------- Training loop ----------------------- #
    for epoch in range(config["num_epochs"]):

        # Ensure each worker shuffles its shard differently every epoch
        data_loader.sampler.set_epoch(epoch)

        # Iterate over batches (sharded across workers)
        for images, labels in data_loader:
            outputs = model(images)  # forward pass
            loss = criterion(outputs, labels)  # compute loss
            optimizer.zero_grad()  # reset gradients

            loss.backward()  # backward pass (grads averaged across workers via DDP)
            optimizer.step()  # update model weights

        # After each epoch: report loss and log metrics
        metrics = print_metrics_ray_train(loss, epoch)

        # Save checkpoint (only rank-0 worker persists the model)
        save_checkpoint_and_metrics_ray_train(model, metrics)


# 06. Define the configuration dictionary passed into the training loop

# train_loop_config is provided to TorchTrainer and injected into
# train_loop_ray_train(config) as the "config" argument.
# → Any values defined here are accessible inside the training loop.

train_loop_config = {
    "num_epochs": 2,  # Number of full passes through the dataset
    "global_batch_size": 128  # Effective batch size across ALL workers
    # (Ray will split this evenly per worker, e.g.
    # with 8 workers → 16 samples/worker/step)
}

# 07. Configure the scaling of the training job

# ScalingConfig defines how many parallel training workers Ray should launch
# and whether each worker should be assigned a GPU or CPU.Z
# → Each worker runs train_loop_ray_train(config) independently,
#    with Ray handling synchronization via DDP under the hood.

scaling_config = ScalingConfig(
    num_workers=8,  # Launch 8 training workers (1 process per worker)
    use_gpu=True,  # Allocate 1 GPU to each worker
)

# 08. Build and prepare the model for Ray Train


def load_model_ray_train() -> torch.nn.Module:
    model = build_resnet18()
    # prepare_model() → move to correct device + wrap in DDP automatically
    model = ray.train.torch.prepare_model(model)
    return model


# 09. Build a Ray Train–ready DataLoader for MNIST


def build_data_loader_ray_train(batch_size: int) -> torch.utils.data.DataLoader:
    # Define preprocessing: convert to tensor + normalize pixel values
    transform = Compose([ToTensor(), Normalize((0.5,), (0.5,))])
    # Load the MNIST training set from persistent cluster storage
    train_data = MNIST(
        root="/mnt/cluster_storage/data",
        train=True,
        download=True,
        transform=transform,
    )

    # Standard PyTorch DataLoader (batching, shuffling, drop last incomplete batch)
    train_loader = torch.utils.data.DataLoader(
        train_data, batch_size=batch_size, shuffle=True, drop_last=True
    )

    # prepare_data_loader():
    # - Adds a DistributedSampler when using multiple workers
    # - Moves batches to the correct device automatically
    train_loader = ray.train.torch.prepare_data_loader(train_loader)

    return train_loader


# 10. Report training metrics from each worker


def print_metrics_ray_train(loss: torch.Tensor, epoch: int) -> None:
    metrics = {"loss": loss.item(), "epoch": epoch}
    world_rank = ray.train.get_context().get_world_rank()  # report from all workers
    print(f"{metrics=} {world_rank=}")
    return metrics


# 11. Save checkpoint and report metrics with Ray Train


def save_checkpoint_and_metrics_ray_train(
    model: torch.nn.Module, metrics: dict[str, float]
) -> None:
    # Create a temporary directory to stage checkpoint files
    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        # Save the model weights.
        # Note: under DDP the model is wrapped in DistributedDataParallel,
        # so we unwrap it with `.module` before calling state_dict().
        torch.save(
            model.module.state_dict(),  # note the `.module` to unwrap the DistributedDataParallel
            os.path.join(temp_checkpoint_dir, "model.pt"),
        )

        # Report metrics and attach a checkpoint to Ray Train.
        # → metrics are logged centrally
        # → checkpoint allows resuming training or running inference later
        ray.train.report(
            metrics,
            checkpoint=ray.train.Checkpoint.from_directory(temp_checkpoint_dir),
        )


# 12. Save checkpoint only from the rank-0 worker


def save_checkpoint_and_metrics_ray_train(
    model: torch.nn.Module, metrics: dict[str, float]
) -> None:
    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        checkpoint = None

        # Only the rank-0 worker writes the checkpoint file
        if ray.train.get_context().get_world_rank() == 0:
            torch.save(
                model.module.state_dict(),  # unwrap DDP before saving
                os.path.join(temp_checkpoint_dir, "model.pt"),
            )
            checkpoint = ray.train.Checkpoint.from_directory(temp_checkpoint_dir)

        # All workers still call ray.train.report()
        # → keeps training loop synchronized
        # → metrics are logged from each worker
        # → only rank-0 attaches a checkpoint
        ray.train.report(
            metrics,
            checkpoint=checkpoint,
        )


# 13. Configure persistent storage and run name

storage_path = "/mnt/cluster_storage/training/"
run_config = RunConfig(
    storage_path=storage_path,  # where to store checkpoints/logs
    name="distributed-mnist-resnet18",  # identifier for this run
)

# 14. Set up the TorchTrainer

trainer = TorchTrainer(
    train_loop_ray_train,  # training loop to run on each worker
    scaling_config=scaling_config,  # number of workers and resource config
    run_config=run_config,  # storage path + run name for artifacts
    train_loop_config=train_loop_config,  # hyperparameters passed to the loop
)

# 15. Launch distributed training

# trainer.fit() starts the training job:
# - Spawns workers according to scaling_config
# - Runs train_loop_ray_train() on each worker
# - Collects metrics and checkpoints into result
result = trainer.fit()

# 16. Show the training results

result  # contains metrics, checkpoints, and run history

# 17. Display the full metrics history as a pandas DataFrame

result.metrics_dataframe

# 18. Define a Ray actor to load the trained model and run inference


@ray.remote(num_gpus=1)  # allocate 1 GPU to this actor
class ModelWorker:
    def __init__(self, checkpoint):
        # Load model weights from the Ray checkpoint (on CPU first)
        with checkpoint.as_directory() as ckpt_dir:
            model_path = os.path.join(ckpt_dir, "model.pt")
            state_dict = torch.load(
                model_path,
                map_location=torch.device("cpu"),
                weights_only=True,
            )
        # Rebuild the model, load weights, move to GPU, and set to eval mode
        self.model = build_resnet18()
        self.model.load_state_dict(state_dict)
        self.model.to("cuda")
        self.model.eval()

    @torch.inference_mode()  # disable autograd for faster inference
    def predict(self, batch):
        """
        batch: torch.Tensor or numpy array with shape [B,C,H,W] or [C,H,W]
        returns: list[int] predicted class indices
        """
        x = torch.as_tensor(batch)
        if x.ndim == 3:  # single image → add batch dimension
            x = x.unsqueeze(0)  # shape becomes [1,C,H,W]
        x = x.to("cuda", non_blocking=True)

        logits = self.model(x)
        preds = torch.argmax(logits, dim=1)
        return preds.detach().cpu().tolist()


# Create a fresh actor instance (avoid naming conflicts)
worker = ModelWorker.remote(result.checkpoint)

# 19. CPU preprocessing + GPU inference via Ray actor

to_tensor = ToTensor()


def normalize_cpu(img):
    # Convert image (PIL) to tensor on CPU → shape [C,H,W]
    t = to_tensor(img)  # [C,H,W] on CPU
    C = t.shape[0]
    # Apply channel-wise normalization (grayscale vs RGB)
    if C == 3:
        norm = Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))
    else:
        norm = Normalize((0.5,), (0.5,))
    return norm(t)


figure = plt.figure(figsize=(8, 8))
cols, rows = 3, 3

# Plot a 3x3 grid of random MNIST samples with predictions
for i in range(1, cols * rows + 1):
    idx = np.random.randint(0, len(dataset))
    img, label = dataset[idx]

    # Preprocess on CPU, add batch dim → [1,C,H,W]
    x = normalize_cpu(img).unsqueeze(0)

    # Run inference on GPU via Ray actor, fetch result
    pred = ray.get(worker.predict.remote(x))[0]  # int

    # Plot image with true label and predicted label
    figure.add_subplot(rows, cols, i)
    plt.title(f"label: {label}; pred: {int(pred)}")
    plt.axis("off")
    arr = np.array(img)
    plt.imshow(arr, cmap="gray" if arr.ndim == 2 else None)

plt.tight_layout()
plt.show()

# 20.

# stop the actor process and free its GPU
ray.kill(worker, no_restart=True)

# drop local references so nothing pins it
del worker

# Forcing garbage collection is optional:
# - Cluster resources are already freed by ray.kill()
# - Python will clean up the local handle eventually
# - gc.collect() is usually unnecessary unless debugging memory issues
gc.collect()

# 01. Training loop using Ray Data


def train_loop_ray_train_ray_data(config: dict):
    # Same as before: define loss, model, optimizer
    criterion = CrossEntropyLoss()
    model = load_model_ray_train()
    optimizer = Adam(model.parameters(), lr=1e-3)

    # Different: build data loader from Ray Data instead of PyTorch DataLoader
    global_batch_size = config["global_batch_size"]
    batch_size = global_batch_size // ray.train.get_context().get_world_size()
    data_loader = build_data_loader_ray_train_ray_data(batch_size=batch_size)

    # Same: loop over epochs
    for epoch in range(config["num_epochs"]):
        # Different: no sampler.set_epoch(), Ray Data handles shuffling internally

        # Different: batches are dicts {"image": ..., "label": ...} not tuples
        for batch in data_loader:
            outputs = model(batch["image"])
            loss = criterion(outputs, batch["label"])
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        # Same: report metrics and save checkpoint each epoch
        metrics = print_metrics_ray_train(loss, epoch)
        save_checkpoint_and_metrics_ray_train(model, metrics)


# 02. Build a Ray Data–backed data loader


def build_data_loader_ray_train_ray_data(batch_size: int, prefetch_batches: int = 2):

    # Different: instead of creating a PyTorch DataLoader,
    # fetch the training dataset shard for this worker
    dataset_iterator = ray.train.get_dataset_shard("train")

    # Convert the shard into a PyTorch-style iterator
    # - Returns dict batches: {"image": ..., "label": ...}
    # - prefetch_batches controls pipeline buffering
    data_loader = dataset_iterator.iter_torch_batches(
        batch_size=batch_size, prefetch_batches=prefetch_batches
    )

    return data_loader


# 03. Convert MNIST dataset into Parquet for Ray Data

# Build a DataFrame with image arrays and labels
df = pd.DataFrame(
    {
        "image": dataset.data.tolist(),  # raw image pixels (as lists)
        "label": dataset.targets,  # digit labels 0–9
    }
)

# Persist the dataset in Parquet format (columnar, efficient for Ray Data)
df.to_parquet("/mnt/cluster_storage/MNIST.parquet")

# 04. Load the Parquet dataset into a Ray Dataset

# Read the Parquet file → creates a distributed Ray Dataset
train_ds = ray.data.read_parquet("/mnt/cluster_storage/MNIST.parquet")

# 05. Define preprocessing transform for Ray Data


def transform_images(row: dict):
    # Convert numpy array to a PIL image, then apply TorchVision transforms
    transform = Compose(
        [
            ToTensor(),  # convert to tensor
            Normalize((0.5,), (0.5,)),  # normalize to [-1, 1]
        ]
    )

    # Ensure image is in uint8 before conversion
    image_arr = np.array(row["image"], dtype=np.uint8)

    # Apply transforms and replace the "image" field with tensor
    row["image"] = transform(Image.fromarray(image_arr))
    return row


# 06. Apply the preprocessing transform across the Ray Dataset

# Run transform_images() on each row (parallelized across cluster workers)
train_ds = train_ds.map(transform_images)

# 07. Configure TorchTrainer with Ray Data integration

# Wrap Ray Dataset in a dict → accessible as "train" inside the training loop
datasets = {"train": train_ds}

trainer = TorchTrainer(
    train_loop_ray_train_ray_data,  # training loop consuming Ray Data
    train_loop_config={  # hyperparameters
        "num_epochs": 1,
        "global_batch_size": 512,
    },
    scaling_config=scaling_config,  # number of workers + GPU/CPU resources
    run_config=RunConfig(
        storage_path=storage_path, name="dist-MNIST-res18-ray-data"
    ),  # where to store checkpoints/logs
    datasets=datasets,  # provide Ray Dataset shards to workers
)

# 08. Start the distributed training job with Ray Data integration

# Launches the training loop across all workers
# - Streams preprocessed Ray Dataset batches into each worker
# - Reports metrics and checkpoints to cluster storage
trainer.fit()

# 01. Training loop with checkpoint loading for fault tolerance


def train_loop_ray_train_with_checkpoint_loading(config: dict):
    # Same setup as before: loss, model, optimizer
    criterion = CrossEntropyLoss()
    model = load_model_ray_train()
    optimizer = Adam(model.parameters(), lr=1e-3)

    # Same data loader logic as before
    global_batch_size = config["global_batch_size"]
    batch_size = global_batch_size // ray.train.get_context().get_world_size()
    data_loader = build_data_loader_ray_train_ray_data(batch_size=batch_size)

    # Default: start at epoch 0 unless a checkpoint is available
    start_epoch = 0

    # Attempt to load from latest checkpoint
    checkpoint = ray.train.get_checkpoint()
    if checkpoint:
        # Continue training from a previous checkpoint
        with checkpoint.as_directory() as ckpt_dir:
            # Restore model + optimizer state
            model_state_dict = torch.load(
                os.path.join(ckpt_dir, "model.pt"),
            )
            # Load the model and optimizer state
            model.module.load_state_dict(model_state_dict)
            optimizer.load_state_dict(
                torch.load(os.path.join(ckpt_dir, "optimizer.pt"))
            )

            # Resume from last epoch + 1
            start_epoch = (
                torch.load(os.path.join(ckpt_dir, "extra_state.pt"))["epoch"] + 1
            )

    # Same training loop as before except it starts at a parameterized start_epoch
    for epoch in range(start_epoch, config["num_epochs"]):
        for batch in data_loader:
            outputs = model(batch["image"])
            loss = criterion(outputs, batch["label"])
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        # Report metrics and save model + optimizer + epoch state
        metrics = print_metrics_ray_train(loss, epoch)

        # We now save the optimizer and epoch state in addition to the model
        save_checkpoint_and_metrics_ray_train_with_extra_state(
            model, metrics, optimizer, epoch
        )


# 02. Save checkpoint with model, optimizer, and epoch state


def save_checkpoint_and_metrics_ray_train_with_extra_state(
    model: torch.nn.Module,
    metrics: dict[str, float],
    optimizer: torch.optim.Optimizer,
    epoch: int,
) -> None:

    with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
        checkpoint = None
        # Only rank-0 worker saves files to disk
        if ray.train.get_context().get_world_rank() == 0:
            # Save all state required for full recovery
            torch.save(
                model.module.state_dict(),  # unwrap DDP before saving
                os.path.join(temp_checkpoint_dir, "model.pt"),
            )
            torch.save(
                optimizer.state_dict(),  # include optimizer state
                os.path.join(temp_checkpoint_dir, "optimizer.pt"),
            )
            torch.save(
                {"epoch": epoch},  # store last completed epoch
                os.path.join(temp_checkpoint_dir, "extra_state.pt"),
            )
            # Package into a Ray checkpoint
            checkpoint = ray.train.Checkpoint.from_directory(temp_checkpoint_dir)

        # Report metrics and attach checkpoint (only rank-0 attaches checkpoint)
        ray.train.report(
            metrics,
            checkpoint=checkpoint,
        )


# 03. Configure TorchTrainer with fault-tolerance enabled

# Allow up to 3 automatic retries if workers fail
failure_config = ray.train.FailureConfig(max_failures=3)

experiment_name = "fault-tolerant-MNIST-vit"

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_ray_train_with_checkpoint_loading,  # fault-tolerant loop
    train_loop_config={  # hyperparameters
        "num_epochs": 1,
        "global_batch_size": 512,
    },
    scaling_config=scaling_config,  # resource scaling as before
    run_config=ray.train.RunConfig(
        name="fault-tolerant-MNIST-vit",
        storage_path=storage_path,  # persistent checkpoint storage
        failure_config=failure_config,  # enable automatic retries
    ),
    datasets=datasets,  # Ray Dataset shard for each worker
)

# 04. Start the fault-tolerant training job

# Launches training with checkpointing + automatic retries enabled
# If workers fail, Ray will reload the latest checkpoint and resume
trainer.fit()

# 05. Manually restore a trainer from the last checkpoint

restored_trainer = TorchTrainer(
    train_loop_per_worker=train_loop_ray_train_with_checkpoint_loading,  # loop supports checkpoint loading
    train_loop_config={  # hyperparameters must match
        "num_epochs": 1,
        "global_batch_size": 512,
    },
    scaling_config=scaling_config,  # same resource setup as before
    run_config=ray.train.RunConfig(
        name="fault-tolerant-MNIST-vit",  # must match previous run name
        storage_path=storage_path,  # path where checkpoints are saved
        failure_config=failure_config,  # still allow retries
    ),
    datasets=datasets,  # same dataset as before
)

# 06. Resume training from the last checkpoint

# Fit the restored trainer → continues from last saved epoch
# If all epochs are already complete, training ends immediately
result = restored_trainer.fit()

# Display final training results (metrics, checkpoints, etc.)
result

# 07. Cleanup Cluster Storage

# Paths to remove → include MNIST data, training outputs, and MNIST.parquet
paths_to_delete = [
    "/mnt/cluster_storage/MNIST",
    "/mnt/cluster_storage/training",
    "/mnt/cluster_storage/MNIST.parquet",
]

for path in paths_to_delete:
    if os.path.exists(path):
        # Handle directories vs. files
        if os.path.isdir(path):
            shutil.rmtree(path)  # recursively delete directory
        else:
            os.remove(path)  # delete single file
        print(f"Deleted: {path}")
    else:
        print(f"Not found: {path}")
