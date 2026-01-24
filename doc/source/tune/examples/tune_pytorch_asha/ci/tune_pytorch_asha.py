# 00. Runtime setup — install same deps as build.sh and set env vars
import os
import sys
import subprocess

# Non-secret env var
os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

# Install Python dependencies
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

# --- Standard libraries ---
import os  # Filesystem utilities (paths, directories)
import tempfile  # Temporary directories for checkpoints
import shutil  # Cleanup of files and directories

# --- Analytics / plotting ---
import pandas as pd  # Converting output to dataframe for plotting
import matplotlib.pyplot as plt  # For generating plots

# --- Scientific computing ---
import numpy as np  # Numerical operations, used for random sampling in search space

# --- PyTorch (deep learning) ---
import torch
import torch.nn as nn  # Neural network modules (layers, models)
import torch.nn.functional as F  # Functional API for activations/losses
import torch.optim as optim  # Optimizers (e.g., SGD, Adam)
import torchvision  # Popular vision datasets and pretrained models
import torchvision.transforms as transforms  # Image preprocessing pipelines
from torch.utils.data import random_split  # Train/validation dataset splitting

# --- Utilities ---
from filelock import (
    FileLock,
)  # Prevents race conditions when multiple workers download CIFAR-10

# --- Ray (tuning and orchestration) ---
from ray import train, tune  # Core APIs for metric reporting and trial execution
from ray.tune.schedulers import (
    ASHAScheduler,
)  # Asynchronous HyperBand for early stopping
from ray.air.config import (
    RunConfig,
)  # Configure experiment metadata (name, storage, logging)

# 02. Load and prepare CIFAR-10 data


def load_data(data_dir="/mnt/cluster_storage/cifar10"):
    """
    Download and load the CIFAR-10 dataset with standard preprocessing.
    Returns the full training set and the test set.
    """
    # Define preprocessing: convert to tensor and normalize channels
    transform = transforms.Compose(
        [
            transforms.ToTensor(),  # Convert images to PyTorch tensors [0,1]
            transforms.Normalize(  # Normalize with dataset mean & std (per channel)
                (0.4914, 0.4822, 0.4465),  # mean (R, G, B)
                (0.2023, 0.1994, 0.2010),  # std (R, G, B)
            ),
        ]
    )

    # FileLock ensures that multiple parallel workers downloading CIFAR-10
    # don't interfere with each other (prevents race conditions).
    with FileLock(os.path.expanduser("~/.data.lock")):
        trainset = torchvision.datasets.CIFAR10(
            root=data_dir, train=True, download=True, transform=transform
        )

        testset = torchvision.datasets.CIFAR10(
            root=data_dir, train=False, download=True, transform=transform
        )

    return trainset, testset


def create_dataloaders(trainset, batch_size, num_workers=8):
    """
    Split the CIFAR-10 training set into train/validation subsets,
    and wrap them in DataLoader objects.
    """
    # Compute split sizes: 80% train, 20% validation
    train_size = int(len(trainset) * 0.8)

    # Randomly partition the dataset into train/val subsets
    train_subset, val_subset = random_split(
        trainset, [train_size, len(trainset) - train_size]
    )

    # Training loader: shuffle for stochastic gradient descent
    train_loader = torch.utils.data.DataLoader(
        train_subset, batch_size=batch_size, shuffle=True, num_workers=num_workers
    )

    # Validation loader: no shuffle (deterministic evaluation)
    val_loader = torch.utils.data.DataLoader(
        val_subset, batch_size=batch_size, shuffle=False, num_workers=num_workers
    )
    return train_loader, val_loader


# 03. Load synthetic test data


def load_test_data():
    """
    Create small synthetic datasets for quick smoke testing.
    Useful to validate the training loop and Ray Tune integration
    without downloading or processing the full CIFAR-10 dataset.
    """

    # Generate a fake training set of 128 samples
    # Each sample is shaped like a CIFAR-10 image: (3 channels, 32x32 pixels)
    # Labels are drawn from 10 possible classes.
    trainset = torchvision.datasets.FakeData(
        size=128,  # number of samples
        image_size=(3, 32, 32),  # match CIFAR-10 format
        num_classes=10,  # same number of categories as CIFAR-10
        transform=transforms.ToTensor(),  # convert to PyTorch tensors
    )

    # Generate a smaller fake test set of 16 samples
    testset = torchvision.datasets.FakeData(
        size=16, image_size=(3, 32, 32), num_classes=10, transform=transforms.ToTensor()
    )

    # Return both sets so they can be wrapped into DataLoaders
    return trainset, testset


# 04. Define CNN model


class Net(nn.Module):
    """
    A simple convolutional neural network (CNN) for CIFAR-10 classification.
    Consists of two convolutional layers with pooling, followed by three
    fully connected (dense) layers. Hidden layer sizes l1 and l2 are tunable.
    """

    def __init__(self, l1=120, l2=84):
        super(Net, self).__init__()

        # --- Convolutional feature extractor ---
        # First conv: input = 3 channels (RGB), output = 6 feature maps, kernel size = 5
        self.conv1 = nn.Conv2d(in_channels=3, out_channels=6, kernel_size=5)

        # Max pooling: downsample feature maps by factor of 2 (2x2 pooling window)
        self.pool = nn.MaxPool2d(kernel_size=2, stride=2)

        # Second conv: input = 6 feature maps, output = 16 feature maps, kernel size = 5
        self.conv2 = nn.Conv2d(in_channels=6, out_channels=16, kernel_size=5)

        # --- Fully connected classifier ---
        # Flattened input size = 16 feature maps * 5 * 5 spatial size = 400
        # Map this to first hidden layer of size l1 (tunable hyperparameter)
        self.fc1 = nn.Linear(in_features=16 * 5 * 5, out_features=l1)

        # Second hidden layer of size l2 (also tunable)
        self.fc2 = nn.Linear(in_features=l1, out_features=l2)

        # Final classification layer: map to 10 classes (CIFAR-10)
        self.fc3 = nn.Linear(in_features=l2, out_features=10)

    def forward(self, x):
        """
        Define forward pass through the network.
        """
        # Apply conv1 → ReLU → pooling
        x = self.pool(F.relu(self.conv1(x)))

        # Apply conv2 → ReLU → pooling
        x = self.pool(F.relu(self.conv2(x)))

        # Flatten from (batch_size, 16, 5, 5) → (batch_size, 400)
        x = x.view(-1, 16 * 5 * 5)

        # Fully connected layers with ReLU activations
        x = F.relu(self.fc1(x))
        x = F.relu(self.fc2(x))

        # Final linear layer (logits for 10 classes)
        x = self.fc3(x)

        return x


# 05. Define a training function


def train_cifar(config):
    """
    Train a CIFAR-10 CNN model with hyperparameters provided in `config`.
    Supports checkpointing and metric reporting for Ray Tune.
    """

    # --- Model setup ---
    # Initialize network with tunable hidden sizes (l1, l2).
    net = Net(config["l1"], config["l2"])
    device = config["device"]

    # If using CUDA with multiple GPUs, wrap in DataParallel for multi-GPU training.
    if device == "cuda":
        net = nn.DataParallel(net)
    net.to(device)

    # --- Loss and optimizer ---
    criterion = nn.CrossEntropyLoss()  # standard classification loss
    optimizer = optim.SGD(
        net.parameters(),
        lr=config["lr"],  # learning rate (tunable)
        momentum=0.9,  # helps accelerate gradients
        weight_decay=5e-5,  # L2 regularization
    )

    # --- Resume from checkpoint (if available) ---
    # This allows interrupted or failed trials to pick up from the last saved state.
    if tune.get_checkpoint():
        loaded_checkpoint = tune.get_checkpoint()
        with loaded_checkpoint.as_directory() as loaded_checkpoint_dir:
            model_state, optimizer_state = torch.load(
                os.path.join(loaded_checkpoint_dir, "checkpoint.pt")
            )
            net.load_state_dict(model_state)  # restore model weights
            optimizer.load_state_dict(optimizer_state)  # restore optimizer state

    # --- Data setup ---
    # Use synthetic data for quick smoke tests, otherwise load full CIFAR-10.
    if config["smoke_test"]:
        trainset, _ = load_test_data()
    else:
        trainset, _ = load_data()

    # Create train/validation DataLoaders
    train_loader, val_loader = create_dataloaders(
        trainset,
        config["batch_size"],  # tunable batch size
        num_workers=0 if config["smoke_test"] else 8,  # no workers for synthetic data
    )

    # --- Training loop ---
    for epoch in range(config["max_num_epochs"]):  # loop over epochs
        net.train()  # set model to training mode
        running_loss = 0.0

        # Iterate over training batches
        for inputs, labels in train_loader:
            inputs, labels = inputs.to(device), labels.to(device)

            # forward, backward, and optimize
            optimizer.zero_grad()  # reset gradients
            outputs = net(inputs)  # forward pass
            loss = criterion(outputs, labels)  # compute loss
            loss.backward()  # backpropagation
            optimizer.step()  # update weights

            running_loss += loss.item()

        # --- Validation loop ---
        net.eval()  # set model to eval mode
        val_loss = 0.0
        correct = total = 0
        with torch.no_grad():  # no gradients during evaluation
            for inputs, labels in val_loader:
                inputs, labels = inputs.to(device), labels.to(device)
                outputs = net(inputs)
                val_loss += criterion(outputs, labels).item()

                # Compute classification accuracy
                _, predicted = outputs.max(1)  # predicted class = argmax
                total += labels.size(0)
                correct += predicted.eq(labels).sum().item()

        # --- Report metrics to Ray Tune ---
        metrics = {
            "loss": val_loss / len(val_loader),  # average validation loss
            "accuracy": correct / total,  # validation accuracy
        }

        # --- Save checkpoint ---
        # Store model and optimizer state so trial can resume later if needed.
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            path = os.path.join(temp_checkpoint_dir, "checkpoint.pt")
            torch.save((net.state_dict(), optimizer.state_dict()), path)
            checkpoint = tune.Checkpoint.from_directory(temp_checkpoint_dir)

            # Report both metrics and checkpoint to Ray Tune
            tune.report(metrics, checkpoint=checkpoint)

    print("Finished Training!")  # Final message at end of training


# 06. Evaluate the best model


def test_best_model(best_result, smoke_test=False):
    """
    Evaluate the best model found during Ray Tune search on the test set.
    Restores the trained weights from the best trial's checkpoint and
    computes classification accuracy.
    """

    # --- Rebuild the best model architecture ---
    # Use the trial’s hyperparameters (hidden layer sizes l1 and l2)
    best_trained_model = Net(best_result.config["l1"], best_result.config["l2"])
    device = best_result.config["device"]

    # If running on GPU(s), wrap the model in DataParallel for multi-GPU support
    if device == "cuda":
        best_trained_model = nn.DataParallel(best_trained_model)
    best_trained_model.to(device)

    # --- Load weights from checkpoint ---
    # Convert checkpoint object to a directory, then restore model state_dict
    checkpoint_path = os.path.join(
        best_result.checkpoint.to_directory(), "checkpoint.pt"
    )
    model_state, _optimizer_state = torch.load(
        checkpoint_path
    )  # optimizer state not needed here
    best_trained_model.load_state_dict(model_state)

    # --- Select test dataset ---
    # Use synthetic FakeData if in smoke test mode, otherwise use real CIFAR-10 test set
    if smoke_test:
        _trainset, testset = load_test_data()
    else:
        _trainset, testset = load_data()

    # --- Prepare DataLoader for evaluation ---
    # Small testing batch size (4) is fine since evaluation isn’t performance-critical
    testloader = torch.utils.data.DataLoader(
        testset, batch_size=4, shuffle=False, num_workers=2
    )

    # --- Run evaluation ---
    correct = 0
    total = 0
    with torch.no_grad():  # disable gradients for faster inference
        for data in testloader:
            images, labels = data
            images, labels = images.to(device), labels.to(device)

            # Forward pass through trained model
            outputs = best_trained_model(images)

            # Get predicted class = index of max logit
            _, predicted = outputs.max(1)

            # Update totals for accuracy calculation
            total += labels.size(0)
            correct += predicted.eq(labels).sum().item()

    # --- Print final accuracy ---
    print(f"Best trial test set accuracy: {correct / total}")


# 07. Smoke test flag

# Set this to True for a smoke test that runs with a small synthetic dataset.
SMOKE_TEST = False

# 08. Define the hyperparameter search space and configuration

config = {
    "l1": tune.sample_from(
        lambda _: 2 ** np.random.randint(2, 9)
    ),  # size of 1st FC layer
    "l2": tune.sample_from(
        lambda _: 2 ** np.random.randint(2, 9)
    ),  # size of 2nd FC layer
    "lr": tune.loguniform(1e-4, 1e-1),  # learning rate
    "batch_size": tune.choice([2, 4, 8, 16]),  # training batch size
    "smoke_test": SMOKE_TEST,  # toggle for FakeData vs CIFAR-10
    "num_trials": 10 if not SMOKE_TEST else 2,  # number of hyperparam trials
    "max_num_epochs": 10 if not SMOKE_TEST else 2,  # training epochs per trial
    "device": "cuda" if torch.cuda.is_available() else "cpu",  # use GPU if available
}

# 09. Run hyperparameter tuning with Ray Tune


def main(config, gpus_per_trial=1):
    """
    Run Ray Tune hyperparameter search on CIFAR-10 with the given config.
    Uses ASHAScheduler for early stopping and reports the best trial result.
    """

    # --- Define scheduler ---
    # ASHAScheduler prunes bad trials early and promotes promising ones.
    scheduler = ASHAScheduler(
        time_attr="training_iteration",  # metric for progress = epoch count
        max_t=config["max_num_epochs"],  # maximum epochs per trial
        grace_period=1,  # min epochs before pruning is allowed
        reduction_factor=2,  # at each rung, keep ~1/2 of trials
    )

    # --- Define Ray Tune tuner ---
    tuner = tune.Tuner(
        # Wrap training function and specify trial resources
        tune.with_resources(
            tune.with_parameters(train_cifar),  # training loop
            resources={"cpu": 2, "gpu": gpus_per_trial},  # per-trial resources
        ),
        tune_config=tune.TuneConfig(
            metric="loss",  # optimize validation loss
            mode="min",  # minimize the metric
            scheduler=scheduler,  # use ASHA for early stopping
            num_samples=config["num_trials"],  # number of hyperparam trials
        ),
        run_config=RunConfig(
            name="cifar10_tune_demo",  # experiment name
            storage_path="/mnt/cluster_storage/ray-results",  # save results here
        ),
        param_space=config,  # hyperparameter search space
    )

    # --- Execute trials ---
    results = tuner.fit()  # launch tuning job

    # --- Retrieve best result ---
    best_result = results.get_best_result("loss", "min")  # lowest validation loss

    # --- Print summary of best trial ---
    print(f"Best trial config: {best_result.config}")
    print(f"Best trial final validation loss: {best_result.metrics['loss']}")
    print(f"Best trial final validation accuracy: {best_result.metrics['accuracy']}")

    # --- Evaluate best model on test set ---
    test_best_model(best_result, smoke_test=config["smoke_test"])

    return results, best_result


# --- Run main entry point ---
# Use 1 GPU per trial if available, otherwise run on CPU only
results, best_result = main(
    config, gpus_per_trial=1 if torch.cuda.is_available() else 0
)

# 10. Analyze and visualize tuning results

# Convert all trial results into a DataFrame
df = results.get_dataframe()

# Show the top 5 trials by validation accuracy
top5 = df.sort_values("accuracy", ascending=False).head(5)

# Plot learning rate versus validation accuracy
plt.figure(figsize=(6, 4))
plt.scatter(df["config/lr"], df["accuracy"], alpha=0.7)
plt.xscale("log")
plt.xlabel("Learning Rate")
plt.ylabel("Validation Accuracy")
plt.title("Learning Rate vs Accuracy")
plt.grid(True)
plt.show()

# Plot batch size versus validation accuracy
plt.figure(figsize=(6, 4))
plt.scatter(df["config/batch_size"], df["accuracy"], alpha=0.7)
plt.xlabel("Batch Size")
plt.ylabel("Validation Accuracy")
plt.title("Batch Size vs Accuracy")
plt.grid(True)
plt.show()

# 11. Plot learning curves across all trials

# Expect: results, best_result already defined from:
# results, best_result = main(config, gpus_per_trial=1 if torch.cuda.is_available() else 0)

import matplotlib.pyplot as plt
import pandas as pd

fig, axes = plt.subplots(2, 1, figsize=(8, 8), sharex=True)

# --- Plot validation loss ---
for res in results:
    hist = res.metrics_dataframe
    if hist is None or hist.empty:
        continue
    epoch = (
        hist["training_iteration"]
        if "training_iteration" in hist
        else pd.Series(range(1, len(hist) + 1))
    )
    axes[0].plot(epoch, hist["loss"], color="blue", alpha=0.15)

best_hist = best_result.metrics_dataframe
epoch_best = (
    best_hist["training_iteration"]
    if "training_iteration" in best_hist
    else pd.Series(range(1, len(best_hist) + 1))
)
axes[0].plot(
    epoch_best,
    best_hist["loss"],
    marker="o",
    linewidth=2.5,
    color="blue",
    label="Best — Val Loss",
)

axes[0].set_ylabel("Validation Loss")
axes[0].set_title("All Trials (faded) + Best Trial (bold)")
axes[0].grid(True)
axes[0].legend()

# --- Plot validation accuracy ---
for res in results:
    hist = res.metrics_dataframe
    if hist is None or hist.empty:
        continue
    epoch = (
        hist["training_iteration"]
        if "training_iteration" in hist
        else pd.Series(range(1, len(hist) + 1))
    )
    axes[1].plot(epoch, hist["accuracy"], color="orange", alpha=0.15)

axes[1].plot(
    epoch_best,
    best_hist["accuracy"],
    marker="s",
    linewidth=2.5,
    color="orange",
    label="Best — Val Accuracy",
)

axes[1].set_xlabel("Epoch")
axes[1].set_ylabel("Validation Accuracy")
axes[1].grid(True)
axes[1].legend()

plt.tight_layout()
plt.show()

# 12. Cleanup cluster storage

# Paths you used in the script
paths_to_clean = [
    "/mnt/cluster_storage/cifar10",  # dataset
    "/mnt/cluster_storage/ray-results",  # Tune results & checkpoints
]

for p in paths_to_clean:
    if os.path.exists(p):
        print(f"Removing {p} ...")
        shutil.rmtree(p, ignore_errors=True)
    else:
        print(f"{p} does not exist, skipping.")

print("Cleanup complete ✅")
