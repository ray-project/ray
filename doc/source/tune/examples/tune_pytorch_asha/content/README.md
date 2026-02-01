<!--
Do not modify this README. This file is a copy of the notebook and is not used to display the content.
Modify tune_pytorch_asha.ipynb instead, then regenerate this file with:
jupyter nbconvert "tune_pytorch_asha.ipynb" --to markdown --output "README.md"
-->

# Hyperparameter tuning with Ray Tune and PyTorch

<div align="left">
  <a target="_blank" href="https://console.anyscale.com/template-preview/tune_pytorch_asha">
    <img src="https://img.shields.io/badge/🚀 Run_on-Anyscale-9hf">
  </a>&nbsp;
  <a href="https://github.com/ray-project/ray/tree/master/doc/source/tune/examples/tune_pytorch_asha" role="button">
    <img src="https://img.shields.io/static/v1?label=&message=View%20On%20GitHub&color=586069&logo=github&labelColor=2f363d">
  </a>&nbsp;
</div>

This notebook runs **hyperparameter tuning experiments** on a PyTorch CNN using **Ray Tune**, a scalable library for experiment management and search. The workload runs on the driver, but it sets the stage for the next tutorial where you combine **Ray Tune with Ray Train** for distributed training.  

## Learning objectives  
* How to define a **search space** for model hyperparameters using Ray Tune's sampling APIs.  
* How to run **multiple training trials in parallel** with Ray's orchestration.  
* How to add **checkpointing and resume** logic to your training function with `tune.get_checkpoint()`.  
* How to use **schedulers like Asynchronous Successive Halving (ASHA)** to early stop under-performing trials and speed up search.  
* How to retrieve and test the **best model checkpoint** at the end of tuning.  

## What problem are you solving? (hyperparameter tuning for image classification)  

You’re training a **convolutional neural network (CNN)** to classify images from the **Canadian Institute For Advanced Research (CIFAR)-10 dataset**, which contains 60,000 color images across 10 categories (airplane, car, bird, cat, etc.).  

Instead of fixing your model’s architecture and training parameters manually, you use **hyperparameter tuning** to automatically search over different configurations—like hidden layer sizes, learning rate, and batch size—and find the best-performing model.  

This approach helps you answer key questions:  
- How large should the fully connected layers be?  
- What learning rate leads to the fastest convergence?  
- Which batch size balances training speed and generalization?  

By running many experiments in parallel and comparing validation accuracy and loss, you discover a set of hyperparameters that gives you the **highest test accuracy** for CIFAR-10 classification.  

### A deeper mathematical view  

You train the CNN to minimize the **cross-entropy loss** between predicted probabilities $\hat{y}$ and ground-truth labels $y$:  

$$
\mathcal{L}(\theta) = - \frac{1}{N} \sum_{i=1}^N \sum_{c=1}^{10} \mathbf{1}[y_i = c] \cdot \log \hat{y}_{i,c}(\theta)
$$  

where $\theta$ are the model parameters, $N$ is the batch size, and $\hat{y}_{i,c}$ is the predicted probability of class $c$ for image $i$.  

Hyperparameter tuning searches over a configuration space:  

$$
\mathcal{H} = \{ l_1, l_2, \eta, B \}
$$  

- $l_1, l_2$: hidden layer widths in the fully connected layers  
- $\eta$: learning rate  
- $B$: batch size  

The goal is to find:  

$$
h^* = \arg\min_{h \in \mathcal{H}} \ \mathbb{E}_{(x,y) \sim \mathcal{D}_{\text{val}}}\big[\mathcal{L}(f(x; \theta_h), y)\big]
$$  

In other words, choose the hyperparameter setting $h^*$ that minimizes validation loss on unseen data.  

By leveraging **Ray Tune’s search algorithms and schedulers**, you efficiently explore $\mathcal{H}$ in parallel, prune poor configurations early, and converge to the hyperparameters that yield the **best generalization on CIFAR-10**.  

### A closer look at ASHA  

When running many hyperparameter trials, it’s wasteful to let every configuration train for the full number of epochs. The **Asynchronous Successive Halving Algorithm (ASHA)** speeds things up by **early stopping** under-performing trials while letting promising ones continue.  

Conceptually, ASHA divides training into **rungs**: after a certain number of epochs $t_r$, the top fraction $1 / \eta$ of trials (where $\eta$ is the *reduction factor*) advance to the next rung, while the algorithm prunes poor performers immediately.   

Formally, if you start with $n$ trials, then at rung $r$:  

$$
n_{r} = \left\lceil \frac{n}{\eta^r} \right\rceil
$$  

trials survive to the next stage.  

Unlike classic **Successive Halving**, ASHA runs in an **asynchronous** mode: trials don’t wait for each other to finish before making pruning decisions. This makes it ideal for distributed environments like Ray, where trial run times can vary and resources become free at different times.  

The key parameters you configure in Ray Tune are:  
- **`max_t` ($T$)**: maximum number of epochs a trial can run  
- **`reduction_factor` ($\eta$)**: how aggressively the algorithm prunes trials    
- **`grace_period`**: the minimum number of epochs the algorithm guarantees for every trial   

This balance between **exploration** (trying many hyperparameter combinations) and **exploitation** (training the best ones longer) is what makes ASHA so effective for scalable tuning.  

## 1. Imports  
Start by importing all the libraries you need for the rest of the notebook. These include standard utilities like `os`, `tempfile`, and `shutil` for filesystem management and cleanup, as well as scientific libraries like **NumPy** for random sampling in the hyperparameter search space.  

Bring in **PyTorch** modules for building and training the CNN model:  
- `torch`, `nn`, and `optim` for model definition and optimization  
- `F` for activation functions and loss operations  
- `torchvision` and `transforms` for dataset loading and preprocessing  
- `random_split` for creating train/validation splits  

Add **FileLock** to prevent race conditions when downloading CIFAR-10 data in parallel across trials.  

Finally, import everything needed for **hyperparameter tuning with Ray**:  
- `tune` for defining and executing trials, checkpointing, and metric reporting  
- `ASHAScheduler` for early stopping under-performing trials  
- `RunConfig` for experiment-level configuration like storage paths and naming  


```python
# 00. Runtime setup — install deps and set env vars
import os, sys, subprocess

# Non-secret env var 
os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

# Install Python dependencies 
subprocess.check_call([
    sys.executable, "-m", "pip", "install", "--no-cache-dir",
    "torch==2.8.0",
    "torchvision==0.23.0",
    "matplotlib==3.10.6",
    "pyarrow==14.0.2",
])
```


```python
# 01. Imports

# --- Standard libraries ---
import os              # Filesystem utilities (paths, directories)
import tempfile        # Temporary directories for checkpoints
import shutil          # Cleanup of files and directories

# --- Analytics / plotting ---
import pandas as pd                    # Converting output to dataframe for plotting
import matplotlib.pyplot as plt        # For generating plots 

# --- Scientific computing ---
import numpy as np     # Numerical operations, used for random sampling in search space

# --- PyTorch (deep learning) ---
import torch
import torch.nn as nn                  # Neural network modules (layers, models)
import torch.nn.functional as F        # Functional API for activations/losses
import torch.optim as optim            # Optimizers (e.g., SGD, Adam)
import torchvision                     # Popular vision datasets and pretrained models
import torchvision.transforms as transforms  # Image preprocessing pipelines
from torch.utils.data import random_split    # Train/validation dataset splitting

# --- Utilities ---
from filelock import FileLock          # Prevents race conditions when multiple workers download CIFAR-10

# --- Ray (tuning and orchestration) ---
from ray import train, tune            # Core APIs for metric reporting and trial execution
from ray.tune.schedulers import ASHAScheduler  # Asynchronous HyperBand for early stopping
from ray.air.config import RunConfig   # Configure experiment metadata (name, storage, logging)
```

## 2. Load and prepare CIFAR-10 data  

Start by creating helper functions to load and preprocess the **CIFAR-10 dataset**.  

- **`load_data`**:  
  - Applies a standard normalization transform with per-channel mean and standard deviation.  
  - Uses a `FileLock` so multiple Ray Tune workers can safely download the dataset without corrupting files.  
  - Returns both the training and test datasets.  

- **`create_dataloaders`**:  
  - Splits the training set into an 80/20 train/validation split using `random_split`.  
  - Wraps each subset into a `DataLoader`, controlling `batch_size`, shuffling, and parallelism with `num_workers`.  
  - Returns a **train loader** (shuffled) and a **validation loader** (deterministic).  

This setup ensures consistent preprocessing and safe parallel dataset access across trials.  


```python
# 02. Load and prepare CIFAR-10 data  

def load_data(data_dir="/mnt/cluster_storage/cifar10"):
    """
    Download and load the CIFAR-10 dataset with standard preprocessing.
    Returns the full training set and the test set.
    """
    # Define preprocessing: convert to tensor and normalize channels
    transform = transforms.Compose([
        transforms.ToTensor(),  # Convert images to PyTorch tensors [0,1]
        transforms.Normalize(    # Normalize with dataset mean & std (per channel)
            (0.4914, 0.4822, 0.4465),   # mean (R, G, B)
            (0.2023, 0.1994, 0.2010)    # std (R, G, B)
        ),
    ])

    # FileLock ensures that multiple parallel workers downloading CIFAR-10
    # don't interfere with each other (prevents race conditions).
    with FileLock(os.path.expanduser("~/.data.lock")):
        trainset = torchvision.datasets.CIFAR10(
            root=data_dir, train=True, download=True, transform=transform)

        testset = torchvision.datasets.CIFAR10(
            root=data_dir, train=False, download=True, transform=transform)

    return trainset, testset

def create_dataloaders(trainset, batch_size, num_workers=8):
    """
    Split the CIFAR-10 training set into train/validation subsets,
    and wrap them in DataLoader objects.
    """
    # Compute split sizes: 80% train, 20% validation
    train_size = int(len(trainset) * 0.8)

    # Randomly partition the dataset into train/val subsets
    train_subset, val_subset = random_split(trainset, [train_size, len(trainset) - train_size])

    # Training loader: shuffle for stochastic gradient descent
    train_loader = torch.utils.data.DataLoader(
        train_subset,
        batch_size=batch_size, 
        shuffle=True,
        num_workers=num_workers
    )

    # Validation loader: no shuffle (deterministic evaluation)
    val_loader = torch.utils.data.DataLoader(
        val_subset,
        batch_size=batch_size,
        shuffle=False, 
        num_workers=num_workers
    )
    return train_loader, val_loader
```

## 3. Load synthetic test data  

Define a helper function to quickly generate **synthetic datasets** for debugging and smoke tests.  

- **`torchvision.datasets.FakeData`** creates random tensors shaped like CIFAR-10 images `(3, 32, 32)` with 10 classes.  
- The function generates a small fake training set (128 samples) and a fake test set (16 samples).    
- This runs much faster than downloading CIFAR-10 and is useful for verifying that your code, model, and Ray Tune setup all work end-to-end before running full experiments.  

The function returns two datasets: a synthetic **train set** and a synthetic **test set**.  


```python
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
        size=128,                     # number of samples
        image_size=(3, 32, 32),       # match CIFAR-10 format
        num_classes=10,               # same number of categories as CIFAR-10
        transform=transforms.ToTensor()  # convert to PyTorch tensors
    )

    # Generate a smaller fake test set of 16 samples
    testset = torchvision.datasets.FakeData(
        size=16,
        image_size=(3, 32, 32),
        num_classes=10,
        transform=transforms.ToTensor()
    )
    
    # Return both sets so they can be wrapped into DataLoaders
    return trainset, testset
```

## 4. Define a CNN model  

Define a simple **convolutional neural network (CNN)** for classifying CIFAR-10 images. This architecture is intentionally lightweight, making it easy to train and fast to evaluate during hyperparameter search.  

- **Convolutional layers**:  
  - `conv1`: 3 input channels (Red/Green/Blue) for 6 feature maps  
  - `conv2`: 6 feature maps for 16 feature maps  
  - Each followed by ReLU activation and 2×2 max pooling  

- **Fully connected layers**:  
  - `fc1`: flattens the feature maps into a vector and maps to a hidden layer of size `l1`  
  - `fc2`: maps to another hidden layer of size `l2`  
  - `fc3`: final classification layer mapping to 10 output classes (CIFAR-10 categories)  

- **Forward pass**:  
  - Applies convolution, ReLU, then pooling twice  
  - Flattens feature maps to a vector  
  - Passes through two ReLU-activated fully connected layers  
  - Outputs logits for 10 classes  

This network provides a baseline model for tuning, and the network treats `l1` and `l2` as hyperparameters to explore different hidden layer sizes.    


```python
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
```

## 5. Define a training function  

Define the **training loop** that Ray Tune executes for each trial. This function encapsulates model setup, checkpoint handling, training, validation, and metric reporting.  

- **Model setup**:  
  - Builds a new `Net` model with tunable hidden sizes `l1` and `l2`.  
  - Moves the model to the appropriate device (CPU or GPU). If multiple GPUs are available, wraps the model in `nn.DataParallel`.  

- **Loss and optimizer**:  
  - Uses **cross-entropy loss** for multi-class classification.  
  - Optimizes with **SGD** including momentum and weight decay.  

- **Checkpoint loading**:  
  - If a checkpoint exists from a previous run, restores both the model and optimizer state using `tune.get_checkpoint()`.  
  - This enables **fault tolerance** and resuming from prior progress.  

- **Data setup**:  
  - Loads either the **real CIFAR-10 dataset** or a **synthetic smoke-test dataset**, based on the `smoke_test` flag.  
  - Wraps data into train/validation `DataLoader`s with configurable batch size and number of workers.  

- **Training loop**:  
  - Runs for `max_num_epochs`, performing forward, backward, and optimization passes.  
  - Computes average training loss across batches.  
  - After each epoch, evaluates on the validation set and calculates both **validation loss** and **accuracy**.  

- **Metric reporting and checkpointing**:  
  - Packages metrics into a dictionary (`loss`, `accuracy`) and reports them to Ray Tune with `tune.report()`.  
  - Saves model and optimizer state into a temporary directory checkpoint, which Ray manages automatically.  

At the end, the function prints `"Finished Training!"` to signal completion of the trial.  


```python
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
    criterion = nn.CrossEntropyLoss() # standard classification loss
    optimizer = optim.SGD(
        net.parameters(),
        lr=config["lr"],          # learning rate (tunable)
        momentum=0.9,             # helps accelerate gradients
        weight_decay=5e-5         # L2 regularization
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
        config["batch_size"],                         # tunable batch size
        num_workers=0 if config["smoke_test"] else 8  # no workers for synthetic data
    )

    # --- Training loop ---
    for epoch in range(config["max_num_epochs"]):  # loop over epochs
        net.train()  # set model to training mode
        running_loss = 0.0

        # Iterate over training batches
        for inputs, labels in train_loader:
            inputs, labels = inputs.to(device), labels.to(device)

            # forward, backward, and optimize
            optimizer.zero_grad()       # reset gradients
            outputs = net(inputs)       # forward pass
            loss = criterion(outputs, labels)  # compute loss
            loss.backward()             # backpropagation
            optimizer.step()            # update weights
            
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
            "loss": val_loss / len(val_loader),     # average validation loss
            "accuracy": correct / total,            # validation accuracy
        }

        # --- Save checkpoint ---
        # Store model and optimizer state so trial can resume later if needed.
        with tempfile.TemporaryDirectory() as temp_checkpoint_dir:
            path = os.path.join(temp_checkpoint_dir, "checkpoint.pt")
            torch.save(
                (net.state_dict(), optimizer.state_dict()), path
            )
            checkpoint = tune.Checkpoint.from_directory(temp_checkpoint_dir)

             # Report both metrics and checkpoint to Ray Tune
            tune.report(metrics, checkpoint=checkpoint)
            
    print("Finished Training!")  # Final message at end of training
```

## 6. Evaluate the best model  

After hyperparameter tuning finishes, verify the performance of the **best trial** on the held-out test set.  

- **Model reconstruction**:  
  - Rebuilds the CNN (`Net`) using the hyperparameters (`l1`, `l2`) from the best configuration.  
  - Moves the model to the same device (CPU or GPU) used during training.  
  - Restores trained weights from the best trial’s final checkpoint.  

- **Dataset selection**:  
  - If running a quick smoke test, loads the synthetic `FakeData` test set.  
  - Otherwise, loads the real CIFAR-10 test set.  

- **Evaluation loop**:  
  - Wraps the test set in a DataLoader with small batches (`batch_size=4`).  
  - Iterates over the test data without gradients (`torch.no_grad()`).  
  - Computes predicted labels with `argmax` on the model outputs.  
  - Accumulates the total number of correct predictions.  

- **Final result**:  
  - Prints the **test set accuracy** of the best trial, providing an unbiased estimate of model generalization after tuning.  


```python
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
    checkpoint_path = os.path.join(best_result.checkpoint.to_directory(), "checkpoint.pt")
    model_state, _optimizer_state = torch.load(checkpoint_path)  # optimizer state not needed here
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
```

## 7. Smoke test flag  

Define a flag to control whether the tutorial runs on the **real CIFAR-10 dataset** or a **small synthetic dataset** for quick debugging.  

- `SMOKE_TEST = True`:  
  - Uses `FakeData` with a tiny dataset size.  
  - Runs only a couple of epochs and trials.  
  - Useful to verify the code and Ray Tune setup without waiting for full training.  

- `SMOKE_TEST = False`:  
  - Uses the full CIFAR-10 dataset.  
  - Runs the complete training and hyperparameter search.  
  - Provides realistic results and accuracy metrics.  

This toggle lets you switch between **fast iteration** (for development) and **full-scale experiments** (for real results).  


```python
# 07. Smoke test flag

# Set this to True for a smoke test that runs with a small synthetic dataset.
SMOKE_TEST = False
```

## 8. Define the hyperparameter search space and configuration  

Define the **search space** and runtime parameters that Ray Tune explores during hyperparameter tuning.  

- **Model architecture**:  
  - `l1`: size of the first fully connected layer, sampled as a power of 2 between 4 and 256.  
  - `l2`: size of the second fully connected layer, sampled similarly.  

- **Optimization**:  
  - `lr`: learning rate, sampled from a log-uniform distribution between $10^{-4}$ and $10^{-1}$.  
  - `batch_size`: chosen from {2, 4, 8, 16}.  

- **Runtime configuration**:  
  - `smoke_test`: whether to run with synthetic `FakeData` or full CIFAR-10.  
  - `num_trials`: number of hyperparameter configurations to test (10 by default, 2 in smoke test).  
  - `max_num_epochs`: number of epochs per trial (10 by default, 2 in smoke test).  
  - `device`: automatically set to `"cuda"` if a GPU is available, otherwise `"cpu"`.  

This configuration dictionary balances **search diversity** (random sampling of layer sizes and learning rates) with **practical runtime controls** for development vs. full experiments.  


```python
# 08. Define the hyperparameter search space and configuration

config = {
    "l1": tune.sample_from(lambda _: 2**np.random.randint(2, 9)),   # size of 1st FC layer
    "l2": tune.sample_from(lambda _: 2**np.random.randint(2, 9)),   # size of 2nd FC layer
    "lr": tune.loguniform(1e-4, 1e-1),                              # learning rate
    "batch_size": tune.choice([2, 4, 8, 16]),                       # training batch size
    "smoke_test": SMOKE_TEST,                                       # toggle for FakeData vs CIFAR-10
    "num_trials": 10 if not SMOKE_TEST else 2,                      # number of hyperparam trials
    "max_num_epochs": 10 if not SMOKE_TEST else 2,                  # training epochs per trial
    "device": "cuda" if torch.cuda.is_available() else "cpu",       # use GPU if available
}
```

## 9. Run hyperparameter tuning with Ray Tune  

Define the **main entry point** for running hyperparameter search with Ray Tune.  

- **Scheduler**:  
  - Uses `ASHAScheduler`, an asynchronous variant of Successive Halving, which prunes poorly performing trials early.  
  - Configured with:  
    - `time_attr="training_iteration"`: each epoch counts as one iteration.  
    - `max_t`: maximum number of epochs per trial.  
    - `grace_period=1`: ensures every trial runs at least one epoch before pruning.  
    - `reduction_factor=2`: halves the number of trials promoted at each rung.  

- **Tuner setup**:  
  - Wraps the `train_cifar` function with `tune.with_resources` to allocate **2 CPUs** and the specified number of GPUs per trial.  
  - `TuneConfig` specifies:  
    - Optimization target (`loss`, minimized).  
    - Scheduler (`ASHA`) for pruning.  
    - Number of samples (`num_trials`).  
  - `RunConfig` gives the experiment a name (`cifar10_tune_demo`) and a persistent storage path for results and checkpoints.  
  - `param_space=config` passes in the hyperparameter search space.  

- **Execution**:  
  - Runs all trials in parallel with `tuner.fit()`.  
  - Retrieves the **best result** by lowest validation loss.  
  - Prints the best configuration, final validation loss, and validation accuracy.  

- **Final test**:  
  - Calls `test_best_model` to evaluate the best checkpoint on the held-out test set.  

The final line calls `main(...)`, automatically choosing `gpus_per_trial=1` if a GPU is available, otherwise defaulting to CPU.  


```python
# 09. Run hyperparameter tuning with Ray Tune 

def main(config, gpus_per_trial=1):
    """
    Run Ray Tune hyperparameter search on CIFAR-10 with the given config.
    Uses ASHAScheduler for early stopping and reports the best trial result.
    """

    # --- Define scheduler ---
    # ASHAScheduler prunes bad trials early and promotes promising ones.
    scheduler = ASHAScheduler(
        time_attr="training_iteration",      # metric for progress = epoch count
        max_t=config["max_num_epochs"],      # maximum epochs per trial
        grace_period=1,                      # min epochs before pruning is allowed
        reduction_factor=2                   # at each rung, keep ~1/2 of trials
    )
    
    # --- Define Ray Tune tuner ---
    tuner = tune.Tuner(
        # Wrap training function and specify trial resources
        tune.with_resources(
            tune.with_parameters(train_cifar),   # training loop
            resources={"cpu": 2, "gpu": gpus_per_trial}  # per-trial resources
        ),
        tune_config=tune.TuneConfig(
            metric="loss",                # optimize validation loss
            mode="min",                   # minimize the metric
            scheduler=scheduler,          # use ASHA for early stopping
            num_samples=config["num_trials"],  # number of hyperparam trials
        ),
        run_config=RunConfig(
            name="cifar10_tune_demo",     # experiment name
            storage_path="/mnt/cluster_storage/ray-results"  # save results here
        ),
        param_space=config,               # hyperparameter search space
    )

    # --- Execute trials ---
    results = tuner.fit()                 # launch tuning job
    
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
results, best_result = main(config, gpus_per_trial=1 if torch.cuda.is_available() else 0)
```

## 10. Analyze and visualize tuning results  

After tuning, it’s useful to dig into the results across trials to understand how different hyperparameters influenced performance.  

With Ray Tune, you can easily extract a summary of all trials and create simple plots:  

- **Trial summary table**: list each trial with its config (`l1`, `l2`, `lr`, `batch_size`) and final metrics (`val_loss`, `val_accuracy`).  
- **Hyperparameter vs. metric plots**: see trends, for example, how learning rate or batch size affects validation accuracy.  
- **Best trials comparison**: sort trials by validation accuracy and inspect the top-k configs.  

This analysis helps you go beyond “what’s the best config?” to **why** certain hyperparameters perform better. It’s also a sanity check to confirm ASHA pruned poor performers effectively.  


```python
# 10. Analyze and visualize tuning results 

# Convert all trial results into a DataFrame
df = results.get_dataframe()

# Show the top 5 trials by validation accuracy
top5 = df.sort_values("accuracy", ascending=False).head(5)
print("🔝 Top 5 Trials by Validation Accuracy:")
display(top5[["config/l1", "config/l2", "config/lr", "config/batch_size", "loss", "accuracy"]])

# Plot learning rate versus validation accuracy
plt.figure(figsize=(6,4))
plt.scatter(df["config/lr"], df["accuracy"], alpha=0.7)
plt.xscale("log")
plt.xlabel("Learning Rate")
plt.ylabel("Validation Accuracy")
plt.title("Learning Rate vs Accuracy")
plt.grid(True)
plt.show()

# Plot batch size versus validation accuracy
plt.figure(figsize=(6,4))
plt.scatter(df["config/batch_size"], df["accuracy"], alpha=0.7)
plt.xlabel("Batch Size")
plt.ylabel("Validation Accuracy")
plt.title("Batch Size vs Accuracy")
plt.grid(True)
plt.show()
```

## 11. Plot learning curves across all trials  

Visualize the **validation loss and accuracy** curves for all trials, with the best trial highlighted.  

- Uses the `metrics_dataframe` from Ray Tune to extract per-epoch metrics.  
- Treats `training_iteration` as the epoch index because the code calls `tune.report()` once per epoch.  
- Plots all trials in the background with low opacity (**blue for loss, orange for accuracy**) so you can see overall trends.  
- Overlays the **best trial** with bold, opaque lines and markers, making it easy to distinguish from the rest.  

In **smoke test mode**, you only see two points because each trial runs for only 2 epochs. In a full CIFAR-10 run, this plot shows richer training curves across all epochs, letting you compare convergence patterns between hyperparameter configurations.  



```python
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
    epoch = hist["training_iteration"] if "training_iteration" in hist else pd.Series(range(1, len(hist) + 1))
    axes[0].plot(epoch, hist["loss"], color="blue", alpha=0.15)

best_hist = best_result.metrics_dataframe
epoch_best = best_hist["training_iteration"] if "training_iteration" in best_hist else pd.Series(range(1, len(best_hist) + 1))
axes[0].plot(epoch_best, best_hist["loss"], marker="o", linewidth=2.5, color="blue", label="Best — Val Loss")

axes[0].set_ylabel("Validation Loss")
axes[0].set_title("All Trials (faded) + Best Trial (bold)")
axes[0].grid(True)
axes[0].legend()

# --- Plot validation accuracy ---
for res in results:
    hist = res.metrics_dataframe
    if hist is None or hist.empty:
        continue
    epoch = hist["training_iteration"] if "training_iteration" in hist else pd.Series(range(1, len(hist) + 1))
    axes[1].plot(epoch, hist["accuracy"], color="orange", alpha=0.15)

axes[1].plot(epoch_best, best_hist["accuracy"], marker="s", linewidth=2.5, color="orange", label="Best — Val Accuracy")

axes[1].set_xlabel("Epoch")
axes[1].set_ylabel("Validation Accuracy")
axes[1].grid(True)
axes[1].legend()

plt.tight_layout()
plt.show()
```

## 12. Clean up cluster storage  

After finishing the tutorial, it’s good practice to clean up the files you generated to free space on shared storage.  

- Defines a list of paths to remove:  
  - `/mnt/cluster_storage/cifar10`: cached CIFAR-10 dataset.  
  - `/mnt/cluster_storage/ray-results`: Ray Tune results and checkpoints.  

- Iterates over each path:  
  - If the path exists, deletes it recursively with `shutil.rmtree()`.  
  - If the path doesn’t exist, prints a message and skips it.  

At the end, prints a confirmation that cleanup is complete.  

This step ensures your cluster stays tidy, especially if you’ve run multiple tuning jobs or smoke tests.  



```python
# 12. Cleanup cluster storage  

# Paths you used in the script
paths_to_clean = [
    "/mnt/cluster_storage/cifar10",       # dataset
    "/mnt/cluster_storage/ray-results",   # Tune results & checkpoints
]

for p in paths_to_clean:
    if os.path.exists(p):
        print(f"Removing {p} ...")
        shutil.rmtree(p, ignore_errors=True)
    else:
        print(f"{p} does not exist, skipping.")

print("Cleanup complete ✅")
```

## Wrap up and next steps  

In this tutorial, you used **Ray Tune with PyTorch** to run hyperparameter search experiments on CIFAR-10, from defining a search space and training loop, to running trials with ASHA, checkpointing progress, and evaluating the best model on a test set. You even visualized learning curves across all trials to see how different configurations performed.  

You should now feel confident:  

* Defining **hyperparameter search spaces** with Ray Tune's sampling APIs.  
* Writing a **training loop with checkpointing and resume** logic.  
* Running multiple **trials in parallel with `ASHAScheduler`** to prune under-performers.  
* Retrieving the **best model checkpoint** and evaluating on a held-out test set.  
* Visualizing and analyzing **trial results** across hyperparameter configurations.  

---

## Next steps  

The following are a few directions to extend or adapt this workload:  

1. **Stronger models**  
   * Replace the small CNN with **ResNet** or another modern architecture from `torchvision.models`.  
   * Add data augmentation (random crops, flips) to improve CIFAR-10 accuracy.  

2. **Distributed training**  
   * Combine Ray Tune with **Ray Train** so each trial runs on multiple GPUs or nodes.  
   * Scale up trials to larger datasets without changing your training code.  

3. **Alternative search algorithms**  
   * Swap ASHA for **Bayesian optimization** or **Population Based Training (PBT)**.  
   * Compare efficiency and results.  

4. **Experiment tracking**  
   * Integrate with **TensorBoard, Weights & Biases, or MLflow** using Ray callbacks.  
   * Track and visualize experiments at scale.  

5. **Production readiness**  
   * Export the best model and serve it with **Ray Serve** for real-time inference.  
   * Wrap the tuning logic into a **Ray job** and schedule it on Anyscale.  

This tutorial gives you a solid foundation for running hyperparameter tuning at scale. With Ray and Anyscale, you can seamlessly move from single-node tuning to multi-node distributed training and beyond—without rewriting your PyTorch model code.  



