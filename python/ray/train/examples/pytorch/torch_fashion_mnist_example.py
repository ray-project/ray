from typing import Dict

import torch
from torch import nn
from torch.utils.data import DataLoader
from torchvision import datasets, transforms
from torchvision.transforms import ToTensor, Normalize
from tqdm import tqdm

import ray.train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


def build_dataset(batch_size):
    # Transform to normalize the input images
    transform = transforms.Compose([ToTensor(), Normalize((0.5,), (0.5,))])

    # Download training data from open datasets.
    training_data = datasets.FashionMNIST(
        root="~/data",
        train=True,
        download=True,
        transform=transform,
    )

    # Download test data from open datasets.
    test_data = datasets.FashionMNIST(
        root="~/data",
        train=False,
        download=True,
        transform=transform,
    )

    # Create data loaders.
    train_dataloader = DataLoader(training_data, batch_size=batch_size)
    test_dataloader = DataLoader(test_data, batch_size=batch_size)

    return train_dataloader, test_dataloader


# Model Definition
class NeuralNetwork(nn.Module):
    def __init__(self):
        super(NeuralNetwork, self).__init__()
        self.flatten = nn.Flatten()
        self.linear_relu_stack = nn.Sequential(
            nn.Linear(28 * 28, 512),
            nn.ReLU(),
            nn.Dropout(0.25),
            nn.Linear(512, 512),
            nn.ReLU(),
            nn.Dropout(0.25),
            nn.Linear(512, 10),
            nn.ReLU(),
        )

    def forward(self, x):
        x = self.flatten(x)
        logits = self.linear_relu_stack(x)
        return logits


def train_func_per_worker(config: Dict):
    lr = config["lr"]
    momentum = config["momentum"]
    epochs = config["epochs"]
    batch_size = config["batch_size_per_worker"]

    # Build datasets inside worker training function
    train_dataloader, test_dataloader = build_dataset(batch_size=batch_size)

    # [2] Prepare Dataloader for distributed training
    # ===============================================
    train_dataloader = ray.train.torch.prepare_data_loader(train_dataloader)
    test_dataloader = ray.train.torch.prepare_data_loader(test_dataloader)

    model = NeuralNetwork()

    # [3] Prepare and wrap your model with DistributedDataParallel
    # ============================================================
    model = ray.train.torch.prepare_model(model)

    loss_fn = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr, momentum=momentum)

    # Model training loop
    for epoch in range(epochs):
        model.train()
        for X, y in tqdm(train_dataloader, desc=f"Train Epoch {epoch}"):
            pred = model(X)
            loss = loss_fn(pred, y)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        model.eval()
        test_loss, num_correct, num_total = 0, 0, 0
        with torch.no_grad():
            for X, y in tqdm(test_dataloader, desc=f"Test Epoch {epoch}"):
                pred = model(X)
                loss = loss_fn(pred, y)

                test_loss += loss.item()
                num_total += y.shape[0]
                num_correct += (pred.argmax(1) == y).sum().item()

        test_loss /= len(test_dataloader)
        accuracy = num_correct / num_total

        # [4] Report metrics to Ray Train
        # ==============================
        ray.train.report(metrics={"test_loss": test_loss, "accuracy": accuracy})


def train_fashion_mnist(num_workers=2, use_gpu=False):
    global_batch_size = 32

    train_config = {
        "lr": 1e-3,
        "momentum": 0.9,
        "epochs": 10,
        "batch_size_per_worker": global_batch_size // num_workers,
    }

    # Configrue computation resources
    scaling_config = ScalingConfig(num_workers=num_workers, use_gpu=use_gpu)

    # Initialize a Ray TorchTrainer
    trainer = TorchTrainer(
        train_loop_per_worker=train_func_per_worker,
        train_loop_config=train_config,
        scaling_config=scaling_config,
    )
    result = trainer.fit()
    print(f"Training result: {result}")


if __name__ == "__main__":
    train_fashion_mnist(num_workers=4, use_gpu=True)
