import argparse
from typing import Dict

import torch
from ray.util.sgd.v2.trainer import Trainer
from torch import nn
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DataLoader, DistributedSampler
from torchvision import datasets
from torchvision.transforms import ToTensor

# Download training data from open datasets.
training_data = datasets.FashionMNIST(
    root="~/data",
    train=True,
    download=True,
    transform=ToTensor(),
)

# Download test data from open datasets.
test_data = datasets.FashionMNIST(
    root="~/data",
    train=False,
    download=True,
    transform=ToTensor(),
)


# Define model
class NeuralNetwork(nn.Module):
    def __init__(self):
        super(NeuralNetwork, self).__init__()
        self.flatten = nn.Flatten()
        self.linear_relu_stack = nn.Sequential(
            nn.Linear(28 * 28, 512), nn.ReLU(), nn.Linear(512, 512), nn.ReLU(),
            nn.Linear(512, 10), nn.ReLU())

    def forward(self, x):
        x = self.flatten(x)
        logits = self.linear_relu_stack(x)
        return logits


def train(dataloader, model, loss_fn, optimizer, device):
    size = len(dataloader.dataset)
    for batch, (X, y) in enumerate(dataloader):
        X, y = X.to(device), y.to(device)

        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        if batch % 100 == 0:
            loss, current = loss.item(), batch * len(X)
            print(f"loss: {loss:>7f}  [{current:>5d}/{size:>5d}]")


def validate(dataloader, model, loss_fn, device):
    size = len(dataloader.dataset)
    num_batches = len(dataloader)
    model.eval()
    test_loss, correct = 0, 0
    with torch.no_grad():
        for X, y in dataloader:
            X, y = X.to(device), y.to(device)
            pred = model(X)
            test_loss += loss_fn(pred, y).item()
            correct += (pred.argmax(1) == y).type(torch.float).sum().item()
    test_loss /= num_batches
    correct /= size
    print(f"Test Error: \n "
          f"Accuracy: {(100 * correct):>0.1f}%, "
          f"Avg loss: {test_loss:>8f} \n")
    return test_loss


def train_func(config: Dict):
    batch_size = config["batch_size"]
    lr = config["lr"]
    epochs = config["epochs"]

    # Create data loaders.
    train_dataloader = DataLoader(
        training_data,
        batch_size=batch_size,
        sampler=DistributedSampler(training_data))
    test_dataloader = DataLoader(
        test_data,
        batch_size=batch_size,
        sampler=DistributedSampler(test_data))

    # Create model.
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model = NeuralNetwork()
    model = model.to(device)
    model = DistributedDataParallel(model)

    loss_fn = nn.CrossEntropyLoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    loss_results = []

    for _ in range(epochs):
        train(train_dataloader, model, loss_fn, optimizer, device)
        loss = validate(test_dataloader, model, loss_fn, device)
        loss_results.append(loss)

    return loss_results


def train_fashion_mnist(num_workers=1, use_gpu=False):
    trainer = Trainer(
        backend="torch",
        num_workers=num_workers,
        num_gpus_per_worker=int(use_gpu))
    trainer.start()
    result = trainer.run(
        train_func=train_func,
        config={
            "lr": 1e-3,
            "batch_size": 64,
            "epochs": 4
        })
    trainer.shutdown()
    print(f"Loss results: {result}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        help="the address to use for Ray")
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=1,
        help="Sets number of workers for training.")
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        default=False,
        help="Enables GPU training")
    parser.add_argument(
        "--tune", action="store_true", default=False, help="Tune training")
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.")

    args, _ = parser.parse_known_args()

    import ray

    if args.smoke_test:
        ray.init(num_cpus=2)
    else:
        ray.init(address=args.address)
    train_fashion_mnist(num_workers=args.num_workers, use_gpu=args.use_gpu)
