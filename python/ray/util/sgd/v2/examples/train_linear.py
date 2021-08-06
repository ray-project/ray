import argparse

import numpy as np
import torch
import torch.nn as nn
from ray.util.sgd.v2 import Trainer, TorchConfig
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DistributedSampler


class LinearDataset(torch.utils.data.Dataset):
    """y = a * x + b"""

    def __init__(self, a, b, size=1000):
        x = np.arange(0, 10, 10 / size, dtype=np.float32)
        self.x = torch.from_numpy(x)
        self.y = torch.from_numpy(a * x + b)

    def __getitem__(self, index):
        return self.x[index, None], self.y[index, None]

    def __len__(self):
        return len(self.x)


def train(dataloader, model, loss_fn, optimizer):
    for X, y in dataloader:
        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()


def validate(dataloader, model, loss_fn):
    num_batches = len(dataloader)
    model.eval()
    loss = 0
    with torch.no_grad():
        for X, y in dataloader:
            pred = model(X)
            loss += loss_fn(pred, y).item()
    loss /= num_batches
    result = {"model": model.state_dict(), "loss": loss}
    return result


def train_func(config):
    data_size = config.get("data_size", 1000)
    val_size = config.get("val_size", 400)
    batch_size = config.get("batch_size", 32)
    hidden_size = config.get("hidden_size", 1)
    lr = config.get("lr", 1e-2)
    epochs = config.get("epochs", 3)

    train_dataset = LinearDataset(2, 5, size=data_size)
    val_dataset = LinearDataset(2, 5, size=val_size)
    train_loader = torch.utils.data.DataLoader(
        train_dataset,
        batch_size=batch_size,
        sampler=DistributedSampler(train_dataset))
    validation_loader = torch.utils.data.DataLoader(
        val_dataset,
        batch_size=batch_size,
        sampler=DistributedSampler(val_dataset))

    model = nn.Linear(1, hidden_size)
    model = DistributedDataParallel(model)

    loss_fn = nn.MSELoss()

    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    results = []

    for _ in range(epochs):
        train(train_loader, model, loss_fn, optimizer)
        result = validate(validation_loader, model, loss_fn)
        results.append(result)

    return results


def train_linear(num_workers=1):
    trainer = Trainer(TorchConfig(backend="gloo"), num_workers=num_workers)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": 3}
    trainer.start()
    results = trainer.run(train_func, config)
    trainer.shutdown()

    print(results)
    return results


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
    train_linear(num_workers=args.num_workers)
