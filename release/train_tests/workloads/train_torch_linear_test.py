import json
import os
import time

import ray

#from ray.train.examples.train_linear_example import train_linear

import argparse

import numpy as np
import torch
import torch.nn as nn
import ray.train as train
from ray.train import Trainer
from ray.train.callbacks import JsonLoggerCallback, TBXLoggerCallback
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


def train_epoch(dataloader, model, loss_fn, optimizer, device):
    for X, y in dataloader:
        X, y = X.to(device), y.to(device)
        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()


def validate_epoch(dataloader, model, loss_fn, device):
    num_batches = len(dataloader)
    model.eval()
    loss = 0
    with torch.no_grad():
        for X, y in dataloader:
            X, y = X.to(device), y.to(device)
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

    assert torch.cuda.is_available()
    device = torch.device(f"cuda:{train.local_rank()}"
                          if torch.cuda.is_available() else "cpu")

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
    model.to(device)
    model = DistributedDataParallel(model, device_ids=[device.index] if torch.cuda.is_available() else None)

    loss_fn = nn.MSELoss()

    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    results = []

    for _ in range(epochs):
        train_epoch(train_loader, model, loss_fn, optimizer, device)
        result = validate_epoch(validation_loader, model, loss_fn, device)
        train.report(**result)
        results.append(result)

    return results


def train_linear(num_workers=2, use_gpu=False, epochs=3):
    trainer = Trainer(
        backend="torch", num_workers=num_workers, use_gpu=use_gpu)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}
    trainer.start()
    results = trainer.run(
        train_func,
        config,
        callbacks=[JsonLoggerCallback(),
                   TBXLoggerCallback()])
    trainer.shutdown()

    print(results)
    return results


if __name__ == "__main__":
    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "horovod_user_test")

    if addr is not None and addr.startswith("anyscale://"):
        ray.init(address=addr, job_name=job_name)
    else:
        ray.init(address="auto")

    results = train_linear(num_workers=6, use_gpu=True, epochs=20)

    taken = time.time() - start
    result = {"time_taken": taken, "train_results": results}
    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/train_torc_linear_test.json")

    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
