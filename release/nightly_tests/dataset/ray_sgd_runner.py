# flake8: noqa
import argparse
import os
import json
import time

import ray
import torch
import torch.nn as nn
import torch.optim as optim
import numpy as np
from ray import train
from ray.data.dataset_pipeline import DatasetPipeline
from ray.train import Trainer
from torch.nn.parallel import DistributedDataParallel


class Net(nn.Module):
    def __init__(self, n_layers, n_features, num_hidden, dropout_every, drop_prob):
        super().__init__()
        self.n_layers = n_layers
        self.dropout_every = dropout_every
        self.drop_prob = drop_prob

        self.fc_input = nn.Linear(n_features, num_hidden)
        self.relu_input = nn.ReLU()

        for i in range(self.n_layers):
            layer = nn.Linear(num_hidden, num_hidden)
            relu = nn.ReLU()
            dropout = nn.Dropout(p=self.drop_prob)

            setattr(self, f"fc_{i}", layer)
            setattr(self, f"relu_{i}", relu)
            if i % self.dropout_every == 0:
                # only apply every few layers
                setattr(self, f"drop_{i}", dropout)
                self.add_module(f"drop_{i}", dropout)

            self.add_module(f"fc_{i}", layer)
            self.add_module(f"relu_{i}", relu)

        self.fc_output = nn.Linear(num_hidden, 1)

    def forward(self, x):
        x = self.fc_input(x)
        x = self.relu_input(x)

        for i in range(self.n_layers):
            x = getattr(self, f"fc_{i}")(x)
            x = getattr(self, f"relu_{i}")(x)
            if i % self.dropout_every == 0:
                x = getattr(self, f"drop_{i}")(x)

        x = self.fc_output(x)
        return x


def train_epoch(dataset, model, device, criterion, optimizer):
    num_correct = 0
    num_total = 0
    running_loss = 0.0

    start = time.time()
    total_time = 0
    total_train_time = 0
    for i, (inputs, labels) in enumerate(dataset):
        train_start = time.time()
        inputs = inputs.to(device)
        labels = labels.to(device)

        # Zero the parameter gradients
        optimizer.zero_grad()

        # Forward + backward + optimize
        outputs = model(inputs.float())
        loss = criterion(outputs, labels.float())
        loss.backward()
        optimizer.step()

        # how are we doing?
        predictions = (torch.sigmoid(outputs) > 0.5).int()
        num_correct += (predictions == labels).sum().item()
        num_total += len(outputs)

        # Save loss to plot
        running_loss += loss.item()

        train_time = time.time() - train_start
        total_time = time.time() - start
        total_train_time += train_time
        if i % 100 == 0:
            print(f"training batch [{i}] loss: {loss.item()}")
            print(f"time breakdown per_batch_time: [{total_time / (i + 1)}]")
            print(f"per_batch_train_time {total_train_time / (i + 1) }")

    return (running_loss, num_correct, num_total)


def test_epoch(dataset, model, device, criterion):
    num_correct = 0
    num_total = 0
    running_loss = 0.0

    with torch.no_grad():
        for i, (inputs, labels) in enumerate(dataset):
            inputs = inputs.to(device)
            labels = labels.to(device)

            # Forward + backward + optimize
            outputs = model(inputs.float())
            loss = criterion(outputs, labels.float())

            # how are we doing?
            predictions = (torch.sigmoid(outputs) > 0.5).int()
            num_correct += (predictions == labels).sum().item()
            num_total += len(outputs)

            # Save loss to plot
            running_loss += loss.item()
            if i % 100 == 0:
                print(f"testing batch [{i}] loss: {loss.item()}")

    return (running_loss, num_correct, num_total)


def train_func(config):
    use_gpu = config["use_gpu"]
    num_epochs = config["num_epochs"]
    batch_size = config["batch_size"]
    num_layers = config["num_layers"]
    num_hidden = config["num_hidden"]
    dropout_every = config["dropout_every"]
    dropout_prob = config["dropout_prob"]
    num_features = config["num_features"]

    print("Defining model, loss, and optimizer...")

    device = torch.device(
        f"cuda:{train.local_rank()}" if use_gpu and torch.cuda.is_available() else "cpu"
    )

    train_dataset_pipeline = train.get_dataset_shard("train_dataset")
    train_dataset_epoch_iterator = train_dataset_pipeline.iter_epochs()
    test_dataset = train.get_dataset_shard("test_dataset")
    test_torch_dataset = test_dataset.to_torch(
        label_column="label", batch_size=batch_size
    )

    net = Net(
        n_layers=num_layers,
        n_features=num_features,
        num_hidden=num_hidden,
        dropout_every=dropout_every,
        drop_prob=dropout_prob,
    ).to(device)
    print(net.parameters)

    net = DistributedDataParallel(net)

    criterion = nn.BCEWithLogitsLoss()
    optimizer = optim.Adam(net.parameters(), weight_decay=0.0001)

    print("Starting training...")
    for epoch in range(num_epochs):
        start = time.time()
        train_dataset = next(train_dataset_epoch_iterator)
        print(f"epoch load time {time.time() - start}")

        train_torch_dataset = train_dataset.to_torch(
            label_column="label", batch_size=batch_size
        )

        train_running_loss, train_num_correct, train_num_total = train_epoch(
            train_torch_dataset, net, device, criterion, optimizer
        )
        train_acc = train_num_correct / train_num_total
        print(
            f"epoch [{epoch + 1}]: training accuracy: {train_num_correct} / {train_num_total} = {train_acc:.4f}"
        )

        test_running_loss, test_num_correct, test_num_total = test_epoch(
            test_torch_dataset, net, device, criterion
        )
        test_acc = test_num_correct / test_num_total
        print(
            f"epoch [{epoch + 1}]: testing accuracy: {test_num_correct} / {test_num_total} = {test_acc:.4f}"
        )

        # Record and log stats.
        train.report(
            train_acc=train_acc,
            train_loss=train_running_loss,
            test_acc=test_acc,
            test_loss=test_running_loss,
        )

    if train.world_rank() == 0:
        return net.module.cpu()
    else:
        return None


def create_dataset_pipeline(files, epochs, num_windows):
    if num_windows > 1:
        file_splits = np.array_split(files, num_windows)

        class Windower:
            def __init__(self):
                self.i = 0
                self.iterations = epochs * num_windows

            def __iter__(self):
                return self

            def __next__(self):
                if self.i >= self.iterations:
                    raise StopIteration()
                split = file_splits[self.i % num_windows]
                self.i += 1
                return lambda: ray.data.read_parquet(list(split))

        pipe = DatasetPipeline.from_iterable(Windower())
        pipe = pipe.random_shuffle_each_window()
    else:
        ds = ray.data.read_parquet(files)
        pipe = ds.repeat(epochs)
        pipe = pipe.random_shuffle_each_window()
    return pipe


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )
    parser.add_argument(
        "--address",
        required=False,
        type=str,
        help="The address to use for Ray. `auto` if running through `ray submit",
    )
    parser.add_argument(
        "--num-workers",
        default=1,
        type=int,
        help="number of Ray workers to use for distributed training",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Use GPU for training."
    )
    parser.add_argument("--num-epochs", default=4, type=int, help="number of epochs")
    parser.add_argument("--num-windows", default=64, type=int, help="number of windows")

    args = parser.parse_args()
    smoke_test = args.smoke_test
    address = args.address
    num_workers = args.num_workers
    use_gpu = args.use_gpu
    num_epochs = args.num_epochs
    num_windows = args.num_windows

    BATCH_SIZE = 50000
    NUM_HIDDEN = 50
    NUM_LAYERS = 3
    DROPOUT_EVERY = 5
    DROPOUT_PROB = 0.2

    start_time = time.time()

    ray.init(address=address)

    files = [
        f"s3://shuffling-data-loader-benchmarks/data-400G/data_{i}.parquet.snappy"
        for i in range(200)
    ]

    test_dataset = ray.data.read_parquet([files.pop(0)])

    num_columns = len(test_dataset.schema().names)
    num_features = num_columns - 1

    if smoke_test:
        # Only read a single file.
        files = files[:1]
        train_dataset_pipeline = create_dataset_pipeline(files, num_epochs, 1)
    else:
        train_dataset_pipeline = create_dataset_pipeline(files, num_epochs, num_windows)

    datasets = {"train_dataset": train_dataset_pipeline, "test_dataset": test_dataset}

    config = {
        "is_distributed": True,
        "use_gpu": use_gpu,
        "num_epochs": num_epochs,
        "batch_size": BATCH_SIZE,
        "num_hidden": NUM_HIDDEN,
        "num_layers": NUM_LAYERS,
        "dropout_every": DROPOUT_EVERY,
        "dropout_prob": DROPOUT_PROB,
        "num_features": num_features,
    }

    num_gpus = 1 if use_gpu else 0
    num_cpus = 0 if use_gpu else 1

    trainer = Trainer(
        backend="torch",
        num_workers=num_workers,
        use_gpu=use_gpu,
        resources_per_worker={"CPU": num_cpus, "GPU": num_gpus},
    )
    trainer.start()
    results = trainer.run(
        train_func=train_func, config=config, callbacks=[], dataset=datasets
    )
    model = results[0]
    trainer.shutdown()

    end_time = time.time()

    total_time = end_time - start_time
    print(f"Job finished in {total_time} seconds.")
    with open(os.environ["TEST_OUTPUT_JSON"], "w") as f:
        f.write(json.dumps({"time": total_time, "success": 1}))
