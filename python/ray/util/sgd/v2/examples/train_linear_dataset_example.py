import argparse

import ray
import torch
import torch.nn as nn
import ray.util.sgd.v2 as sgd
from ray.util.sgd.v2 import Trainer, TorchConfig
from ray.util.sgd.v2.callbacks import JsonLoggerCallback, TBXLoggerCallback
from torch.nn.parallel import DistributedDataParallel


def get_dataset(a, b, size=1000) -> ray.data.Dataset:
    dataset = ray.data.from_items([{"x": x, "y": a * x + b} for x in range(size)])
    return dataset


def train(iterable_dataset, model, loss_fn, optimizer):
    for X, y in iterable_dataset:
        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()


def validate(iterable_dataset, model, loss_fn):
    num_batches = len(iterable_dataset)
    model.eval()
    loss = 0
    with torch.no_grad():
        for X, y in iterable_dataset:
            pred = model(X)
            loss += loss_fn(pred, y).item()
    loss /= num_batches
    result = {"model": model.state_dict(), "loss": loss}
    return result


def train_func(config):
    batch_size = config.get("batch_size", 32)
    hidden_size = config.get("hidden_size", 1)
    lr = config.get("lr", 1e-2)
    epochs = config.get("epochs", 3)

    train_dataset_pipeline_shard = sgd.get_dataset_shard("train")
    validation_dataset_pipeline_shard = sgd.get_dataset_shard("validation")

    model = nn.Linear(1, hidden_size)
    model = DistributedDataParallel(model)

    loss_fn = nn.MSELoss()

    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    results = []

    train_dataset_iterator = train_dataset_pipeline_shard.iter_datasets()
    validation_dataset_iterator = \
        validation_dataset_pipeline_shard.iter_datasets()

    for _ in range(epochs):
        train_dataset = next(train_dataset_iterator)
        validation_dataset = next(validation_dataset_iterator)

        train_torch_dataset = train_dataset.to_torch(label_column="y",
                                                     batch_size=batch_size)
        validation_torch_dataset = validation_dataset.to_torch(label_column="y",
                                                               batch_size=batch_size)

        train(train_torch_dataset, model, loss_fn, optimizer)
        result = validate(validation_torch_dataset, model, loss_fn)
        sgd.report(**result)
        results.append(result)

    return results


def train_linear(num_workers=2):
    dataset = get_dataset(5, 10)

    split_index = int(dataset.count() * 0.8)

    train_dataset, validation_dataset = \
        dataset.random_shuffle().split_at_indices([split_index])

    train_dataset_pipeline = train_dataset.repeat().random_shuffle()
    validation_dataset_pipeline = validation_dataset.repeat()

    datasets = {"train": train_dataset_pipeline,
                "validation": validation_dataset_pipeline}

    trainer = Trainer(TorchConfig(backend="gloo"), num_workers=num_workers)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": 3}
    trainer.start()
    results = trainer.run(
        train_func,
        config,
        dataset=datasets,
        callbacks=[JsonLoggerCallback(),
                   TBXLoggerCallback()])
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
        default=2,
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
