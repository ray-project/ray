import argparse
from typing import Dict, Tuple

import torch
import torch.nn as nn

import ray
import ray.train as train
from ray.air import session
from ray.air.config import DatasetConfig, ScalingConfig
from ray.data import Dataset
from ray.train.torch import TorchTrainer


def get_datasets_and_configs(
    a=5, b=10, size=1000, split=0.8
) -> Tuple[Dict[str, Dataset], Dict[str, DatasetConfig]]:
    def get_dataset(a, b, size) -> Dataset:
        items = [i / size for i in range(size)]
        dataset = ray.data.from_items([{"x": x, "y": a * x + b} for x in items])
        return dataset

    dataset = get_dataset(a, b, size)

    train_dataset, validation_dataset = dataset.random_shuffle().split_proportionately(
        [split]
    )

    datasets = {
        "train": train_dataset,
        "validation": validation_dataset,
    }

    # Use dataset pipelining
    dataset_configs = {
        "train": DatasetConfig(use_stream_api=True),
        "validation": DatasetConfig(use_stream_api=True),
    }

    return datasets, dataset_configs


def train_epoch(iterable_dataset, model, loss_fn, optimizer, device):
    model.train()
    for X, y in iterable_dataset:
        X = X.to(device)
        y = y.to(device)

        # Compute prediction error
        pred = model(X)
        loss = loss_fn(pred, y)

        # Backpropagation
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()


def validate_epoch(iterable_dataset, model, loss_fn, device):
    num_batches = 0
    model.eval()
    loss = 0
    with torch.no_grad():
        for X, y in iterable_dataset:
            X = X.to(device)
            y = y.to(device)
            num_batches += 1
            pred = model(X)
            loss += loss_fn(pred, y).item()
    loss /= num_batches
    result = {"loss": loss}
    return result


def train_func(config):
    batch_size = config.get("batch_size", 32)
    hidden_size = config.get("hidden_size", 1)
    lr = config.get("lr", 1e-2)
    epochs = config.get("epochs", 3)

    train_dataset_pipeline_shard = session.get_dataset_shard("train")
    validation_dataset_pipeline_shard = session.get_dataset_shard("validation")

    model = nn.Linear(1, hidden_size)
    model = train.torch.prepare_model(model)

    loss_fn = nn.MSELoss()

    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    train_dataset_iterator = train_dataset_pipeline_shard.iter_epochs()
    validation_dataset_iterator = validation_dataset_pipeline_shard.iter_epochs()

    for _ in range(epochs):
        train_dataset = next(train_dataset_iterator)
        validation_dataset = next(validation_dataset_iterator)

        train_torch_dataset = train_dataset.to_torch(
            label_column="y",
            feature_columns=["x"],
            label_column_dtype=torch.float,
            feature_column_dtypes=torch.float,
            batch_size=batch_size,
        )
        validation_torch_dataset = validation_dataset.to_torch(
            label_column="y",
            feature_columns=["x"],
            label_column_dtype=torch.float,
            feature_column_dtypes=torch.float,
            batch_size=batch_size,
        )

        device = train.torch.get_device()

        train_epoch(train_torch_dataset, model, loss_fn, optimizer, device)
        result = validate_epoch(validation_torch_dataset, model, loss_fn, device)
        session.report(result)


def train_linear(num_workers=2, use_gpu=False):
    datasets, dataset_configs = get_datasets_and_configs()

    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": 3}
    trainer = TorchTrainer(
        train_func,
        train_loop_config=config,
        datasets=datasets,
        dataset_config=dataset_configs,
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
    )
    results = trainer.fit()
    print(results.metrics)
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Use GPU for training."
    )

    args, _ = parser.parse_known_args()

    if args.smoke_test:
        # 1 for datasets, 1 for Trainable actor
        num_cpus = args.num_workers + 2
        num_gpus = args.num_workers if args.use_gpu else 0
        ray.init(num_cpus=num_cpus, num_gpus=num_gpus)
    else:
        ray.init(address=args.address)
    train_linear(num_workers=args.num_workers, use_gpu=args.use_gpu)
