import argparse
import random
from typing import Tuple

import torch
import torch.nn as nn

import ray
import ray.train as train
from ray.data import Dataset
from ray.ml import train_test_split
from ray.ml.batch_predictor import BatchPredictor
from ray.ml.predictors.integrations.torch import TorchPredictor
from ray.ml.result import Result
from ray.ml.train.integrations.torch import TorchTrainer


def get_datasets(a=5, b=10, size=1000, split=0.8) -> Tuple[Dataset]:
    def get_dataset(a, b, size) -> Dataset:
        items = [i / size for i in range(size)]
        dataset = ray.data.from_items([{"x": x, "y": a * x + b} for x in items])
        return dataset

    dataset = get_dataset(a, b, size)

    train_dataset, validation_dataset = train_test_split(dataset, split, shuffle=True)
    return train_dataset, validation_dataset


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

    train_dataset_shard = train.get_dataset_shard("train")
    validation_dataset = train.get_dataset_shard("validation")

    model = nn.Linear(1, hidden_size)
    model = train.torch.prepare_model(model)

    loss_fn = nn.MSELoss()

    optimizer = torch.optim.SGD(model.parameters(), lr=lr)

    results = []

    for _ in range(epochs):
        train_torch_dataset = train_dataset_shard.to_torch(
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
        if train.world_rank() == 0:
            result = validate_epoch(validation_torch_dataset, model, loss_fn, device)
        else:
            result = {}
        train.report(**result)
        results.append(result)
        train.save_checkpoint(model=model)

    return results


def train_linear(num_workers=2, use_gpu=False):
    train_dataset, val_dataset = get_datasets()
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": 3}

    scaling_config = {"num_workers": num_workers, "use_gpu": use_gpu}

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
        datasets={"train": train_dataset, "validation": val_dataset},
    )

    result = trainer.fit()
    print(result.metrics)
    return result


def predict_linear(result: Result):
    batch_predictor = BatchPredictor.from_checkpoint(result.checkpoint, TorchPredictor)

    items = [{"x": random.uniform(0, 1) for _ in range(10)}]
    prediction_dataset = ray.data.from_items(items)

    predictions = batch_predictor.predict(prediction_dataset, dtype=torch.float)

    return predictions


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
        # 2 workers, 1 for trainer, 1 for datasets
        ray.init(num_cpus=4)
        result = train_linear()
    else:
        ray.init(address=args.address)
        result = train_linear(num_workers=args.num_workers, use_gpu=args.use_gpu)
    predict_linear(result)
