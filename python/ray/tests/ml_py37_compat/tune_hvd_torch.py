import ray
from ray import tune
from ray.air import ScalingConfig, session
from ray.data.preprocessors import Concatenator, Chain, StandardScaler
from ray.train.horovod import HorovodTrainer
from ray.tune import Tuner, TuneConfig
import numpy as np


# Torch-specific
from ray.train.torch import TorchCheckpoint

import horovod.torch as hvd

import torch
import torch.nn as nn


def create_model(input_features):
    return nn.Sequential(
        nn.Linear(in_features=input_features, out_features=16),
        nn.ReLU(),
        nn.Linear(16, 16),
        nn.ReLU(),
        nn.Linear(16, 1),
        nn.Sigmoid(),
    )


def torch_train_loop(config):
    lr = config["lr"]
    epochs = config["epochs"]
    batch_size = config["batch_size"]
    num_features = config["num_features"]

    hvd.init()
    lr_scaler = hvd.size()

    torch.set_num_threads(1)

    dataset = session.get_dataset_shard("train")

    model = create_model(num_features)

    loss_fn = nn.BCELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=lr * lr_scaler)

    optimizer = hvd.DistributedOptimizer(
        optimizer,
        named_parameters=model.named_parameters(),
        op=hvd.Average,
    )

    for cur_epoch in range(epochs):
        for batch in dataset.iter_torch_batches(
            batch_size=batch_size, dtypes=torch.float32
        ):
            # "concat_out" is the output column of the Concatenator.
            inputs, labels = batch["concat_out"], batch["target"]
            optimizer.zero_grad()
            predictions = model(inputs)
            train_loss = loss_fn(predictions, labels.unsqueeze(1))
            train_loss.backward()
            optimizer.step()
        loss = train_loss.item()
        session.report({"loss": loss}, checkpoint=TorchCheckpoint.from_model(model))


def tune_horovod_torch(num_workers, num_samples, use_gpu):
    dataset = ray.data.read_csv("s3://anonymous@air-example-data/breast_cancer.csv")
    num_features = len(dataset.schema().names) - 1

    preprocessor = Chain(
        StandardScaler(columns=["mean radius", "mean texture"]),
        Concatenator(exclude=["target"], dtype=np.float32),
    )

    horovod_trainer = HorovodTrainer(
        train_loop_per_worker=torch_train_loop,
        train_loop_config={"epochs": 10, "num_features": num_features},
        scaling_config=ScalingConfig(num_workers=num_workers, use_gpu=use_gpu),
        datasets={"train": dataset},
        preprocessor=preprocessor,
    )

    tuner = Tuner(
        horovod_trainer,
        param_space={
            "train_loop_config": {
                "lr": tune.uniform(0.1, 1),
                "batch_size": tune.choice([32, 64]),
            }
        },
        tune_config=TuneConfig(mode="min", metric="loss", num_samples=num_samples),
        _tuner_kwargs={"fail_fast": True},
    )

    result_grid = tuner.fit()

    print("Best hyperparameters found were: ", result_grid.get_best_result().config)


if __name__ == "__main__":
    ray.init(num_cpus=16)
    tune_horovod_torch(num_workers=2, num_samples=4, use_gpu=False)
