# flake8: noqa
# isort: skip_file

# __start__

# First, pip install torchmetrics
# This code is tested with torchmetrics==1.1.1

import ray.train.torch
from ray.air import session, ScalingConfig
from ray.train.torch import TorchTrainer

import torch
import torch.nn as nn
import torchmetrics
from torch.optim import Adam
import numpy as np


def train_func(config):
    n = 100
    # create a toy dataset
    X = torch.Tensor(np.random.normal(0, 1, size=(n, 4)))
    X_valid = torch.Tensor(np.random.normal(0, 1, size=(n, 4)))
    Y = torch.Tensor(np.random.uniform(0, 1, size=(n, 1)))
    Y_valid = torch.Tensor(np.random.uniform(0, 1, size=(n, 1)))
    # toy neural network : 1-layer
    # wrap the model in DDP
    model = ray.train.torch.prepare_model(nn.Linear(4, 1))
    criterion = nn.MSELoss()

    mape = torchmetrics.MeanAbsolutePercentageError()
    # for averaging loss
    mean_valid_loss = torchmetrics.MeanMetric()

    optimizer = Adam(model.parameters(), lr=3e-4)
    for epoch in range(config["num_epochs"]):
        model.train()
        y = model.forward(X)

        # compute loss
        loss = criterion(y, Y)

        # back-propagate loss
        optimizer.zero_grad()
        loss.backward()
        optimizer.step()

        # evaluate
        model.eval()
        with torch.no_grad():
            pred = model(X_valid)
            valid_loss = criterion(pred, Y_valid)
            # save loss in aggregator
            mean_valid_loss(valid_loss)
            mape(pred, Y_valid)

        # collect all metrics
        # use .item() to obtain a value that can be reported
        valid_loss = valid_loss.item()
        mape_collected = mape.compute().item()
        mean_valid_loss_collected = mean_valid_loss.compute().item()

        session.report(
            {
                "mape_collected": mape_collected,
                "valid_loss": valid_loss,
                "mean_valid_loss_collected": mean_valid_loss_collected,
            }
        )

        # reset for next epoch
        mape.reset()
        mean_valid_loss.reset()


trainer = TorchTrainer(
    train_func,
    train_loop_config={"num_epochs": 5},
    scaling_config=ScalingConfig(num_workers=2),
)
result = trainer.fit()
print(result.metrics["valid_loss"], result.metrics["mean_valid_loss_collected"])
# 0.5109779238700867 0.5512474775314331
