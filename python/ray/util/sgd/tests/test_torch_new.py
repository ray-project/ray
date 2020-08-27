from unittest.mock import patch
import numpy as np
import os
import pytest
import time
import torch
import torch.nn as nn
import torch.distributed as dist
from torch.utils.data import DataLoader

import ray
from ray import tune
from ray.util.sgd.torch import TorchTrainer
from ray.util.sgd.torch.training_operator import (_TestingOperator,
                                                  _TestMetricsOperator,
                                                  TrainingOperator)
from ray.util.sgd.torch.constants import SCHEDULER_STEP
from ray.util.sgd.utils import (check_for_failure, NUM_SAMPLES, BATCH_COUNT,
                                BATCH_SIZE)

from ray.util.sgd.data.examples import mlp_identity
from ray.util.sgd.torch.examples.train_example import LinearDataset

@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Ensure that tests don't ALL fail
    if dist.is_initialized():
        dist.destroy_process_group()


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()
    # Ensure that tests don't ALL fail
    if dist.is_initialized():
        dist.destroy_process_group()

class TestTrainingOperator(TrainingOperator):
    def setup(self, config):
        model = nn.Linear(1, config.get("hidden_size", 1))
        optimizer = torch.optim.SGD(model.parameters(), lr=config.get("lr",
                                                                    1e-2))
        scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=5, gamma=0.9)
        loss = nn.MSELoss()
        train_dataset = LinearDataset(2, 5, size=config.get("data_size", 1000))
        val_dataset = LinearDataset(2, 5, size=config.get("val_size", 400))
        train_loader = torch.utils.data.DataLoader(
            train_dataset,
            batch_size=config.get("batch_size", 32),
        )
        validation_loader = torch.utils.data.DataLoader(
            val_dataset,
            batch_size=config.get("batch_size", 32))

        self.register(models=model, optimizers=optimizer, schedulers=scheduler,
                      criterion=loss, train_loader=train_loader,
                      validation_loader=validation_loader)

def test_single_step(ray_start_2_cpus):  # noqa: F811
    trainer = TorchTrainer(
        training_operator_cls=TestTrainingOperator,
        num_workers=1)
    metrics = trainer.train(num_steps=1)
    assert metrics[BATCH_COUNT] == 1

    val_metrics = trainer.validate(num_steps=1)
    assert val_metrics[BATCH_COUNT] == 1
    trainer.shutdown()

def test_resize(ray_start_2_cpus):  # noqa: F811
    trainer = TorchTrainer(
        training_operator_cls=TestTrainingOperator,
        num_workers=1)
    trainer.train(num_steps=1)
    trainer.max_replicas = 2
    results = trainer.train(num_steps=1, reduce_results=False)
    assert len(results) == 2



