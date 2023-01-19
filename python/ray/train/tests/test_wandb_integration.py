"""
If a user uses Trainer API directly with wandb integration, they expect to see
* train_loop_config to show up in wandb.config.

This test uses mocked call into wandb API.
"""

import pytest

import ray
from ray.air import RunConfig, ScalingConfig
from ray.air.tests.mocked_wandb_integration import WandbTestExperimentLogger
from ray.train.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)
from ray.train.torch import TorchTrainer


@pytest.fixture
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_trainer_wandb_integration(ray_start_4_cpus):
    def train_func(config):
        result = linear_train_func(config)
        assert len(result) == epochs
        assert result[-1]["loss"] < result[0]["loss"]

    epochs = 3
    scaling_config = ScalingConfig(num_workers=2)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}
    logger = WandbTestExperimentLogger(project="test_project")
    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
        run_config=RunConfig(callbacks=[logger]),
    )
    trainer.fit()
    # We use local actor for mocked logger.
    # As a result, `._wandb`, `.config` and `.queue` are
    # guaranteed to be available by the time `trainer.fit()` returns.
    # This is so because they are generated in corresponding initializer
    # in a sync fashion.
    config = list(logger.trial_processes.values())[0]._wandb.config.queue.get(
        timeout=10
    )

    assert "train_loop_config" in config
