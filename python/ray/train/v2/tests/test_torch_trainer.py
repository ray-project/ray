import pytest

import ray
from ray.train import ScalingConfig
from ray.train.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    ray.init(num_cpus=4)
    yield
    ray.shutdown()


@pytest.fixture(autouse=True)
def reduce_health_check_interval(monkeypatch):
    monkeypatch.setenv(HEALTH_CHECK_INTERVAL_S_ENV_VAR, "0.2")
    yield


def test_minimal(ray_start_4_cpus):
    def train_func():
        pass

    trainer = TorchTrainer(train_func)
    trainer.fit()


@pytest.mark.parametrize("num_workers", [1, 2])
def test_torch_linear(ray_start_4_cpus, num_workers):
    def train_func(config):
        result = linear_train_func(config)
        assert len(result) == epochs
        assert result[-1]["loss"] < result[0]["loss"]

    num_workers = num_workers
    epochs = 3
    scaling_config = ScalingConfig(num_workers=num_workers)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": epochs}
    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config=config,
        scaling_config=scaling_config,
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
