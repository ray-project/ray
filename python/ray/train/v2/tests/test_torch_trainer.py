import pytest
import torch

import ray
from ray.train import ScalingConfig
from ray.train.constants import TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S
from ray.train.examples.pytorch.torch_linear_example import (
    train_func as linear_train_func,
)
from ray.train.torch import TorchConfig, TorchTrainer
from ray.train.torch.config import _is_backend_nccl
from ray.train.v2._internal.constants import HEALTH_CHECK_INTERVAL_S_ENV_VAR
from ray.train.v2.torch.torchft_config import TorchftConfig


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


@pytest.mark.parametrize(
    "torch_config,expected_world_size",
    [
        (TorchConfig(backend="gloo", init_method="env"), 2),
        (TorchConfig(backend="gloo", init_method="tcp"), 2),
        # TODO(tseah): enable this after CI has torchft dependencies
        # (
        #     TorchftConfig(
        #         backend="gloo", init_method="env", lighthouse_kwargs={"min_replicas": 1}
        #     ),
        #     1,
        # ),
        # (
        #     TorchftConfig(
        #         backend="gloo", init_method="tcp", lighthouse_kwargs={"min_replicas": 1}
        #     ),
        #     1,
        # ),
    ],
)
def test_torch_start_shutdown(ray_start_4_cpus, torch_config, expected_world_size):
    def check_process_group():
        assert (
            torch.distributed.is_initialized()
            and torch.distributed.get_world_size() == expected_world_size
        )

    trainer = TorchTrainer(
        train_loop_per_worker=check_process_group,
        scaling_config=ScalingConfig(num_workers=2),
        torch_config=torch_config,
    )
    trainer.fit()


@pytest.mark.parametrize("timeout_s", [5, 0])
def test_torch_process_group_shutdown_timeout(ray_start_4_cpus, monkeypatch, timeout_s):
    """Tests that we don't more than a predefined timeout
    on Torch process group shutdown."""

    monkeypatch.setenv(TORCH_PROCESS_GROUP_SHUTDOWN_TIMEOUT_S, str(timeout_s))

    trainer = TorchTrainer(
        train_loop_per_worker=lambda: None,
        scaling_config=ScalingConfig(num_workers=2),
        torch_config=TorchConfig(backend="gloo"),
    )
    # Even if shutdown times out (timeout_s=0),
    # the training should complete successfully.
    trainer.fit()


@pytest.mark.skip(reason="TODO(tseah): enable this after CI has torchft dependencies")
def test_torchft_linear(ray_start_4_cpus):
    """Test torchft linear training: loss goes down and models are equal across workers."""

    from ray.train.v2.examples.pytorch.torchft_linear_example import (
        train_func as torchft_linear_train_func,
    )

    @ray.remote
    class WeightCollector:
        def __init__(self):
            self.weights = {}

        def report(self, rank, weight, bias):
            self.weights[rank] = {"weight": weight, "bias": bias}

        def get_weights(self):
            return self.weights

    collector = WeightCollector.remote()

    def train_func(config):
        result = torchft_linear_train_func(config)
        assert result[-1]["loss"] < result[0]["loss"]
        world_rank = ray.train.get_context().get_world_rank()
        ray.get(
            config["collector"].report.remote(
                world_rank, result[-1]["weight"], result[-1]["bias"]
            )
        )

    trainer = TorchTrainer(
        train_loop_per_worker=train_func,
        train_loop_config={"collector": collector},
        scaling_config=ScalingConfig(num_workers=2),
        torch_config=TorchftConfig(
            backend="gloo", lighthouse_kwargs={"min_replicas": 2}
        ),
    )
    result = trainer.fit()
    assert result.error is None

    # Check that models converged across workers.
    weights = ray.get(collector.get_weights.remote())
    assert len(weights) == 2
    assert weights[0]["weight"] == pytest.approx(weights[1]["weight"], abs=1e-4)
    assert weights[0]["bias"] == pytest.approx(weights[1]["bias"], abs=1e-4)


def test_is_backend_nccl():
    assert _is_backend_nccl("nccl")
    assert _is_backend_nccl("cuda:nccl")
    assert _is_backend_nccl("cpu:gloo,cuda:nccl")
    assert not _is_backend_nccl("gloo")
    assert not _is_backend_nccl("cpu:nccl")
    assert not _is_backend_nccl("cuda:gloo")
    assert not _is_backend_nccl("cpu:gloo,cuda:gloo")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
