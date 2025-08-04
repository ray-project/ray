import pytest
import torch

import ray
from ray.air._internal.device_manager import (
    CUDATorchDeviceManager,
    NPUTorchDeviceManager,
    get_torch_device_manager_by_context,
)
from ray.air._internal.device_manager.npu import NPU_TORCH_PACKAGE_AVAILABLE
from ray.cluster_utils import Cluster
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer

if NPU_TORCH_PACKAGE_AVAILABLE:
    import torch_npu  # noqa: F401


@pytest.fixture
def ray_2_node_2_npus():
    cluster = Cluster()
    for _ in range(2):
        cluster.add_node(num_cpus=4, resources={"NPU": 2})

    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


@pytest.fixture
def ray_1_node_1_gpu_1_npu():
    cluster = Cluster()
    cluster.add_node(num_cpus=4, num_gpus=1, resources={"NPU": 1})
    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


def test_cuda_device_manager(ray_2_node_2_gpu):
    def train_fn():
        assert isinstance(get_torch_device_manager_by_context(), CUDATorchDeviceManager)

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn,
        scaling_config=ScalingConfig(
            num_workers=1, use_gpu=True, resources_per_worker={"GPU": 1}
        ),
    )

    trainer.fit()


def test_npu_device_manager(ray_2_node_2_npus):
    def train_fn():
        assert isinstance(get_torch_device_manager_by_context(), NPUTorchDeviceManager)

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn,
        scaling_config=ScalingConfig(num_workers=1, resources_per_worker={"NPU": 1}),
    )

    if NPU_TORCH_PACKAGE_AVAILABLE and torch.npu.is_available():
        # Except test run successfully when torch npu is available.
        trainer.fit()
    else:
        # A RuntimeError will be triggered when NPU resources are declared
        # but the torch npu is actually not available
        with pytest.raises(RuntimeError):
            trainer.fit()


def test_device_manager_conflict(ray_1_node_1_gpu_1_npu):
    trainer = TorchTrainer(
        train_loop_per_worker=lambda: None,
        scaling_config=ScalingConfig(
            num_workers=1, use_gpu=True, resources_per_worker={"GPU": 1, "NPU": 1}
        ),
    )
    # TODO: Do validation at the `ScalingConfig.__post_init__` level instead.
    with pytest.raises(RuntimeError):
        trainer.fit()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
