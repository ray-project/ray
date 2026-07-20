import os
import sys
from unittest.mock import MagicMock, patch

# Disable torch backend autoload to prevent torch_tpu from crashing the process
# on import due to ABI mismatches when running on a CPU-only head/worker node.
os.environ["TORCH_DEVICE_BACKEND_AUTOLOAD"] = "0"

import pytest
import torch
import torch.distributed as dist

import ray
from ray.air._internal.device_manager import (
    CUDATorchDeviceManager,
    NPUTorchDeviceManager,
    TPUTorchDeviceManager,
    get_torch_device_manager_by_context,
)
from ray.air._internal.device_manager.npu import NPU_TORCH_PACKAGE_AVAILABLE
from ray.cluster_utils import Cluster
from ray.train import ScalingConfig, TrainingFailedError
from ray.train.torch import TorchTrainer
from ray.train.torch.config import _validate_tpu_resources
from ray.train.v2._internal.constants import is_v2_enabled

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
def ray_tpu_cluster():
    cluster = Cluster()

    # 1. Two single-host TPU nodes (v6e-8 / 2x4 topology)
    for i in range(2):
        cluster.add_node(
            num_cpus=16,
            resources={
                "TPU": 8,
                "accelerator_type:TPU-V6E": 1,
                "TPU-v6e-8-head": 1,
            },
            env_vars={
                "TPU_NAME": f"slice-single-{i}",
                "TPU_WORKER_ID": "0",
                "TPU_ACCELERATOR_TYPE": "v6e-8",
                "TPU_TOPOLOGY": "2x4",
            },
            labels={
                "ray.io/tpu-slice-name": f"slice-single-{i}",
                "ray.io/tpu-worker-id": "0",
                "ray.io/tpu-pod-type": "v6e-8",
            },
        )

    # 2. Multi-host TPU slice (v6e-16 / 4x4 topology / 4 nodes / 4 chips each)
    pod_type = "v6e-16"
    topology = "4x4"
    for i in range(4):
        slice_env = {
            "TPU_NAME": "slice-A",
            "TPU_WORKER_ID": str(i),
            "TPU_ACCELERATOR_TYPE": pod_type,
            "TPU_TOPOLOGY": topology,
        }
        slice_labels = {
            "ray.io/tpu-slice-name": "slice-A",
            "ray.io/tpu-worker-id": str(i),
            "ray.io/tpu-pod-type": pod_type,
        }
        resources = {
            "TPU": 4,
            "accelerator_type:TPU-V6E": 1,
        }
        if i == 0:
            resources[f"TPU-{pod_type}-head"] = 1

        cluster.add_node(
            num_cpus=4,
            resources=resources,
            env_vars=slice_env,
            labels=slice_labels,
        )

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


@pytest.mark.skipif(
    not hasattr(dist, "is_nccl_available") or not dist.is_nccl_available(),
    reason="NCCL is not available in this PyTorch installation.",
)
def test_cuda_device_manager(ray_2_node_2_gpu):
    def train_fn():
        assert isinstance(get_torch_device_manager_by_context(), CUDATorchDeviceManager)

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn,
        scaling_config=ScalingConfig(
            num_workers=1, use_gpu=True, resources_per_worker={"GPU": 1}
        ),
    )

    if torch.cuda.is_available():
        trainer.fit()
    else:
        with pytest.raises(TrainingFailedError):
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
        with pytest.raises(TrainingFailedError):
            trainer.fit()


@pytest.mark.skipif(
    not is_v2_enabled(),
    reason="TPU device manager and backend are V2-only features.",
)
@pytest.mark.parametrize(
    "num_workers,topology,accelerator_type",
    [
        (8, "2x4", "v6e"),
        (16, "4x4", "v6e"),
    ],
)
def test_tpu_device_manager(ray_tpu_cluster, num_workers, topology, accelerator_type):
    def train_fn():

        assert isinstance(get_torch_device_manager_by_context(), TPUTorchDeviceManager)

        # Verify distributed environment variables injected correctly by TorchTrainer.
        assert "TPU_VISIBLE_CHIPS" in os.environ
        assert "RANK" in os.environ
        assert "WORLD_SIZE" in os.environ
        assert "MASTER_ADDR" in os.environ
        assert "MASTER_PORT" in os.environ

        assert dist.is_initialized()
        assert dist.get_backend() == "tpu_dist"

        # Verify distributed setup works by running a basic collective
        world_size = dist.get_world_size()
        tensor = torch.ones(1, device="tpu")
        dist.all_reduce(tensor)
        assert tensor.item() == world_size

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn,
        scaling_config=ScalingConfig(
            num_workers=num_workers,
            use_tpu=True,
            topology=topology,
            accelerator_type=accelerator_type,
            resources_per_worker={"TPU": 1},
        ),
    )

    try:
        import torch_tpu._loader  # noqa: F401
    except ImportError:
        pytest.skip(
            "torch_tpu is not installed. Skipping this test because we cannot "
            "run PyTorch TPU distributed collectives without the real PJRT runtime."
        )

    trainer.fit()


def test_device_manager_conflict(ray_1_node_1_gpu_1_npu):
    trainer = TorchTrainer(
        train_loop_per_worker=lambda: None,
        scaling_config=ScalingConfig(
            num_workers=1, use_gpu=True, resources_per_worker={"GPU": 1, "NPU": 1}
        ),
    )
    # TODO: Do validation at the `ScalingConfig.__post_init__` level instead.
    with pytest.raises(TrainingFailedError):
        trainer.fit()


@pytest.mark.skipif(
    not is_v2_enabled(),
    reason="TPU device manager and backend are V2-only features.",
)
def test_tpu_torch_multi_tpu_warning():
    mock_worker_group = MagicMock()
    mock_worker_group.get_resources_per_worker.return_value = {"TPU": 2}
    mock_worker_group.get_worker_group_context.return_value.num_slices = 1

    with patch("ray.train.torch.config.logger.warning") as mock_warning:
        _validate_tpu_resources(mock_worker_group)

    mock_warning.assert_called_once()
    assert (
        "it is recommended that each worker has exactly 1 TPU device"
        in mock_warning.call_args[0][0]
    )


@pytest.mark.skipif(
    not is_v2_enabled(),
    reason="TPU device manager and backend are V2-only features.",
)
def test_tpu_torch_multislice_validation_error(ray_tpu_cluster):
    # PyTorch TPU currently does not support training across multiple slices.
    # We specify a topology that requires 8 workers, but request 16 workers, which implies 2 slices.
    trainer = TorchTrainer(
        train_loop_per_worker=lambda: None,
        scaling_config=ScalingConfig(
            num_workers=16,
            use_tpu=True,
            topology="2x4",
            accelerator_type="TPU-V6E",
            resources_per_worker={"TPU": 1},
        ),
    )

    with pytest.raises(TrainingFailedError) as exc_info:
        trainer.fit()

    assert (
        "PyTorch TPU training across multiple slices (num_slices > 1) is not currently supported"
        in str(exc_info.value)
    )


def test_tpu_torch_import_error():
    with patch.dict(sys.modules, {"torch_tpu._loader": None}):
        with pytest.raises(ImportError) as exc_info:
            TPUTorchDeviceManager.register_custom_torch_dist_backend()

    assert "The `torch_tpu` module is required" in str(exc_info.value)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
