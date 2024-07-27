import pytest

import ray
from ray.air._internal.device_manager import (
    CUDATorchDeviceManager,
    NPUTorchDeviceManager,
    get_torch_device_manager,
)
from ray.cluster_utils import Cluster
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer


@pytest.fixture
def ray_1_node_1_gpu_1_npu():
    cluster = Cluster()
    cluster.add_node(num_cpus=4, num_gpus=1, resources={"NPU": 1})
    ray.init(address=cluster.address)

    yield

    ray.shutdown()
    cluster.shutdown()


def test_cuda_devcie_manager(ray_2_node_2_gpu):
    def train_fn():
        assert isinstance(get_torch_device_manager(), CUDATorchDeviceManager)

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn,
        scaling_config=ScalingConfig(
            num_workers=1, use_gpu=True, resources_per_worker={"GPU": 1}
        ),
    )

    trainer.fit()


def test_npu_device_mananger(ray_2_node_2_npus):
    def train_fn():
        assert isinstance(get_torch_device_manager(), NPUTorchDeviceManager)

    trainer = TorchTrainer(
        train_loop_per_worker=train_fn,
        scaling_config=ScalingConfig(num_workers=1, resources_per_worker={"NPU": 1}),
    )

    trainer.fit()


def test_device_manager_conflict(ray_1_node_1_gpu_1_npu):
    trainer = TorchTrainer(
        train_loop_per_worker=lambda: None,
        scaling_config=ScalingConfig(
            num_workers=1, use_gpu=True, resources_per_worker={"GPU": 1, "NPU": 1}
        ),
    )
    with pytest.raises(RuntimeError) as exc_info:
        trainer.fit()

    # the RuntimeError catched is is "The Ray Train run failed."
    # instead of  "Unable to determine the appropriate DeviceManager" in the remote.
    assert "The Ray Train run failed" in str(exc_info.value)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
