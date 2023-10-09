import json
import os
import time
from pathlib import Path
from typing import Dict, List, Union
from unittest.mock import patch

import pytest
import torch
import torchvision
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DataLoader, DistributedSampler

import ray
import ray.data
from ray import train
from ray.exceptions import RayTaskError
from ray.train import ScalingConfig
from ray.train._internal.worker_group import WorkerGroup
from ray.train.constants import DEFAULT_NCCL_SOCKET_IFNAME
from ray.train.examples.pytorch.torch_linear_example import LinearDataset
from ray.train.torch.config import TorchConfig, _TorchBackend
from ray.train.torch.torch_trainer import TorchTrainer
from ray.train.trainer import TrainingFailedError


class LinearDatasetDict(LinearDataset):
    """Modifies the LinearDataset to return a Dict instead of a Tuple."""

    def __getitem__(self, index):
        return {"x": self.x[index, None], "y": self.y[index, None]}


class NonTensorDataset(LinearDataset):
    """Modifies the LinearDataset to also return non-tensor objects."""

    def __getitem__(self, index):
        return {"x": self.x[index, None], "y": 2}


def write_rank_data(tmp_path: Path, data: Union[int, List, Dict]):
    rank = train.get_context().get_world_rank()
    with open(tmp_path / f"{rank}.json", "w") as f:
        json.dump(data, f)


def get_data_from_all_ranks(tmp_path: Path) -> Dict[int, Union[int, List, Dict]]:
    rank_data = {}
    for rank_file in tmp_path.glob("*.json"):
        rank = int(rank_file.stem)
        with open(rank_file, "r") as f:
            data = json.load(f)
        rank_data[rank] = data
    return rank_data


@pytest.mark.parametrize("cuda_visible_devices", ["", "1,2"])
@pytest.mark.parametrize("num_gpus_per_worker", [0.5, 1, 2])
def test_torch_get_device(
    shutdown_only, num_gpus_per_worker, cuda_visible_devices, monkeypatch, tmp_path
):
    if cuda_visible_devices:
        # Test if `get_device` is correct even with user specified env var.
        monkeypatch.setenv("CUDA_VISIBLE_DEVICES", cuda_visible_devices)

    ray.init(num_cpus=4, num_gpus=2)

    def train_fn():
        # Make sure environment variable is being set correctly.
        if cuda_visible_devices:
            visible_devices = os.environ["CUDA_VISIBLE_DEVICES"]
            assert visible_devices == "1,2"

        devices = (
            sorted([device.index for device in train.torch.get_device()])
            if num_gpus_per_worker > 1
            else train.torch.get_device().index
        )
        write_rank_data(tmp_path, devices)

    trainer = TorchTrainer(
        train_fn,
        scaling_config=ScalingConfig(
            num_workers=int(2 / num_gpus_per_worker),
            use_gpu=True,
            resources_per_worker={"GPU": num_gpus_per_worker},
        ),
    )
    trainer.fit()

    rank_data = get_data_from_all_ranks(tmp_path)
    devices = list(rank_data.values())

    if num_gpus_per_worker == 0.5:
        assert sorted(devices) == [0, 0, 1, 1]
    elif num_gpus_per_worker == 1:
        assert sorted(devices) == [0, 1]
    elif num_gpus_per_worker == 2:
        assert sorted(devices[0]) == [0, 1]
    else:
        raise RuntimeError(
            "New parameter for this test has been added without checking that the "
            "correct devices have been returned."
        )


@pytest.mark.parametrize("num_gpus_per_worker", [0.5, 1, 2])
def test_torch_get_device_dist(ray_2_node_2_gpu, num_gpus_per_worker, tmp_path):
    @patch("torch.cuda.is_available", lambda: True)
    def train_fn():
        devices = (
            sorted([device.index for device in train.torch.get_device()])
            if num_gpus_per_worker > 1
            else train.torch.get_device().index
        )
        write_rank_data(tmp_path, devices)

    trainer = TorchTrainer(
        train_fn,
        # use gloo instead of nccl, since nccl is not supported
        # on this virtual gpu ray environment
        torch_config=TorchConfig(backend="gloo"),
        scaling_config=ScalingConfig(
            num_workers=int(4 / num_gpus_per_worker),
            use_gpu=True,
            resources_per_worker={"GPU": num_gpus_per_worker},
        ),
    )
    trainer.fit()

    rank_data = get_data_from_all_ranks(tmp_path)
    devices = list(rank_data.values())

    # cluster setups: 2 nodes, 2 gpus per node
    # `CUDA_VISIBLE_DEVICES` is set to "0,1" on node 1 and node 2
    if num_gpus_per_worker == 0.5:
        # worker gpu topology:
        # 4 workers on node 1, 4 workers on node 2
        # `ray.get_gpu_ids()` returns [0], [0], [1], [1] on node 1
        # and [0], [0], [1], [1] on node 2
        assert sorted(devices) == [0, 0, 0, 0, 1, 1, 1, 1]
    elif num_gpus_per_worker == 1:
        # worker gpu topology:
        # 2 workers on node 1, 2 workers on node 2
        # `ray.get_gpu_ids()` returns [0], [1] on node 1 and [0], [1] on node 2
        assert sorted(devices) == [0, 0, 1, 1]
    elif num_gpus_per_worker == 2:
        # worker gpu topology:
        # 1 workers on node 1, 1 workers on node 2
        # `ray.get_gpu_ids()` returns {0, 1} on node 1 and {0, 1} on node 2
        # and `device_id` returns the one index from each set.
        # So total count of devices should be 2.
        assert devices == [[0, 1], [0, 1]]
    else:
        raise RuntimeError(
            "New parameter for this test has been added without checking that the "
            "correct devices have been returned."
        )


def test_torch_prepare_model(ray_start_4_cpus_2_gpus):
    """Tests if ``prepare_model`` correctly wraps in DDP."""

    def train_fn():
        model = torch.nn.Linear(1, 1)

        # Wrap in DDP.
        model = train.torch.prepare_model(model)

        # Make sure model is wrapped in DDP.
        assert isinstance(model, DistributedDataParallel)

        # Make sure model is on cuda.
        assert next(model.parameters()).is_cuda

    trainer = TorchTrainer(
        train_fn, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    trainer.fit()

    def train_fn_manual_override():
        model = torch.nn.Linear(1, 1)

        # Wrap in DDP and manually specify CPU.
        model = train.torch.prepare_model(model, device=torch.device("cpu"))

        # Make sure model is wrapped in DDP.
        assert isinstance(model, DistributedDataParallel)

        # Make sure model is NOT on cuda since we manually specified CPU.
        assert not next(model.parameters()).is_cuda

    trainer = TorchTrainer(
        train_fn, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    trainer.fit()


def test_torch_prepare_model_uses_device(ray_start_4_cpus_2_gpus):
    """Tests if `prepare_model` uses the train.torch.get_device even if it does not
    match with the local rank."""
    # The below test should pass without errors.

    @patch.object(
        ray.train.torch.train_loop_utils,
        "get_device",
        lambda: torch.device(f"cuda:{1 - train.get_context().get_local_rank()}"),
    )
    def train_func():
        # These assert statements must hold for prepare_model to wrap with DDP.
        assert torch.cuda.is_available()
        assert train.get_context().get_world_size() > 1
        model = torch.nn.Linear(1, 1)
        data = torch.ones(1)
        data = data.to(train.torch.get_device())
        model = train.torch.prepare_model(model)
        model(data)

    trainer = TorchTrainer(
        train_func, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    trainer.fit()


@pytest.mark.parametrize(
    "dataset", (LinearDataset, LinearDatasetDict, NonTensorDataset)
)
def test_torch_prepare_dataloader(ray_start_4_cpus_2_gpus, dataset):
    data_loader = DataLoader(dataset(a=1, b=2, size=10))

    def train_fn():
        wrapped_data_loader = train.torch.prepare_data_loader(data_loader)

        # Check that DistributedSampler has been added to the data loader.
        assert isinstance(wrapped_data_loader.sampler, DistributedSampler)

        # Make sure you can properly iterate through the DataLoader.
        # Case where the dataset returns a tuple or list from __getitem__.
        if isinstance(dataset, LinearDataset):
            for batch in wrapped_data_loader:
                x = batch[0]
                y = batch[1]

                # Make sure the data is on the correct device.
                assert x.is_cuda and y.is_cuda
        # Case where the dataset returns a dict from __getitem__.
        elif isinstance(dataset, LinearDatasetDict):
            for batch in wrapped_data_loader:
                for x, y in zip(batch["x"], batch["y"]):
                    # Make sure the data is on the correct device.
                    assert x.is_cuda and y.is_cuda

        elif isinstance(dataset, NonTensorDataset):
            for batch in wrapped_data_loader:
                for x, y in zip(batch["x"], batch["y"]):
                    # Make sure the data is on the correct device.
                    assert x.is_cuda and y == 2

    trainer = TorchTrainer(
        train_fn, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    trainer.fit()


@pytest.mark.parametrize("data_loader_num_workers", (0, 2))
def test_enable_reproducibility(ray_start_4_cpus_2_gpus, data_loader_num_workers):
    # NOTE: Reproducible results aren't guaranteed between seeded executions, even with
    # identical hardware and software dependencies. This test should be okay given that
    # it only runs for two epochs on a small dataset.
    # NOTE: I've chosen to use a ResNet model over a more simple model, because
    # `enable_reproducibility` disables CUDA convolution benchmarking, and a simpler
    # model (e.g., linear) might not test this feature.
    def train_func():
        train.torch.enable_reproducibility()

        model = torchvision.models.resnet18()
        model = train.torch.prepare_model(model)

        dataset_length = 128
        dataset = torch.utils.data.TensorDataset(
            torch.randn(dataset_length, 3, 32, 32),
            torch.randint(low=0, high=1000, size=(dataset_length,)),
        )

        # num_workers > 0 tests for https://github.com/ray-project/ray/issues/30247
        dataloader = torch.utils.data.DataLoader(
            dataset, batch_size=64, num_workers=data_loader_num_workers
        )
        dataloader = train.torch.prepare_data_loader(dataloader)

        optimizer = torch.optim.SGD(model.parameters(), lr=0.001)

        model.train()
        for epoch in range(2):
            for images, targets in dataloader:
                optimizer.zero_grad()

                outputs = model(images)
                loss = torch.nn.functional.cross_entropy(outputs, targets)

                loss.backward()
                optimizer.step()

        train.report(dict(loss=loss.item()))

    trainer = TorchTrainer(
        train_func, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    result1 = trainer.fit()

    trainer = TorchTrainer(
        train_func, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    result2 = trainer.fit()

    assert result1.metrics["loss"] == result2.metrics["loss"]


@pytest.mark.parametrize("nccl_socket_ifname", ["", "ens3"])
def test_torch_backend_nccl_socket_ifname(ray_start_4_cpus_2_gpus, nccl_socket_ifname):
    worker_group = WorkerGroup(num_workers=2, num_gpus_per_worker=1)

    if nccl_socket_ifname:

        def set_env_var():
            os.environ["NCCL_SOCKET_IFNAME"] = nccl_socket_ifname

        worker_group.execute(set_env_var)

    def assert_env_var_set():
        value = nccl_socket_ifname if nccl_socket_ifname else DEFAULT_NCCL_SOCKET_IFNAME
        assert os.environ["NCCL_SOCKET_IFNAME"] == value

    torch_backend = _TorchBackend()
    torch_backend.on_start(worker_group, backend_config=TorchConfig(backend="nccl"))

    worker_group.execute(assert_env_var_set)


def test_torch_fail_on_nccl_timeout(ray_start_4_cpus_2_gpus):
    """Tests that TorchTrainer raises exception on NCCL timeouts."""

    def train_fn():
        model = torch.nn.Linear(1, 1)
        model = train.torch.prepare_model(model)

        # Rank 0 worker will never reach the collective operation.
        # NCCL should timeout.
        if train.get_context().get_world_rank() == 0:
            while True:
                time.sleep(100)

        torch.distributed.barrier()

    trainer = TorchTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=True),
        torch_config=TorchConfig(timeout_s=5),
    )

    # Training should fail and not hang.
    with pytest.raises(TrainingFailedError) as exc_info:
        trainer.fit()
    assert isinstance(exc_info.value.__cause__, RayTaskError)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
