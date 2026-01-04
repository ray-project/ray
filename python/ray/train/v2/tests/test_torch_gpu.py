import time
from typing import List

import pytest
import torch
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DataLoader, DistributedSampler

import ray
from ray.train import RunConfig, ScalingConfig
from ray.train.examples.pytorch.torch_linear_example import LinearDataset
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.worker_group import Worker
from ray.train.v2.api.exceptions import WorkerGroupError


def test_torch_trainer_cuda_initialization(ray_start_4_cpus_2_gpus):
    """Test that Torch CUDA initialization works with TorchTrainer.

    This test verifies that PyTorch can properly initialize CUDA on multiple
    workers before the training context is set up, ensuring that GPU resources
    are available and accessible across all training workers.

    See https://github.com/ray-project/ray/pull/56509 for more details.
    """

    def train_func():
        """Empty training function for this initialization test.

        Since we're only testing CUDA initialization, the actual training
        logic is not needed for this test case.
        """
        pass

    def init_torch():
        """Trigger (lazy) initialization of CUDA."""
        torch.cuda.is_available()

    class InitTorchCallback(WorkerGroupCallback):
        """Callback to initialize PyTorch CUDA before training begins.

        Implements before_init_train_context because this is where torch is typically imported,
        ensuring that the CUDA environment is properly initialized.
        """

        def before_init_train_context(self, workers: List[Worker]):
            """Execute CUDA initialization on all workers."""
            futures = []
            for worker in workers:
                futures.append(worker.execute_async(init_torch))
            ray.get(futures)
            return {}

    callback = InitTorchCallback()

    trainer = TorchTrainer(
        train_func,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=True),
        run_config=RunConfig(callbacks=[callback]),
    )

    trainer.fit()


@pytest.mark.parametrize("num_gpus_per_worker", [0.5, 1, 2])
def test_torch_get_devices(ray_start_2x2_gpu_cluster, num_gpus_per_worker):
    # cluster setups: 2 nodes, 2 gpus per node
    # `CUDA_VISIBLE_DEVICES` is set to "0,1" on node 1 and node 2
    if num_gpus_per_worker == 0.5:
        # worker gpu topology:
        # 4 workers on node 1, 4 workers on node 2
        # `ray.get_gpu_ids()` returns [0], [0], [1], [1] on node 1
        # and [0], [0], [1], [1] on node 2
        expected_devices_per_rank = [[0], [0], [1], [1], [0], [0], [1], [1]]
    elif num_gpus_per_worker == 1:
        # worker gpu topology:
        # 2 workers on node 1, 2 workers on node 2
        # `ray.get_gpu_ids()` returns [0], [1] on node 1 and [0], [1] on node 2
        expected_devices_per_rank = [[0], [1], [0], [1]]
    elif num_gpus_per_worker == 2:
        # worker gpu topology:
        # 1 workers on node 1, 1 workers on node 2
        # `ray.get_gpu_ids()` returns {0, 1} on node 1 and {0, 1} on node 2
        # and `device_id` returns the one index from each set.
        # So total count of devices should be 2.
        expected_devices_per_rank = [[0, 1], [0, 1]]
    else:
        raise RuntimeError(
            "New parameter for this test has been added without checking that the "
            "correct devices have been returned."
        )

    def train_fn():
        assert torch.cuda.current_device() == ray.train.torch.get_device().index
        devices = sorted([device.index for device in ray.train.torch.get_devices()])
        rank = ray.train.get_context().get_world_rank()
        assert devices == expected_devices_per_rank[rank]

    trainer = TorchTrainer(
        train_fn,
        scaling_config=ray.train.ScalingConfig(
            num_workers=int(4 / num_gpus_per_worker),
            use_gpu=True,
            resources_per_worker={"GPU": num_gpus_per_worker},
        ),
    )
    trainer.fit()


def test_torch_prepare_model(ray_start_4_cpus_2_gpus):
    """Tests if ``prepare_model`` correctly wraps in DDP."""

    def train_fn():
        model = torch.nn.Linear(1, 1)

        # Wrap in DDP.
        model = ray.train.torch.prepare_model(model)

        # Make sure model is wrapped in DDP.
        assert isinstance(model, DistributedDataParallel)

        # Make sure model is on cuda.
        assert next(model.parameters()).is_cuda

    trainer = TorchTrainer(
        train_fn, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    trainer.fit()


class LinearDatasetDict(LinearDataset):
    """Modifies the LinearDataset to return a Dict instead of a Tuple."""

    def __getitem__(self, index):
        return {"x": self.x[index, None], "y": self.y[index, None]}


class NonTensorDataset(LinearDataset):
    """Modifies the LinearDataset to also return non-tensor objects."""

    def __getitem__(self, index):
        return {"x": self.x[index, None], "y": 2}


@pytest.mark.parametrize(
    "dataset", (LinearDataset, LinearDatasetDict, NonTensorDataset)
)
def test_torch_prepare_dataloader(ray_start_4_cpus_2_gpus, dataset):
    data_loader = DataLoader(dataset(a=1, b=2, size=10))

    def train_fn():
        wrapped_data_loader = ray.train.torch.prepare_data_loader(data_loader)

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


def test_torch_fail_on_nccl_timeout(ray_start_4_cpus_2_gpus):
    """Tests that TorchTrainer raises exception on NCCL timeouts."""

    def train_fn():
        model = torch.nn.Linear(1, 1)
        model = ray.train.torch.prepare_model(model)

        # Rank 0 worker will never reach the collective operation.
        # NCCL should timeout.
        if ray.train.get_context().get_world_rank() == 0:
            while True:
                time.sleep(100)

        torch.distributed.barrier()

    trainer = TorchTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=True),
        torch_config=ray.train.torch.TorchConfig(timeout_s=2),
    )

    # Training should fail and not hang.
    with pytest.raises(WorkerGroupError):
        trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
