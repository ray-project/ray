import os
import time

from unittest.mock import patch
import pytest
import numpy as np
import torch
import torchvision
from torch.nn.parallel import DistributedDataParallel
from torch.utils.data import DataLoader, DistributedSampler

import ray
import ray.data
from ray.exceptions import RayTaskError
from ray.air import session
from ray import tune

import ray.train as train
from ray.air.config import ScalingConfig
from ray.train.constants import DEFAULT_NCCL_SOCKET_IFNAME
from ray.train.examples.pytorch.torch_linear_example import LinearDataset
from ray.train.torch.config import TorchConfig, _TorchBackend
from ray.train.torch.torch_trainer import TorchTrainer
from ray.train.trainer import TrainingFailedError
from ray.train._internal.worker_group import WorkerGroup


class LinearDatasetDict(LinearDataset):
    """Modifies the LinearDataset to return a Dict instead of a Tuple."""

    def __getitem__(self, index):
        return {"x": self.x[index, None], "y": self.y[index, None]}


class NonTensorDataset(LinearDataset):
    """Modifies the LinearDataset to also return non-tensor objects."""

    def __getitem__(self, index):
        return {"x": self.x[index, None], "y": 2}


# Currently in DataParallelTrainers we only report metrics from rank 0.
# For testing purposes here, we need to be able to report from all
# workers.
class TorchTrainerPatchedMultipleReturns(TorchTrainer):
    def _report(self, training_iterator) -> None:
        for results in training_iterator:
            tune.report(results=results)


@pytest.mark.parametrize("use_gpu", (True, False))
def test_torch_iter_torch_batches_auto_device(ray_start_4_cpus_2_gpus, use_gpu):
    """
    Tests that iter_torch_batches in TorchTrainer worker function uses the
    default device.
    """

    def train_fn():
        dataset = train.get_dataset_shard("train")
        for batch in dataset.iter_torch_batches(dtypes=torch.float, device="cpu"):
            assert str(batch["data"].device) == "cpu"

        # Autodetect
        for batch in dataset.iter_torch_batches(dtypes=torch.float):
            assert str(batch["data"].device) == str(train.torch.get_device())

    dataset = ray.data.from_numpy(np.array([[1, 2, 3, 4, 5], [1, 2, 3, 4, 5]]).T)
    # Test that this works outside a Train function
    for batch in dataset.iter_torch_batches(dtypes=torch.float, device="cpu"):
        assert str(batch["data"].device) == "cpu"

    trainer = TorchTrainer(
        train_fn,
        scaling_config=ScalingConfig(num_workers=2, use_gpu=use_gpu),
        datasets={"train": dataset},
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
