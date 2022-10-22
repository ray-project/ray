import pytest
import torch
from torch.distributed.fsdp import FullyShardedDataParallel

import ray

from ray import train
from ray.train.torch import TorchTrainer
from ray.air.config import ScalingConfig


@pytest.fixture
def ray_start_4_cpus_2_gpus():
    address_info = ray.init(num_cpus=4, num_gpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


def test_torch_fsdp(ray_start_4_cpus_2_gpus):
    """Tests if ``prepare_model`` correctly wraps in FSDP."""

    def train_fn():
        model = torch.nn.Linear(1, 1)

        # Wrap in FSDP.
        model = train.torch.prepare_model(model, parallel_strategy="fsdp")

        # Make sure model is wrapped in FSDP.
        assert isinstance(model, FullyShardedDataParallel)

        # Make sure the model is on cuda.
        assert next(model.parameters()).is_cuda

    trainer = TorchTrainer(
        train_fn, scaling_config=ScalingConfig(num_workers=2, use_gpu=True)
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", "-s", __file__]))
