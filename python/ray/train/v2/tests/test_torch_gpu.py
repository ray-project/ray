from typing import List

import pytest
import torch

import ray
from ray.train import RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.worker_group import Worker


def test_torch_trainer_cuda_initialization():
    """Test that Torch CUDA initialization works with TorchTrainer."""

    def train_func():
        pass

    def init_torch():
        return torch.cuda.is_available()

    class InitTorchCallback(WorkerGroupCallback):
        def before_init_train_context(self, workers: List[Worker]):
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
