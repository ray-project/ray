from typing import List

import pytest
import torch

import ray
from ray.train import RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer
from ray.train.v2._internal.execution.callback import WorkerGroupCallback
from ray.train.v2._internal.execution.worker_group import Worker


def test_torch_trainer_cuda_initialization():
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
