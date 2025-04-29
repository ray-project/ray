import sys

import pytest

import ray.train
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2._internal.execution.callback import ControllerCallback


def test_captured_imports(ray_start_4_cpus):
    import torch

    def train_fn():
        # torch is captured in the closure of the train_fn
        # and should be re-imported on each worker.
        torch.ones(1)

    class AssertImportsCallback(ControllerCallback):
        def after_controller_start(self):
            # Check that torch is not imported in the controller process.
            # The train_fn should be deserialized directly on the workers.
            assert "torch" not in sys.modules

    trainer = DataParallelTrainer(
        train_fn,
        run_config=ray.train.RunConfig(callbacks=[AssertImportsCallback()]),
        scaling_config=ray.train.ScalingConfig(num_workers=2),
    )
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
