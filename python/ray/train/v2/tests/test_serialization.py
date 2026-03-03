import sys

import pytest

import ray
import ray.cloudpickle as ray_pickle
from ray.train.v2._internal.execution.callback import (
    ControllerCallback,
    WorkerGroupCallback,
)
from ray.train.v2._internal.execution.context import TrainRunContext
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer
from ray.train.v2.api.exceptions import ControllerError, WorkerGroupError


def block_import(import_name):
    import sys

    class BlockTorchImport:
        def find_spec(self, fullname, path, target=None):
            if fullname == import_name or fullname.startswith(import_name + "."):
                raise ImportError(
                    f"Test error: {import_name} not installed on this node"
                )

    sys.meta_path.insert(0, BlockTorchImport())


def test_captured_imports(ray_start_4_cpus):
    import torch

    def capture_torch_import_fn():
        # torch is captured in the closure of the train_fn
        # and should be re-imported on each worker.
        torch.ones(1)

    class AssertImportsCallback(ControllerCallback):
        def after_controller_start(self, train_run_context: TrainRunContext):
            # Check that torch is not imported in the controller process.
            # The train_fn should be deserialized directly on the workers.
            assert "torch" not in sys.modules

    trainer = DataParallelTrainer(
        capture_torch_import_fn,
        run_config=ray.train.RunConfig(callbacks=[AssertImportsCallback()]),
        scaling_config=ray.train.ScalingConfig(num_workers=2),
    )
    trainer.fit()


def test_deserialization_error(ray_start_4_cpus):
    """Test that train_fn deserialization errors are propagated properly.

    This test showcases a common deserialization error example, where
    the driver script successfully imports torch, but torch is not
    installed on the worker nodes.
    """
    import torch

    def capture_torch_import_fn():
        torch.ones(1)

    class BlockTorchImportCallback(WorkerGroupCallback):
        def after_worker_group_start(self, worker_group):
            # Make it so that the torch import that happens on
            # train_fn deserialization will fail on workers.
            worker_group.execute(block_import, "torch")

    trainer = DataParallelTrainer(
        capture_torch_import_fn,
        run_config=ray.train.RunConfig(callbacks=[BlockTorchImportCallback()]),
        scaling_config=ray.train.ScalingConfig(num_workers=2),
    )
    with pytest.raises(ControllerError, match="torch not installed on this node"):
        trainer.fit()


@pytest.mark.parametrize(
    "error",
    [
        WorkerGroupError(
            "Training failed on multiple workers",
            {0: ValueError("worker 0 failed"), 1: RuntimeError("worker 1 failed")},
        ),
        ControllerError(Exception("Controller crashed")),
    ],
)
def test_exceptions_are_picklable(error):
    """Test that WorkerGroupError and ControllerError are picklable."""

    # Test pickle/unpickle for WorkerGroupError
    pickled_error = ray_pickle.dumps(error)
    unpickled_error = ray_pickle.loads(pickled_error)

    # Verify attributes are preserved
    assert str(unpickled_error) == str(error)
    assert type(unpickled_error) is type(error)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
