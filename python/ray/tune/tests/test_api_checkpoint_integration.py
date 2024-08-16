import os
import sys
import tempfile
from pathlib import Path

import pytest

import ray
from ray import train
from ray.air import ScalingConfig
from ray.air.constants import TRAINING_ITERATION
from ray.air.execution import FixedResourceManager
from ray.train import CheckpointConfig
from ray.train._internal.storage import StorageContext
from ray.train.tests.util import mock_storage_context
from ray.tune import Trainable, register_trainable
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial

STORAGE = mock_storage_context()


@pytest.fixture(scope="function")
def ray_start_4_cpus_2_gpus_extra():
    address_info = ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
    yield address_info
    ray.shutdown()


@pytest.mark.parametrize("trainable_type", ["class", "function", "data_parallel"])
@pytest.mark.parametrize("patch_iter", [False, True])
def test_checkpoint_freq_dir_name(
    ray_start_4_cpus_2_gpus_extra, trainable_type, patch_iter, tmp_path
):
    """Test that trial checkpoint IDs are correctly set across trainable types.

    This includes a current workaround to set checkpoint IDs according to reported
    metrics.
    """

    def num_checkpoints(trial):
        return sum(
            item.startswith("checkpoint_")
            for item in os.listdir(trial.storage.trial_fs_path)
        )

    def last_checkpoint_dir(trial):
        return max(
            item
            for item in os.listdir(trial.storage.trial_fs_path)
            if item.startswith("checkpoint_")
        )

    checkpoint_config = None

    if trainable_type == "class":

        class MyTrainable(Trainable):
            def step(self):
                # `training_iteration` is increased after the report, so we
                # +1 here.
                return {"step": self.iteration + 1}

            def save_checkpoint(self, checkpoint_dir):
                return {"test": self.iteration}

            def load_checkpoint(self, checkpoint_dir):
                pass

        register_trainable("test_checkpoint_freq", MyTrainable)
        checkpoint_config = CheckpointConfig(checkpoint_frequency=3)

    elif trainable_type in {"function", "data_parallel"}:

        def train_fn(config):
            for step in range(1, 10):
                if step > 0 and step % 3 == 0:
                    with tempfile.TemporaryDirectory() as checkpoint_dir:
                        (Path(checkpoint_dir) / "data.ckpt").write_text(str(step))
                        train.report(
                            {"step": step},
                            checkpoint=train.Checkpoint.from_directory(checkpoint_dir),
                        )
                else:
                    train.report({"step": step})

        if trainable_type == "function":
            register_trainable("test_checkpoint_freq", train_fn)
        elif trainable_type == "data_parallel":
            from ray.train.data_parallel_trainer import DataParallelTrainer

            trainer = DataParallelTrainer(
                train_loop_per_worker=train_fn,
                scaling_config=ScalingConfig(num_workers=1),
            )
            register_trainable("test_checkpoint_freq", trainer.as_trainable())

    else:
        raise RuntimeError("Invalid trainable type")

    if patch_iter:

        class CustomStorageContext(StorageContext):
            def _update_checkpoint_index(self, metrics):
                # Todo: Support auto-fille metrics for function trainables
                self.current_checkpoint_index = metrics.get(
                    "step", self.current_checkpoint_index + 1
                )

        storage = mock_storage_context(
            storage_context_cls=CustomStorageContext,
            storage_path=tmp_path,
        )
    else:
        storage = mock_storage_context(storage_path=tmp_path)

    trial = Trial(
        "test_checkpoint_freq",
        checkpoint_config=checkpoint_config,
        storage=storage,
    )
    runner = TuneController(
        resource_manager_factory=lambda: FixedResourceManager(),
        storage=STORAGE,
        checkpoint_period=0,
    )
    runner.add_trial(trial)

    while not trial.is_saving:
        runner.step()
    runner.step()
    assert trial.last_result[TRAINING_ITERATION] == 3
    assert num_checkpoints(trial) == 1

    if patch_iter:
        assert last_checkpoint_dir(trial) == "checkpoint_000003"
    else:
        assert last_checkpoint_dir(trial) == "checkpoint_000000"

    while not trial.is_saving:
        runner.step()
    runner.step()
    assert trial.last_result[TRAINING_ITERATION] == 6
    assert num_checkpoints(trial) == 2

    if patch_iter:
        assert last_checkpoint_dir(trial) == "checkpoint_000006"
    else:
        assert last_checkpoint_dir(trial) == "checkpoint_000001"

    while not trial.is_saving:
        runner.step()
    runner.step()
    assert trial.last_result[TRAINING_ITERATION] == 9
    assert num_checkpoints(trial) == 3

    if patch_iter:
        assert last_checkpoint_dir(trial) == "checkpoint_000009"
    else:
        assert last_checkpoint_dir(trial) == "checkpoint_000002"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
