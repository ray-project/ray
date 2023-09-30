import os

import pytest
import sys

import ray
from ray.train import CheckpointConfig
from ray.air.execution import FixedResourceManager
from ray.air.constants import TRAINING_ITERATION
from ray.train._internal.storage import StorageContext
from ray.tune import Trainable, register_trainable
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial

from ray.train.tests.util import mock_storage_context

STORAGE = mock_storage_context()


@pytest.fixture(scope="function")
def ray_start_4_cpus_2_gpus_extra():
    address_info = ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
    yield address_info
    ray.shutdown()


@pytest.mark.parametrize("trainable_type", ["class"])  # , "function", "data_parallel"])
@pytest.mark.parametrize("patch_iter", [False, True])
def test_checkpoint_freq_dir_name(
    ray_start_4_cpus_2_gpus_extra, trainable_type, patch_iter, tmp_path
):
    """Test that trial checkpoints"""

    def num_checkpoints(trial):
        return sum(
            item.startswith("checkpoint_") for item in os.listdir(trial.local_path)
        )

    def last_checkpoint_dir(trial):
        return max(
            item
            for item in os.listdir(trial.local_path)
            if item.startswith("checkpoint_")
        )

    if trainable_type == "class":

        class MyTrainable(Trainable):
            def step(self):
                return {"metric": self.iteration + 100}

            def save_checkpoint(self, checkpoint_dir):
                return {"test": self.iteration}

            def load_checkpoint(self, checkpoint_dir):
                pass

        register_trainable("test_checkpoint_freq", MyTrainable)
    else:
        raise RuntimeError("Invalid trainable type")

    if patch_iter:

        class CustomStorageContext(StorageContext):
            def _update_checkpoint_index(self, metrics):
                self.current_checkpoint_index = metrics.get(
                    "training_iteration", self.current_checkpoint_index + 1
                )

        storage = mock_storage_context(
            delete_syncer=False, storage_context_cls=CustomStorageContext
        )
    else:
        storage = mock_storage_context(delete_syncer=False)

    trial = Trial(
        "test_checkpoint_freq",
        checkpoint_config=CheckpointConfig(checkpoint_frequency=3),
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
