import os
import sys
import pytest

from ray.train import Checkpoint
from ray.train._internal.session import _TrainingResult
from ray.train._internal.storage import StorageContext
from ray.tune.experiment import Trial

from ray.train.tests.util import mock_storage_context


def test_load_trial_from_json_state():
    """Check that serializing a trial to a JSON string with `Trial.get_json_state`
    and then creating a new trial using the `Trial.from_json_state` alternate
    constructor loads the trial with equivalent state."""
    trial = Trial(
        "MockTrainable",
        stub=True,
        trial_id="abcd1234",
        storage=mock_storage_context(),
    )
    trial.create_placement_group_factory()
    trial.init_local_path()
    trial.status = Trial.TERMINATED

    # After loading, the trial state should be the same
    json_state, _ = trial.get_json_state()
    new_trial = Trial.from_json_state(json_state, stub=True)
    assert new_trial.get_json_state()[0] == json_state


def test_set_storage(tmp_path):
    """Test that setting the trial's storage context will update the tracked
    checkpoint paths."""
    original_storage = mock_storage_context()
    trial = Trial(
        "MockTrainable",
        stub=True,
        trial_id="abcd1234",
        storage=original_storage,
    )

    result_1 = _TrainingResult(
        checkpoint=Checkpoint.from_directory(original_storage.checkpoint_fs_path),
        metrics={},
    )
    trial.on_checkpoint(result_1)

    result_2 = _TrainingResult(
        checkpoint=Checkpoint.from_directory(original_storage.checkpoint_fs_path),
        metrics={},
    )
    trial.on_checkpoint(result_2)

    new_storage = StorageContext(
        storage_path=tmp_path / "new_storage_path",
        experiment_dir_name="new_name",
        trial_dir_name="new_trial",
    )
    trial.set_storage(new_storage)

    assert result_1.checkpoint.path.startswith(new_storage.trial_fs_path)
    assert result_2.checkpoint.path.startswith(new_storage.trial_fs_path)


def test_trial_logdir_length():
    """Test that trial local paths with a long logdir are truncated"""
    trial = Trial(
        trainable_name="none",
        stub=True,
        config={"a" * 50: 5.0 / 7, "b" * 50: "long" * 40},
        storage=mock_storage_context(),
    )
    trial.init_local_path()
    assert len(os.path.basename(trial.local_path)) < 200


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
