import os
import shutil
import sys
import tempfile
import pytest

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


def test_change_trial_local_dir(tmpdir):
    # TODO(justinvyu): [handle_moved_storage_path]
    pytest.skip("Changing the storage path is not supported yet.")


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
