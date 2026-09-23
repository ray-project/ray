import logging
import sys

import pytest

from ray.exceptions import RayActorError, RayTaskError
from ray.tests.conftest import propagate_logs  # noqa
from ray.train._internal.session import _TrainingResult
from ray.train._internal.storage import StorageContext
from ray.train.constants import RAY_TRAIN_COUNT_PREEMPTION_AS_FAILURE
from ray.train.tests.util import mock_storage_context
from ray.tune import Checkpoint
from ray.tune.experiment import Trial


@pytest.fixture
def trial(tmp_path):
    yield Trial(
        "mock",
        stub=True,
        storage=mock_storage_context(storage_path=str(tmp_path)),
    )


@pytest.mark.parametrize("count_preemption_errors", [False, True])
def test_handle_preemption_error(
    trial: Trial, count_preemption_errors: bool, monkeypatch
):
    """Check that the Trial counts preemption errors correctly."""
    if count_preemption_errors:
        monkeypatch.setenv(RAY_TRAIN_COUNT_PREEMPTION_AS_FAILURE, "1")

    # Case 1: Directly raised (preemption) RayActorError
    class PreemptionRayActorError(RayActorError):
        def preempted(self) -> bool:
            return True

    err = PreemptionRayActorError()
    trial.handle_error(err)
    assert trial.num_failures == (1 if count_preemption_errors else 0)

    # Case 2: RayTaskError, where the cause is a (preemption) RayActorError
    wrapped_err = RayTaskError(
        function_name="test", traceback_str="traceback_str", cause=err
    )
    trial.handle_error(wrapped_err)
    assert trial.num_failures == (2 if count_preemption_errors else 0)

    # Case 3: Non-preemption error
    non_preempted_err = RayActorError()
    trial.handle_error(non_preempted_err)
    assert trial.num_failures == (3 if count_preemption_errors else 1)


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
    assert len(trial.storage.trial_dir_name) < 200


def test_generate_dirname_strips_reserved_characters(tmp_path):
    """Test that `Trial._generate_dirname` strips characters that are illegal
    in Windows directory names (and unsafe for rsync on any platform), e.g.
    an env id like "ale_py:ALE/Pong-v5" (see #49477)."""
    trial = Trial(
        trainable_name="PPO",
        stub=True,
        config={"env": "ale_py:ALE/Pong-v5"},
        storage=mock_storage_context(storage_path=str(tmp_path)),
    )
    dirname = trial._generate_dirname()

    illegal_chars = set('/()<>:"\\|?*')
    assert not (
        illegal_chars & set(dirname)
    ), f"generated dirname {dirname!r} still contains a reserved character"

    # The directory must actually be creatable -- this is what fails on
    # Windows with a raw ":" in the name (NotADirectoryError / WinError 267).
    (tmp_path / dirname).mkdir()


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("simple_name", "simple_name"),
        ('a<b>c:d"e/f\\g|h?i*j(k)l', "a_b_c_d_e_f_g_h_i_j_k_l"),
    ],
)
def test_generate_dirname_custom_dirname_is_sanitized(tmp_path, raw, expected):
    """`custom_dirname` goes through the same sanitization as the generated
    name, so a user-supplied trial name with reserved characters is still
    safe to use as a directory name."""
    trial = Trial(
        trainable_name="PPO",
        stub=True,
        storage=mock_storage_context(storage_path=str(tmp_path)),
    )
    trial.custom_dirname = raw
    assert trial._generate_dirname() == expected


def test_should_stop(caplog, propagate_logs):  # noqa
    """Test whether `Trial.should_stop()` works as expected given a result dict."""
    trial = Trial(
        "MockTrainable",
        stub=True,
        trial_id="abcd1234",
        stopping_criterion={"a": 10.0, "b/c": 20.0},
    )

    # Criterion is not reached yet -> don't stop.
    result = {"a": 9.999, "b/c": 0.0, "some_other_key": True}
    assert not trial.should_stop(result)

    # Criterion is exactly reached -> stop.
    result = {"a": 10.0, "b/c": 0.0, "some_other_key": False}
    assert trial.should_stop(result)

    # Criterion is exceeded -> stop.
    result = {"a": 10000.0, "b/c": 0.0, "some_other_key": False}
    assert trial.should_stop(result)

    # Test nested criterion.
    result = {"a": 5.0, "b/c": 1000.0, "some_other_key": False}
    assert trial.should_stop(result)

    # Test criterion NOT found in result metrics.
    result = {"b/c": 1000.0}
    with caplog.at_level(logging.WARNING):
        trial.should_stop(result)
    assert (
        "Stopping criterion 'a' not found in result dict! Available keys are ['b/c']."
    ) in caplog.text


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
