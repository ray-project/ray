import logging
import os
from pathlib import Path
import pickle
import tempfile
from typing import Callable
import pytest
import sys
import time

import ray
from ray import train, tune, logger
from ray.train import CheckpointConfig
from ray.train.constants import RAY_AIR_NEW_PERSISTENCE_MODE
from ray.tune import Trainable, run_experiments, register_trainable
from ray.tune.error import TuneError
from ray.tune.result_grid import ResultGrid
from ray.tune.schedulers.trial_scheduler import FIFOScheduler, TrialScheduler
from ray.tune.tune import _check_mixin

from ray.train.tests.util import create_dict_checkpoint, load_dict_checkpoint


@pytest.fixture
def ray_start_1_cpu():
    address_info = ray.init(num_cpus=1)
    os.environ["TUNE_STATE_REFRESH_PERIOD"] = "0.1"
    yield address_info
    ray.shutdown()
    os.environ.pop("TUNE_STATE_REFRESH_PERIOD", None)


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    ray.shutdown()


@pytest.fixture
def ray_start_4_cpus_extra():
    address_info = ray.init(num_cpus=4, resources={"extra": 4})
    yield address_info
    ray.shutdown()


class FrequentPausesScheduler(FIFOScheduler):
    def on_trial_result(self, tune_controller, trial, result):
        return TrialScheduler.PAUSE


class MyResettableClass(Trainable):
    def setup(self, config):
        self.config = config
        self.num_resets = 0
        self.iter = 0
        self.msg = config.get("message", None)
        self.sleep = int(config.get("sleep", 0))
        self.fail = config.get("fail", False)

    def step(self):
        self.iter += 1

        if self.msg:
            print("PRINT_STDOUT: {}".format(self.msg))
            print("PRINT_STDERR: {}".format(self.msg), file=sys.stderr)
            logger.info("LOG_STDERR: {}".format(self.msg))

        if self.fail:
            raise RuntimeError("Failing")

        if self.sleep:
            time.sleep(self.sleep)

        return {
            "id": self.config.get("id", 0),
            "num_resets": self.num_resets,
            "done": self.iter > 1,
            "iter": self.iter,
        }

    def save_checkpoint(self, chkpt_dir):
        return {"iter": self.iter}

    def load_checkpoint(self, item):
        self.iter = item["iter"]

    def reset_config(self, new_config):
        if "fake_reset_not_supported" in self.config:
            return False
        self.num_resets += 1
        self.iter = 0
        self.msg = new_config.get("message", None)
        self.fail = new_config.get("fail", False)
        return True

    @classmethod
    def default_resource_request(cls, config):
        required_resources = config.get("required_resources", None)
        if required_resources:
            return required_resources
        return None


def train_fn(config):
    # Determine whether or not we reset to a new trial
    marker_dir = config.get("marker_dir")
    num_resets = 0
    marker = Path(marker_dir) / f"{os.getpid()}.txt"
    if marker.exists():
        num_resets = int(marker.read_text()) + 1

    checkpoint = train.get_checkpoint()
    it = load_dict_checkpoint(checkpoint)["iter"] if checkpoint else 0

    msg = config.get("message", None)
    sleep = int(config.get("sleep", 0))
    fail = config.get("fail", False)

    while it < 2:
        it += 1
        if msg:
            print("PRINT_STDOUT: {}".format(msg))
            print("PRINT_STDERR: {}".format(msg), file=sys.stderr)
            logger.info("LOG_STDERR: {}".format(msg))

        if fail:
            raise RuntimeError("Failing")

        # Dump the current config
        marker.write_text(str(num_resets))

        if sleep:
            time.sleep(sleep)

        metrics = {
            "id": config.get("id", 0),
            "num_resets": num_resets,
            "iter": it,
            "done": it > 1,
        }
        if config.get("save_checkpoint", True):
            with create_dict_checkpoint({"iter": it}) as checkpoint:
                train.report(metrics, checkpoint=checkpoint)
        else:
            train.report(metrics, checkpoint=checkpoint)


@pytest.fixture(params=["function"])
def trainable(request, monkeypatch):
    """Fixture that sets up a checkpoint on different filesystems."""
    trainable_type = request.param
    if trainable_type == "function":
        monkeypatch.setenv(RAY_AIR_NEW_PERSISTENCE_MODE, "1")
        yield train_fn
        monkeypatch.setenv(RAY_AIR_NEW_PERSISTENCE_MODE, "0")
    elif trainable_type == "class":
        yield MyResettableClass
    else:
        raise NotImplementedError


def _run_trials_with_frequent_pauses(trainable, reuse=False, **kwargs):
    tempdir = tempfile.mkdtemp()
    marker_dir = Path(tempdir)
    analysis = tune.run(
        trainable,
        num_samples=1,
        config={
            "id": tune.grid_search([0, 1, 2, 3]),
            "marker_dir": marker_dir,
        },
        reuse_actors=reuse,
        scheduler=FrequentPausesScheduler(),
        verbose=0,
        **kwargs,
    )
    return analysis


def test_trial_reuse_disabled(ray_start_1_cpu, trainable):
    """Test that reuse=False disables actor re-use.

    Setup: Pass `reuse_actors=False` to tune.run()

    We assert the `num_resets` of each trainable class to be 0 (no reuse).
    """
    analysis = _run_trials_with_frequent_pauses(trainable, reuse=False)
    trials = analysis.trials
    assert [t.last_result["id"] for t in trials] == [0, 1, 2, 3]
    assert [t.last_result["iter"] for t in trials] == [2, 2, 2, 2]
    assert [t.last_result["num_resets"] for t in trials] == [0, 0, 0, 0]


def test_trial_reuse_disabled_per_default(ray_start_1_cpu, trainable):
    """Test that reuse=None disables actor re-use for class trainables.

    Setup: Pass `reuse_actors=None` to tune.run()

    We assert the `num_resets` of each trainable class to be 0 (no reuse).
    """
    analysis = _run_trials_with_frequent_pauses(MyResettableClass, reuse=None)
    trials = analysis.trials
    assert [t.last_result["id"] for t in trials] == [0, 1, 2, 3]
    assert [t.last_result["iter"] for t in trials] == [2, 2, 2, 2]
    assert [t.last_result["num_resets"] for t in trials] == [0, 0, 0, 0]


def test_trial_reuse_enabled(ray_start_1_cpu, trainable):
    """Test that reuse=True enables actor re-use.

    Setup: Pass `reuse_actors=True` to tune.run()

    We assert the `num_resets` of each trainable class to be 4, 5, 6, and 7,
    respectively:

    - Each trial runs for 2 iterations
    - Only one trial can run at a time
    - After each iteration, trials are paused and actors cached for reuse
    - Thus, the first trial finishes after 4 resets, the second after 5, etc.
    """
    analysis = _run_trials_with_frequent_pauses(trainable, reuse=True)
    trials = analysis.trials
    assert [t.last_result["id"] for t in trials] == [0, 1, 2, 3]
    assert [t.last_result["iter"] for t in trials] == [2, 2, 2, 2]
    assert [t.last_result["num_resets"] for t in trials] == [4, 5, 6, 7]


def test_trial_reuse_with_failing(ray_start_1_cpu, tmp_path, trainable):
    """Test that failing actors won't be reused.

    - 1 trial can run at a time
    - Some trials are failing
    - We assert that trials after failing trials are scheduled on fresh actors
        (num_resets = 0)
    - We assert that trials after successful trials are schedule on reused actors
        (num_reset = last_num_resets + 1)
    """
    fail = [False, True, False, False, True, True, False, False, False]
    trials = tune.run(
        trainable,
        reuse_actors=True,
        config={
            "id": tune.grid_search(list(range(9))),
            "fail": tune.sample_from(lambda config: fail[config["id"]]),
            "marker_dir": tmp_path,
        },
        raise_on_failed_trial=False,
    ).trials

    assert [t.last_result.get("iter") for t in trials] == [
        2,
        None,
        2,
        2,
        None,
        None,
        2,
        2,
        2,
    ]
    assert [t.last_result.get("num_resets") for t in trials] == [
        0,
        None,
        0,
        1,
        None,
        None,
        0,
        1,
        2,
    ]


def test_reuse_enabled_error(ray_start_1_cpu):
    """Test that a class without reset() enabled throws an error on actor reuse."""
    with pytest.raises(TuneError):
        run_experiments(
            {
                "foo": {
                    "run": MyResettableClass,
                    "max_failures": 1,
                    "num_samples": 1,
                    "config": {
                        "id": tune.grid_search([0, 1, 2, 3]),
                        "fake_reset_not_supported": True,
                    },
                }
            },
            reuse_actors=True,
            scheduler=FrequentPausesScheduler(),
        )


def test_trial_reuse_log_to_file(ray_start_1_cpu):
    """Check that log outputs from trainables are correctly stored with actor reuse.

    We run two trials with actor reuse. When the actor is reused, we expect
    the log output to be written to the log file of the new trial - i.e. we expect
    that the old trial logfile is closed and a new one is open.
    """
    register_trainable("foo2", MyResettableClass)

    # Log to default files
    [trial1, trial2] = tune.run(
        "foo2",
        config={"message": tune.grid_search(["First", "Second"]), "id": -1},
        log_to_file=True,
        scheduler=FrequentPausesScheduler(),
        reuse_actors=True,
    ).trials

    # Check trial 1
    assert trial1.last_result["num_resets"] == 2
    assert os.path.exists(os.path.join(trial1.local_path, "stdout"))
    assert os.path.exists(os.path.join(trial1.local_path, "stderr"))

    # We expect that only "First" output is found in the first trial output
    with open(os.path.join(trial1.local_path, "stdout"), "rt") as fp:
        content = fp.read()
        assert "PRINT_STDOUT: First" in content
        assert "PRINT_STDOUT: Second" not in content
    with open(os.path.join(trial1.local_path, "stderr"), "rt") as fp:
        content = fp.read()
        assert "PRINT_STDERR: First" in content
        assert "LOG_STDERR: First" in content
        assert "PRINT_STDERR: Second" not in content
        assert "LOG_STDERR: Second" not in content

    # Check trial 2
    assert trial2.last_result["num_resets"] == 3
    assert os.path.exists(os.path.join(trial2.local_path, "stdout"))
    assert os.path.exists(os.path.join(trial2.local_path, "stderr"))

    # We expect that only "Second" output is found in the first trial output
    with open(os.path.join(trial2.local_path, "stdout"), "rt") as fp:
        content = fp.read()
        assert "PRINT_STDOUT: Second" in content
        assert "PRINT_STDOUT: First" not in content
    with open(os.path.join(trial2.local_path, "stderr"), "rt") as fp:
        content = fp.read()
        assert "PRINT_STDERR: Second" in content
        assert "LOG_STDERR: Second" in content
        assert "PRINT_STDERR: First" not in content
        assert "LOG_STDERR: First" not in content


def test_multi_trial_reuse(ray_start_4_cpus_extra):
    """Test that actors from multiple trials running in parallel will be reused.

    - 2 trials can run at the same time
    - Trial 3 will be scheduled after trial 1 succeeded, so will reuse actor
    - Trial 4 will be scheduled after trial 2 succeeded, so will reuse actor
    """
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "2"

    register_trainable("foo2", MyResettableClass)

    # We sleep here for one second so that the third actor
    # does not finish training before the fourth can be scheduled.
    # This helps ensure that both remote runners are re-used and
    # not just one.
    [trial1, trial2, trial3, trial4] = tune.run(
        "foo2",
        config={
            "message": tune.grid_search(["First", "Second", "Third", "Fourth"]),
            "id": -1,
            "sleep": 2,
        },
        reuse_actors=True,
        resources_per_trial={"cpu": 2},
    ).trials

    assert trial3.last_result["num_resets"] == 1
    assert trial4.last_result["num_resets"] == 1


def test_multi_trial_reuse_with_failing(ray_start_4_cpus_extra):
    """Test that failing trial's actors are not reused.

    - 2 trials can run at the same time
    - Trial 1 succeeds, trial 2 fails
    - Trial 3 will be scheduled after trial 2 failed, so won't reuse actor
    - Trial 4 will be scheduled after trial 1 succeeded, so will reuse actor
    """
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "2"

    register_trainable("foo2", MyResettableClass)

    [trial1, trial2, trial3, trial4] = tune.run(
        "foo2",
        config={
            "fail": tune.grid_search([False, True, False, False]),
            "id": -1,
            "sleep": 2,
        },
        reuse_actors=True,
        resources_per_trial={"cpu": 2},
        raise_on_failed_trial=False,
    ).trials

    assert trial1.last_result["num_resets"] == 0
    assert trial3.last_result["num_resets"] == 0
    assert trial4.last_result["num_resets"] == 1


def test_multi_trial_reuse_one_by_one(ray_start_4_cpus_extra):
    """Test that we still reuse actors even if we run with concurrency = 1.

    - Run 6 trials, but only 1 concurrent at the time
    - This means there won't be any PENDING trials until the trial completed
    - We still want to reuse actors

    """
    register_trainable("foo2", MyResettableClass)

    trials = tune.run(
        "foo2",
        config={"id": -1},
        reuse_actors=True,
        num_samples=6,
        max_concurrent_trials=1,
    ).trials

    assert sorted([t.last_result["num_resets"] for t in trials]) == [0, 1, 2, 3, 4, 5]


def test_multi_trial_reuse_heterogeneous(ray_start_4_cpus_extra):
    """Test that actors with heterogeneous resource requirements are reused efficiently.

    - Run 6 trials in total
    - Only 1 trial can run at the same time
    - Trials 1 and 6, 2 and 4, and 3 and 5, have the same resource request, respectively
    - Assert that trials 4, 5, and 6 re-use their respective previous actors

    """
    # We need to set this to 6 so that all trials will be scheduled and actors will
    # be correctly cached.
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = "6"

    register_trainable("foo2", MyResettableClass)

    trials = tune.run(
        "foo2",
        config={
            # The extra resources are selected so that only any 1 placement group
            # can be scheduled at the same time at all times (to force sequential
            # execution of trials)
            "required_resources": tune.grid_search(
                [
                    {"cpu": 4, "custom_resources": {"extra": 4}},
                    {"cpu": 2, "custom_resources": {"extra": 4}},
                    {"cpu": 1, "custom_resources": {"extra": 4}},
                    {"cpu": 2, "custom_resources": {"extra": 4}},
                    {"cpu": 1, "custom_resources": {"extra": 4}},
                    {"cpu": 4, "custom_resources": {"extra": 4}},
                ]
            ),
            "id": -1,
        },
        reuse_actors=True,
    ).trials

    # Actors may be re-used in a different order as the staged_trials set is unsorted
    assert sorted([t.last_result["num_resets"] for t in trials]) == [0, 0, 0, 1, 1, 1]


def test_detect_reuse_mixins():
    class DummyMixin:
        pass

    def dummy_mixin(func: Callable):
        func.__mixins__ = (DummyMixin,)
        return func

    assert not _check_mixin("PPO")

    def train(config):
        pass

    assert not _check_mixin(train)
    assert _check_mixin(dummy_mixin(train))

    class MyTrainable(Trainable):
        pass

    assert not _check_mixin(MyTrainable)
    assert _check_mixin(dummy_mixin(MyTrainable))


def test_remote_trial_dir_with_reuse_actors(ray_start_2_cpus, tmp_path):
    """Check that the trainable has its remote directory set to the right
    location, when new trials get swapped in on actor reuse.
    Each trial runs for 2 iterations, with checkpoint_frequency=1, so each
    remote trial dir should have 2 checkpoints.
    """
    tmp_target = str(tmp_path / "upload_dir")
    exp_name = "remote_trial_dir_update_on_actor_reuse"

    def get_remote_trial_dir(trial_id: int):
        return os.path.join(tmp_target, exp_name, str(trial_id))

    class _MyResettableClass(MyResettableClass):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._should_raise = False

        def load_checkpoint(self, *args, **kwargs):
            super().load_checkpoint(*args, **kwargs)

            # Make sure that `remote_checkpoint_dir` gets updated correctly
            trial_id = self.config.get("id")
            remote_trial_dir = get_remote_trial_dir(trial_id)

            if self.remote_checkpoint_dir != "file://" + remote_trial_dir:
                # Delay raising the exception, since raising here would cause
                # an unhandled exception that doesn't fail the test.
                self._should_raise = True

        def step(self):
            trial_id = self.config.get("id")
            remote_trial_dir = get_remote_trial_dir(trial_id)

            if self._should_raise:
                raise RuntimeError(
                    f"Failing! Remote path not updated properly "
                    f"for trial {self.config.get('id')}. "
                    f"\nExpected: file://{remote_trial_dir}"
                    f"\nGot: {self.remote_checkpoint_dir}"
                )
            return super().step()

    analysis = _run_trials_with_frequent_pauses(
        _MyResettableClass,
        reuse=True,
        max_concurrent_trials=2,
        local_dir=str(tmp_path),
        name=exp_name,
        storage_path=f"file://{tmp_target}",
        trial_dirname_creator=lambda t: str(t.config.get("id")),
        checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
    )
    result_grid = ResultGrid(analysis)
    assert not result_grid.errors

    # Check that each remote trial dir has 2 checkpoints.
    for result in result_grid:
        trial_id = result.config["id"]
        remote_dir = get_remote_trial_dir(trial_id)
        num_checkpoints = len(
            [file for file in os.listdir(remote_dir) if file.startswith("checkpoint_")]
        )
        assert num_checkpoints == 2


def test_artifact_syncing_with_actor_reuse(
    ray_start_2_cpus, tmp_path, propagate_logs, caplog
):
    """Check that artifacts get synced to the right places with actor reuse.

    In this test, 4 trials are paused and reused.
    max_concurrent_trials=2 to force trials to swap places with each other.
    On each pause and Trainable.reset, artifacts should get synced to their respective
    locations in the target directory.
    """
    tmp_target = str(tmp_path / "target")
    local_dir = str(tmp_path / "local_dir")

    exp_name = "sync_artifacts_with_actor_reuse"

    class MyResettableClassWithArtifacts(MyResettableClass):
        """Helper class that implements `reset_config` and also saves artifacts."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._should_raise = False
            self._failure_message = ""

        def step(self) -> dict:
            if self._should_raise:
                raise RuntimeError(self._failure_message)
            result = super().step()
            # Mock some artifact logging (appending to a log)
            with open(os.path.join(self.logdir, "artifact.txt"), "a") as f:
                f.write(f"{self.config.get('id')}\n")
            return result

        def reset(self, *args, **kwargs):
            return super().reset(*args, **kwargs)

        def load_checkpoint(self, *args, **kwargs):
            super().load_checkpoint(*args, **kwargs)

            # Make sure that `remote_checkpoint_dir` gets updated correctly
            trial_id = self.config.get("id")
            remote_trial_dir = os.path.join(tmp_target, exp_name, str(trial_id))
            assert self.remote_checkpoint_dir == "file://" + remote_trial_dir

            # Check the artifact contents of the current trial being restored
            artifact_path = os.path.join(remote_trial_dir, "artifact.txt")
            with open(artifact_path, "r") as f:
                artifact_data = f.read()
            artifact_data = artifact_data.split("\n")[:-1]
            expected = [str(trial_id)] * self.iteration
            self._should_raise = artifact_data != expected
            if self._should_raise:
                self._failure_message = (
                    "Failing! `artifact.txt` is out of sync. "
                    f"Expected {expected}, but got {artifact_data}."
                )

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        analysis = _run_trials_with_frequent_pauses(
            MyResettableClassWithArtifacts,
            reuse=True,
            max_concurrent_trials=2,
            local_dir=str(local_dir),
            name=exp_name,
            storage_path=f"file://{tmp_target}",
            sync_config=tune.SyncConfig(sync_artifacts=True),
            trial_dirname_creator=lambda t: str(t.config.get("id")),
            checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
        )
    result_grid = ResultGrid(analysis)
    assert not result_grid.errors

    # Make sure that no sync errors/warnings get logged
    assert "Trial Runner checkpointing failed" not in caplog.text
    assert "Caught sync error:" not in caplog.text


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
