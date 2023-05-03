import json
import logging
import os
from pathlib import Path
import shutil
import time
import unittest

import pytest

import ray
import ray.cloudpickle as ray_pickle
from ray import tune
from ray.air import (
    Checkpoint,
    CheckpointConfig,
    FailureConfig,
    RunConfig,
    ScalingConfig,
    session,
)
from ray.air._internal.remote_storage import (
    delete_at_uri,
    download_from_uri,
    upload_to_uri,
    list_at_uri,
)
from ray.train.data_parallel_trainer import DataParallelTrainer
from ray.tune import Callback, Trainable
from ray.tune.execution.trial_runner import _find_newest_experiment_checkpoint
from ray.tune.experiment import Trial
from ray.tune.result_grid import ResultGrid
from ray.tune.schedulers.async_hyperband import ASHAScheduler
from ray.tune.search.optuna import OptunaSearch
from ray.tune.tune_config import TuneConfig
from ray.tune.tuner import Tuner


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2, configure_logging=False)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_shutdown():
    yield
    ray.shutdown()


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4, configure_logging=False)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def chdir_tmpdir(tmpdir):
    old_cwd = os.getcwd()
    os.chdir(tmpdir)
    yield tmpdir
    os.chdir(old_cwd)


@pytest.fixture
def clear_memory_filesys():
    yield
    delete_at_uri("memory:///")


def _dummy_train_fn(config):
    return 1


def _dummy_train_fn_with_report(config):
    session.report({"score": 1})


def _train_fn_sometimes_failing(config):
    # Fails if failing is set and marker file exists.
    # Hangs if hanging is set and marker file exists.
    failing, hanging = config["failing_hanging"]

    checkpoint = session.get_checkpoint()
    if checkpoint:
        checkpoint_dict = checkpoint.to_dict()
        state = {"it": checkpoint_dict["it"]}
    else:
        state = {"it": 0}

    for i in range(config.get("num_epochs", 1)):
        state["it"] += 1

        session.report(state, checkpoint=Checkpoint.from_dict(state))

    # We fail after reporting num_epochs checkpoints.
    if failing and failing.exists():
        raise RuntimeError("I am failing")

    if hanging and hanging.exists():
        time.sleep(60)

    state["it"] += 1
    session.report(state, checkpoint=Checkpoint.from_dict(state))


class _FailOnStats(Callback):
    """Fail when at least num_trials exist and num_finished have finished."""

    def __init__(self, num_trials: int, num_finished: int = 0, delay: int = 1):
        self.num_trials = num_trials
        self.num_finished = num_finished
        self.delay = delay
        self.fail_at = None

    def on_step_begin(self, iteration: int, trials: list, **info):
        if self.fail_at and iteration >= self.fail_at:
            print(
                "Actually failing after delay:",
                [(t.status, t.last_result.get("it")) for t in trials],
            )
            raise RuntimeError("Failing")

        if len(trials) < self.num_trials:
            return

        if (
            len([t for t in trials if t.status in [Trial.TERMINATED, Trial.ERROR]])
            >= self.num_finished
        ):
            self.fail_at = iteration + self.delay
            print(
                f"Triggering fail in {self.delay} iterations:",
                [(t.status, t.last_result.get("it")) for t in trials],
            )
        else:
            print("Not failing:", [(t.status, t.last_result.get("it")) for t in trials])


class MockData:
    def __init__(self):
        import numpy as np

        self.data = np.random.rand((2 * 1024 * 1024))


def test_tuner_restore_num_trials(ray_start_2_cpus, tmpdir):
    """Number of trials after restoring a finished run should be the same"""
    tuner = Tuner(
        _dummy_train_fn,
        tune_config=TuneConfig(num_samples=4, metric="_metric", mode="max"),
        run_config=RunConfig(
            name="test_tuner_restore_num_trials", storage_path=str(tmpdir)
        ),
    )
    results = tuner.fit()
    assert len(results) == 4
    assert results.get_best_result().metrics["_metric"] == 1

    del tuner
    tuner = Tuner.restore(
        str(tmpdir / "test_tuner_restore_num_trials"), trainable=_dummy_train_fn
    )

    # Check restored results
    results = tuner.get_results()
    assert len(results) == 4
    assert results.get_best_result().metrics["_metric"] == 1

    results = tuner.fit()
    assert len(results) == 4
    assert results.get_best_result().metrics["_metric"] == 1


def test_tuner_restore_resume_errored(ray_start_2_cpus, tmpdir):
    """Resuming errored trials should pick up from previous state"""
    fail_marker = tmpdir / "fail_marker"
    fail_marker.write_text("", encoding="utf-8")

    tuner = Tuner(
        _train_fn_sometimes_failing,
        tune_config=TuneConfig(
            num_samples=1,
        ),
        run_config=RunConfig(
            name="test_tuner_restore_resume_errored", storage_path=str(tmpdir)
        ),
        param_space={
            # Second and third trial fail
            "failing_hanging": tune.grid_search(
                [(None, None), (fail_marker, None), (None, None), (fail_marker, None)]
            ),
        },
    )
    results = tuner.fit()

    assert len(results) == 4
    assert len(results.errors) == 2
    # Second and third trial are at iter 1 because they failed after first report
    assert [r.metrics["it"] for r in results] == [2, 1, 2, 1]

    del tuner
    fail_marker.remove(ignore_errors=True)

    tuner = Tuner.restore(
        str(tmpdir / "test_tuner_restore_resume_errored"),
        trainable=_train_fn_sometimes_failing,
        resume_errored=True,
    )

    # Check restored results
    results = tuner.get_results()
    assert len(results) == 4
    assert len(results.errors) == 2
    # Second and third trial are at iter 1 because they failed after first report
    assert [r.metrics["it"] for r in results] == [2, 1, 2, 1]

    # Get new results
    results = tuner.fit()

    assert len(results) == 4
    assert len(results.errors) == 0
    # Since the errored trials are being resumed from previous state and then report
    # two more times, we should observe 3 here.
    assert sorted([r.metrics["it"] for r in results]) == sorted([2, 3, 2, 3])


def test_tuner_restore_restart_errored(ray_start_2_cpus, tmpdir):
    """Restarting errored trials should re-start from scratch"""
    fail_marker = tmpdir / "fail_marker"
    fail_marker.write_text("", encoding="utf-8")

    tuner = Tuner(
        _train_fn_sometimes_failing,
        tune_config=TuneConfig(num_samples=1),
        run_config=RunConfig(
            name="test_tuner_restore_restart_errored",
            storage_path=str(tmpdir),
        ),
        param_space={
            # Second and third trial fail
            "failing_hanging": tune.grid_search(
                [(None, None), (fail_marker, None), (None, None), (fail_marker, None)]
            ),
        },
    )
    results = tuner.fit()

    assert len(results) == 4
    assert len(results.errors) == 2
    assert [r.metrics["it"] for r in results] == [2, 1, 2, 1]

    del tuner
    fail_marker.remove(ignore_errors=True)

    tuner = Tuner.restore(
        str(tmpdir / "test_tuner_restore_restart_errored"),
        trainable=_train_fn_sometimes_failing,
        restart_errored=True,
    )

    # Check restored results
    results = tuner.get_results()
    assert len(results) == 4
    assert len(results.errors) == 2
    assert [r.metrics["it"] for r in results] == [2, 1, 2, 1]

    # Get new results
    results = tuner.fit()
    assert len(results) == 4
    assert len(results.errors) == 0
    # Since the errored trials are being restarted from scratch, they should report 2
    assert [r.metrics["it"] for r in results] == [2, 2, 2, 2]


def test_tuner_resume_unfinished(ray_start_2_cpus, tmpdir):
    """Resuming unfinished trials should pick up existing state"""
    os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"

    fail_marker = tmpdir / "fail_marker"
    fail_marker.write_text("", encoding="utf-8")

    hang_marker = tmpdir / "hang_marker"
    hang_marker.write_text("", encoding="utf-8")

    tuner = Tuner(
        _train_fn_sometimes_failing,
        tune_config=TuneConfig(num_samples=1),
        run_config=RunConfig(
            name="test_tuner_resume_unfinished",
            storage_path=str(tmpdir),
            failure_config=FailureConfig(fail_fast=False),
            callbacks=[_FailOnStats(num_trials=4, num_finished=2, delay=1)],
        ),
        param_space={
            # First trial succeeds, second hangs, third fails, fourth hangs
            "failing_hanging": tune.grid_search(
                [
                    (None, None),
                    (None, hang_marker),
                    (fail_marker, None),
                    (None, hang_marker),
                ]
            ),
        },
    )
    # Catch the FailOnStats error
    with pytest.raises(RuntimeError):
        tuner.fit()

    # After this run we have the following trial states (status, metric):
    # [('TERMINATED', 2), ('RUNNING', 1), ('ERROR', 1), ('PENDING', None)]

    # Restarting without hanging/failing should lead to the results:
    # [2, 3, 1, 2], because:
    # the TERMINATED trial is finished (state = 2),
    # the RUNNING trial is continued (and picks up from state = 1 for 2 iterations),
    # the ERROR trial is not continued (remains at 1 and errored)
    # and the PENDING trial has not state, yet.

    del tuner
    fail_marker.remove(ignore_errors=True)
    hang_marker.remove(ignore_errors=True)

    tuner = Tuner.restore(
        str(tmpdir / "test_tuner_resume_unfinished"),
        trainable=_train_fn_sometimes_failing,
    )
    tuner._local_tuner._run_config.callbacks = None

    results = tuner.fit()
    assert len(results) == 4
    assert len(results.errors) == 1
    assert sorted([r.metrics["it"] for r in results]) == sorted([2, 3, 1, 2])


def test_tuner_resume_errored_only(ray_start_2_cpus, tmpdir):
    """Not resuming unfinished trials (but only errored and pending) should work"""
    os.environ["TUNE_GLOBAL_CHECKPOINT_S"] = "0"

    fail_marker = tmpdir / "fail_marker"
    fail_marker.write_text("", encoding="utf-8")

    hang_marker = tmpdir / "hang_marker"
    hang_marker.write_text("", encoding="utf-8")

    tuner = Tuner(
        _train_fn_sometimes_failing,
        tune_config=TuneConfig(num_samples=1),
        run_config=RunConfig(
            name="test_tuner_resume_errored_only",
            storage_path=str(tmpdir),
            failure_config=FailureConfig(fail_fast=False),
            callbacks=[_FailOnStats(num_trials=4, num_finished=2, delay=1)],
        ),
        param_space={
            # First trial succeeds, second hangs, third fails, fourth hangs.
            "failing_hanging": tune.grid_search(
                [
                    (None, None),
                    (None, hang_marker),
                    (fail_marker, None),
                    (None, hang_marker),
                ]
            ),
        },
    )
    # Catch the FailOnStats error
    with pytest.raises(RuntimeError):
        tuner.fit()

    # After this run we have the following trial states (status, metric):
    # [('TERMINATED', 2), ('RUNNING', 1), ('ERROR', 1), ('PENDING', None)]

    # Restarting without continuing existing trials should lead to the results
    # [2, 1, 3, 0], because
    # the TERMINATED trial is finished (state = 2),
    # the RUNNING trial is not continued (marked as terminated),
    # the ERROR trial is not continued (remains at 1 and errored)
    # and the PENDING trial is not continued (marked as terminated).

    del tuner
    fail_marker.remove(ignore_errors=True)
    hang_marker.remove(ignore_errors=True)

    tuner = Tuner.restore(
        str(tmpdir / "test_tuner_resume_errored_only"),
        trainable=_train_fn_sometimes_failing,
        resume_unfinished=False,
        resume_errored=True,
    )
    tuner._local_tuner._run_config.callbacks = None

    results = tuner.fit()
    assert len(results) == 4
    assert len(results.errors) == 0
    assert sorted([r.metrics.get("it", 0) for r in results]) == sorted([2, 1, 3, 0])


def _test_tuner_restore_from_cloud(tmpdir, configure_storage_path, storage_path):
    """Check that restoring Tuner() objects from cloud storage works"""
    tuner = Tuner(
        _dummy_train_fn,
        run_config=RunConfig(
            name="exp_dir",
            storage_path=configure_storage_path,
            local_dir=str(tmpdir / "ray_results"),
        ),
    )
    tuner.fit()

    check_path = tmpdir / "check_save"
    download_from_uri(storage_path, str(check_path))
    remote_contents = os.listdir(check_path / "exp_dir")

    assert "tuner.pkl" in remote_contents

    prev_cp = _find_newest_experiment_checkpoint(str(check_path / "exp_dir"))
    prev_lstat = os.lstat(prev_cp)

    (tmpdir / "ray_results").remove(ignore_errors=True)

    tuner2 = Tuner.restore(storage_path + "/exp_dir", trainable=_dummy_train_fn)
    results = tuner2.fit()

    assert results[0].metrics["_metric"] == 1
    local_contents = os.listdir(tmpdir / "ray_results" / "exp_dir")
    assert "tuner.pkl" in local_contents

    after_cp = _find_newest_experiment_checkpoint(
        str(tmpdir / "ray_results" / "exp_dir")
    )
    after_lstat = os.lstat(after_cp)

    # Experiment checkpoint was updated
    assert os.path.basename(prev_cp) != os.path.basename(after_cp)
    # Old experiment checkpoint still exists in dir
    assert os.path.basename(prev_cp) in local_contents
    # Contents changed
    assert prev_lstat.st_size != after_lstat.st_size

    # Overwriting should work
    tuner3 = Tuner.restore(storage_path + "/exp_dir", trainable=_dummy_train_fn)
    tuner3.fit()


def test_tuner_restore_from_cloud_manual_path(
    ray_start_2_cpus, tmpdir, clear_memory_filesys
):
    storage_path = "memory:///test/restore"
    _test_tuner_restore_from_cloud(
        tmpdir, configure_storage_path=storage_path, storage_path=storage_path
    )


def test_tuner_restore_from_cloud_ray_storage(ray_shutdown, tmpdir):
    storage_path = "mock:///test/restore"

    ray.init(num_cpus=2, configure_logging=False, storage=storage_path)

    _test_tuner_restore_from_cloud(
        tmpdir / "local", configure_storage_path=None, storage_path=storage_path
    )


@pytest.mark.parametrize(
    "upload_uri",
    [None, "memory:///test/test_tuner_restore_latest_available_checkpoint"],
)
def test_tuner_restore_latest_available_checkpoint(
    ray_start_2_cpus, tmpdir, upload_uri, clear_memory_filesys
):
    """Resuming errored trials should pick up from previous state"""
    fail_marker = tmpdir / "fail_marker"
    fail_marker.write_text("", encoding="utf-8")

    tuner = Tuner(
        _train_fn_sometimes_failing,
        tune_config=TuneConfig(
            num_samples=1,
        ),
        run_config=RunConfig(
            name="test_tuner_restore_latest_available_checkpoint",
            storage_path=upload_uri,
            local_dir=str(tmpdir),
        ),
        param_space={"failing_hanging": (fail_marker, None), "num_epochs": 4},
    )

    results = tuner.fit()

    assert len(results) == 1
    assert len(results.errors) == 1

    [result] = list(results)
    # num_epochs = 4, so it = 4
    assert result.metrics["it"] == 4

    del tuner
    fail_marker.remove(ignore_errors=True)

    shutil.rmtree(os.path.join(result.log_dir, "checkpoint_000003"))
    shutil.rmtree(os.path.join(result.log_dir, "checkpoint_000002"))

    if upload_uri:
        delete_at_uri(upload_uri + "/checkpoint_000003")
        delete_at_uri(upload_uri + "/checkpoint_000002")

    tuner = Tuner.restore(
        str(tmpdir / "test_tuner_restore_latest_available_checkpoint"),
        trainable=_train_fn_sometimes_failing,
        resume_errored=True,
    )
    results = tuner.fit()
    assert len(results) == 1
    assert len(results.errors) == 0
    [result] = list(results)
    # restored from 2, plus num_epochs = 4, plus one additional epoch
    assert result.metrics["it"] == 7
    assert result.metrics["iterations_since_restore"] == 5


@pytest.mark.parametrize("retry_num", [0, 2])
def test_restore_retry(ray_start_2_cpus, tmpdir, retry_num):
    """Test retrying restore on a trial level by setting `TUNE_RESTORE_RETRY_NUM`."""

    class MockTrainable(Trainable):
        """A trainable that can generate one failure during training and
        another `config["retry_num_to_fail"]` times during restoring."""

        def setup(self, config):
            self.idx = 0
            self.tag_file_path = config["tag_file_path"]
            self.retry_num_to_fail = config.get("retry_num_to_fail", 2)
            self._is_restored = False

        def step(self):
            time.sleep(1)
            if self.idx == 0 and self._is_restored:
                raise RuntimeError(
                    "===== Restored trial cannot start from scratch ====="
                )
            elif self.idx == 2 and not self._is_restored:
                raise RuntimeError("===== First run fails at idx=2 =====")
            self.idx += 1
            return {"score": self.idx}

        def save_checkpoint(self, checkpoint_dir):
            path = os.path.join(checkpoint_dir, "checkpoint")
            with open(path, "w") as f:
                f.write(json.dumps({"idx": self.idx}))
            return path

        def load_checkpoint(self, checkpoint_path):
            self._is_restored = True
            with open(self.tag_file_path, "r") as f:
                retried_num = json.loads(f.read())["retried_num"]

            with open(self.tag_file_path, "w") as f:
                f.write(json.dumps({"retried_num": retried_num + 1}))

            if retried_num < self.retry_num_to_fail:
                raise RuntimeError(f"===== Failing restore #{retried_num + 1} =====")
            with open(checkpoint_path) as f:
                self.idx = json.loads(f.read())["idx"]

    # Set environment variable just for this test
    with unittest.mock.patch.dict(
        os.environ, {"TUNE_RESTORE_RETRY_NUM": str(retry_num)}
    ):
        tag_file = os.path.join(tmpdir, "tag")
        # set up tag file
        with open(tag_file, "w") as f:
            f.write(json.dumps({"retried_num": 0}))
        tuner = Tuner(
            MockTrainable,
            run_config=RunConfig(
                name="tryout_restore",
                stop={"training_iteration": 5},
                storage_path=str(tmpdir),
                failure_config=FailureConfig(max_failures=1),
                checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
            ),
            param_space={"tag_file_path": tag_file},
        )
        results = tuner.fit()
        [result] = list(results)
        if retry_num > 0:
            assert result.metrics["score"] == 5
        else:
            assert result.metrics["score"] == 2


def test_restore_overwrite_trainable(ray_start_2_cpus, tmpdir):
    """Test validation for trainable compatibility, when re-specifying a trainable
    on restore."""

    def train_func_1(config):
        data = {"data": config["data"]}
        session.report(data, checkpoint=Checkpoint.from_dict(data))
        raise RuntimeError("Failing!")

    tuner = Tuner(
        train_func_1,
        run_config=RunConfig(name="overwrite_trainable", storage_path=str(tmpdir)),
        param_space={"data": 1},
    )
    tuner.fit()

    del tuner

    # Can't overwrite with a different Trainable type
    with pytest.raises(ValueError):
        tuner = Tuner.restore(
            str(tmpdir / "overwrite_trainable"),
            trainable="__fake",
            resume_errored=True,
        )

    # Can't overwrite with a different Trainable name
    def train_func_2(config):
        checkpoint = session.get_checkpoint()
        assert checkpoint and checkpoint.to_dict()["data"] == config["data"]

    with pytest.raises(ValueError):
        tuner = Tuner.restore(
            str(tmpdir / "overwrite_trainable"),
            trainable=train_func_2,
            resume_errored=True,
        )

    # Can technically change trainable code (not recommended!)
    def train_func_1(config):
        checkpoint = session.get_checkpoint()
        assert checkpoint and checkpoint.to_dict()["data"] == config["data"]

    tuner = Tuner.restore(
        str(tmpdir / "overwrite_trainable"),
        trainable=train_func_1,
        resume_errored=True,
    )
    results = tuner.fit()
    assert not results.errors


@pytest.mark.parametrize("use_function_trainable", [True, False])
def test_restore_with_parameters(ray_start_2_cpus, tmp_path, use_function_trainable):
    """Tests Tuner restoration for a `tune.with_parameters` wrapped trainable."""

    def train_func(config, data_str=None, data_obj=None):
        assert data_str is not None and data_obj is not None
        fail_marker = config.pop("fail_marker", None)
        config["failing_hanging"] = (fail_marker, None)
        _train_fn_sometimes_failing(config)

    class FailingTrainable(Trainable):
        def setup(self, config, data_str=None, data_obj=None):
            assert data_str is not None and data_obj is not None
            self.idx = 0
            self.fail_marker = config.get("fail_marker", None)

        def step(self):
            if self.fail_marker and self.fail_marker.exists():
                raise RuntimeError("==== Run is failing ====")
            self.idx += 1
            return {"score": self.idx}

        def save_checkpoint(self, checkpoint_dir):
            return {"idx": self.idx}

        def load_checkpoint(self, checkpoint_dict):
            self.idx = checkpoint_dict["idx"]

    trainable = train_func if use_function_trainable else FailingTrainable

    def create_trainable_with_params():
        data = MockData()
        trainable_with_params = tune.with_parameters(
            trainable, data_str="data", data_obj=data
        )
        return trainable_with_params

    exp_name = "restore_with_params"
    fail_marker = tmp_path / "fail_marker"
    fail_marker.write_text("", encoding="utf-8")

    tuner = Tuner(
        create_trainable_with_params(),
        run_config=RunConfig(
            name=exp_name,
            storage_path=str(tmp_path),
            stop={"training_iteration": 3},
            failure_config=FailureConfig(max_failures=0),
            checkpoint_config=CheckpointConfig(
                checkpoint_frequency=0 if use_function_trainable else 1
            ),
        ),
        param_space={"fail_marker": fail_marker},
    )
    results = tuner.fit()
    assert results.errors

    fail_marker.unlink()
    tuner = Tuner.restore(
        str(tmp_path / exp_name),
        trainable=create_trainable_with_params(),
        resume_errored=True,
    )
    results = tuner.fit()
    assert not results.errors


@pytest.mark.parametrize("use_tune_run", [True, False])
def test_tuner_restore_from_moved_experiment_path(
    ray_start_2_cpus, tmp_path, use_tune_run
):
    """Check that restoring a Tuner from a moved experiment directory works."""
    # Create a fail_marker dummy file that causes the first Tune run to fail and
    # the second run to succeed
    fail_marker = tmp_path / "fail_marker"
    fail_marker.write_text("", encoding="utf-8")

    old_local_dir = tmp_path / "ray_results"
    old_exp_name = "exp_dir"

    new_local_dir = tmp_path / "new_ray_results"
    new_exp_name = "new_exp_dir"

    # Initial training run (that errors out in the middle)
    num_to_keep = 2
    tuner = Tuner(
        _train_fn_sometimes_failing,
        tune_config=TuneConfig(
            num_samples=1,
        ),
        run_config=RunConfig(
            name=old_exp_name,
            storage_path=str(old_local_dir),
            checkpoint_config=CheckpointConfig(num_to_keep=num_to_keep),
        ),
        param_space={
            "failing_hanging": (fail_marker, None),
        },
    )
    results = tuner.fit()
    assert len(results.errors) == 1
    training_iteration = results[0].metrics["training_iteration"]
    assert (
        training_iteration == 1
    ), f"Should only have 1 session.report before erroring, got {training_iteration}"

    # Move experiment from `tmp_path/ray_results/exp_dir`
    # to `tmp_path/moved_ray_results/new_exp_dir`, changing both `local_dir` and
    # the experiment `name`
    shutil.move(str(old_local_dir), str(new_local_dir))
    os.rename(str(new_local_dir / old_exp_name), str(new_local_dir / new_exp_name))

    del tuner
    # Remove fail_marker so that the restored Tuner doesn't error again
    fail_marker.unlink()

    # Restore from moved experiment directory location, and launch resumed training
    if use_tune_run:
        analysis = tune.run(
            _train_fn_sometimes_failing,
            name=new_exp_name,
            storage_path=str(new_local_dir),
            resume="AUTO+ERRORED",
        )
        results = ResultGrid(analysis)
    else:
        restore_path = str(new_local_dir / new_exp_name)
        tuner = Tuner.restore(
            restore_path, trainable=_train_fn_sometimes_failing, resume_errored=True
        )
        results = tuner.fit()

    assert len(results.errors) == 0
    # Check that we restored iter=1, then made 2 calls to session.report -> iter=3
    training_iteration = results[0].metrics["training_iteration"]
    assert training_iteration == 3, training_iteration

    # Make sure that checkpoints are loaded properly
    assert results[0].checkpoint
    assert len(results[0].best_checkpoints) == num_to_keep
    checkpoint_dirs = [
        path
        for path in os.listdir(results[0].log_dir)
        if path.startswith("checkpoint_")
    ]
    assert sorted(checkpoint_dirs) == ["checkpoint_000001", "checkpoint_000002"]

    # Make sure that we did not create a logdir in the old location
    assert not old_local_dir.exists()


def test_tuner_restore_from_moved_cloud_uri(
    ray_start_2_cpus, tmp_path, clear_memory_filesys
):
    """Test that restoring an experiment that was moved to a new remote URI
    resumes and continues saving new results at that URI."""

    (tmp_path / "moved").mkdir()

    def failing_fn(config):
        data = {"score": 1}
        session.report(data, checkpoint=Checkpoint.from_dict(data))
        raise RuntimeError("Failing!")

    tuner = Tuner(
        failing_fn,
        run_config=RunConfig(
            name="exp_dir",
            storage_path="memory:///original",
            local_dir=str(tmp_path / "ray_results"),
        ),
        tune_config=TuneConfig(trial_dirname_creator=lambda _: "test"),
    )
    tuner.fit()

    # mv memory:///original/exp_dir memory:///moved/new_exp_dir
    download_from_uri(
        "memory:///original/exp_dir", str(tmp_path / "moved" / "new_exp_dir")
    )
    delete_at_uri("memory:///original")
    upload_to_uri(str(tmp_path / "moved"), "memory:///moved")

    tuner = Tuner.restore(
        "memory:///moved/new_exp_dir", trainable=failing_fn, resume_errored=True
    )
    # Just for the test, since we're using `memory://` to mock a remote filesystem,
    # the checkpoint needs to be copied to the new local directory.
    # This is because the trainable actor uploads its checkpoints to a
    # different `memory://` filesystem than the driver and is not
    # downloaded along with the other parts of the experiment dir.
    # NOTE: A new local directory is used since the experiment name got modified.
    shutil.move(
        tmp_path / "ray_results/exp_dir/test/checkpoint_000000",
        tmp_path / "ray_results/new_exp_dir/test/checkpoint_000000",
    )
    results = tuner.fit()

    assert list_at_uri("memory:///") == ["moved"]
    num_experiment_checkpoints = len(
        [
            path
            for path in list_at_uri("memory:///moved/new_exp_dir")
            if path.startswith("experiment_state")
        ]
    )
    assert num_experiment_checkpoints == 2

    num_trial_checkpoints = len(
        [
            path
            for path in os.listdir(results[0].log_dir)
            if path.startswith("checkpoint_")
        ]
    )
    assert num_trial_checkpoints == 2


def test_restore_from_relative_path(ray_start_2_cpus, chdir_tmpdir):
    tuner = Tuner(
        _dummy_train_fn_with_report,
        run_config=RunConfig(storage_path="relative_dir", name="exp_name"),
    )
    tuner.fit()

    tuner = Tuner.restore(
        "relative_dir/exp_name", trainable=_dummy_train_fn_with_report
    )
    results = tuner.fit()
    assert not results.errors
    assert results[0].metrics["score"] == 1


def test_custom_searcher_and_scheduler_restore(ray_start_2_cpus, tmpdir):
    """Check that a restored Tune experiment uses the original searcher/scheduler."""
    fail_marker = tmpdir / "fail_marker"
    fail_marker.write_text("", encoding="utf-8")

    class MockSearcher(OptunaSearch):
        def on_trial_result(self, trial_id: str, result: dict):
            super().on_trial_result(trial_id, result)
            if not hasattr(self, "_test_result_counter"):
                self._test_result_counter = 0
            self._test_result_counter += 1

    class MockScheduler(ASHAScheduler):
        def on_trial_result(self, runner, trial, result):
            decision = super().on_trial_result(runner, trial, result)
            if not hasattr(self, "_test_result_counter"):
                self._test_result_counter = 0
            self._test_result_counter += 1
            return decision

    tuner = Tuner(
        _train_fn_sometimes_failing,
        run_config=RunConfig(storage_path=str(tmpdir), name="exp_name"),
        tune_config=TuneConfig(
            search_alg=MockSearcher(),
            scheduler=MockScheduler(),
            metric="it",
            mode="max",
        ),
        param_space={"a": tune.uniform(0, 1), "failing_hanging": (fail_marker, None)},
    )
    tuner.fit()

    del tuner
    fail_marker.remove(ignore_errors=True)

    tuner = Tuner.restore(
        str(tmpdir / "exp_name"),
        trainable=_train_fn_sometimes_failing,
        resume_errored=True,
    )
    tuner.fit()
    searcher = tuner._local_tuner._tune_config.search_alg
    scheduler = tuner._local_tuner._tune_config.scheduler
    assert isinstance(searcher, MockSearcher)
    assert isinstance(scheduler, MockScheduler)
    # Searcher state should get loaded correctly
    # Total of 3 reported results (1 from before failure, 2 after restore)
    assert searcher._test_result_counter == 3
    # Make sure that the restored scheduler is at least used
    assert (
        hasattr(scheduler, "_test_result_counter")
        and scheduler._test_result_counter > 0
    )


@pytest.mark.parametrize("use_air_trainer", [True, False])
def test_checkpoints_saved_after_resume(ray_start_2_cpus, tmp_path, use_air_trainer):
    """Checkpoints saved after experiment restore should pick up at the correct
    iteration and should not overwrite the checkpoints from the original run.
    Old checkpoints should still be deleted if the total number of checkpoints
    (old + new) exceeds `num_to_keep`.

    In this test, `num_to_keep=4`:
    - Initial run saves checkpoint_000000 and checkpoint_000001
    - Restored run saves checkpoint_000002, checkpoint_000003, and checkpoint_000004
    - Checkpoint 000000 should be deleted.
    """

    def get_checkpoints(experiment_dir):
        checkpoint_dirs = [
            path
            for path in os.listdir(experiment_dir)
            if path.startswith("checkpoint_")
        ]
        sorted_checkpoint_dirs = sorted(checkpoint_dirs)
        checkpoints = [
            Checkpoint.from_directory(os.path.join(experiment_dir, d))
            for d in sorted_checkpoint_dirs
        ]
        return sorted_checkpoint_dirs, checkpoints

    fail_marker = tmp_path / "fail_marker"
    fail_marker.write_text("", encoding="utf-8")

    trainable = (
        DataParallelTrainer(
            _train_fn_sometimes_failing, scaling_config=ScalingConfig(num_workers=1)
        )
        if use_air_trainer
        else _train_fn_sometimes_failing
    )
    param_space = {
        "failing_hanging": (fail_marker, None),
        "num_epochs": 2,
    }
    if use_air_trainer:
        param_space = {"train_loop_config": param_space}

    num_to_keep = 4
    tuner = Tuner(
        trainable,
        tune_config=TuneConfig(num_samples=1),
        run_config=RunConfig(
            name="exp_name",
            storage_path=str(tmp_path),
            checkpoint_config=CheckpointConfig(num_to_keep=num_to_keep),
        ),
        param_space=param_space,
    )
    results = tuner.fit()
    training_iteration = results[0].metrics["training_iteration"]
    assert (
        training_iteration == 2
    ), f"Should be at 2 iters before erroring, got {training_iteration}"

    # Initial run saves the first 2 checkpoints
    checkpoint_dirs, checkpoints = get_checkpoints(results[0].log_dir)
    assert checkpoint_dirs == ["checkpoint_000000", "checkpoint_000001"]
    assert [ckpt.to_dict()["it"] for ckpt in checkpoints] == [1, 2]

    fail_marker.unlink()
    tuner = Tuner.restore(
        str(tmp_path / "exp_name"), trainable=trainable, resume_errored=True
    )
    results = tuner.fit()

    assert len(results.errors) == 0
    training_iteration = results[0].metrics["training_iteration"]
    # Restored at it=2, reported 3 more times -> should have it=5
    assert training_iteration == 5

    # Restored run saves the 3 more checkpoints, and first checkpoint should be deleted
    checkpoint_dirs, checkpoints = get_checkpoints(results[0].log_dir)
    assert checkpoint_dirs == [f"checkpoint_00000{i}" for i in range(1, 5)]
    assert [ckpt.to_dict()["it"] for ckpt in checkpoints] == [2, 3, 4, 5]


@pytest.mark.parametrize("upload_dir", [None, "memory:///test/"])
def test_tuner_can_restore(tmp_path, upload_dir):
    """Make sure that `can_restore` detects an existing experiment at a
    local/remote path and only returns True if it's at the experiment dir root.
    """
    name = "exp_name"
    if upload_dir:
        path = Path(upload_dir) / name
    else:
        path = tmp_path / name

    assert not Tuner.can_restore(path)
    Tuner(
        lambda config: None,
        run_config=RunConfig(
            name=name,
            storage_path=upload_dir,
            local_dir=str(tmp_path),
        ),
        tune_config=TuneConfig(trial_dirname_creator=lambda t: "trial_dir"),
    )
    (path / "trial_dir").mkdir(parents=True, exist_ok=True)
    if upload_dir:
        upload_to_uri(str(tmp_path / name), str(path))
    assert Tuner.can_restore(path)
    # Can't restore from the trial level
    assert not Tuner.can_restore(path / "trial_dir")
    # Can't restore from the local_dir level
    assert not Tuner.can_restore(tmp_path)

    if upload_dir:
        assert not Tuner.can_restore(Path(upload_dir) / "new_exp")
    else:
        assert not Tuner.can_restore(tmp_path / "new_exp")


def testParamSpaceOverwriteValidation(ray_start_4_cpus, tmp_path):
    """Check that validation on restore fails if we try adding or removing
    hyperparameters to the param_space."""
    name = "test_param_space_valid"
    param_space = {"a": 1, "b": {"c": tune.choice([0, 1])}, "d": tune.uniform(0, 1)}
    tuner = Tuner(
        _train_fn_sometimes_failing,
        param_space=param_space,
        run_config=RunConfig(storage_path=str(tmp_path), name=name),
    )
    tuner.fit()

    bad_param_spaces = [
        {},
        {"a": 1, "b": {}, "d": 2},
        {"a": 1, "b": {"c": 2, "e": 3}, "d": 4},
    ]
    for bad_param_space in bad_param_spaces:
        with pytest.raises(ValueError):
            Tuner.restore(
                str(tmp_path / name),
                trainable=_train_fn_sometimes_failing,
                param_space=bad_param_space,
            )

    # Should work with the original param space
    Tuner.restore(
        str(tmp_path / name),
        trainable=_train_fn_sometimes_failing,
        param_space=param_space,
    )


def testParamSpaceOverwrite(ray_start_4_cpus, tmp_path, monkeypatch):
    """Test that overwriting param space on restore propagates new refs to existing
    trials and newly generated trials."""

    # Limit the number of generated trial configs -- so restore tests
    # newly generated trials.
    monkeypatch.setenv("TUNE_MAX_PENDING_TRIALS_PG", "1")

    class FakeDataset:
        def __init__(self, name):
            self.name = name

        def __repr__(self):
            return f"<FakeDataset {self.name}>"

    def train_fn(config):
        raise RuntimeError("Failing!")

    param_space = {
        "test": tune.grid_search(
            [FakeDataset("1"), FakeDataset("2"), FakeDataset("3")]
        ),
        "test2": tune.grid_search(
            [
                FakeDataset("4"),
                FakeDataset("5"),
                FakeDataset("6"),
                FakeDataset("7"),
            ]
        ),
    }

    tuner = Tuner(
        train_fn,
        param_space=param_space,
        tune_config=TuneConfig(num_samples=1),
        run_config=RunConfig(
            storage_path=str(tmp_path),
            name="param_space_overwrite",
            callbacks=[_FailOnStats(num_trials=4, num_finished=2)],
        ),
    )
    with pytest.raises(RuntimeError):
        tuner.fit()

    # Just suppress the error this time with a new trainable
    def train_fn(config):
        pass

    param_space = {
        "test": tune.grid_search(
            [FakeDataset("8"), FakeDataset("9"), FakeDataset("10")]
        ),
        "test2": tune.grid_search(
            [
                FakeDataset("11"),
                FakeDataset("12"),
                FakeDataset("13"),
                FakeDataset("14"),
            ]
        ),
    }

    tuner = Tuner.restore(
        str(tmp_path / "param_space_overwrite"),
        trainable=train_fn,
        param_space=param_space,
        resume_errored=True,
    )
    tuner._local_tuner._run_config.callbacks = None
    result_grid = tuner.fit()
    assert not result_grid.errors
    assert len(result_grid) == 12

    for r in result_grid:
        # Make sure that test and test2 are updated.
        assert r.config["test"].name in ["8", "9", "10"]
        assert r.config["test2"].name in ["11", "12", "13", "14"]


def test_tuner_pkl_backwards_compatibility(tmp_path, caplog):
    tuner_internal = Tuner(
        _train_fn_sometimes_failing, param_space={"a": 1}
    )._local_tuner
    with open(tmp_path / "tuner.pkl", "wb") as f:
        ray_pickle.dump(tuner_internal, f)

    with caplog.at_level(logging.WARNING, "ray.tune.impl.tuner_internal"):
        tuner_internal._load_tuner_state(tmp_path / "tuner.pkl")
        assert "run with an older version of Ray" in caplog.text


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
