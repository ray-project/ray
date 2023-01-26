import json
import os
import shutil
import time
import unittest
from pathlib import Path
import logging

import pytest

import ray
from ray import tune
from ray.air import (
    Checkpoint,
    CheckpointConfig,
    FailureConfig,
    RunConfig,
    ScalingConfig,
    session,
)
from ray.air._internal.remote_storage import delete_at_uri, download_from_uri
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

    def __init__(self, num_trials: int, num_finished: int, delay: int = 1):
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


def test_tuner_restore_num_trials(ray_start_4_cpus, tmpdir):
    """Number of trials after restoring a finished run should be the same"""
    tuner = Tuner(
        lambda config: 1,
        tune_config=TuneConfig(num_samples=4, metric="_metric", mode="max"),
        run_config=RunConfig(
            name="test_tuner_restore_num_trials", local_dir=str(tmpdir)
        ),
    )
    results = tuner.fit()
    assert len(results) == 4
    assert results.get_best_result().metrics["_metric"] == 1

    del tuner
    tuner = Tuner.restore(str(tmpdir / "test_tuner_restore_num_trials"))

    # Check restored results
    results = tuner.get_results()
    assert len(results) == 4
    assert results.get_best_result().metrics["_metric"] == 1

    results = tuner.fit()
    assert len(results) == 4
    assert results.get_best_result().metrics["_metric"] == 1


def test_tuner_restore_resume_errored(ray_start_4_cpus, tmpdir):
    """Resuming errored trials should pick up from previous state"""
    fail_marker = tmpdir / "fail_marker"
    fail_marker.write_text("", encoding="utf-8")

    tuner = Tuner(
        _train_fn_sometimes_failing,
        tune_config=TuneConfig(
            num_samples=1,
        ),
        run_config=RunConfig(
            name="test_tuner_restore_resume_errored", local_dir=str(tmpdir)
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
        str(tmpdir / "test_tuner_restore_resume_errored"), resume_errored=True
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


def test_tuner_restore_restart_errored(ray_start_4_cpus, tmpdir):
    """Restarting errored trials should re-start from scratch"""
    fail_marker = tmpdir / "fail_marker"
    fail_marker.write_text("", encoding="utf-8")

    tuner = Tuner(
        _train_fn_sometimes_failing,
        tune_config=TuneConfig(num_samples=1),
        run_config=RunConfig(
            name="test_tuner_restore_restart_errored",
            local_dir=str(tmpdir),
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
        str(tmpdir / "test_tuner_restore_restart_errored"), restart_errored=True
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
            local_dir=str(tmpdir),
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

    tuner = Tuner.restore(str(tmpdir / "test_tuner_resume_unfinished"))
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
            local_dir=str(tmpdir),
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
        resume_unfinished=False,
        resume_errored=True,
    )
    tuner._local_tuner._run_config.callbacks = None

    results = tuner.fit()
    assert len(results) == 4
    assert len(results.errors) == 0
    assert sorted([r.metrics.get("it", 0) for r in results]) == sorted([2, 1, 3, 0])


def test_tuner_restore_from_cloud(ray_start_2_cpus, tmpdir):
    """Check that restoring Tuner() objects from cloud storage works"""
    tuner = Tuner(
        lambda config: 1,
        run_config=RunConfig(
            name="exp_dir",
            local_dir=str(tmpdir / "ray_results"),
            sync_config=tune.SyncConfig(upload_dir="memory:///test/restore"),
        ),
    )
    tuner.fit()

    check_path = tmpdir / "check_save"
    download_from_uri("memory:///test/restore", str(check_path))
    remote_contents = os.listdir(check_path / "exp_dir")

    assert "tuner.pkl" in remote_contents
    assert "trainable.pkl" in remote_contents

    prev_cp = _find_newest_experiment_checkpoint(str(check_path / "exp_dir"))
    prev_lstat = os.lstat(prev_cp)

    (tmpdir / "ray_results").remove(ignore_errors=True)

    tuner2 = Tuner.restore("memory:///test/restore/exp_dir")
    results = tuner2.fit()

    assert results[0].metrics["_metric"] == 1
    local_contents = os.listdir(tmpdir / "ray_results" / "exp_dir")
    assert "tuner.pkl" in local_contents
    assert "trainable.pkl" in local_contents

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
    tuner3 = Tuner.restore("memory:///test/restore/exp_dir")
    tuner3.fit()


@pytest.mark.parametrize(
    "upload_uri",
    [None, "memory:///test/test_tuner_restore_latest_available_checkpoint"],
)
def test_tuner_restore_latest_available_checkpoint(
    ray_start_4_cpus, tmpdir, upload_uri
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
            local_dir=str(tmpdir),
            sync_config=tune.SyncConfig(upload_dir=upload_uri),
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
        resume_errored=True,
    )
    results = tuner.fit()
    assert len(results) == 1
    assert len(results.errors) == 0
    [result] = list(results)
    # restored from 2, plus num_epochs = 4, plus one additional epoch
    assert result.metrics["it"] == 7
    assert result.metrics["iterations_since_restore"] == 5


@pytest.mark.parametrize("retry_num", [0, 1])
def test_restore_retry(ray_start_4_cpus, tmpdir, retry_num):
    """Test retrying restore on a trial level."""

    class MockTrainable(Trainable):
        def setup(self, config):
            self.idx = 0
            self.tag_file_path = config["tag_file_path"]
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
            if not os.path.exists(self.tag_file_path):
                Path(self.tag_file_path).touch()
                raise RuntimeError("===== Failing first restore =====")
            # The following restore should pass!
            with open(checkpoint_path) as f:
                self.idx = json.loads(f.read())["idx"]

    # Set environment variable just for this test
    with unittest.mock.patch.dict(
        os.environ, {"TUNE_RESTORE_RETRY_NUM": str(retry_num)}
    ):
        tag_file = os.path.join(tmpdir, "tag")
        tuner = Tuner(
            MockTrainable,
            run_config=RunConfig(
                name="tryout_restore",
                stop={"training_iteration": 5},
                local_dir=str(tmpdir),
                failure_config=FailureConfig(max_failures=1),
                checkpoint_config=CheckpointConfig(checkpoint_frequency=1),
            ),
            param_space={"tag_file_path": tag_file},
        )
        results = tuner.fit()
        [result] = list(results)
        if retry_num == 1:
            assert result.metrics["score"] == 5
        else:
            assert result.metrics["score"] == 2


def test_restore_overwrite_trainable(ray_start_4_cpus, tmpdir, caplog):
    """Test validation for trainable compatibility, when re-specifying a trainable
    on restore."""

    def train_func_1(config):
        data = {"data": config["data"]}
        session.report(data, checkpoint=Checkpoint.from_dict(data))
        raise RuntimeError("Failing!")

    tuner = Tuner(
        train_func_1,
        run_config=RunConfig(name="overwrite_trainable", local_dir=str(tmpdir)),
        param_space={"data": 1},
    )
    tuner.fit()

    del tuner

    # Can't overwrite with a different Trainable type
    with pytest.raises(ValueError):
        tuner = Tuner.restore(
            str(tmpdir / "overwrite_trainable"),
            overwrite_trainable="__fake",
            resume_errored=True,
        )

    # Can't overwrite with a different Trainable name
    def train_func_2(config):
        checkpoint = session.get_checkpoint()
        assert checkpoint and checkpoint.to_dict()["data"] == config["data"]

    with pytest.raises(ValueError):
        tuner = Tuner.restore(
            str(tmpdir / "overwrite_trainable"),
            overwrite_trainable=train_func_2,
            resume_errored=True,
        )

    # Can still change trainable code, but logs a warning
    def train_func_1(config):
        checkpoint = session.get_checkpoint()
        assert checkpoint and checkpoint.to_dict()["data"] == config["data"]

    caplog.clear()
    with caplog.at_level(logging.WARNING, logger="ray.tune.impl.tuner_internal"):
        tuner = Tuner.restore(
            str(tmpdir / "overwrite_trainable"),
            overwrite_trainable=train_func_1,
            resume_errored=True,
        )
        assert "The trainable will be overwritten" in caplog.text

    results = tuner.fit()
    assert not results.errors


@pytest.mark.parametrize("use_function_trainable", [True, False])
def test_restore_with_parameters(ray_start_4_cpus, tmp_path, use_function_trainable):
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
            local_dir=str(tmp_path),
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

    tuner = Tuner.restore(
        str(tmp_path / exp_name),
        resume_errored=True,
    )
    # Should still be able to access results
    assert len(tuner.get_results().errors) == 1

    # Continuing to fit should fail if we didn't re-specify the trainable
    with pytest.raises(ValueError):
        tuner.fit()

    fail_marker.unlink()
    tuner = Tuner.restore(
        str(tmp_path / exp_name),
        resume_errored=True,
        overwrite_trainable=create_trainable_with_params(),
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
            local_dir=str(old_local_dir),
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
            local_dir=str(new_local_dir),
            resume="AUTO+ERRORED",
        )
        results = ResultGrid(analysis)
    else:
        restore_path = str(new_local_dir / new_exp_name)
        tuner = Tuner.restore(restore_path, resume_errored=True)
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


def test_restore_from_relative_path(ray_start_4_cpus, chdir_tmpdir):
    tuner = Tuner(
        lambda config: session.report({"score": 1}),
        run_config=RunConfig(local_dir="relative_dir", name="exp_name"),
    )
    tuner.fit()

    tuner = Tuner.restore("relative_dir/exp_name")
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
        run_config=RunConfig(local_dir=str(tmpdir), name="exp_name"),
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

    tuner = Tuner.restore(str(tmpdir / "exp_name"), resume_errored=True)
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
def test_checkpoints_saved_after_resume(tmp_path, use_air_trainer):
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
            local_dir=str(tmp_path),
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
    tuner = Tuner.restore(str(tmp_path / "exp_name"), resume_errored=True)
    results = tuner.fit()

    assert len(results.errors) == 0
    training_iteration = results[0].metrics["training_iteration"]
    # Restored at it=2, reported 3 more times -> should have it=5
    assert training_iteration == 5

    # Restored run saves the 3 more checkpoints, and first checkpoint should be deleted
    checkpoint_dirs, checkpoints = get_checkpoints(results[0].log_dir)
    assert checkpoint_dirs == [f"checkpoint_00000{i}" for i in range(1, 5)]
    assert [ckpt.to_dict()["it"] for ckpt in checkpoints] == [2, 3, 4, 5]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
