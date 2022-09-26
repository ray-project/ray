import os
import glob
import shutil
import time

import pytest
import ray
from ray import tune
from ray.air import RunConfig, Checkpoint, session, FailureConfig
from ray.air._internal.remote_storage import (
    download_from_uri,
    delete_at_uri,
    upload_to_uri,
)
from ray.tune import Callback
from ray.tune.execution.trial_runner import _find_newest_experiment_checkpoint
from ray.tune.experiment import Trial
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


def _train_fn_sometimes_failing(config):
    # Fails if failing is set and marker file exists.
    # Hangs if hanging is set and marker file exists.
    failing, hanging = config["failing_hanging"]

    if session.get_checkpoint():
        state = session.get_checkpoint().to_dict()
    else:
        state = {"it": 0}

    state["it"] += 1

    session.report(state, checkpoint=Checkpoint.from_dict(state))

    # We fail after reporting one checkpoint.
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


def test_tuner_restore_num_trials(ray_start_4_cpus, tmpdir):
    """Number of trials after restoring a finished run should be the same"""
    tuner = Tuner(
        lambda config: 1,
        tune_config=TuneConfig(num_samples=4),
        run_config=RunConfig(
            name="test_tuner_restore_num_trials", local_dir=str(tmpdir)
        ),
    )
    tuner.fit()

    del tuner
    tuner = Tuner.restore(str(tmpdir / "test_tuner_restore_num_trials"))
    results = tuner.fit()
    assert len(results) == 4


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
    # Catch the FailOnStats erro
    with pytest.raises(tune.TuneError):
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
    with pytest.raises(tune.TuneError):
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
    "cloud_checkpointing",
    [True, False],
)
def test_tuner_restore_from_moved_experiment_path(
    ray_start_2_cpus, tmp_path, cloud_checkpointing
):
    """Check that restoring Tuner() objects from a moved directory works"""
    # Create a fail_marker dummy file that causes the first Tune run to fail and
    # the second run to succeed
    fail_marker = tmp_path / "fail_marker"
    fail_marker.write_text("", encoding="utf-8")

    # 1. Initial training run (that errors out in the middle)
    sync_config, cloud_uri = None, None
    if cloud_checkpointing:
        # Use fsspec file system to mimic cloud checkpoint storage
        cloud_uri = "memory:///original_dir/restore"
        sync_config = tune.SyncConfig(upload_dir=cloud_uri)

    exp_name = "exp_dir"

    def init_tuner(local_dir, sync_config):
        return Tuner(
            _train_fn_sometimes_failing,
            tune_config=TuneConfig(
                num_samples=1,
            ),
            run_config=RunConfig(
                name=exp_name,
                local_dir=local_dir,
                sync_config=sync_config,
            ),
            param_space={
                "failing_hanging": (fail_marker, None),
            },
        )

    tuner = init_tuner(str(tmp_path / "ray_results"), sync_config)
    results = tuner.fit()
    assert len(results.errors) == 1
    training_iteration = results[0].metrics["it"]
    assert (
        training_iteration == 1
    ), f"Should only have 1 session.report before erroring, got {training_iteration}"

    # 2. Move the experiment parent directory (on cloud/local)
    new_cloud_uri = None
    if cloud_checkpointing:
        # Move from memory:///original_dir/restore -> memory:///new_dir/restore
        check_path = tmp_path / "check_save"
        assert cloud_uri
        download_from_uri(cloud_uri, str(check_path))
        delete_at_uri(cloud_uri)
        new_cloud_uri = "memory:///new_dir/restore"
        upload_to_uri(check_path, new_cloud_uri)

    # Move local dir from tmp_path/ray_results -> tmp_path/moved_ray_results
    shutil.move(str(tmp_path / "ray_results"), str(tmp_path / "moved_ray_results"))
    if cloud_checkpointing:
        # Move all checkpoints back -- simulating "sync down from cloud"
        # NOTE: This is a workaround for cloud checkpointing to a mock `memory://`
        # filesys. The remote Trainable doesn't save to the same filesys, which is why
        # we move the checkpoints back to the original folder where they would be
        # if synced down from the cloud.
        checkpoints_glob = tmp_path / "moved_ray_results" / exp_name / "*/checkpoint_*"
        for ckpt_dir in glob.iglob(str(checkpoints_glob)):
            relpath = os.path.relpath(ckpt_dir, tmp_path / "moved_ray_results")
            shutil.move(ckpt_dir, tmp_path / "ray_results" / relpath)

    # Remove fail_marker so that the restored Tuner doesn't error again
    del tuner
    fail_marker.unlink()

    # 3. Restore from moved experiment directory location
    restore_path = (
        "memory:///new_dir/restore/exp_dir"
        if cloud_checkpointing
        else str(tmp_path / "moved_ray_results" / "exp_dir")
    )
    tuner = Tuner.restore(restore_path, resume_errored=True)

    # 4. Should be able to fit using the restored Tuner
    results = tuner.fit()
    assert len(results.errors) == 0
    # Check that we restored iter=1, then made 2 calls to session.report -> iter=3
    training_iteration = results[0].metrics["it"]
    assert training_iteration == 3, training_iteration

    # 5. Make sure that we did not create a logdir in the old location
    if cloud_checkpointing:
        cloud_root_path = tmp_path / "cloud_root_dir"
        download_from_uri("memory:///", str(cloud_root_path))
        remote_contents = os.listdir(cloud_root_path)
        assert "new_dir" in remote_contents
        assert "original_dir" not in remote_contents

        # We should have synced down from the cloud, which results in:
        # - tmp_path/ray_results being downloaded from cloud (newly created)
        # - tmp_path/moved_ray_results exists as the moved copy of the first run, but
        #   not used at all
        assert (tmp_path / "ray_results").exists()
        assert (tmp_path / "moved_ray_results").exists()
    else:
        assert not (tmp_path / "ray_results").exists()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
