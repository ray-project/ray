import json
import logging
import os
import sys
import tempfile
import time
from unittest import mock

import pytest

import ray
from ray.air.constants import TRAINING_ITERATION
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.train import Checkpoint, CheckpointConfig
from ray.train._internal.session import _TrainingResult
from ray.train._internal.storage import StorageContext
from ray.train.tests.util import mock_storage_context
from ray.tune import PlacementGroupFactory, ResumeConfig
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial
from ray.tune.result import DONE
from ray.tune.schedulers import FIFOScheduler
from ray.tune.search import BasicVariantGenerator
from ray.tune.tests.tune_test_util import TrialResultObserver

STORAGE = mock_storage_context()


@pytest.fixture(scope="function")
def ray_start_4_cpus_2_gpus_extra():
    address_info = ray.init(num_cpus=4, num_gpus=2, resources={"a": 2})
    yield address_info
    ray.shutdown()


def create_mock_components():
    class _MockScheduler(FIFOScheduler):
        errored_trials = []

        def on_trial_error(self, tune_controller, trial):
            self.errored_trials += [trial]

    class _MockSearchAlg(BasicVariantGenerator):
        errored_trials = []

        def on_trial_complete(self, trial_id, error=False, **kwargs):
            if error:
                self.errored_trials += [trial_id]

    searchalg = _MockSearchAlg()
    scheduler = _MockScheduler()
    return searchalg, scheduler


def num_checkpoints(trial):
    return sum(
        item.startswith("checkpoint_")
        for item in os.listdir(trial.storage.trial_fs_path)
    )


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_checkpoint_save_restore(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmpdir
):
    """Test that a checkpoint is saved and can be used to restore a trainable.

    The trainable saves a checkpoint and terminates. We then start another trial
    that should restore from the saved checkpoint and assert that it picks up
    the state and continues to run to termination.

    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testCheckpointing
    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testRestoreMetricsAfterCheckpointing  # noqa
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(), storage=STORAGE
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 1},
        "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "GPU": 1}]),
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
        "storage": STORAGE,
    }
    runner.add_trial(Trial("__fake", **kwargs))
    trials = runner.get_trials()

    runner.step()  # Start trial

    while trials[0].status != Trial.RUNNING:
        runner.step()

    # Set some state that will be saved in the checkpoint
    assert ray.get(trials[0].temporary_state.ray_actor.set_info.remote(1)) == 1

    while trials[0].status != Trial.TERMINATED:
        runner.step()

    assert trials[0].latest_checkpoint_result.metrics[TRAINING_ITERATION] == 1
    assert trials[0].last_result[TRAINING_ITERATION] == 1
    assert trials[0].last_result["iterations_since_restore"] == 1

    # Prepare new trial
    kwargs["restore_path"] = trials[0].checkpoint.path
    new_trial = Trial("__fake", **kwargs)
    runner.add_trial(new_trial)
    trials = runner.get_trials()

    assert trials[1].status == Trial.PENDING

    # Start trial, restore, run to termination
    while trials[1].status != Trial.RUNNING:
        runner.step()

    # Restore
    runner.step()

    assert ray.get(trials[1].temporary_state.ray_actor.get_info.remote()) == 1

    # Run to termination
    while trials[1].status != Trial.TERMINATED:
        runner.step()

    assert trials[0].latest_checkpoint_result.metrics[TRAINING_ITERATION] == 1
    assert trials[1].last_result[TRAINING_ITERATION] == 1
    assert trials[1].last_result["iterations_since_restore"] == 1


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_checkpoint_at_end(ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmpdir):
    """Test that a checkpoint is saved at end for class trainables with that config.

    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testCheckpointingAtEnd
    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testResultDone
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 2},
        "checkpoint_config": CheckpointConfig(checkpoint_at_end=True),
        "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "GPU": 1}]),
        "storage": STORAGE,
    }
    runner.add_trial(Trial("__fake", **kwargs))
    trials = runner.get_trials()

    while not runner.is_finished():
        runner.step()

    assert trials[0].has_checkpoint()
    assert trials[0].last_result[DONE]


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_pause_resume_trial(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmpdir
):
    """Test that trial that is paused and resumed picks up its last checkpoint.

    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testPauseThenResume
    """
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
    )
    kwargs = {
        "stopping_criterion": {"training_iteration": 2},
        "placement_group_factory": PlacementGroupFactory([{"CPU": 1, "GPU": 1}]),
        "checkpoint_config": CheckpointConfig(checkpoint_frequency=1),
        "storage": STORAGE,
    }
    runner.add_trial(Trial("__fake", **kwargs))
    trials = runner.get_trials()

    while trials[0].status != Trial.RUNNING:
        runner.step()

    assert ray.get(trials[0].temporary_state.ray_actor.get_info.remote()) is None
    assert ray.get(trials[0].temporary_state.ray_actor.set_info.remote(1)) == 1

    runner._schedule_trial_pause(trials[0], should_checkpoint=True)

    while trials[0].status != Trial.PAUSED:
        runner.step()

    assert trials[0].has_checkpoint()
    assert not trials[0].last_result.get(DONE), trials[0].last_result

    # Start again
    runner._set_trial_status(trials[0], Trial.PENDING)

    while trials[0].status != Trial.RUNNING:
        runner.step()

    assert ray.get(trials[0].temporary_state.ray_actor.get_info.remote()) == 1

    while trials[0].status != Trial.TERMINATED:
        runner.step()

    assert trials[0].checkpoint
    assert trials[0].last_result[TRAINING_ITERATION] == 2
    assert trials[0].last_result["iterations_since_restore"] == 1
    assert trials[0].last_result["time_since_restore"] > 0


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_checkpoint_num_to_keep(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmp_path
):
    """Test that only num_to_keep checkpoints are kept.

    This should also hold true when the experiment is resumed.

    Legacy test: test_trial_runner_2.py::TrialRunnerTest::testPauseResumeCheckpointCount
    """
    trial = Trial(
        "__fake", checkpoint_config=CheckpointConfig(num_to_keep=2), storage=STORAGE
    )
    trial.init_local_path()

    def write_checkpoint(trial: Trial, index: int):
        checkpoint_dir = tmp_path / StorageContext._make_checkpoint_dir_name(index)
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        result = {"training_iteration": index}
        with open(os.path.join(checkpoint_dir, "cp.json"), "w") as f:
            json.dump(result, f)

        checkpoint = Checkpoint.from_directory(checkpoint_dir)
        return _TrainingResult(checkpoint=checkpoint, metrics=result)

    def get_checkpoint_dirs(trial: Trial):
        return [d for d in os.listdir(tmp_path) if d.startswith("checkpoint_")]

    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(), storage=STORAGE
    )

    runner.add_trial(trial)

    # Write 1 checkpoint
    result = write_checkpoint(trial, 1)
    runner._on_saving_result(trial, result)

    # Expect 1 checkpoint
    cp_dirs = get_checkpoint_dirs(trial)
    assert len(cp_dirs) == 1, f"Checkpoint dirs: {cp_dirs}"

    # Write second checkpoint
    result = write_checkpoint(trial, 2)
    runner._on_saving_result(trial, result)

    # Expect 2 checkpoints
    cp_dirs = get_checkpoint_dirs(trial)
    assert len(cp_dirs) == 2, f"Checkpoint dirs: {cp_dirs}"

    # Write third checkpoint
    result = write_checkpoint(trial, 3)
    runner._on_saving_result(trial, result)

    # Expect 2 checkpoints because num_to_keep = 2
    cp_dirs = get_checkpoint_dirs(trial)
    assert len(cp_dirs) == 2, f"Checkpoint dirs: {cp_dirs}"

    # Re-instantiate trial runner and resume
    runner.checkpoint(force=True, wait=True)
    runner = TuneController(
        resource_manager_factory=lambda: resource_manager_cls(),
        storage=STORAGE,
        resume_config=ResumeConfig(),
    )

    trial = runner.get_trials()[0]

    # Write fourth checkpoint
    result = write_checkpoint(trial, 4)
    runner._on_saving_result(trial, result)

    # Expect 2 checkpoints because num_to_keep = 2
    cp_dirs = get_checkpoint_dirs(trial)
    assert len(cp_dirs) == 2, f"Checkpoint dirs: {cp_dirs}"

    # Write fifth checkpoint
    result = write_checkpoint(trial, 5)
    runner._on_saving_result(trial, result)

    # Expect 2 checkpoints because num_to_keep = 2
    cp_dirs = get_checkpoint_dirs(trial)
    assert len(cp_dirs) == 2, f"Checkpoint dirs: {cp_dirs}"

    # Checkpoints before restore should be deleted
    assert "checkpoint_000004" in cp_dirs
    assert "checkpoint_000005" in cp_dirs

    assert "checkpoint_000002" not in cp_dirs
    assert "checkpoint_000003" not in cp_dirs


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_checkpoint_freq_buffered(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmp_path
):
    """Test that trial checkpoints are a lower bound for buffered training iterations.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testCheckpointFreqBuffered
    """
    with mock.patch.dict(
        os.environ,
        {"TUNE_RESULT_BUFFER_LENGTH": "7", "TUNE_RESULT_BUFFER_MIN_TIME_S": "1"},
    ):
        trial = Trial(
            "__fake",
            checkpoint_config=CheckpointConfig(checkpoint_frequency=3),
            storage=STORAGE,
        )
        runner = TuneController(
            resource_manager_factory=lambda: resource_manager_cls(),
            storage=STORAGE,
            checkpoint_period=0,
        )
        runner.add_trial(trial)

        while not trial.is_saving:
            runner.step()
        runner.step()
        assert trial.last_result[TRAINING_ITERATION] == 3
        assert num_checkpoints(trial) == 1

        while not trial.is_saving:
            runner.step()
        runner.step()
        assert trial.last_result[TRAINING_ITERATION] == 6
        assert num_checkpoints(trial) == 2

        while not trial.is_saving:
            runner.step()
        runner.step()
        assert trial.last_result[TRAINING_ITERATION] == 9
        assert num_checkpoints(trial) == 3


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_checkpoint_at_end_not_buffered(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmp_path
):
    """Test that trials with `checkpoint_at_end=True` are never buffered.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testCheckpointAtEndNotBuffered
    """
    with mock.patch.dict(
        os.environ,
        {"TUNE_RESULT_BUFFER_LENGTH": "7", "TUNE_RESULT_BUFFER_MIN_TIME_S": "0.5"},
    ):
        trial = Trial(
            "__fake",
            checkpoint_config=CheckpointConfig(
                checkpoint_at_end=True,
            ),
            stopping_criterion={"training_iteration": 4},
            storage=STORAGE,
        )
        observer = TrialResultObserver()
        runner = TuneController(
            resource_manager_factory=lambda: resource_manager_cls(),
            storage=STORAGE,
            callbacks=[observer],
        )
        runner.add_trial(trial)

        while not observer.just_received_a_result():
            runner.step()
        assert trial.last_result[TRAINING_ITERATION] == 1
        assert num_checkpoints(trial) == 0

        while True:
            runner.step()
            if observer.just_received_a_result():
                break
        assert trial.last_result[TRAINING_ITERATION] == 2
        assert num_checkpoints(trial) == 0

        while True:
            runner.step()
            if observer.just_received_a_result():
                break
        assert trial.last_result[TRAINING_ITERATION] == 3
        assert num_checkpoints(trial) == 0

        while True:
            runner.step()
            if observer.just_received_a_result():
                break
        assert trial.last_result[TRAINING_ITERATION] == 4

        while not runner.is_finished():
            runner.step()
        assert num_checkpoints(trial) == 1


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_checkpoint_user_checkpoint(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmp_path
):
    """Test that user checkpoint freq is respected.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testUserCheckpoint
    """
    with mock.patch.dict(
        os.environ,
        {"TUNE_RESULT_BUFFER_LENGTH": "1", "TUNE_MAX_PENDING_TRIALS_PG": "1"},
    ):
        runner = TuneController(
            resource_manager_factory=lambda: resource_manager_cls(), storage=STORAGE
        )
        runner.add_trial(
            Trial("__fake", config={"user_checkpoint_freq": 2}, storage=STORAGE)
        )
        trials = runner.get_trials()

        while not trials[0].status == Trial.RUNNING:
            runner.step()
        assert ray.get(trials[0].temporary_state.ray_actor.set_info.remote(1)) == 1

        while trials[0].last_result.get(TRAINING_ITERATION, 0) < 1:
            runner.step()  # Process result
        assert not trials[0].has_checkpoint()
        while trials[0].last_result.get(TRAINING_ITERATION, 99) < 2:
            runner.step()  # Process result
        assert not trials[0].has_checkpoint()

        while trials[0].last_result.get(TRAINING_ITERATION, 99) < 3:
            runner.step()  # Process result
        runner.step()

        assert trials[0].has_checkpoint()
        runner.checkpoint(force=True, wait=True)

        runner2 = TuneController(
            resource_manager_factory=lambda: resource_manager_cls(),
            storage=STORAGE,
            resume_config=ResumeConfig(),
        )
        trials2 = runner2.get_trials()
        while not trials2[0].status == Trial.RUNNING:
            runner2.step()
        assert ray.get(trials2[0].temporary_state.ray_actor.get_info.remote()) == 1


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_checkpoint_user_checkpoint_buffered(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmp_path
):
    """Test that user checkpoint freq is respected with buffered training.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testUserCheckpointBuffered
    """

    with mock.patch.dict(
        os.environ,
        {"TUNE_RESULT_BUFFER_LENGTH": "8", "TUNE_RESULT_BUFFER_MIN_TIME_S": "1"},
    ):
        runner = TuneController(
            resource_manager_factory=lambda: resource_manager_cls(),
            storage=STORAGE,
            checkpoint_period=0,
        )
        runner.add_trial(
            Trial("__fake", config={"user_checkpoint_freq": 10}, storage=STORAGE)
        )
        trials = runner.get_trials()

        while trials[0].status != Trial.RUNNING:
            runner.step()
        assert ray.get(trials[0].temporary_state.ray_actor.set_info.remote(1)) == 1
        assert num_checkpoints(trials[0]) == 0

        while trials[0].last_result.get(TRAINING_ITERATION, 0) < 8:
            runner.step()

        assert not trials[0].has_checkpoint()
        assert num_checkpoints(trials[0]) == 0

        while trials[0].last_result.get(TRAINING_ITERATION) < 11:
            runner.step()
        runner.step()
        assert trials[0].has_checkpoint()
        assert num_checkpoints(trials[0]) == 1

        while trials[0].last_result.get(TRAINING_ITERATION) < 19:
            runner.step()
        runner.step()
        assert trials[0].has_checkpoint()
        assert num_checkpoints(trials[0]) == 1

        while trials[0].last_result.get(TRAINING_ITERATION) < 21:
            runner.step()
        runner.step()
        assert trials[0].has_checkpoint()
        assert num_checkpoints(trials[0]) == 2

        while trials[0].last_result.get(TRAINING_ITERATION) < 29:
            runner.step()
        runner.step()
        assert trials[0].has_checkpoint()
        assert num_checkpoints(trials[0]) == 2


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
def test_checkpoint_auto_period(
    ray_start_4_cpus_2_gpus_extra, resource_manager_cls, tmp_path
):
    """Test that the checkpoint auto period is adjusted when syncing takes a long time.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::testCheckpointAutoPeriod
    """
    storage = mock_storage_context()

    with tempfile.TemporaryDirectory() as local_dir:
        storage.storage_local_path = local_dir

        runner = TuneController(
            resource_manager_factory=lambda: resource_manager_cls(),
            storage=storage,
            checkpoint_period="auto",
        )

        with mock.patch.object(runner, "save_to_dir") as save_to_dir:
            save_to_dir.side_effect = lambda *a, **kw: time.sleep(2)

            runner.add_trial(
                Trial("__fake", config={"user_checkpoint_freq": 1}, storage=storage)
            )

            runner.step()  # Run one step, this will trigger checkpointing

        assert runner._checkpoint_manager._checkpoint_period > 38.0


def test_checkpoint_force_with_num_to_keep(ray_start_4_cpus_2_gpus_extra, tmp_path):
    """Test that cloud syncing is forced if one of the trials has made more
    than num_to_keep checkpoints since last sync.
    Legacy test: test_trial_runner_3.py::TrialRunnerTest::
        testCloudCheckpointForceWithNumToKeep
    """
    storage = mock_storage_context()
    # Needed to avoid infinite recursion error on CI runners
    storage.syncer.__getstate__ = lambda *a, **kw: {}

    with mock.patch.object(storage.syncer, "sync_up") as sync_up:
        num_to_keep = 2
        checkpoint_config = CheckpointConfig(
            num_to_keep=num_to_keep, checkpoint_frequency=1
        )

        runner = TuneController(
            resource_manager_factory=lambda: PlacementGroupResourceManager(),
            storage=storage,
            checkpoint_period=100,  # only rely on force syncing
            trial_checkpoint_config=checkpoint_config,
        )

        class CheckpointingTrial(Trial):
            def should_checkpoint(self):
                return True

            def get_json_state(self):
                return "", ""

        trial = CheckpointingTrial(
            "__fake",
            checkpoint_config=checkpoint_config,
            stopping_criterion={"training_iteration": 10},
            storage=storage,
        )
        runner.add_trial(trial)

        # also check if the warning is printed
        buffer = []
        from ray.tune.execution.experiment_state import logger

        with mock.patch.object(logger, "warning", lambda x: buffer.append(x)):
            while not runner.is_finished():
                runner.step()

        assert any(
            "Experiment state snapshotting has been triggered multiple times" in x
            for x in buffer
        )
        # We should sync 6 times:
        # The first checkpoint happens when the experiment starts,
        # since no checkpoints have happened yet
        # (This corresponds to the new_trial event in the runner loop)
        # Then, every num_to_keep=2 checkpoints, we should perform a forced checkpoint
        # which results in 5 more checkpoints (running for 10 iterations),
        # giving a total of 6
        assert sync_up.call_count == 6


def test_checkpoint_sync_up_timeout(
    ray_start_4_cpus_2_gpus_extra, tmp_path, monkeypatch
):
    """Test that trial runner experiment checkpointing times out correctly.

    Legacy test: test_trial_runner_3.py::TrialRunnerTest::
        testForcedCloudCheckpointSyncTimeout
    """
    storage = mock_storage_context(sync_config=ray.train.SyncConfig(sync_timeout=0.5))
    monkeypatch.setenv("TUNE_WARN_SLOW_EXPERIMENT_CHECKPOINT_SYNC_THRESHOLD_S", "0.25")

    def _hanging_upload_to_fs_path(*args, **kwargs):
        time.sleep(200)

    monkeypatch.setattr(
        ray.train._internal.storage,
        "_upload_to_fs_path",
        _hanging_upload_to_fs_path,
    )

    runner = TuneController(
        resource_manager_factory=lambda: PlacementGroupResourceManager(),
        storage=storage,
    )

    # Start a hanging sync that should not block the controller
    runner.checkpoint()

    buffer = []
    logger = logging.getLogger("ray.tune.execution.experiment_state")
    with mock.patch.object(logger, "error", lambda x, **kwargs: buffer.append(x)):
        with mock.patch.object(logger, "warning", lambda x: buffer.append(x)):
            runner.checkpoint(force=True, wait=True)

    # We should see a log about the timeout
    assert any("Saving experiment state to storage" in x for x in buffer)
    # We should also have a warning about the slow upload
    assert any("may be a performance bottleneck" in x for x in buffer)


def test_checkpoint_sync_up_error(ray_start_4_cpus_2_gpus_extra, tmp_path, monkeypatch):
    """Test that trial runner experiment checkpointing handles errors correctly."""
    storage = mock_storage_context()

    def _failing_upload_to_fs_path(*args, **kwargs):
        raise RuntimeError("Upload failing...")

    monkeypatch.setattr(
        ray.train._internal.storage,
        "_upload_to_fs_path",
        _failing_upload_to_fs_path,
    )

    runner = TuneController(
        resource_manager_factory=lambda: PlacementGroupResourceManager(),
        storage=storage,
    )

    # Launching a failing upload task should not crash the controller / main thread
    runner.checkpoint()

    buffer = []
    logger = logging.getLogger("ray.tune.execution.experiment_state")
    with mock.patch.object(logger, "error", lambda x, **kwargs: buffer.append(x)):
        runner.checkpoint(force=True)

    # We should see a log about the failure
    assert any("Saving experiment state to storage" in x for x in buffer)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
