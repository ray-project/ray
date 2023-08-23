import logging
import os
from pathlib import Path
import shutil
import tempfile
from typing import Optional

import pytest
from freezegun import freeze_time

import ray.util
from ray.air._internal.checkpoint_manager import CheckpointStorage, _TrackedCheckpoint
from ray.air.constants import TRAINING_ITERATION, REENABLE_DEPRECATED_SYNC_TO_HEAD_NODE
from ray.exceptions import RayActorError
from ray.tune import TuneError
from ray.tune.logger import NoopLogger
from ray.tune.result import TIME_TOTAL_S
from ray.tune.syncer import (
    _SYNC_TO_HEAD_DEPRECATION_MESSAGE,
    DEFAULT_SYNC_PERIOD,
    SyncConfig,
    SyncerCallback,
    _BackgroundProcess,
)
from ray.tune.trainable import wrap_function
from ray.tune.trainable.function_trainable import NULL_MARKER
from ray.tune.utils.callback import _create_default_callbacks
from ray.tune.utils.file_transfer import sync_dir_between_nodes


@pytest.fixture
def propagate_logs():
    # Ensure that logs are propagated to ancestor handles. This is required if using the
    # caplog fixture with Ray's logging.
    # NOTE: This only enables log propagation in the driver process, not the workers!
    logger = logging.getLogger("ray")
    logger.propagate = True
    yield
    logger.propagate = False


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2, configure_logging=False)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture(autouse=True)
def enable_legacy_head_node_syncing(monkeypatch):
    monkeypatch.setenv(REENABLE_DEPRECATED_SYNC_TO_HEAD_NODE, "1")


@pytest.fixture
def temp_data_dirs():
    tmp_source = os.path.realpath(tempfile.mkdtemp())
    tmp_target = os.path.realpath(tempfile.mkdtemp())

    os.makedirs(os.path.join(tmp_source, "subdir", "nested"))
    os.makedirs(os.path.join(tmp_source, "subdir_exclude", "something"))

    files = [
        "level0.txt",
        "level0_exclude.txt",
        "subdir/level1.txt",
        "subdir/level1_exclude.txt",
        "subdir/nested/level2.txt",
        "subdir_nested_level2_exclude.txt",
        "subdir_exclude/something/somewhere.txt",
    ]

    for file in files:
        with open(os.path.join(tmp_source, file), "w") as f:
            f.write("Data")

    yield tmp_source, tmp_target

    shutil.rmtree(tmp_source)
    shutil.rmtree(tmp_target)


@pytest.fixture
def syncer_callback_test_setup(ray_start_2_cpus, temp_data_dirs):
    """Harness that sets up a sync directory and syncs one file (level0.txt) to start.
    This test also writes another file (level0_new.txt) and advances time such that
    the next `on_trial_result` will happen after the `sync_period` and start a new
    sync process.
    """
    tmp_source, tmp_target = temp_data_dirs

    sync_period = 60
    with freeze_time() as frozen:
        syncer_callback = TestSyncerCallback(
            sync_period=sync_period, local_logdir_override=tmp_target
        )

        trial1 = MockTrial(trial_id="a", logdir=tmp_source)

        syncer_callback.on_trial_result(iteration=1, trials=[], trial=trial1, result={})
        syncer_callback.wait_for_all()

        assert_file(True, tmp_target, "level0.txt")
        assert_file(False, tmp_target, "level0_new.txt")

        # Add new file to source directory
        with open(os.path.join(tmp_source, "level0_new.txt"), "w") as f:
            f.write("Data\n")

        assert_file(False, tmp_target, "level0_new.txt")

        frozen.tick(sync_period)

        expected_filenames = ["level0.txt", "level0_new.txt"]
        expected_files_after_sync = [
            os.path.join(tmp_target, fn) for fn in expected_filenames
        ]

        yield syncer_callback, trial1, expected_files_after_sync, tmp_target


def assert_file(exists: bool, root: str, path: str = ""):
    full_path = Path(root) / path
    assert exists == full_path.exists()


class MockTrial:
    def __init__(
        self,
        trial_id: str,
        logdir: str,
        on_dead_node: bool = False,
        runner_ip: str = None,
    ):
        self.trial_id = trial_id
        self.uses_cloud_checkpointing = False
        self.sync_on_checkpoint = True

        self.logdir = logdir
        self.local_path = logdir
        self._local_ip = runner_ip or ray.util.get_node_ip_address()
        self._on_dead_node = on_dead_node

    def get_ray_actor_ip(self):
        if self._on_dead_node:
            raise RayActorError()
        return self._local_ip


class TestSyncerCallback(SyncerCallback):
    def __init__(
        self,
        enabled: bool = True,
        sync_period: float = DEFAULT_SYNC_PERIOD,
        local_logdir_override: Optional[str] = None,
        remote_logdir_override: Optional[str] = None,
    ):
        super(TestSyncerCallback, self).__init__(
            enabled=enabled, sync_period=sync_period
        )
        self._min_iter_threshold = 0
        self._min_time_s_threshold = 0

        self.local_logdir_override = local_logdir_override
        self.remote_logdir_override = remote_logdir_override

    def _local_trial_logdir(self, trial):
        if self.local_logdir_override:
            return self.local_logdir_override
        return super(TestSyncerCallback, self)._local_trial_logdir(trial)

    def _remote_trial_logdir(self, trial):
        if self.remote_logdir_override:
            return self.remote_logdir_override
        return super(TestSyncerCallback, self)._remote_trial_logdir(trial)

    def _get_trial_sync_process(self, trial):
        return self._sync_processes.setdefault(
            trial.trial_id, MaybeFailingProcess(sync_dir_between_nodes)
        )


class MaybeFailingProcess(_BackgroundProcess):
    should_fail = False

    def wait(self):
        result = super(MaybeFailingProcess, self).wait()
        if self.should_fail:
            raise TuneError("Syncing failed.")
        return result


def test_syncer_callback_disabled():
    """Check that syncer=None disables callback"""
    callbacks = _create_default_callbacks(
        callbacks=[], sync_config=SyncConfig(syncer=None)
    )
    syncer_callback = None
    for cb in callbacks:
        if isinstance(cb, SyncerCallback):
            syncer_callback = cb

    trial1 = MockTrial(trial_id="a", logdir=None)
    trial1.uses_cloud_checkpointing = False

    assert syncer_callback
    assert not syncer_callback._enabled
    # Syncer disabled, so no-op
    assert not syncer_callback._sync_trial_dir(trial1)

    # This should not raise any error for not existing directory
    syncer_callback.on_checkpoint(
        iteration=1,
        trials=[],
        trial=trial1,
        checkpoint=_TrackedCheckpoint(
            dir_or_data="/does/not/exist", storage_mode=CheckpointStorage.PERSISTENT
        ),
    )


def test_syncer_callback_noop_on_trial_cloud_checkpointing():
    """Check that trial using cloud checkpointing disables sync to driver"""
    callbacks = _create_default_callbacks(callbacks=[], sync_config=SyncConfig())
    syncer_callback = None
    for cb in callbacks:
        if isinstance(cb, SyncerCallback):
            syncer_callback = cb

    trial1 = MockTrial(trial_id="a", logdir=None)
    trial1.uses_cloud_checkpointing = True

    assert syncer_callback
    assert syncer_callback._enabled
    # Cloud checkpointing set, so no-op
    assert not syncer_callback._sync_trial_dir(trial1)

    # This should not raise any error for not existing directory
    syncer_callback.on_checkpoint(
        iteration=1,
        trials=[],
        trial=trial1,
        checkpoint=_TrackedCheckpoint(
            dir_or_data="/does/not/exist", storage_mode=CheckpointStorage.PERSISTENT
        ),
    )


def test_syncer_callback_op_on_no_cloud_checkpointing():
    """Check that without cloud checkpointing sync to driver is enabled"""
    callbacks = _create_default_callbacks(callbacks=[], sync_config=SyncConfig())
    syncer_callback = None
    for cb in callbacks:
        if isinstance(cb, SyncerCallback):
            syncer_callback = cb

    assert syncer_callback

    syncer_callback._min_iter_threshold = 0
    syncer_callback._min_time_s_threshold = 0

    trial1 = MockTrial(trial_id="a", logdir=None)
    trial1.uses_cloud_checkpointing = False

    assert syncer_callback._enabled
    assert syncer_callback._sync_trial_dir(trial1)


def test_syncer_callback_sync(ray_start_2_cpus, temp_data_dirs):
    """Check that on_trial_result triggers syncing"""
    tmp_source, tmp_target = temp_data_dirs

    syncer_callback = TestSyncerCallback(local_logdir_override=tmp_target)

    trial1 = MockTrial(trial_id="a", logdir=tmp_source)

    syncer_callback.on_trial_result(iteration=1, trials=[], trial=trial1, result={})
    syncer_callback.wait_for_all()

    assert_file(True, tmp_target, "level0.txt")
    assert_file(True, tmp_target, "level0_exclude.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")
    assert_file(True, tmp_target, "subdir/level1_exclude.txt")
    assert_file(True, tmp_target, "subdir/nested/level2.txt")
    assert_file(True, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(True, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_syncer_callback_sync_with_invalid_ip(ray_start_2_cpus, temp_data_dirs):
    """Check that the sync client updates the IP correctly"""
    tmp_source, tmp_target = temp_data_dirs

    syncer_callback = TestSyncerCallback(local_logdir_override=tmp_target)

    trial1 = MockTrial(trial_id="a", logdir=tmp_source)

    syncer_callback._trial_ips[trial1.trial_id] = "invalid"
    syncer_callback.on_trial_start(iteration=0, trials=[], trial=trial1)

    syncer_callback.on_trial_result(iteration=1, trials=[], trial=trial1, result={})
    syncer_callback.wait_for_all()

    assert_file(True, tmp_target, "level0.txt")
    assert_file(True, tmp_target, "level0_exclude.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")
    assert_file(True, tmp_target, "subdir/level1_exclude.txt")
    assert_file(True, tmp_target, "subdir/nested/level2.txt")
    assert_file(True, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(True, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_syncer_callback_no_size_limit(temp_data_dirs):
    """Check if max_size_bytes is set to None for sync function"""
    tmp_source, _ = temp_data_dirs

    syncer_callback = SyncerCallback()
    trial1 = MockTrial(trial_id="a", logdir=tmp_source)

    sync_fn = syncer_callback._get_trial_sync_process(trial1)._fn
    assert sync_fn.keywords["max_size_bytes"] is None


def test_syncer_callback_sync_period(ray_start_2_cpus, temp_data_dirs):
    """Check that on_trial_result triggers syncing, obeying sync period"""
    tmp_source, tmp_target = temp_data_dirs

    with freeze_time() as frozen:
        syncer_callback = TestSyncerCallback(
            sync_period=60, local_logdir_override=tmp_target
        )

        trial1 = MockTrial(trial_id="a", logdir=tmp_source)

        syncer_callback.on_trial_result(iteration=1, trials=[], trial=trial1, result={})
        syncer_callback.wait_for_all()

        assert_file(True, tmp_target, "level0.txt")
        assert_file(False, tmp_target, "level0_new.txt")

        # Add new file to source directory
        with open(os.path.join(tmp_source, "level0_new.txt"), "w") as f:
            f.write("Data\n")

        frozen.tick(30)

        # Should not sync after 30 seconds
        syncer_callback.on_trial_result(iteration=2, trials=[], trial=trial1, result={})
        syncer_callback.wait_for_all()

        assert_file(True, tmp_target, "level0.txt")
        assert_file(False, tmp_target, "level0_new.txt")

        frozen.tick(30)

        # Should sync after 60 seconds
        syncer_callback.on_trial_result(iteration=3, trials=[], trial=trial1, result={})
        syncer_callback.wait_for_all()

        assert_file(True, tmp_target, "level0.txt")
        assert_file(True, tmp_target, "level0_new.txt")


@pytest.mark.parametrize("on", ["checkpoint", "trial_complete", "experiment_end"])
def test_syncer_callback_force_on_hooks(syncer_callback_test_setup, on):
    """Check that on_experiment_end forces syncing before the Tune loop exits."""
    syncer_callback, trial, filepaths, target_dir = syncer_callback_test_setup

    if on == "checkpoint":
        syncer_callback.on_checkpoint(
            iteration=2,
            trials=[trial],
            trial=trial,
            checkpoint=_TrackedCheckpoint(
                dir_or_data=target_dir, storage_mode=CheckpointStorage.PERSISTENT
            ),
        )
        # `on_checkpoint` syncing is not awaited, so do this manually
        syncer_callback.wait_for_all()
    elif on == "trial_complete":
        syncer_callback.on_trial_complete(iteration=2, trials=[trial], trial=trial)
        # `on_trial_complete` syncing is not awaited, so do this manually
        syncer_callback.wait_for_all()
    elif on == "experiment_end":
        # We still need to launch a new sync process, for `on_experiment_end` to await
        syncer_callback.on_trial_result(
            iteration=2, trials=[trial], trial=trial, result={}
        )
        syncer_callback.on_experiment_end(trials=[trial])

    # Assert that all expected files have been synced.
    for fp in filepaths:
        assert_file(True, fp)


@pytest.mark.parametrize("threshold", [TRAINING_ITERATION, TIME_TOTAL_S])
def test_syncer_callback_min_thresholds(ray_start_2_cpus, temp_data_dirs, threshold):
    """Check that the min_iter/min_time_s thresholds are respected."""
    tmp_source, tmp_target = temp_data_dirs

    # Keep the other metric at 0
    other = TRAINING_ITERATION if threshold == TIME_TOTAL_S else TIME_TOTAL_S

    syncer_callback = TestSyncerCallback(local_logdir_override=tmp_target)

    syncer_callback._min_iter_threshold = 8
    syncer_callback._min_time_s_threshold = 8

    trial1 = MockTrial(trial_id="a", logdir=tmp_source)

    syncer_callback._trial_ips[trial1.trial_id] = "invalid"
    syncer_callback.on_trial_start(iteration=0, trials=[], trial=trial1)

    for i in range(7):
        syncer_callback.on_trial_result(
            iteration=i, trials=[], trial=trial1, result={threshold: i, other: 0}
        )
        syncer_callback.wait_for_all()
        assert_file(False, tmp_target, "level0.txt")

    syncer_callback.on_trial_result(
        iteration=8, trials=[], trial=trial1, result={threshold: 8, other: 0}
    )
    syncer_callback.wait_for_all()

    assert_file(True, tmp_target, "level0.txt")
    assert_file(True, tmp_target, "level0_exclude.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")
    assert_file(True, tmp_target, "subdir/level1_exclude.txt")
    assert_file(True, tmp_target, "subdir/nested/level2.txt")
    assert_file(True, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(True, tmp_target, "subdir_exclude/something/somewhere.txt")

    # Also trigger delayed syncer process removal
    syncer_callback._remove_trial_sync_process(trial_id=trial1.trial_id)
    assert trial1.trial_id in syncer_callback._trial_sync_processes_to_remove

    # Syncing finished so syncer should be removed now
    syncer_callback._cleanup_trial_sync_processes()
    assert trial1.trial_id not in syncer_callback._trial_sync_processes_to_remove


def test_syncer_callback_wait_for_all_error(ray_start_2_cpus, temp_data_dirs):
    """Check that syncer errors are caught correctly in wait_for_all()"""
    tmp_source, tmp_target = temp_data_dirs

    syncer_callback = TestSyncerCallback(
        sync_period=0,
        local_logdir_override=tmp_target,
    )

    trial1 = MockTrial(trial_id="a", logdir=tmp_source)

    # Inject FailingProcess into callback
    sync_process = syncer_callback._get_trial_sync_process(trial1)
    sync_process.should_fail = True

    # This sync will fail because the remote location does not exist
    syncer_callback.on_trial_result(iteration=1, trials=[], trial=trial1, result={})

    with pytest.raises(TuneError) as e:
        syncer_callback.wait_for_all()
        assert "At least one" in e


def test_syncer_callback_log_error(
    propagate_logs, caplog, ray_start_2_cpus, temp_data_dirs
):
    """Check that errors in a previous sync are logged correctly"""
    caplog.set_level(logging.ERROR, logger="ray.tune.syncer")

    tmp_source, tmp_target = temp_data_dirs

    syncer_callback = TestSyncerCallback(
        sync_period=0,
        local_logdir_override=tmp_target,
    )

    trial1 = MockTrial(trial_id="a", logdir=tmp_source)

    # Inject FailingProcess into callback
    sync_process = syncer_callback._get_trial_sync_process(trial1)

    syncer_callback.on_trial_result(iteration=1, trials=[], trial=trial1, result={})

    # So far we haven't wait()ed, so no error, yet
    assert not caplog.text
    assert_file(False, tmp_target, "level0.txt")

    sync_process.should_fail = True

    # When the previous sync processes fails, an error is logged but sync is restarted
    syncer_callback.on_trial_complete(iteration=2, trials=[], trial=trial1)

    assert (
        "An error occurred during the checkpoint syncing of the previous checkpoint"
        in caplog.text
    )

    sync_process.should_fail = False

    syncer_callback.wait_for_all()
    assert_file(True, tmp_target, "level0.txt")


def test_syncer_callback_dead_node_log_error(
    propagate_logs, caplog, ray_start_2_cpus, temp_data_dirs
):
    """Check that we catch + log errors when trying syncing with a dead remote node."""
    caplog.set_level(logging.ERROR, logger="ray.tune.syncer")

    tmp_source, tmp_target = temp_data_dirs

    syncer_callback = TestSyncerCallback(
        sync_period=0,
        local_logdir_override=tmp_target,
    )

    trial1 = MockTrial(trial_id="a", logdir=tmp_source, on_dead_node=True)

    syncer_callback.on_trial_result(iteration=1, trials=[], trial=trial1, result={})

    assert (
        "An error occurred when trying to get the node ip where this trial is running"
        in caplog.text
    )


def test_sync_directory_exclude(ray_start_2_cpus, temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    def logger_creator(config):
        return NoopLogger(config, tmp_source)

    def train_fn(config):
        pass

    trainable_cls = wrap_function(train_fn)
    trainable = ray.remote(trainable_cls).remote(
        config={}, logger_creator=logger_creator
    )

    ray.get(trainable.save.remote())

    # Create some bogus test directories for testing
    os.mkdir(os.path.join(tmp_source, "checkpoint_tmp123"))
    os.link(
        os.path.join(tmp_source, "level0.txt"),
        os.path.join(tmp_source, "checkpoint_tmp123", "some_content.txt"),
    )
    os.mkdir(os.path.join(tmp_source, "save_to_object1234"))
    os.link(
        os.path.join(tmp_source, "level0.txt"),
        os.path.join(tmp_source, "save_to_object1234", "some_content.txt"),
    )

    # Sanity check
    assert_file(True, tmp_source, "checkpoint_tmp123")
    assert_file(True, tmp_source, "save_to_object1234")

    trial1 = MockTrial(trial_id="a", logdir=tmp_source)
    syncer_callback = TestSyncerCallback(
        sync_period=0,
        local_logdir_override=tmp_target,
    )
    syncer_callback.on_trial_complete(iteration=1, trials=[], trial=trial1)
    syncer_callback.wait_for_all()

    # Regular files are synced
    assert_file(True, tmp_target, "level0.txt")
    # Temporary checkpoints are not synced
    assert_file(False, tmp_target, "checkpoint_-00001/" + NULL_MARKER)
    assert_file(False, tmp_target, "checkpoint_-00001")
    assert_file(False, tmp_target, "checkpoint_tmp123")
    assert_file(False, tmp_target, "save_to_object1234")


# TODO(ml-team): [Deprecation - head node syncing]
def test_head_node_syncing_disabled_error(monkeypatch, tmp_path):
    syncer_callback = SyncerCallback(sync_period=0)
    trial = MockTrial(trial_id="a", logdir=None)

    # Raise a deprecation error if checkpointing in a multi-node cluster
    monkeypatch.setenv(REENABLE_DEPRECATED_SYNC_TO_HEAD_NODE, "0")
    with pytest.raises(DeprecationWarning):
        syncer_callback.on_checkpoint(
            iteration=1,
            trials=[],
            trial=trial,
            checkpoint=_TrackedCheckpoint(
                dir_or_data="/does/not/exist", storage_mode=CheckpointStorage.PERSISTENT
            ),
        )

    # Setting the env var raises the original TuneError instead of a deprecation
    monkeypatch.setenv(REENABLE_DEPRECATED_SYNC_TO_HEAD_NODE, "1")
    with pytest.raises(TuneError):
        syncer_callback.on_checkpoint(
            iteration=1,
            trials=[],
            trial=trial,
            checkpoint=_TrackedCheckpoint(
                dir_or_data="/does/not/exist", storage_mode=CheckpointStorage.PERSISTENT
            ),
        )

    # Make sure we don't raise an error if running on a single node or using NFS,
    # where the checkpoint can be accessed from the driver.
    monkeypatch.setenv(REENABLE_DEPRECATED_SYNC_TO_HEAD_NODE, "0")
    path_that_exists = tmp_path / "exists"
    path_that_exists.mkdir()
    syncer_callback.on_checkpoint(
        iteration=1,
        trials=[],
        trial=trial,
        checkpoint=_TrackedCheckpoint(
            dir_or_data=str(path_that_exists), storage_mode=CheckpointStorage.PERSISTENT
        ),
    )


# TODO(ml-team): [Deprecation - head node syncing]
def test_head_node_syncing_disabled_warning(propagate_logs, caplog, monkeypatch):
    monkeypatch.setenv(REENABLE_DEPRECATED_SYNC_TO_HEAD_NODE, "0")
    syncer_callback = SyncerCallback(sync_period=0)
    remote_trial_a = MockTrial(trial_id="a", logdir=None, runner_ip="remote")
    remote_trial_b = MockTrial(trial_id="b", logdir=None, runner_ip="remote")
    local_trial_c = MockTrial(trial_id="c", logdir=None)

    with caplog.at_level(logging.WARNING):
        # The log should only be displayed once for the first remote trial.
        syncer_callback._sync_trial_dir(local_trial_c)
        assert caplog.text.count(_SYNC_TO_HEAD_DEPRECATION_MESSAGE) == 0

        # Any attempts to sync from remote trials should no-op.
        # Instead, print a warning message to the user explaining that
        # no checkpoints or artifacts are pulled to the head node.
        syncer_callback._sync_trial_dir(remote_trial_a)
        assert caplog.text.count(_SYNC_TO_HEAD_DEPRECATION_MESSAGE) == 1

        # More sync attempts shouldn't add any extra warnings.
        syncer_callback._sync_trial_dir(remote_trial_b)
        syncer_callback._sync_trial_dir(remote_trial_a)
        syncer_callback._sync_trial_dir(local_trial_c)

        assert caplog.text.count(_SYNC_TO_HEAD_DEPRECATION_MESSAGE) == 1

    disabled_syncer_callback = SyncerCallback(enabled=False)
    remote_trial_d = MockTrial(trial_id="d", logdir=None, runner_ip="remote")
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        # No warning if syncing is explicitly disabled
        disabled_syncer_callback._sync_trial_dir(remote_trial_d)
        assert caplog.text.count(_SYNC_TO_HEAD_DEPRECATION_MESSAGE) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
