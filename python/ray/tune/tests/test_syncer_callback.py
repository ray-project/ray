import logging
import os
import shutil
import tempfile
from typing import Optional

import pytest
from freezegun import freeze_time

import ray.util
from ray.air._internal.checkpoint_manager import CheckpointStorage, _TrackedCheckpoint
from ray.exceptions import RayActorError
from ray.tune import TuneError
from ray.tune.logger import NoopLogger
from ray.tune.syncer import (
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
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2, configure_logging=False)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


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


def assert_file(exists: bool, root: str, path: str):
    full_path = os.path.join(root, path)

    if exists:
        assert os.path.exists(full_path)
    else:
        assert not os.path.exists(full_path)


class MockTrial:
    def __init__(self, trial_id: str, logdir: str, on_dead_node: bool = False):
        self.trial_id = trial_id
        self.uses_cloud_checkpointing = False
        self.sync_on_checkpoint = True

        self.logdir = logdir
        self._local_ip = ray.util.get_node_ip_address()
        self._on_dead_node = on_dead_node

    def get_runner_ip(self):
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

    trial1 = MockTrial(trial_id="a", logdir=None)
    trial1.uses_cloud_checkpointing = False

    assert syncer_callback
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


def test_syncer_callback_force_on_checkpoint(ray_start_2_cpus, temp_data_dirs):
    """Check that on_checkpoint forces syncing"""
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

        assert_file(False, tmp_target, "level0_new.txt")

        frozen.tick(30)

        # Should sync as checkpoint observed
        syncer_callback.on_checkpoint(
            iteration=2,
            trials=[],
            trial=trial1,
            checkpoint=_TrackedCheckpoint(
                dir_or_data=tmp_target, storage_mode=CheckpointStorage.PERSISTENT
            ),
        )
        syncer_callback.wait_for_all()

        assert_file(True, tmp_target, "level0.txt")
        assert_file(True, tmp_target, "level0_new.txt")


def test_syncer_callback_force_on_complete(ray_start_2_cpus, temp_data_dirs):
    """Check that on_trial_complete forces syncing"""
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

        assert_file(False, tmp_target, "level0_new.txt")

        frozen.tick(30)

        # Should sync as checkpoint observed
        syncer_callback.on_trial_complete(
            iteration=2,
            trials=[],
            trial=trial1,
        )
        syncer_callback.wait_for_all()

        assert_file(True, tmp_target, "level0.txt")
        assert_file(True, tmp_target, "level0_new.txt")


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


def test_syncer_callback_log_error(caplog, ray_start_2_cpus, temp_data_dirs):
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


def test_syncer_callback_dead_node_log_error(caplog, ray_start_2_cpus, temp_data_dirs):
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

    ray.get(trainable.save_to_object.remote())

    # Temporary directory exists
    assert_file(True, tmp_source, "checkpoint_-00001/" + NULL_MARKER)
    assert_file(True, tmp_source, "checkpoint_-00001")

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

    # Regular files are synced
    assert_file(True, tmp_target, "level0.txt")
    # Temporary checkpoints are not synced
    assert_file(False, tmp_target, "checkpoint_-00001/" + NULL_MARKER)
    assert_file(False, tmp_target, "checkpoint_-00001")
    assert_file(False, tmp_target, "checkpoint_tmp123")
    assert_file(False, tmp_target, "save_to_object1234")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
