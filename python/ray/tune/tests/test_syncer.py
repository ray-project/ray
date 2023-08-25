import logging
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from typing import List, Optional
import unittest
from unittest.mock import patch

from freezegun import freeze_time
import numpy as np
import pyarrow.fs
import pytest

import ray
import ray.cloudpickle as pickle
from ray import train, tune
from ray.train import Checkpoint, CheckpointConfig, RunConfig, ScalingConfig
from ray.air._internal.remote_storage import (
    upload_to_uri,
    download_from_uri,
    get_fs_and_path,
)
from ray.air._internal.uri_utils import URI
from ray.train.torch import TorchTrainer
from ray.tune import TuneError
from ray.train._internal.syncer import (
    _BackgroundProcess,
    _DefaultSyncer,
    Syncer,
    SyncConfig,
)
from ray.tune.utils.file_transfer import _pack_dir, _unpack_dir


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
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4, configure_logging=False)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2, configure_logging=False)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture
def shutdown_only():
    yield None
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


class TestTrainable(tune.Trainable):
    def __init__(self, logdir=None, **kwargs):
        super().__init__(**kwargs)
        if logdir:
            self._logdir = logdir

    def save_checkpoint(self, checkpoint_dir: str):
        with open(os.path.join(checkpoint_dir, "checkpoint.data"), "w") as f:
            f.write("Data")
        return checkpoint_dir

    def load_checkpoint(self, checkpoint):
        pass

    def step(self):
        # Mock some artifact logging (appending to a log)
        with open(os.path.join(self.logdir, "artifact.txt"), "a") as f:
            f.write("test\n")
        return {"loss": 1}


class CustomSyncer(Syncer):
    def __init__(self, sync_period: float = 300.0):
        super(CustomSyncer, self).__init__(sync_period=sync_period)
        self._sync_status = {}

    def sync_up(
        self, local_dir: str, remote_dir: str, exclude: Optional[List] = None
    ) -> bool:
        with open(os.path.join(local_dir, "custom_syncer.txt"), "w") as f:
            f.write("Data\n")
        self._sync_status[remote_dir] = _pack_dir(local_dir)
        return True

    def sync_down(
        self, remote_dir: str, local_dir: str, exclude: Optional[List] = None
    ) -> bool:
        if remote_dir not in self._sync_status:
            return False
        _unpack_dir(self._sync_status[remote_dir], local_dir)
        return True

    def delete(self, remote_dir: str) -> bool:
        self._sync_status.pop(remote_dir, None)
        return True

    def retry(self):
        raise NotImplementedError

    def wait(self):
        pass


class CustomCommandSyncer(Syncer):
    def __init__(
        self,
        sync_up_template: str,
        sync_down_template: str,
        delete_template: str,
        sync_period: float = 300.0,
    ):
        self.sync_up_template = sync_up_template
        self.sync_down_template = sync_down_template
        self.delete_template = delete_template

        super().__init__(sync_period=sync_period)

    def sync_up(self, local_dir: str, remote_dir: str, exclude: list = None) -> bool:
        cmd_str = self.sync_up_template.format(
            source=local_dir,
            target=remote_dir,
        )
        try:
            subprocess.check_call(cmd_str, shell=True)
        except Exception as e:
            print(f"Exception when syncing up {local_dir} to {remote_dir}: {e}")
            return False
        return True

    def sync_down(self, remote_dir: str, local_dir: str, exclude: list = None) -> bool:
        cmd_str = self.sync_down_template.format(
            source=remote_dir,
            target=local_dir,
        )
        try:
            subprocess.check_call(cmd_str, shell=True)
        except Exception as e:
            print(f"Exception when syncing down {remote_dir} to {local_dir}: {e}")
            return False
        return True

    def delete(self, remote_dir: str) -> bool:
        cmd_str = self.delete_template.format(
            target=remote_dir,
        )
        try:
            subprocess.check_call(cmd_str, shell=True)
        except Exception as e:
            print(f"Exception when deleting {remote_dir}: {e}")
            return False
        return True

    def retry(self):
        raise NotImplementedError

    def wait(self):
        pass


def test_syncer_sync_up_down(temp_data_dirs):
    """Check that syncing up and down works"""
    tmp_source, tmp_target = temp_data_dirs

    syncer = _DefaultSyncer()

    syncer.sync_up(
        local_dir=tmp_source, remote_dir="memory:///test/test_syncer_sync_up_down"
    )
    syncer.wait()

    syncer.sync_down(
        remote_dir="memory:///test/test_syncer_sync_up_down", local_dir=tmp_target
    )
    syncer.wait()

    # Target dir should have all files
    assert_file(True, tmp_target, "level0.txt")
    assert_file(True, tmp_target, "level0_exclude.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")
    assert_file(True, tmp_target, "subdir/level1_exclude.txt")
    assert_file(True, tmp_target, "subdir/nested/level2.txt")
    assert_file(True, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(True, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_syncer_sync_up_down_custom(temp_data_dirs):
    """Check that syncing up and down works"""
    tmp_source, tmp_target = temp_data_dirs

    syncer = CustomCommandSyncer(
        sync_up_template="cp -rf {source} `echo '{target}' | cut -c 8-`",
        sync_down_template="cp -rf `echo '{source}' | cut -c 8-` {target}",
        delete_template="rm -rf `echo '{target}' | cut -c 8-`",
    )

    # remove target dir (otherwise OS will copy into)
    shutil.rmtree(tmp_target)

    syncer.sync_up(local_dir=tmp_source, remote_dir=f"file://{tmp_target}")
    syncer.wait()

    # remove target dir to test sync down
    shutil.rmtree(tmp_source)

    syncer.sync_down(remote_dir=f"file://{tmp_target}", local_dir=tmp_source)
    syncer.wait()

    # Target dir should have all files
    assert_file(True, tmp_source, "level0.txt")
    assert_file(True, tmp_source, "level0_exclude.txt")
    assert_file(True, tmp_source, "subdir/level1.txt")
    assert_file(True, tmp_source, "subdir/level1_exclude.txt")
    assert_file(True, tmp_source, "subdir/nested/level2.txt")
    assert_file(True, tmp_source, "subdir_nested_level2_exclude.txt")
    assert_file(True, tmp_source, "subdir_exclude/something/somewhere.txt")


def test_syncer_sync_exclude(temp_data_dirs):
    """Check that the exclude parameter works"""
    tmp_source, tmp_target = temp_data_dirs

    syncer = _DefaultSyncer()

    syncer.sync_up(
        local_dir=tmp_source,
        remote_dir="memory:///test/test_syncer_sync_exclude",
        exclude=["*_exclude*"],
    )
    syncer.wait()

    syncer.sync_down(
        remote_dir="memory:///test/test_syncer_sync_exclude", local_dir=tmp_target
    )
    syncer.wait()

    # Excluded files should not be found in target
    assert_file(True, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "level0_exclude.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")
    assert_file(False, tmp_target, "subdir/level1_exclude.txt")
    assert_file(True, tmp_target, "subdir/nested/level2.txt")
    assert_file(False, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(False, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_sync_up_if_needed(temp_data_dirs):
    """Check that we only sync up again after sync period"""
    tmp_source, tmp_target = temp_data_dirs

    with freeze_time() as frozen:
        syncer = _DefaultSyncer(sync_period=60)
        assert syncer.sync_up_if_needed(
            local_dir=tmp_source, remote_dir="memory:///test/test_sync_up_not_needed"
        )
        syncer.wait()

        frozen.tick(30)

        # Sync period not over, yet
        assert not syncer.sync_up_if_needed(
            local_dir=tmp_source, remote_dir="memory:///test/test_sync_up_not_needed"
        )

        frozen.tick(30)

        # Sync period over, sync again
        assert syncer.sync_up_if_needed(
            local_dir=tmp_source, remote_dir="memory:///test/test_sync_up_not_needed"
        )


def test_sync_down_if_needed(temp_data_dirs):
    """Check that we only sync down again after sync period"""
    tmp_source, tmp_target = temp_data_dirs

    with freeze_time() as frozen:
        syncer = _DefaultSyncer(sync_period=60)

        # Populate remote directory
        syncer.sync_up(
            local_dir=tmp_source, remote_dir="memory:///test/test_sync_down_if_needed"
        )
        syncer.wait()

        assert syncer.sync_down_if_needed(
            remote_dir="memory:///test/test_sync_down_if_needed", local_dir=tmp_target
        )
        syncer.wait()

        frozen.tick(30)

        # Sync period not over, yet
        assert not syncer.sync_down_if_needed(
            remote_dir="memory:///test/test_sync_down_if_needed", local_dir=tmp_target
        )

        frozen.tick(30)

        # Sync period over, sync again
        assert syncer.sync_down_if_needed(
            remote_dir="memory:///test/test_sync_down_if_needed", local_dir=tmp_target
        )


def test_syncer_still_running_no_sync(temp_data_dirs):
    """Check that no new sync is issued if old sync is still running"""
    tmp_source, tmp_target = temp_data_dirs

    class FakeSyncProcess:
        @property
        def is_running(self):
            return True

        @property
        def start_time(self):
            # Don't consider the sync process timeout
            return float("inf")

    syncer = _DefaultSyncer(sync_period=60)
    syncer._sync_process = FakeSyncProcess()
    assert not syncer.sync_up_if_needed(
        local_dir=tmp_source,
        remote_dir="memory:///test/test_syncer_still_running_no_sync",
    )


def test_syncer_not_running_sync(temp_data_dirs):
    """Check that new sync is issued if old sync completed"""
    tmp_source, tmp_target = temp_data_dirs

    class FakeSyncProcess:
        @property
        def is_running(self):
            return False

        def wait(self):
            return True

    syncer = _DefaultSyncer(sync_period=60)
    syncer._sync_process = FakeSyncProcess()
    assert syncer.sync_up_if_needed(
        local_dir=tmp_source,
        remote_dir="memory:///test/test_syncer_not_running_sync",
    )


def test_syncer_hanging_sync_with_timeout(temp_data_dirs):
    """Check that syncing times out when the sync process is hanging."""
    tmp_source, tmp_target = temp_data_dirs

    def _hanging_sync_up_command(*args, **kwargs):
        time.sleep(200)

    class _HangingSyncer(_DefaultSyncer):
        def _sync_up_command(
            self, local_path: str, uri: str, exclude: Optional[List] = None
        ):
            return _hanging_sync_up_command, {}

    syncer = _HangingSyncer(sync_period=60, sync_timeout=10)

    def sync_up():
        return syncer.sync_up(
            local_dir=tmp_source, remote_dir="memory:///test/test_syncer_timeout"
        )

    with freeze_time() as frozen:
        assert sync_up()
        frozen.tick(5)
        # 5 seconds - initial sync hasn't reached the timeout yet
        # It should continue running without launching a new sync
        assert not sync_up()
        frozen.tick(5)
        # Reached the timeout - start running a new sync command
        assert sync_up()
        frozen.tick(20)
        # We're 10 seconds past the timeout, waiting should result in a timeout error
        with pytest.raises(TimeoutError):
            syncer.wait()


def test_syncer_not_running_sync_last_failed(propagate_logs, caplog, temp_data_dirs):
    """Check that new sync is issued if old sync completed"""
    caplog.set_level(logging.WARNING)

    tmp_source, tmp_target = temp_data_dirs

    class FakeSyncProcess(_BackgroundProcess):
        @property
        def is_running(self):
            return False

        def wait(self, *args, **kwargs):
            raise RuntimeError("Sync failed")

    syncer = _DefaultSyncer(sync_period=60)
    syncer._sync_process = FakeSyncProcess(lambda: None)
    assert syncer.sync_up_if_needed(
        local_dir=tmp_source,
        remote_dir="memory:///test/test_syncer_not_running_sync",
    )
    assert "Last sync command failed" in caplog.text


def test_syncer_delete(temp_data_dirs):
    """Check that deletion on remote storage works"""
    tmp_source, tmp_target = temp_data_dirs

    syncer = _DefaultSyncer(sync_period=60)

    # Populate remote directory
    syncer.sync_up(local_dir=tmp_source, remote_dir="memory:///test/test_syncer_delete")
    syncer.wait()

    syncer.delete(remote_dir="memory:///test/test_syncer_delete")

    syncer.sync_down(
        remote_dir="memory:///test/test_syncer_delete", local_dir=tmp_target
    )
    # Downloading from the deleted directory will raise some exception.
    with pytest.raises(Exception):
        syncer.wait()

    # Remote storage was deleted, so target should be empty
    assert_file(False, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "level0_exclude.txt")
    assert_file(False, tmp_target, "subdir/level1.txt")
    assert_file(False, tmp_target, "subdir/level1_exclude.txt")
    assert_file(False, tmp_target, "subdir/nested/level2.txt")
    assert_file(False, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(False, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_syncer_wait_or_retry_failure(temp_data_dirs):
    """Check that the wait or retry API fails after max_retries."""
    tmp_source, tmp_target = temp_data_dirs

    syncer = _DefaultSyncer(sync_period=60)

    # Will fail as dir does not exist
    syncer.sync_down(
        remote_dir="memory:///test/test_syncer_wait_or_retry", local_dir=tmp_target
    )
    with pytest.raises(RuntimeError) as e:
        syncer.wait_or_retry(max_retries=3, backoff_s=0)
        assert "Failed sync even after 3 retries." in str(e)


def test_syncer_wait_or_retry_timeout(temp_data_dirs):
    """Check that the wait or retry API raises a timeout error after `sync_timeout`."""
    tmp_source, tmp_target = temp_data_dirs

    def slow_upload(*args, **kwargs):
        time.sleep(5)

    class HangingSyncer(_DefaultSyncer):
        def _sync_up_command(
            self, local_path: str, uri: str, exclude: Optional[List] = None
        ):
            return (
                slow_upload,
                dict(local_path=local_path, uri=uri, exclude=exclude),
            )

    syncer = HangingSyncer(sync_period=60, sync_timeout=0.1)

    syncer.sync_up(local_dir=tmp_source, remote_dir=f"memory://{str(tmp_target)}")
    with pytest.raises(RuntimeError) as e:
        syncer.wait_or_retry(max_retries=3, backoff_s=0)
        assert "Failed sync even after 3 retries." in str(e.value)
        assert isinstance(e.value.__cause__, TimeoutError)


def test_syncer_wait_or_retry_eventual_success(temp_data_dirs, tmp_path):
    """Check that the wait or retry API succeeds for a sync_down that
    fails, times out, then succeeds."""
    tmp_source, tmp_target = temp_data_dirs

    success = tmp_path / "success"
    fail_marker = tmp_path / "fail_marker"
    hang_marker = tmp_path / "hang_marker"

    def eventual_upload(*args, **kwargs):
        if not fail_marker.exists():
            fail_marker.write_text(".", encoding="utf-8")
            raise RuntimeError("Failing")
        elif not hang_marker.exists():
            hang_marker.write_text(".", encoding="utf-8")
            time.sleep(5)
        else:
            success.write_text(".", encoding="utf-8")

    class EventualSuccessSyncer(_DefaultSyncer):
        def _sync_up_command(
            self, local_path: str, uri: str, exclude: Optional[List] = None
        ):
            return (
                eventual_upload,
                dict(local_path=local_path, uri=uri, exclude=exclude),
            )

    syncer = EventualSuccessSyncer(sync_period=60, sync_timeout=0.5)

    syncer.sync_up(local_dir=tmp_source, remote_dir=f"memory://{str(tmp_target)}")
    # The syncer will retry 2 times, running 3 times in total and eventually succeeding.
    syncer.wait_or_retry(max_retries=2, backoff_s=0)
    assert success.exists()


def test_trainable_syncer_default(ray_start_2_cpus, temp_data_dirs):
    """Check that Trainable.save() triggers syncing using default syncing"""
    tmp_source, tmp_target = temp_data_dirs

    trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir=f"file://{tmp_target}"
    )

    checkpoint_dir = ray.get(trainable.save.remote())

    assert_file(True, tmp_target, os.path.join(checkpoint_dir, "checkpoint.data"))
    assert_file(False, tmp_target, os.path.join(checkpoint_dir, "custom_syncer.txt"))

    ray.get(trainable.delete_checkpoint.remote(checkpoint_dir))

    assert_file(False, tmp_target, os.path.join(checkpoint_dir, "checkpoint.data"))


@pytest.mark.skip("This codepath will be removed soon.")
@pytest.mark.parametrize("num_retries", [None, 1, 2])
def test_trainable_syncer_retry(shutdown_only, temp_data_dirs, num_retries):
    """Check that Trainable.save() default syncing can retry"""
    tmp_source, tmp_target = temp_data_dirs
    num_retries = num_retries or 3
    ray.init(
        num_cpus=2,
        configure_logging=False,
        runtime_env={
            "env_vars": {
                "TUNE_CHECKPOINT_CLOUD_RETRY_WAIT_TIME_S": "0",
                "TUNE_CHECKPOINT_CLOUD_RETRY_NUM": str(num_retries),
            }
        },
    )

    class FailingSyncer(_DefaultSyncer):
        def _sync_up_command(
            self, local_path: str, uri: str, exclude: Optional[List] = None
        ):
            def failing_upload(*args, **kwargs):
                raise RuntimeError("Upload failing!")

            return (
                failing_upload,
                dict(local_path=local_path, uri=uri, exclude=exclude),
            )

    syncer = FailingSyncer(sync_period=60, sync_timeout=0.1)

    class TestTrainableRetry(TestTrainable):
        def _maybe_save_to_cloud(self, checkpoint_dir: str) -> bool:
            from ray.tune.trainable.trainable import logger

            output = []

            def mock_error(x):
                output.append(x)

            with patch.object(logger, "error", mock_error):
                ret = super()._maybe_save_to_cloud(checkpoint_dir)
            assert f"after {num_retries}" in output[0]
            return ret

    trainable = ray.remote(TestTrainableRetry).remote(
        remote_checkpoint_dir=f"file://{tmp_target}",
        sync_config=SyncConfig(syncer=syncer),
    )

    ray.get(trainable.save.remote())


def test_trainable_syncer_custom(ray_start_2_cpus, temp_data_dirs):
    """Check that Trainable.save() triggers syncing using custom syncer"""
    tmp_source, tmp_target = temp_data_dirs

    sync_config = SyncConfig(syncer=CustomSyncer())
    trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir=f"file://{tmp_target}",
        sync_config=sync_config,
    )

    checkpoint_dir = ray.get(trainable.save.remote())

    assert_file(True, tmp_target, os.path.join(checkpoint_dir, "checkpoint.data"))
    assert_file(True, tmp_target, os.path.join(checkpoint_dir, "custom_syncer.txt"))

    ray.get(trainable.delete_checkpoint.remote(checkpoint_dir))

    assert_file(False, tmp_target, os.path.join(checkpoint_dir, "checkpoint.data"))
    assert_file(False, tmp_target, os.path.join(checkpoint_dir, "custom_syncer.txt"))


def test_trainable_syncer_custom_command(ray_start_2_cpus, temp_data_dirs):
    """Check that Trainable.save() triggers syncing using custom syncer"""
    tmp_source, tmp_target = temp_data_dirs

    sync_config = SyncConfig(
        syncer=CustomCommandSyncer(
            sync_up_template="cp -rf {source} `echo '{target}' | cut -c 8-`",
            sync_down_template="cp -rf `echo '{source}' | cut -c 8-` {target}",
            delete_template="rm -rf `echo '{target}' | cut -c 8-`",
        ),
    )
    trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir=f"file://{tmp_target}",
        sync_config=sync_config,
    )

    checkpoint_dir = ray.get(trainable.save.remote())

    assert_file(True, tmp_target, os.path.join(checkpoint_dir, "checkpoint.data"))

    ray.get(trainable.delete_checkpoint.remote(checkpoint_dir))

    assert_file(False, tmp_target, os.path.join(checkpoint_dir, "checkpoint.data"))


def test_artifact_syncing_on_save_restore(ray_start_2_cpus, temp_data_dirs, tmp_path):
    """Test that the trainable syncs artifacts along with checkpoints.
    In this test:
    - `tmp_target` == mocked remote storage location where Tune syncs to
    - `tmp_path/dir1` == local storage location of initial run
    - `tmp_path/dir2` == local storage location of restored trainable
    """
    _, tmp_target = temp_data_dirs

    local_dir_1 = tmp_path / "dir1"
    local_dir_2 = tmp_path / "dir2"
    local_dir_1.mkdir()
    local_dir_2.mkdir()

    trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir=f"file://{tmp_target}", logdir=str(local_dir_1)
    )

    for i in range(1, 4):
        # Step, save, then check that artifacts are uploaded
        ray.get(trainable.train.remote())
        checkpoint_dir = ray.get(trainable.save.remote())
        assert_file(True, tmp_target, os.path.join(checkpoint_dir, "checkpoint.data"))
        assert_file(True, tmp_target, "artifact.txt")
        with open(os.path.join(tmp_target, "artifact.txt"), "r") as f:
            artifact_data = f.read()
            assert artifact_data.split("\n")[:-1] == ["test"] * i

    # Check that artifacts are syncd when a trainable is restored.
    shutil.rmtree(local_dir_1)
    restored_trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir=f"file://{tmp_target}", logdir=str(local_dir_2)
    )

    new_ckpt_dir = str(local_dir_2 / Path(checkpoint_dir).relative_to(local_dir_1))
    ray.get(restored_trainable.restore.remote(new_ckpt_dir))
    with open(os.path.join(local_dir_2, "artifact.txt"), "r") as f:
        artifact_data = f.read()
        assert artifact_data.split("\n")[:-1] == ["test"] * 3


def test_artifact_syncing_disabled(ray_start_2_cpus, temp_data_dirs, tmp_path):
    """Test that the trainable does NOT sync artifacts when disabled via SyncConfig."""
    _, tmp_target = temp_data_dirs

    local_dir_1 = tmp_path / "dir1"
    local_dir_2 = tmp_path / "dir2"
    local_dir_1.mkdir()
    local_dir_2.mkdir()

    trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir=f"file://{tmp_target}",
        logdir=str(local_dir_1),
        sync_config=SyncConfig(sync_artifacts=False),
    )

    ray.get(trainable.train.remote())
    checkpoint_dir = ray.get(trainable.save.remote())
    assert_file(True, tmp_target, os.path.join(checkpoint_dir, "checkpoint.data"))
    assert_file(False, tmp_target, "artifact.txt")

    restored_trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir=f"file://{tmp_target}", logdir=str(local_dir_2)
    )
    ray.get(restored_trainable.restore.remote(checkpoint_dir))
    assert_file(False, str(local_dir_2), "artifact.txt")


def test_artifact_syncing_on_stop(ray_start_2_cpus, temp_data_dirs, tmp_path):
    """Check that artifacts get uploaded on trial stop (ex: on complete/error)."""
    _, tmp_target = temp_data_dirs

    trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir=f"file://{tmp_target}",
        logdir=str(tmp_path),
    )

    ray.get(trainable.train.remote())
    assert_file(False, tmp_target, "artifact.txt")
    ray.get(trainable.stop.remote())
    assert_file(True, tmp_target, "artifact.txt")


def test_artifact_syncing_on_reset(ray_start_2_cpus, temp_data_dirs, tmp_path):
    """Check that artifacts get uploaded on trial reset
    (for paused actors when actor reuse is enabled)."""
    _, tmp_target = temp_data_dirs

    trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir=f"file://{tmp_target}",
        logdir=str(tmp_path),
    )

    ray.get(trainable.train.remote())
    assert_file(False, tmp_target, "artifact.txt")
    ray.get(trainable.reset.remote(new_config={}))
    assert_file(True, tmp_target, "artifact.txt")


def test_avoid_duplicate_artifact_sync(ray_start_2_cpus, temp_data_dirs, tmp_path):
    """Checks that artifacts are not uploaded twice if not needed.
    For example, a trial uploads artifacts on a final checkpoint, and
    there is no need to upload again on stop or trial complete."""
    _, tmp_target = temp_data_dirs

    trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir=f"file://{tmp_target}",
        logdir=str(tmp_path),
    )

    ray.get(trainable.train.remote())
    ray.get(trainable.save.remote())  # Saves an artifact
    assert_file(True, tmp_target, "artifact.txt")
    # Delete the artifact to check if it gets uploaded again.
    os.remove(os.path.join(tmp_target, "artifact.txt"))
    # Should skip saving the artifact again...
    ray.get(trainable._maybe_save_artifacts_to_cloud.remote())
    assert_file(False, tmp_target, "artifact.txt")

    # Step again, then stop --> this time, it should save.
    ray.get(trainable.train.remote())
    ray.get(trainable.stop.remote())  # Saves an artifact
    assert_file(True, tmp_target, "artifact.txt")


def test_syncer_serialize(temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    syncer = _DefaultSyncer()

    syncer.sync_up(
        local_dir=tmp_source, remote_dir="memory:///test/test_syncer_sync_up_down"
    )

    serialized = pickle.dumps(syncer)
    loaded_syncer = pickle.loads(serialized)
    assert not loaded_syncer._sync_process


def test_final_experiment_checkpoint_sync(ray_start_2_cpus, tmpdir):
    class SlowSyncer(_DefaultSyncer):
        def __init__(self, **kwargs):
            super(_DefaultSyncer, self).__init__(**kwargs)
            self._num_syncs = 0

        def _sync_up_command(self, local_path, uri, exclude):
            def slow_upload(local_path, uri, exclude):
                # Sleep to check that experiment doesn't exit without waiting
                time.sleep(2)
                upload_to_uri(local_path, uri, exclude)
                self._num_syncs += 1

            return (
                slow_upload,
                dict(local_path=local_path, uri=uri, exclude=exclude),
            )

    # Long sync period so there will only be 2 experiment checkpoints:
    # One at the beginning which always happens, then a forced checkpoint at the
    # end of the experiment.
    syncer = SlowSyncer(sync_period=60)

    def train_func(config):
        for i in range(8):
            train.report({"score": i})
            time.sleep(0.5)

    tuner = tune.Tuner(
        train_func,
        run_config=RunConfig(
            name="exp_name",
            storage_path="memory:///test_upload_dir",
            sync_config=train.SyncConfig(syncer=syncer),
        ),
    )
    results = tuner.fit()
    assert not results.errors

    # Check the contents of the upload_dir immediately after the experiment
    # This won't be up to date if we don't wait on the last sync
    download_from_uri("memory:///test_upload_dir/exp_name", tmpdir)
    cloud_results = tune.Tuner.restore(str(tmpdir), trainable=train_func).get_results()
    last_reported_iter = cloud_results[0].metrics.get("training_iteration", None)
    assert last_reported_iter == 8, (
        "Experiment did not wait to finish the final experiment sync before exiting. "
        "The last reported training iteration synced to the remote dir was "
        f"{last_reported_iter}. (None if no results are synced.)"
    )
    assert syncer._num_syncs == 2, (
        "Should have seen 2 syncs, once at the beginning of the experiment, and one "
        f"forced sync at the end. Got {syncer._num_syncs} syncs instead."
    )


def _test_sync_folder_with_many_files_s3(mock_s3_bucket_uri, tmp_path):
    source_dir = tmp_path / "source"
    check_dir = tmp_path / "check"
    source_dir.mkdir()
    check_dir.mkdir()

    # Create 256 files to upload
    for i in range(256):
        (source_dir / str(i)).write_text("", encoding="utf-8")

    upload_to_uri(source_dir, mock_s3_bucket_uri)
    download_from_uri(mock_s3_bucket_uri, check_dir)
    assert (check_dir / "255").exists()


def test_sync_folder_with_many_files_s3_native(mock_s3_bucket_uri, tmp_path):
    with patch("ray.air._internal.remote_storage.fsspec", None):
        fs, path = get_fs_and_path(mock_s3_bucket_uri)

        assert isinstance(fs, pyarrow.fs.S3FileSystem)

        _test_sync_folder_with_many_files_s3(mock_s3_bucket_uri, tmp_path)


def test_sync_folder_with_many_files_s3_fsspec(mock_s3_bucket_uri, tmp_path):
    try:
        import s3fs  # noqa: F401
    except Exception as exc:
        raise AssertionError("This test requires s3fs to be installed") from exc

    fs, path = get_fs_and_path(mock_s3_bucket_uri)

    assert isinstance(fs, pyarrow.fs.PyFileSystem)

    _test_sync_folder_with_many_files_s3(mock_s3_bucket_uri, tmp_path)


def test_sync_folder_with_many_files_fs(tmpdir):
    # Create 256 files to upload
    for i in range(256):
        (tmpdir / str(i)).write_text("", encoding="utf-8")

    # Upload to file URI
    with tempfile.TemporaryDirectory() as upload_dir:
        target_uri = "file://" + upload_dir
        upload_to_uri(tmpdir, target_uri)

        assert (tmpdir / "255").exists()


def test_e2e_sync_to_s3(ray_start_4_cpus, mock_s3_bucket_uri, tmp_path):
    """Tests an end to end Tune run with syncing to a mock s3 bucket.
    This test includes the restoration path as well to make sure that
    files are synced down correctly."""
    download_dir = tmp_path / "upload_dir"
    download_dir.mkdir()

    local_dir = str(tmp_path / "local_dir")

    exp_name = "test_e2e_sync_to_s3"

    def train_fn(config):
        train.report({"score": 1}, checkpoint=Checkpoint.from_dict({"data": 1}))
        raise RuntimeError

    tuner = tune.Tuner(
        train_fn,
        param_space={"id": tune.grid_search([0, 1, 2, 3])},
        run_config=RunConfig(
            name=exp_name,
            storage_path=mock_s3_bucket_uri,
            local_dir=local_dir,
        ),
        tune_config=tune.TuneConfig(
            trial_dirname_creator=lambda t: str(t.config.get("id"))
        ),
    )
    result_grid = tuner.fit()

    assert result_grid.errors

    shutil.rmtree(local_dir)  # Rely on sync-down from cloud
    tuner = tune.Tuner.restore(
        str(URI(mock_s3_bucket_uri) / exp_name), trainable=train_fn, resume_errored=True
    )
    result_grid = tuner.fit()

    # Download remote dir to do some sanity checks
    download_from_uri(uri=mock_s3_bucket_uri, local_path=str(download_dir))

    def get_remote_trial_dir(trial_id: int):
        return os.path.join(download_dir, exp_name, str(trial_id))

    # Check that each remote trial dir has a checkpoint
    for result in result_grid:
        trial_id = result.config["id"]
        remote_dir = get_remote_trial_dir(trial_id)
        num_checkpoints = len(
            [file for file in os.listdir(remote_dir) if file.startswith("checkpoint_")]
        )
        assert result.metrics["training_iteration"] == 2
        assert num_checkpoints == 2  # 1 before restore + 1 after


def test_distributed_checkpointing_to_s3(
    ray_start_4_cpus, mock_s3_bucket_uri, tmp_path
):
    """Tests a Tune run with distributed checkpointing to a mock s3 bucket.

    This test runs a Tune run with 3 distributed DDP workers.
    We run 10 steps in total and checkpoint every 3 steps.
    At the end of the test, we check the ranked index files are
    available both locally and on the cloud.
    We also make sure the model checkpoint files are only available
    on the cloud.
    """
    exp_name = "test_dist_ckpt_to_s3"
    local_dir = os.path.join(tmp_path, "local_dir")

    def train_fn(config):
        world_rank = train.get_context().get_world_rank()
        for step in range(config["num_steps"]):
            time.sleep(0.1)
            checkpoint = None
            if step % 3 == 0:
                checkpoint_dir = tempfile.mkdtemp(dir=tmp_path)
                path = os.path.join(checkpoint_dir, f"optim-{world_rank}.pt")
                with open(path, "wb") as f:
                    f.write(
                        pickle.dumps(
                            {
                                "optimizer": "adam",
                                "lr": 0.001,
                                "optimizer_state": np.random.random((100, 100)),
                            }
                        )
                    )
                path = os.path.join(checkpoint_dir, f"model-{world_rank}.pt")
                with open(path, "wb") as f:
                    f.write(
                        pickle.dumps(
                            {
                                "model": "resnet",
                                "weights": np.random.random((100, 100)),
                            }
                        )
                    )
                checkpoint = Checkpoint.from_directory(checkpoint_dir)
            train.report({"score": step}, checkpoint=checkpoint)

    def _check_dir_content(checkpoint_dir, exist=True):
        # Double check local checkpoint dir.
        local_trial_data = os.listdir(
            os.path.join(local_dir, "test_dist_ckpt_to_s3", "trial_0")
        )
        if exist:
            # checkpoint in local trial folder.
            assert checkpoint_dir in local_trial_data
            local_checkpoint_data = os.listdir(
                os.path.join(
                    local_dir, "test_dist_ckpt_to_s3", "trial_0", checkpoint_dir
                )
            )
            # Local folder has index files.
            assert ".RANK_0.files" in local_checkpoint_data
            assert ".RANK_1.files" in local_checkpoint_data
            assert ".RANK_2.files" in local_checkpoint_data
            # But no data files.
            assert "model-0.pt" not in local_checkpoint_data
            assert "model-1.pt" not in local_checkpoint_data
            assert "model-2.pt" not in local_checkpoint_data
        else:
            assert checkpoint_dir not in local_trial_data

        cloud_trial_data = os.listdir(
            os.path.join(download_dir, "test_dist_ckpt_to_s3", "trial_0")
        )
        if exist:
            # Checkpoint in cloud trial folder.
            assert checkpoint_dir in cloud_trial_data
            cloud_checkpoint_data = os.listdir(
                os.path.join(
                    download_dir, "test_dist_ckpt_to_s3", "trial_0", checkpoint_dir
                )
            )
            # Cloud folder has index files.
            assert ".RANK_0.files" in cloud_checkpoint_data
            assert ".RANK_1.files" in cloud_checkpoint_data
            assert ".RANK_2.files" in cloud_checkpoint_data
            # And all the data files.
            assert "model-0.pt" in cloud_checkpoint_data
            assert "model-1.pt" in cloud_checkpoint_data
            assert "model-2.pt" in cloud_checkpoint_data
        else:
            assert checkpoint_dir not in cloud_trial_data

    with unittest.mock.patch.dict(os.environ, {"RAY_AIR_LOCAL_CACHE_DIR": local_dir}):
        trainer = TorchTrainer(
            train_fn,
            train_loop_config={"num_steps": 10},
            scaling_config=ScalingConfig(
                num_workers=3,
                use_gpu=False,
            ),
            # Note(jungong) : Trainers ignore the RunConfig specified via
            # Tuner below. So to specify proper cloud paths and CheckpointConfig,
            # we must pass another dummy RunConfig here.
            # TODO(jungong) : this is extremely awkward. Refactor and clean up.
            run_config=RunConfig(
                storage_path=mock_s3_bucket_uri,
                checkpoint_config=CheckpointConfig(
                    num_to_keep=3,
                    checkpoint_frequency=3,
                    _checkpoint_keep_all_ranks=True,
                    _checkpoint_upload_from_workers=True,
                ),
            ),
        )

        tuner = tune.Tuner(
            trainer,
            run_config=RunConfig(
                name=exp_name,
                storage_path=mock_s3_bucket_uri,
                checkpoint_config=CheckpointConfig(
                    num_to_keep=3,
                ),
            ),
            tune_config=tune.TuneConfig(
                # Only running 1 trial.
                trial_dirname_creator=lambda t: "trial_0"
            ),
        )
        result_grid = tuner.fit()
        # Run was successful.
        assert not result_grid.errors
        # Make sure checkpoint is backed by the full s3 checkpoint uri.
        assert result_grid[0].checkpoint.uri.startswith("s3://")

        # Download remote dir locally to do some sanity checks
        download_dir = os.path.join(tmp_path, "download")

        shutil.rmtree(download_dir, ignore_errors=True)
        download_from_uri(uri=mock_s3_bucket_uri, local_path=str(download_dir))

        # Step 0 checkpoint is deleted.
        _check_dir_content("checkpoint_000000", exist=False)
        _check_dir_content("checkpoint_000001")  # Step 3
        _check_dir_content("checkpoint_000002")  # Step 6
        _check_dir_content("checkpoint_000003")  # Step 9


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
