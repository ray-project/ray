import logging
import os
import shutil
import subprocess
import tempfile
from typing import List, Optional
from unittest.mock import patch

import pytest
from freezegun import freeze_time

import ray
import ray.cloudpickle as pickle
from ray import tune
from ray.air import Checkpoint
from ray.tune import TuneError
from ray.tune.syncer import Syncer, _DefaultSyncer, _validate_upload_dir
from ray.tune.utils.file_transfer import _pack_dir, _unpack_dir


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
    def save_checkpoint(self, checkpoint_dir: str):
        with open(os.path.join(checkpoint_dir, "checkpoint.data"), "w") as f:
            f.write("Data")
        return checkpoint_dir


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


def test_sync_string_invalid_uri():
    with pytest.raises(ValueError):
        _validate_upload_dir(tune.SyncConfig(upload_dir="invalid://some/url"))


def test_sync_string_invalid_local():
    with pytest.raises(ValueError):
        _validate_upload_dir(tune.SyncConfig(upload_dir="/invalid/dir"))


def test_sync_string_valid_local():
    _validate_upload_dir(tune.SyncConfig(upload_dir="file:///valid/dir"))


def test_sync_string_valid_s3():
    _validate_upload_dir(tune.SyncConfig(upload_dir="s3://valid/bucket"))


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


def test_syncer_not_running_sync_last_failed(caplog, temp_data_dirs):
    """Check that new sync is issued if old sync completed"""
    caplog.set_level(logging.WARNING)

    tmp_source, tmp_target = temp_data_dirs

    class FakeSyncProcess:
        @property
        def is_running(self):
            return False

        def wait(self):
            raise RuntimeError("Sync failed")

    syncer = _DefaultSyncer(sync_period=60)
    syncer._sync_process = FakeSyncProcess()
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
    with pytest.raises(TuneError):
        syncer.wait()

    # Remote storage was deleted, so target should be empty
    assert_file(False, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "level0_exclude.txt")
    assert_file(False, tmp_target, "subdir/level1.txt")
    assert_file(False, tmp_target, "subdir/level1_exclude.txt")
    assert_file(False, tmp_target, "subdir/nested/level2.txt")
    assert_file(False, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(False, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_syncer_wait_or_retry(temp_data_dirs):
    """Check that the wait or retry API works"""
    tmp_source, tmp_target = temp_data_dirs

    syncer = _DefaultSyncer(sync_period=60)

    # Will fail as dir does not exist
    syncer.sync_down(
        remote_dir="memory:///test/test_syncer_wait_or_retry", local_dir=tmp_target
    )
    with pytest.raises(TuneError) as e:
        syncer.wait_or_retry(max_retries=3, backoff_s=0)
        assert "Failed sync even after 3 retries." in str(e)


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

    class FaultyCheckpoint(Checkpoint):
        def to_uri(self, uri: str) -> str:
            raise subprocess.CalledProcessError(-1, "dummy")

    class TestTrainableRetry(TestTrainable):
        _checkpoint_cls = FaultyCheckpoint

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
        remote_checkpoint_dir=f"file://{tmp_target}"
    )

    ray.get(trainable.save.remote())


def test_trainable_syncer_custom(ray_start_2_cpus, temp_data_dirs):
    """Check that Trainable.save() triggers syncing using custom syncer"""
    tmp_source, tmp_target = temp_data_dirs

    trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir=f"file://{tmp_target}",
        custom_syncer=CustomSyncer(),
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

    trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir=f"file://{tmp_target}",
        custom_syncer=CustomCommandSyncer(
            sync_up_template="cp -rf {source} `echo '{target}' | cut -c 8-`",
            sync_down_template="cp -rf `echo '{source}' | cut -c 8-` {target}",
            delete_template="rm -rf `echo '{target}' | cut -c 8-`",
        ),
    )

    checkpoint_dir = ray.get(trainable.save.remote())

    assert_file(True, tmp_target, os.path.join(checkpoint_dir, "checkpoint.data"))

    ray.get(trainable.delete_checkpoint.remote(checkpoint_dir))

    assert_file(False, tmp_target, os.path.join(checkpoint_dir, "checkpoint.data"))


def test_syncer_serialize(temp_data_dirs):
    """Check that syncing up and down works"""
    tmp_source, tmp_target = temp_data_dirs

    syncer = _DefaultSyncer()

    syncer.sync_up(
        local_dir=tmp_source, remote_dir="memory:///test/test_syncer_sync_up_down"
    )

    pickle.dumps(syncer)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
