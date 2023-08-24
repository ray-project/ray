import logging
import os
import shutil
import subprocess
import tempfile
import time
from typing import List, Optional
from unittest.mock import patch

from freezegun import freeze_time
import pyarrow.fs
import pytest

import ray
import ray.cloudpickle as pickle
from ray import tune
from ray.air._internal.remote_storage import (
    upload_to_uri,
    download_from_uri,
    get_fs_and_path,
)
from ray.tune import TuneError
from ray.tune.syncer import _BackgroundProcess, _DefaultSyncer, Syncer
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
        sync_config = tune.SyncConfig()
        sync_config.validate_upload_dir("invalid://some/url")


def test_sync_string_invalid_local():
    with pytest.raises(ValueError):
        sync_config = tune.SyncConfig()
        sync_config.validate_upload_dir("/invalid/dir")


def test_sync_string_valid_local():
    sync_config = tune.SyncConfig()
    sync_config.validate_upload_dir("file:///valid/dir")


def test_sync_string_valid_s3():
    sync_config = tune.SyncConfig()
    sync_config.validate_upload_dir("s3://valid/bucket")


def test_sync_config_validate():
    sync_config = tune.SyncConfig()
    sync_config.validate_upload_dir()


def test_sync_config_validate_custom_syncer():
    class CustomSyncer(_DefaultSyncer):
        @classmethod
        def validate_upload_dir(cls, upload_dir: str) -> bool:
            return True

    sync_config = tune.SyncConfig(syncer=CustomSyncer())
    sync_config.validate_upload_dir("/invalid/dir")


def test_sync_config_upload_dir_custom_syncer_mismatch():
    # Shouldn't be able to disable syncing if upload dir is specified
    with pytest.raises(ValueError):
        sync_config = tune.SyncConfig(syncer=None)
        sync_config.validate_upload_dir("s3://valid/bucket")

    # Shouldn't be able to use a custom cloud syncer without specifying cloud dir
    with pytest.raises(ValueError):
        sync_config = tune.SyncConfig(syncer=_DefaultSyncer())
        sync_config.validate_upload_dir(None)


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
    with pytest.raises(TuneError) as e:
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
    with pytest.raises(TuneError) as e:
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


def test_syncer_serialize(temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    syncer = _DefaultSyncer()

    syncer.sync_up(
        local_dir=tmp_source, remote_dir="memory:///test/test_syncer_sync_up_down"
    )

    serialized = pickle.dumps(syncer)
    loaded_syncer = pickle.loads(serialized)
    assert not loaded_syncer._sync_process


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
