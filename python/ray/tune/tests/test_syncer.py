import logging
import os
import shutil
import subprocess
import tempfile
import time
from typing import List, Optional
from unittest.mock import patch

import pytest
from freezegun import freeze_time

import ray
import ray.cloudpickle as pickle
from ray import tune
from ray.air import session, Checkpoint, RunConfig
from ray.tune import TuneError
from ray.tune.syncer import Syncer, _DefaultSyncer
from ray.tune.utils.file_transfer import _pack_dir, _unpack_dir
from ray.air._internal.remote_storage import upload_to_uri, download_from_uri


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
        sync_config = tune.SyncConfig(upload_dir="invalid://some/url")
        sync_config.validate_upload_dir()


def test_sync_string_invalid_local():
    with pytest.raises(ValueError):
        sync_config = tune.SyncConfig(upload_dir="/invalid/dir")
        sync_config.validate_upload_dir()


def test_sync_string_valid_local():
    sync_config = tune.SyncConfig(upload_dir="file:///valid/dir")
    sync_config.validate_upload_dir()


def test_sync_string_valid_s3():
    sync_config = tune.SyncConfig(upload_dir="s3://valid/bucket")
    sync_config.validate_upload_dir()


def test_sync_config_validate():
    sync_config = tune.SyncConfig()
    sync_config.validate_upload_dir()


def test_sync_config_validate_custom_syncer():
    class CustomSyncer(_DefaultSyncer):
        @classmethod
        def validate_upload_dir(cls, upload_dir: str) -> bool:
            return True

    sync_config = tune.SyncConfig(upload_dir="/invalid/dir", syncer=CustomSyncer())
    sync_config.validate_upload_dir()


def test_sync_config_upload_dir_custom_syncer_mismatch():
    # Shouldn't be able to disable syncing if upload dir is specified
    with pytest.raises(ValueError):
        tune.SyncConfig(upload_dir="s3://valid/bucket", syncer=None)

    # Shouldn't be able to use a custom cloud syncer without specifying cloud dir
    with pytest.raises(ValueError):
        tune.SyncConfig(upload_dir=None, syncer=_DefaultSyncer())


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


def test_final_experiment_checkpoint_sync(tmpdir):
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
            session.report({"score": i})
            time.sleep(0.5)

    tuner = tune.Tuner(
        train_func,
        run_config=RunConfig(
            name="exp_name",
            sync_config=tune.SyncConfig(
                upload_dir="memory:///test_upload_dir", syncer=syncer
            ),
        ),
    )
    results = tuner.fit()
    assert not results.errors

    # Check the contents of the upload_dir immediately after the experiment
    # This won't be up to date if we don't wait on the last sync
    download_from_uri("memory:///test_upload_dir/exp_name", tmpdir)
    cloud_results = tune.Tuner.restore(str(tmpdir)).get_results()
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
