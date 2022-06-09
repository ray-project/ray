from typing import List, Optional

import os

import pytest
import shutil
import tempfile

from freezegun import freeze_time

import ray

from ray import tune
from ray.tune import TuneError
from ray.tune.syncer import _DefaultSyncer, Syncer
from ray.tune.utils.file_transfer import _pack_dir, _unpack_dir


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


class TestTrainable(tune.Trainable):
    def save_checkpoint(self, checkpoint_dir: str):
        with open(os.path.join(checkpoint_dir, "checkpoint.data"), "w") as f:
            f.write("Data")
        return checkpoint_dir

    def check_exists(self, checkpoint_path: str, file_path: str) -> bool:
        if os.path.exists(checkpoint_path):
            shutil.rmtree(checkpoint_path)

        try:
            self._maybe_load_from_cloud(checkpoint_path)
            return os.path.exists(os.path.join(checkpoint_path, file_path))
        except Exception:
            return False


class CustomSyncer(Syncer):
    def __init__(self, sync_period: float = 300.0):
        super(CustomSyncer, self).__init__(sync_period=sync_period)
        self._sync_status = {}

    def sync_up(
        self, local_dir: str, remote_dir: str, exclude: Optional[List] = None
    ) -> bool:
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
        raise NotImplementedError

    def retry(self):
        raise NotImplementedError

    def wait(self):
        pass


def test_syncer_sync_up_down(temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    syncer = _DefaultSyncer()

    syncer.sync_up(local_dir=tmp_source, remote_dir="memory:///test/test_syncer_sync_up_down")
    syncer.wait()

    syncer.sync_down(
        remote_dir="memory:///test/test_syncer_sync_up_down", local_dir=tmp_target
    )
    syncer.wait()

    assert_file(True, tmp_target, "level0.txt")
    assert_file(True, tmp_target, "level0_exclude.txt")
    assert_file(True, tmp_target, "subdir/level1.txt")
    assert_file(True, tmp_target, "subdir/level1_exclude.txt")
    assert_file(True, tmp_target, "subdir/nested/level2.txt")
    assert_file(True, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(True, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_sync_up_if_needed(temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    with freeze_time() as frozen:
        syncer = _DefaultSyncer(sync_period=60)
        assert syncer.sync_up_if_needed(
            local_dir=tmp_source, remote_dir="memory:///test/test_sync_up_not_needed"
        )
        syncer.wait()

        frozen.tick(30)

        assert not syncer.sync_up_if_needed(
            local_dir=tmp_source, remote_dir="memory:///test/test_sync_up_not_needed"
        )

        frozen.tick(30)

        assert syncer.sync_up_if_needed(
            local_dir=tmp_source, remote_dir="memory:///test/test_sync_up_not_needed"
        )


def test_sync_down_if_needed(temp_data_dirs):
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

        assert not syncer.sync_down_if_needed(
            remote_dir="memory:///test/test_sync_down_if_needed", local_dir=tmp_target
        )

        frozen.tick(30)

        assert syncer.sync_down_if_needed(
            remote_dir="memory:///test/test_sync_down_if_needed", local_dir=tmp_target
        )


def test_syncer_delete(temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    syncer = _DefaultSyncer(sync_period=60)

    # Populate remote directory
    syncer.sync_up(local_dir=tmp_source, remote_dir="memory:///test/test_syncer_delete")
    syncer.wait()

    syncer.delete(remote_dir="memory:///test/test_syncer_delete")

    syncer.sync_down(
        remote_dir="memory:///test/test_syncer_delete", local_dir=tmp_target
    )
    syncer.wait()

    assert_file(False, tmp_target, "level0.txt")
    assert_file(False, tmp_target, "level0_exclude.txt")
    assert_file(False, tmp_target, "subdir/level1.txt")
    assert_file(False, tmp_target, "subdir/level1_exclude.txt")
    assert_file(False, tmp_target, "subdir/nested/level2.txt")
    assert_file(False, tmp_target, "subdir_nested_level2_exclude.txt")
    assert_file(False, tmp_target, "subdir_exclude/something/somewhere.txt")


def test_syncer_wait_or_retry(temp_data_dirs):
    tmp_source, tmp_target = temp_data_dirs

    syncer = _DefaultSyncer(sync_period=60)

    # Will fail as dir does not exist
    syncer.sync_down(
        remote_dir="memory:///test/test_syncer_wait_or_retry", local_dir=tmp_target
    )
    with pytest.raises(TuneError) as e:
        syncer.wait_or_retry(max_retries=3, backoff_s=0)
        assert "Failed sync even after 3 retries." in str(e)


def test_trainable_syncer_default(ray_start_2_cpus):
    trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir="memory:///test/test_trainable_syncer_default"
    )

    checkpoint_dir = ray.get(trainable.save.remote())

    assert ray.get(trainable.check_exists.remote(checkpoint_dir, "checkpoint.data"))


def test_trainable_syncer_custom(ray_start_2_cpus):
    trainable = ray.remote(TestTrainable).remote(
        remote_checkpoint_dir="memory:///test/test_trainable_syncer_default",
        custom_syncer=CustomSyncer(),
    )

    checkpoint_dir = ray.get(trainable.save.remote())

    assert ray.get(trainable.check_exists.remote(checkpoint_dir, "checkpoint.data"))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
