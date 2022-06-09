import os
from typing import Optional

import pytest
import shutil
import tempfile

import ray.util
from freezegun import freeze_time
from ray.tune.result import NODE_IP

from ray.tune.syncer import SyncerCallback, DEFAULT_SYNC_PERIOD
from ray.util.ml_utils.checkpoint_manager import _TrackedCheckpoint, CheckpointStorage


class MockTrial:
    def __init__(self, trial_id: str, logdir: str):
        self.trial_id = trial_id
        self.last_result = {NODE_IP: ray.util.get_node_ip_address()}
        self.uses_cloud_checkpointing = False
        self.sync_on_checkpoint = True

        self.logdir = logdir


class TestSyncerCallback(SyncerCallback):
    def __init__(
        self,
        enabled: bool = True,
        sync_period: float = DEFAULT_SYNC_PERIOD,
        local_logdir_override: Optional[str] = None,
    ):
        super(TestSyncerCallback, self).__init__(
            enabled=enabled, sync_period=sync_period
        )
        self._local_logdir_override = local_logdir_override

    def _local_trial_logdir(self, trial):
        if self._local_logdir_override:
            return self._local_logdir_override
        return super(TestSyncerCallback, self)._local_trial_logdir(trial)


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
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


def test_syncer_callback_sync(ray_start_2_cpus, temp_data_dirs):
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


def test_syncer_callback_sync_period(ray_start_2_cpus, temp_data_dirs):
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
