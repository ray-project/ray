import json
import os
import tempfile
import time
import uuid
from typing import Dict, Union
from unittest.mock import patch

import pytest

import ray
from ray import tune
from ray.air import session, Checkpoint
from ray.air._internal.remote_storage import (
    download_from_uri,
    upload_to_uri,
    delete_at_uri,
)
from ray.tune.logger import NoopLogger
from ray.tune.trainable import wrap_function


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


class SavingTrainable(tune.Trainable):
    def __init__(self, return_type: str, *args, **kwargs):
        self.return_type = return_type
        super(SavingTrainable, self).__init__(*args, **kwargs)

    def step(self):
        return {"iter": self.training_iteration}

    def save_checkpoint(self, tmp_checkpoint_dir: str):
        checkpoint_data = {"data": 1}

        if self.return_type == "object":
            return checkpoint_data

        subdir = os.path.join(tmp_checkpoint_dir, "subdir")
        os.makedirs(subdir, exist_ok=True)
        checkpoint_file = os.path.join(subdir, "checkpoint.pkl")
        with open(checkpoint_file, "w") as f:
            f.write(json.dumps(checkpoint_data))

        if self.return_type == "root":
            return tmp_checkpoint_dir
        elif self.return_type == "subdir":
            return subdir
        elif self.return_type == "checkpoint":
            return checkpoint_file

    def load_checkpoint(self, checkpoint: Union[Dict, str]):
        if self.return_type == "object":
            assert isinstance(checkpoint, dict)
            checkpoint_data = checkpoint
            checkpoint_file = None
        elif self.return_type == "root":
            assert "subdir" not in checkpoint
            checkpoint_file = os.path.join(checkpoint, "subdir", "checkpoint.pkl")
        elif self.return_type == "subdir":
            assert "subdir" in checkpoint
            assert "checkpoint.pkl" not in checkpoint
            checkpoint_file = os.path.join(checkpoint, "checkpoint.pkl")
        else:  # self.return_type == "checkpoint"
            assert checkpoint.endswith("subdir/checkpoint.pkl")
            checkpoint_file = checkpoint

        if checkpoint_file:
            with open(checkpoint_file, "rb") as f:
                checkpoint_data = json.load(f)

        checkpoint_data = {
            key: value
            for key, value in checkpoint_data.items()
            if not key.startswith("_")
        }
        assert checkpoint_data == {"data": 1}, checkpoint_data


def function_trainable_dict(config):
    session.report(
        {"metric": 2}, checkpoint=Checkpoint.from_dict({"checkpoint_data": 3})
    )


def function_trainable_directory(config):
    tmpdir = tempfile.mkdtemp("checkpoint_test")
    with open(os.path.join(tmpdir, "data.json"), "w") as f:
        json.dump({"checkpoint_data": 5}, f)
    session.report({"metric": 4}, checkpoint=Checkpoint.from_directory(tmpdir))


@pytest.mark.parametrize("return_type", ["object", "root", "subdir", "checkpoint"])
def test_save_load_checkpoint_path_class(ray_start_2_cpus, return_type):
    """Assert that restoring from a Trainable.save() future works with
    class trainables.

    Needs Ray cluster so we get actual futures.
    """
    trainable = ray.remote(SavingTrainable).remote(return_type=return_type)

    saving_future = trainable.save.remote()

    # Check for errors
    ray.get(saving_future)

    restoring_future = trainable.restore.remote(saving_future)

    ray.get(restoring_future)


@pytest.mark.parametrize("return_type", ["object", "root", "subdir", "checkpoint"])
def test_save_load_checkpoint_object_class(ray_start_2_cpus, return_type):
    """Assert that restoring from a Trainable.save_to_object() future works with
    class trainables.

    Needs Ray cluster so we get actual futures.
    """
    trainable = ray.remote(SavingTrainable).remote(return_type=return_type)

    saving_future = trainable.save_to_object.remote()

    # Check for errors
    ray.get(saving_future)

    restoring_future = trainable.restore_from_object.remote(saving_future)

    ray.get(restoring_future)


@pytest.mark.parametrize(
    "fn_trainable", [function_trainable_dict, function_trainable_directory]
)
def test_save_load_checkpoint_path_fn(ray_start_2_cpus, fn_trainable):
    """Assert that restoring from a Trainable.save() future works with
    function trainables.

    Needs Ray cluster so we get actual futures.
    """
    trainable_cls = wrap_function(fn_trainable)
    trainable = ray.remote(trainable_cls).remote()
    ray.get(trainable.train.remote())

    saving_future = trainable.save.remote()

    # Check for errors
    ray.get(saving_future)

    restoring_future = trainable.restore.remote(saving_future)

    ray.get(restoring_future)


@pytest.mark.parametrize(
    "fn_trainable", [function_trainable_dict, function_trainable_directory]
)
def test_save_load_checkpoint_object_fn(ray_start_2_cpus, fn_trainable):
    """Assert that restoring from a Trainable.save_to_object() future works with
    function trainables.

    Needs Ray cluster so we get actual futures.
    """
    trainable_cls = wrap_function(fn_trainable)
    trainable = ray.remote(trainable_cls).remote()
    ray.get(trainable.train.remote())

    saving_future = trainable.save_to_object.remote()

    # Check for errors
    ray.get(saving_future)

    restoring_future = trainable.restore_from_object.remote(saving_future)

    ray.get(restoring_future)


def test_checkpoint_object_no_sync(tmpdir):
    """Asserts that save_to_object() and restore_from_object() do not sync up/down"""
    trainable = SavingTrainable(
        "object", remote_checkpoint_dir="memory:///test/location"
    )

    # Save checkpoint
    trainable.save()

    check_dir = tmpdir / "check_save"
    download_from_uri(uri="memory:///test/location", local_path=str(check_dir))
    assert os.listdir(str(check_dir)) == ["checkpoint_000000"]

    # Save to object
    obj = trainable.save_to_object()

    check_dir = tmpdir / "check_save_obj"
    download_from_uri(uri="memory:///test/location", local_path=str(check_dir))
    assert os.listdir(str(check_dir)) == ["checkpoint_000000"]

    # Restore from object
    trainable.restore_from_object(obj)


@pytest.mark.parametrize("hanging", [True, False])
def test_sync_timeout(tmpdir, hanging):
    orig_upload_fn = upload_to_uri

    def _hanging_upload(*args, **kwargs):
        time.sleep(200 if hanging else 0)
        orig_upload_fn(*args, **kwargs)

    trainable = SavingTrainable(
        "object",
        remote_checkpoint_dir=f"memory:///test/location_hanging_{hanging}",
        sync_timeout=0.5,
    )

    with patch("ray.air.checkpoint.upload_to_uri", _hanging_upload):
        trainable.save()

    check_dir = tmpdir / "check_save_obj"

    try:
        download_from_uri(
            uri=f"memory:///test/location_hanging_{hanging}", local_path=str(check_dir)
        )
    except FileNotFoundError:
        hung = True
    else:
        hung = False

    assert hung == hanging

    if hanging:
        assert not check_dir.exists()
    else:
        assert check_dir.listdir()


def test_find_latest_checkpoint_local(tmpdir):
    """Tests that we identify the latest available checkpoint correctly.

    When new checkpoints are created, they should be the latest available ones.
    When the latest checkpoint is deleted, we should go back to the previous one.
    """

    def _logger(config):
        return NoopLogger(config, str(tmpdir))

    trainable = SavingTrainable(
        "object",
        logger_creator=_logger,
        remote_checkpoint_dir=None,
        sync_timeout=0.5,
    )
    assert trainable._get_latest_local_available_checkpoint() is None

    trainable.save()  # 0

    assert trainable._get_latest_local_available_checkpoint() == str(
        tmpdir / "checkpoint_000000"
    )

    trainable.train()
    trainable.save()  # 1

    assert trainable._get_latest_local_available_checkpoint() == str(
        tmpdir / "checkpoint_000001"
    )

    trainable.train()
    trainable.save()  # 2

    assert trainable._get_latest_local_available_checkpoint() == str(
        tmpdir / "checkpoint_000002"
    )

    assert (tmpdir / "checkpoint_000002").exists()
    (tmpdir / "checkpoint_000002").remove()

    assert trainable._get_latest_local_available_checkpoint() == str(
        tmpdir / "checkpoint_000001"
    )


def test_find_latest_checkpoint_remote(tmpdir):
    """Tests that we identify the latest available checkpoint correctly.

    When new checkpoints are created, they should be the latest available ones.
    When the latest checkpoint is deleted, we should go back to the previous one.
    """
    remote_uri = "memory:///test/location_latest_checkpoint"

    def _logger(config):
        return NoopLogger(config, str(tmpdir))

    trainable = SavingTrainable(
        "object",
        logger_creator=_logger,
        remote_checkpoint_dir=remote_uri,
        sync_timeout=0.5,
    )
    assert trainable._get_latest_remote_available_checkpoint() is None

    trainable.save()  # 0

    assert trainable._get_latest_remote_available_checkpoint() == str(
        tmpdir / "checkpoint_000000"
    )

    trainable.train()
    trainable.save()  # 1

    assert trainable._get_latest_remote_available_checkpoint() == str(
        tmpdir / "checkpoint_000001"
    )

    trainable.train()
    trainable.save()  # 2

    assert trainable._get_latest_remote_available_checkpoint() == str(
        tmpdir / "checkpoint_000002"
    )

    delete_at_uri(remote_uri + "/checkpoint_000002")

    assert trainable._get_latest_remote_available_checkpoint() == str(
        tmpdir / "checkpoint_000001"
    )


@pytest.mark.parametrize("upload_uri", [None, "memory:///test/location_recover_latest"])
@pytest.mark.parametrize("fetch_from_cloud", [False, True])
def test_recover_from_latest(tmpdir, upload_uri, fetch_from_cloud):
    """Test that trainable recovery falls back to recovery from latest checkpoint.

    Creates a trainable, saves a few checkpoints.

    Asserts that restoring from a non-existing path falls back to the latest saved
    checkpoint.

    Asserts that restoring from a previously-existing path falls back to the latest
    saved checkpoints.

    If `fetch_from_cloud=True`, asserts that newer checkpoints on cloud are preferred
    over older checkpoints on local disk.
    """

    def _logger(config):
        return NoopLogger(config, str(tmpdir))

    remote_checkpoint_dir = None
    if upload_uri:
        remote_checkpoint_dir = upload_uri + str(uuid.uuid4())

    trainable = SavingTrainable(
        "object",
        logger_creator=_logger,
        remote_checkpoint_dir=remote_checkpoint_dir,
        sync_timeout=0.5,
    )

    assert trainable._get_latest_available_checkpoint() is None
    trainable.save()

    assert trainable._get_latest_available_checkpoint() == str(
        tmpdir / "checkpoint_000000"
    )

    trainable.train()
    trainable.save()

    assert trainable._get_latest_available_checkpoint() == str(
        tmpdir / "checkpoint_000001"
    )
    trainable.train()
    trainable.save()

    assert trainable._get_latest_available_checkpoint() == str(
        tmpdir / "checkpoint_000002"
    )

    # Re-create trainable
    trainable = SavingTrainable(
        "object",
        logger_creator=_logger,
        remote_checkpoint_dir=remote_checkpoint_dir,
        sync_timeout=0.5,
    )

    if remote_checkpoint_dir and fetch_from_cloud:
        # Make us fetch checkpoint 2 from cloud
        (tmpdir / "checkpoint_000002").remove()

    trainable.restore(str(tmpdir / "not_found"), fallback_to_latest=True)
    assert trainable.training_iteration == 2

    # Delete existing checkpoint
    (tmpdir / "checkpoint_000002").remove()

    if remote_checkpoint_dir:
        delete_at_uri(remote_checkpoint_dir + "/checkpoint_000002")

        if fetch_from_cloud:
            # Make us fetch checkpoint 1 from cloud
            (tmpdir / "checkpoint_000001").remove()

    trainable.restore(str(tmpdir / "checkpoint_000002"), fallback_to_latest=True)
    # Fallback to checkpoint 1
    assert trainable.training_iteration == 1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
