import json
import os
import tempfile
from typing import Dict, Union

import pytest

import ray
from ray import tune
from ray.air import session, Checkpoint
from ray.air._internal.remote_storage import download_from_uri
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
