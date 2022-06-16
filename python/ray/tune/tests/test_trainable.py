import json
import os
from typing import Dict, Union

import pytest

import ray
from ray import tune


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
        elif self.return_type == "root":
            assert "subdir" not in checkpoint
        elif self.return_type == "subdir":
            assert "subdir" in checkpoint
            assert "checkpoint.pkl" not in checkpoint
        else:  # self.return_type == "checkpoint"
            assert checkpoint.endswith("subdir/checkpoint.pkl")


@pytest.mark.parametrize("return_type", ["object", "root", "subdir", "checkpoint"])
def test_save_load_checkpoint_path(ray_start_2_cpus, return_type):
    trainable = ray.remote(SavingTrainable).remote(return_type=return_type)

    saving_future = trainable.save.remote()

    # Check for errors
    ray.get(saving_future)

    restoring_future = trainable.restore.remote(saving_future)

    ray.get(restoring_future)


@pytest.mark.parametrize("return_type", ["object", "root", "subdir", "checkpoint"])
def test_save_load_checkpoint_object(ray_start_2_cpus, return_type):
    trainable = ray.remote(SavingTrainable).remote(return_type=return_type)

    saving_future = trainable.save_to_object.remote()

    # Check for errors
    ray.get(saving_future)

    restoring_future = trainable.restore_from_object.remote(saving_future)

    ray.get(restoring_future)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
