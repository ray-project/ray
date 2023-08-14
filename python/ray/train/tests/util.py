import contextlib
import os
import tempfile
from typing import Any, Dict

import ray.cloudpickle as ray_pickle
from ray.train._checkpoint import Checkpoint


@contextlib.contextmanager
def create_dict_checkpoint(data: Dict[str, Any]) -> Checkpoint:
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "data.pkl"), "wb") as f:
            ray_pickle.dump(data, f)
        yield Checkpoint.from_directory(tmpdir)


def load_dict_checkpoint(checkpoint: Checkpoint) -> Dict[str, Any]:
    with checkpoint.as_directory() as checkpoint_dir:
        with open(os.path.join(checkpoint_dir, "data.pkl"), "rb") as f:
            return ray_pickle.load(f)
