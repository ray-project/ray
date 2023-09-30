import contextlib
import os
import tempfile
from typing import Any, Dict, Optional, Type

import ray.cloudpickle as ray_pickle
from ray.train import Checkpoint
from ray.train._internal.storage import StorageContext


@contextlib.contextmanager
def create_dict_checkpoint(
    data: Dict[str, Any], checkpoint_cls: Type[Checkpoint] = None
) -> Checkpoint:
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "data.pkl"), "wb") as f:
            ray_pickle.dump(data, f)
        checkpoint_cls = checkpoint_cls or Checkpoint
        yield checkpoint_cls.from_directory(tmpdir)


def load_dict_checkpoint(checkpoint: Checkpoint) -> Dict[str, Any]:
    with checkpoint.as_directory() as checkpoint_dir:
        with open(os.path.join(checkpoint_dir, "data.pkl"), "rb") as f:
            return ray_pickle.load(f)


def mock_storage_context(
    exp_name: str = "exp_name",
    delete_syncer: bool = True,
    storage_path: Optional[str] = None,
    storage_context_cls: Type = StorageContext,
) -> StorageContext:
    storage_path = storage_path or tempfile.mkdtemp()
    exp_name = exp_name
    trial_name = "trial_name"
    storage = storage_context_cls(
        storage_path=storage_path,
        experiment_dir_name=exp_name,
        trial_dir_name=trial_name,
    )
    storage.storage_local_path = storage_path
    if delete_syncer:
        storage.syncer = None
    os.makedirs(os.path.join(storage_path, exp_name, trial_name), exist_ok=True)
    return storage
