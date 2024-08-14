import contextlib
import os
import tempfile
from typing import Any, Dict, Optional, Type

import ray.cloudpickle as ray_pickle
from ray.train import Checkpoint, SyncConfig
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
    storage_path: Optional[str] = None,
    storage_context_cls: Type = StorageContext,
    sync_config: Optional[SyncConfig] = None,
) -> StorageContext:
    storage_path = storage_path or tempfile.mkdtemp()
    exp_name = exp_name
    trial_name = "trial_name"

    storage = storage_context_cls(
        storage_path=storage_path,
        experiment_dir_name=exp_name,
        trial_dir_name=trial_name,
        sync_config=sync_config,
    )
    # Patch the default /tmp/ray/session_* so we don't require ray
    # to be initialized in unit tests.
    session_path = tempfile.mkdtemp()
    storage._get_session_path = lambda: session_path

    os.makedirs(storage.trial_fs_path, exist_ok=True)
    os.makedirs(storage.trial_driver_staging_path, exist_ok=True)

    return storage
