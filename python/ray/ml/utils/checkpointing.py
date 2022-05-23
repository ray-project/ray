import contextlib
import os
import shutil
import tempfile
import uuid
from copy import copy
from filelock import FileLock
from pathlib import Path
from typing import Iterator, Optional

import ray
import ray.cloudpickle as cpickle
from ray.ml.checkpoint import Checkpoint
from ray.ml.preprocessor import Preprocessor
from ray.ml.constants import PREPROCESSOR_KEY
from ray.tune.utils.file_transfer import sync_dir_between_nodes
from ray.util import get_node_ip_address
from ray.util.annotations import DeveloperAPI


def save_preprocessor_to_dir(
    preprocessor: Preprocessor,
    parent_dir: os.PathLike,
) -> os.PathLike:
    """Save preprocessor to file. Returns path saved to."""
    parent_dir = Path(parent_dir)
    with open(parent_dir.joinpath(PREPROCESSOR_KEY), "wb") as f:
        cpickle.dump(preprocessor, f)


def load_preprocessor_from_dir(
    parent_dir: os.PathLike,
) -> Optional[Preprocessor]:
    """Loads preprocessor from directory, if file exists."""
    parent_dir = Path(parent_dir)
    preprocessor_path = parent_dir.joinpath(PREPROCESSOR_KEY)
    if preprocessor_path.exists():
        with open(preprocessor_path, "rb") as f:
            preprocessor = cpickle.load(f)
    else:
        preprocessor = None
    return preprocessor


@DeveloperAPI
class SyncCheckpoint(Checkpoint):
    """Checkpoint with special sync logic for dirs."""

    def __init__(
        self,
        local_path: Optional[str] = None,
        data_dict: Optional[dict] = None,
        uri: Optional[str] = None,
        obj_ref: Optional[ray.ObjectRef] = None,
    ):
        self._init_tmp_dir_name()
        super().__init__(local_path, data_dict, uri, obj_ref)

    def _init_tmp_dir_name(self):
        self._tmp_dir_name = uuid.uuid4().hex

    @property
    def remote_ip(self) -> Optional[str]:
        return getattr(self, "_remote_ip", None)

    def _get_temporary_checkpoint_dir(self) -> str:
        return str(Path(tempfile.gettempdir()).joinpath(self._tmp_dir_name))

    def _to_directory_from_dict(self, data_dict: dict, path: str):
        remote_ip = self.remote_ip
        if not remote_ip:
            return super()._to_directory_from_dict(data_dict, path)
        sync_dir_between_nodes(
            source_ip=remote_ip,
            source_path=self._local_path,
            target_ip=get_node_ip_address(),
            target_path=path,
            max_size_bytes=None,
        )

    def _to_directory_from_local_path_or_uri(self, path: str):
        remote_ip = self.remote_ip
        if not remote_ip:
            return super()._to_directory_from_local_path_or_uri(path)
        sync_dir_between_nodes(
            source_ip=remote_ip,
            source_path=self._local_path,
            target_ip=get_node_ip_address(),
            target_path=path,
            max_size_bytes=None,
        )

    def _make_dir(self, path: str) -> None:
        super()._make_dir(path)
        del_lock_path = Path(path).joinpath(f".del_lock_{os.getpid()}")
        open(del_lock_path, "a").close()

    @contextlib.contextmanager
    def as_directory(self) -> Iterator[str]:
        if self._local_path and not self.remote_ip:
            yield self._local_path
        else:
            temp_dir = self.to_directory()
            del_lock_path = Path(temp_dir).joinpath(f".del_lock_{os.getpid()}")
            yield temp_dir
            try:
                os.remove(str(del_lock_path))
                print(f"removed {del_lock_path}")
            except Exception:
                pass
            # check if any lock files are remaining
            if not list(Path(temp_dir).glob(".del_lock_*")):
                print(f"removing {temp_dir}")
                with FileLock(f"{temp_dir}.lock"):
                    shutil.rmtree(temp_dir, ignore_errors=True)

    def _to_sync(self) -> dict:
        assert self._local_path
        print("SyncCheckpoint._to_sync")
        state = self.__dict__.copy()
        state["_remote_ip"] = get_node_ip_address()
        return state

    def __getstate__(self):
        if self._local_path:
            return self._to_sync()
        return self.__dict__

    @classmethod
    def from_checkpoint(self, checkpoint: Checkpoint) -> "SyncCheckpoint":
        """Convert Checkpoint to SyncCheckpoint."""
        if isinstance(checkpoint, SyncCheckpoint):
            return checkpoint
        sync_checkpoint = copy(checkpoint)
        sync_checkpoint.__class__ = SyncCheckpoint
        sync_checkpoint._init_tmp_dir_name()
        return sync_checkpoint
