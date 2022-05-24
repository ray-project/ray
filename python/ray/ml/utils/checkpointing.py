import contextlib
import os
import shutil
import tempfile
import uuid
from copy import copy
from filelock import FileLock
from pathlib import Path
from typing import Iterator, Optional

import ray.cloudpickle as cpickle
from ray.ml.checkpoint import Checkpoint
from ray.ml.preprocessor import Preprocessor
from ray.ml.constants import PREPROCESSOR_KEY
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
    """Checkpoint with special sync logic for dirs.

    This class contains a ``_tmp_dir_name`` attribute set
    on initailization, used to provide the same temporary
    directory name for all workers, in order to avoid
    multiple workers on the same node using separate
    but equal temporary directories."""

    @staticmethod
    def get_tmp_dir_name() -> str:
        return uuid.uuid4().hex

    @property
    def tmp_dir_name(self) -> str:
        if not hasattr(self, "_tmp_dir_name"):
            self._tmp_dir_name = self.get_tmp_dir_name()
        return self._tmp_dir_name

    @tmp_dir_name.setter
    def tmp_dir_name(self, value: str):
        self._tmp_dir_name = value

    def _get_temporary_checkpoint_dir(self) -> str:
        return str(Path(tempfile.gettempdir()).joinpath(self.tmp_dir_name))

    def _make_dir(self, path: str) -> None:
        super()._make_dir(path)
        del_lock_path = self._get_del_lock_path(path)
        open(del_lock_path, "a").close()

    def _get_del_lock_path(self, path: str) -> Path:
        return Path(path).joinpath(f".del_lock_{os.getpid()}")

    @contextlib.contextmanager
    def as_directory(self) -> Iterator[str]:
        if self._local_path:
            yield self._local_path
        else:
            temp_dir = self.to_directory()
            del_lock_path = self._get_del_lock_path(temp_dir)
            yield temp_dir
            try:
                os.remove(str(del_lock_path))
                print(f"removed {del_lock_path}")
            except Exception:
                pass
            # check if any lock files are remaining
            if not list(Path(temp_dir).glob(".del_lock_*")):
                try:
                    with FileLock(f"{temp_dir}.lock", timeout=0):
                        print(f"removing {temp_dir}")
                        shutil.rmtree(temp_dir, ignore_errors=True)
                except TimeoutError:
                    pass

    @classmethod
    def from_checkpoint(self, checkpoint: Checkpoint) -> "SyncCheckpoint":
        """Convert Checkpoint to SyncCheckpoint."""
        if isinstance(checkpoint, SyncCheckpoint):
            return checkpoint
        sync_checkpoint = copy(checkpoint)
        sync_checkpoint.__class__ = SyncCheckpoint
        sync_checkpoint._init_tmp_dir_name()
        return sync_checkpoint
