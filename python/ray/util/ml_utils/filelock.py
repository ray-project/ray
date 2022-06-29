import hashlib
import os
import tempfile
from pathlib import Path

from filelock import FileLock

from ray.util.annotations import Deprecated

RAY_LOCKFILE_DIR = "_ray_lockfiles"


@Deprecated
class TempFileLock:
    """FileLock wrapper that uses temporary file locks."""

    def __init__(self, path: str, **kwargs):
        self.path = path
        self._lock_dir = Path(tempfile.gettempdir()).resolve() / RAY_LOCKFILE_DIR
        self._path_hash = hashlib.md5(
            str(Path(self.path).resolve()).encode("utf-8")
        ).hexdigest()
        self._lock_path = self._lock_dir / f"{self._path_hash}.lock"

        os.makedirs(str(self._lock_dir), exist_ok=True)
        self._lock = FileLock(self._lock_path, **kwargs)

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, type, value, traceback):
        self._lock.release()

    def __getattr__(self, name):
        return getattr(self._lock, name)
