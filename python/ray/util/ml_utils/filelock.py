from filelock import FileLock
from pathlib import Path
import hashlib
import os
import tempfile

RAY_LOCKFILE_DIR = "ray_lockfiles"


class TempFileLock:
    """FileLock wrapper that uses temporary file locks."""

    def __init__(self, path: str):
        self.path = path
        os.makedirs(str(self.lock_dir), exist_ok=True)
        self._lock = FileLock(self.lock_path)

    @property
    def lock_dir(self) -> Path:
        return Path(tempfile.gettempdir()) / RAY_LOCKFILE_DIR

    @property
    def path_hash(self) -> str:
        return hashlib.md5(str(Path(self.path).resolve()).encode("utf-8")).hexdigest()

    @property
    def lock_path(self) -> Path:
        self.lock_dir / f"{self.path_hash}.lock"

    def __enter__(self):
        self._lock.acquire()

    def __exit__(self, type, value, traceback):
        self._lock.release()

    def __getattr__(self, name):
        return getattr(self._lock, name)
