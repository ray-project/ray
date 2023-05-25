from filelock import FileLock
import hashlib
import os
from pathlib import Path

import ray


RAY_LOCKFILE_DIR = "_ray_lockfiles"


class TempFileLock:
    """FileLock wrapper that uses temporary file locks.

    The temporary directory that these locks are saved to can be configured via
    the `RAY_TMPDIR` environment variable.

    Args:
        path: The file path that this temporary file lock is used for.
            This will be used to generate the lockfile filename.
            Ex: For concurrent writes to a file, this is the common filepath
            that multiple processes are writing to.
        **kwargs: Additional keyword arguments to pass to the underlying `FileLock`.
    """

    def __init__(self, path: str, **kwargs):
        self.path = path
        temp_dir = Path(ray._private.utils.get_user_temp_dir()).resolve()
        self._lock_dir = temp_dir / RAY_LOCKFILE_DIR
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
