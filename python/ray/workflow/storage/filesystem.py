import os
import contextlib
import json
import shutil
import pathlib
from typing import Any, List
import uuid

from ray.workflow.storage.base import Storage, KeyNotFoundError

import ray.cloudpickle


@contextlib.contextmanager
def _open_atomic(path: pathlib.Path, mode="r"):
    """Open file with atomic file writing support. File reading is also
    adapted to atomic file writing (for example, the backup file
    is used when an atomic write failed previously.)

    TODO(suquark): race condition like two processes writing the
    same file is still not safe. This may not be an issue, because
    in our current implementation, we only need to guarantee the
    file is either fully written or not existing.

    Args:
        path: The file path.
        mode: Open mode same as "open()".

    Returns:
        File object.
    """
    if "a" in mode or "+" in mode:
        raise ValueError("Atomic open does not support appending.")
    # backup file is hidden by default
    backup_path = path.with_name(f".{path.name}.backup")
    if "r" in mode:  # read mode
        if _file_exists(path):
            f = open(path, mode)
        else:
            raise KeyNotFoundError(path)
        try:
            yield f
        finally:
            f.close()
    elif "x" in mode:  # create mode
        if path.exists():
            raise FileExistsError(path)
        tmp_new_fn = path.with_suffix(f".{path.name}.{uuid.uuid4().hex}")
        if not tmp_new_fn.parent.exists():
            tmp_new_fn.parent.mkdir(parents=True)
        f = open(tmp_new_fn, mode)
        write_ok = True
        try:
            yield f
        except Exception:
            write_ok = False
            raise
        finally:
            f.close()
            if write_ok:
                # "commit" file if writing succeeded
                tmp_new_fn.rename(path)
            else:
                # remove file if writing failed
                tmp_new_fn.unlink()
    elif "w" in mode:  # overwrite mode
        # backup existing file
        if path.exists():
            # remove an even older backup file
            if backup_path.exists():
                backup_path.unlink()
            path.rename(backup_path)
        tmp_new_fn = path.with_suffix(f".{path.name}.{uuid.uuid4().hex}")
        if not tmp_new_fn.parent.exists():
            tmp_new_fn.parent.mkdir(parents=True)
        f = open(tmp_new_fn, mode)
        write_ok = True
        try:
            yield f
        except Exception:
            write_ok = False
            raise
        finally:
            f.close()
            if write_ok:
                tmp_new_fn.rename(path)
                # cleanup the backup file
                if backup_path.exists():
                    backup_path.unlink()
            else:
                # remove file if writing failed
                tmp_new_fn.unlink()
    else:
        raise ValueError(f"Unknown file open mode {mode}.")


def _file_exists(path: pathlib.Path) -> bool:
    """During atomic writing, we backup the original file. If the writing
    failed during the middle, then only the backup exists. We consider the
    file exists if the file or the backup file exists. We also automatically
    restore the backup file to the original path if only backup file exists.

    Args:
        path: File path.

    Returns:
        True if the file and backup exists.
    """
    backup_path = path.with_name(f".{path.name}.backup")
    if path.exists():
        return True
    elif backup_path.exists():
        backup_path.rename(path)
        return True
    return False


class FilesystemStorageImpl(Storage):
    """Filesystem implementation for accessing workflow storage.

    We do not repeat the same comments for abstract methods in the base class.
    """

    def __init__(self, workflow_root_dir: str):
        self._workflow_root_dir = pathlib.Path(workflow_root_dir)
        if self._workflow_root_dir.exists():
            if not self._workflow_root_dir.is_dir():
                raise ValueError(
                    f"storage path {workflow_root_dir} must be" " a directory."
                )
        else:
            self._workflow_root_dir.mkdir(parents=True)

    def make_key(self, *names: str) -> str:
        return os.path.join(str(self._workflow_root_dir), *names)

    async def put(self, key: str, data: Any, is_json: bool = False) -> None:
        if is_json:
            with _open_atomic(pathlib.Path(key), "w") as f:
                return json.dump(data, f)
        else:
            with _open_atomic(pathlib.Path(key), "wb") as f:
                return ray.cloudpickle.dump(data, f)

    async def get(self, key: str, is_json: bool = False) -> Any:
        if is_json:
            with _open_atomic(pathlib.Path(key)) as f:
                return json.load(f)
        else:
            with _open_atomic(pathlib.Path(key), "rb") as f:
                return ray.cloudpickle.load(f)

    async def delete_prefix(self, key_prefix: str) -> None:
        path = pathlib.Path(key_prefix)
        if path.is_dir():
            shutil.rmtree(str(path))
        else:
            path.unlink()

    async def scan_prefix(self, key_prefix: str) -> List[str]:
        try:
            path = pathlib.Path(key_prefix)
            return [p.name for p in path.iterdir()]
        except FileNotFoundError:
            return []

    @property
    def storage_url(self) -> str:
        return "file://" + str(self._workflow_root_dir.absolute())

    def __reduce__(self):
        return FilesystemStorageImpl, (self._workflow_root_dir,)
