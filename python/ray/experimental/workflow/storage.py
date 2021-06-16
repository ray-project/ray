import json
import pathlib
from typing import Any, Union
import uuid

import ray
import ray.cloudpickle
import ray._private.utils

# TODO(suquark): in the future we may use general URLs to support
# different storages, e.g. "s3://xxxx/xxx". Currently we just use
# 'pathlib.Path' for convenience.


class Storage:
    """Base class for accessing workflow storage.

    TODO(suquark): Support S3 etc. in the future. Currently only support
    local/shared filesystems.
    """

    def __init__(self, workflow_root_dir: Union[pathlib.Path, str]):
        self._workflow_root_dir = pathlib.Path(workflow_root_dir)

    def _write_atomic(self, writer, mode, obj, path: pathlib.Path, overwrite):
        # TODO(suquark): race conditions like two processes writing the
        # same file is still not safe. This may not be an issue, because
        # in our current implementation, we only need to guarantee the
        # file is either fully written or not existing.
        fullpath = self._workflow_root_dir / path
        if fullpath.exists() and not overwrite:
            raise FileExistsError(fullpath)
        tmp_new_filename = fullpath.with_suffix(fullpath.suffix + "." +
                                                uuid.uuid4().hex)
        with open(tmp_new_filename, mode) as f:
            writer(obj, f)
        if fullpath.exists():
            backup_path = fullpath.with_suffix(fullpath.suffix + ".backup")
            if backup_path.exists():
                backup_path.unlink()
            fullpath.rename(backup_path)
        tmp_new_filename.rename(fullpath)

    def _read_object(self, reader, mode, path):
        fullpath = self._workflow_root_dir / path
        if fullpath.exists():
            with open(fullpath, mode) as f:
                return reader(f)
        backup_path = fullpath.with_suffix(fullpath.suffix + ".backup")
        if backup_path.exists():
            with open(backup_path, mode) as f:
                return reader(f)
        raise FileNotFoundError(fullpath)

    def file_exists(self, path: pathlib.Path) -> bool:
        fullpath = self._workflow_root_dir / path
        backup_path = fullpath.with_suffix(fullpath.suffix + ".backup")
        return fullpath.exists() or backup_path.exists()

    def directory_exists(self, path: pathlib.Path) -> bool:
        fullpath = self._workflow_root_dir / path
        return fullpath.exists()

    def write_object_atomic(self, obj, path: pathlib.Path,
                            overwrite=True) -> None:
        self._write_atomic(ray.cloudpickle.dump, "wb", obj, path, overwrite)

    def write_json_atomic(self, json_obj, path: pathlib.Path,
                          overwrite=True) -> None:
        self._write_atomic(json.dump, "w", json_obj, path, overwrite)

    def read_object(self, path: pathlib.Path) -> Any:
        return self._read_object(ray.cloudpickle.load, "rb", path)

    def read_json(self, path: pathlib.Path) -> Any:
        return self._read_object(json.load, "r", path)

    def create_directory(self, path: pathlib.Path, parents=True,
                         exist_ok=True) -> None:
        dir_path = self._workflow_root_dir / path
        dir_path.mkdir(parents=parents, exist_ok=exist_ok)


_global_storage = Storage(
    pathlib.Path(ray._private.utils.get_ray_temp_dir()) / "workflows")


def get_global_storage():
    return _global_storage


def set_global_storage(workflow_root_dir):
    global _global_storage
    _global_storage = Storage(workflow_root_dir)
