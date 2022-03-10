from typing import Dict, Union, Optional, Any, List, TYPE_CHECKING
import os

import ray
from ray import cloudpickle
from ray.types import ObjectRef
from ray.util.annotations import PublicAPI
from ray._private.client_mode_hook import client_mode_hook

if TYPE_CHECKING:
    import pyarrow.fs


# The full storage argument specified, e.g., in ``ray.init(storage="s3://foo/bar")``
_storage_uri = None

# The storage prefix, e.g., "foo/bar" under which files should be written.
_storage_prefix = None

# The pyarrow.fs.FileSystem instantiated for the storage.
_filesystem = None


def _init_storage(storage_uri: str, is_head: bool):
    global _storage_uri
    _storage_uri = storage_uri

    if is_head:
        _init_filesystem(storage_uri, create_valid_file=True)


def _init_filesystem(storage_uri: str, create_valid_file: bool = False):
    global _filesystem, _storage_prefix
    if not storage_uri:
        raise ValueError(
            "No storage URI has been configured for the cluster. "
            "Specify a storage URI via `ray.init(storage=<uri>)`"
        )

    import pyarrow.fs

    _filesystem, _storage_prefix = pyarrow.fs.FileSystem.from_uri(storage_uri)

    valid_file = os.path.join(_storage_prefix, "_valid")
    if create_valid_file:
        _filesystem.create_dir(_storage_prefix)
        with _filesystem.open_output_stream(valid_file):
            pass
    else:
        valid = _filesystem.get_file_info([valid_file + "x"])[0]
        if valid.type == pyarrow.fs.FileType.NotFound:
            raise RuntimeError(
                "Unable to initialize storage: {} flag not found. "
                "Check that configured cluster storage path is readable from all "
                "worker nodes of the cluster.".format(valid_file)
            )

    return _filesystem, _storage_prefix


@PublicAPI
@client_mode_hook(auto_init=True)
def get_filesystem() -> ("pyarrow.fs.FileSystem", str):
    global _filesystem, _storage_prefix
    if _filesystem is None:
        _init_filesystem()
    return _filesystem, _storage_prefix


def save_objects(
    namespace: str,
    objects: Dict[str, Union[Any, ObjectRef]],
    overwrite_if_exists: bool = True,
) -> None:
    fs, prefix = get_filesystem()
    base = os.path.join(prefix, namespace)
    for path, obj in objects.items():
        full_path = os.path.join(base, path)
        fs.create_dir(os.path.dirname(full_path))
        with fs.open_output_stream(full_path) as f:
            # TODO: do this efficiently
            if isinstance(obj, ray.ObjectRef):
                data = cloudpickle.dumps(ray.get(obj))
            else:
                data = cloudpickle.dumps(obj)
            f.write(data)


def delete_object(namespace: str, path: str) -> bool:
    fs, prefix = get_filesystem()
    base = os.path.join(prefix, namespace)
    full_path = os.path.join(base, path)
    try:
        fs.delete_file(full_path)
        return True
    except FileNotFoundError:
        return False


def get_object_info(namespace: str, path: str) -> List["pyarrow.fs.FileInfo"]:
    fs, prefix = get_filesystem()
    base = os.path.join(prefix, namespace)
    full_path = os.path.join(base, path)
    return fs.get_file_info([full_path])


def list_objects(
    namespace: str,
    path: str,
    recursive: bool = False,
) -> List["pyarrow.fs.FileInfo"]:
    fs, prefix = get_filesystem()
    from pyarrow.fs import FileSelector

    base = os.path.join(prefix, namespace)
    full_path = os.path.join(base, path)
    selector = FileSelector(full_path, recursive=recursive)
    files = fs.get_file_info(selector)
    return files


def load_object(namespace: str, path: str) -> ObjectRef:
    fs, prefix = get_filesystem()
    base = os.path.join(prefix, namespace)
    full_path = os.path.join(base, path)
    with fs.open_input_stream(full_path) as f:
        # TODO: do this efficiently
        data = cloudpickle.loads(f.read())
        return data
