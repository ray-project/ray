from typing import Dict, Union, Optional, Any, List, TYPE_CHECKING
import os

import ray
from ray import cloudpickle
from ray.types import ObjectRef
from ray.util.annotations import PublicAPI
from ray._private.client_mode_hook import client_mode_hook

if TYPE_CHECKING:
    import pyarrow.fs


_filesystem = None
_storage_prefix = None


@PublicAPI
@client_mode_hook(auto_init=True)
def get_filesystem() -> ("pyarrow.fs.FileSystem", str):
    global _filesystem, _storage_prefix
    if _filesystem is None:
        worker = ray.worker.global_worker
        base_uri = os.environ.get("RAY_storage", os.path.join(worker.node._temp_dir, "storage"))
        import pyarrow.fs
        _filesystem, _storage_prefix = pyarrow.fs.FileSystem.from_uri(base_uri)
        print("Storage initialized", _filesystem, _storage_prefix)
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
    namespace: str, path: str, recursive: bool = False,
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
