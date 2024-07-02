import ray
import dataclasses
from typing import Dict


@dataclasses.dataclass
class RefCountedObject:
    """
    Ref-counted bytes.
    """

    object_ref: ray.ObjectRef
    ref_count: int


# For circular imports, we can't use `ray.remote` here.
# Instead we wrap the actor in get_data_holder().
# @ray.remote(num_cpus=0)
class DataHolder:
    """
    Global singleton that holds ref-counted bytes to be shared.
    """

    def __init__(self):
        self.data: Dict[str, RefCountedObject] = {}

    def create(self, key: str, value: bytes):
        if not isinstance(key, str):
            raise TypeError(f"Key must be str, got {type(key)}")
        if not isinstance(value, bytes):
            raise TypeError(f"Value must be bytes, got {type(value)}")
        if key in self.data:
            raise KeyError(f"Key {key} already exists.")
        object_ref = ray.put(value)
        self.data[key] = RefCountedObject(object_ref, 1)
        return object_ref

    def exists(self, key: str):
        return key in self.data

    def increment(self, key: str):
        if key not in self.data:
            raise KeyError(f"Key {key} does not exist.")
        self.data[key].ref_count += 1
        return self.data[key].object_ref

    def decrement(self, key: str):
        """
        Returns True if the object is deleted.

        TODO: Now it seems there's no clean up in packaging.py. It imports
        _internal_kv_put but not _internal_kv_del. For a true "forever running cluster"
        we will need to clean up.
        """
        if key not in self.data:
            raise KeyError(f"Key {key} does not exist.")
        self.data[key].ref_count -= 1
        if self.data[key].ref_count == 0:
            del self.data[key]
            return True
        return False

    def get(self, key: str):
        if key not in self.data:
            raise KeyError(f"Key {key} does not exist.")
        return self.data[key].object_ref


DATA_HOLDER_NAME = "data_holder"
DATA_HOLDER_NAMESPACE = "_data_holder"

_global_data_holder = None


def get_data_holder() -> DataHolder:
    global _global_data_holder
    if _global_data_holder is None:
        _global_data_holder = (
            ray.remote(num_cpus=0)(DataHolder)
            .options(
                name=DATA_HOLDER_NAME,
                namespace=DATA_HOLDER_NAMESPACE,
                get_if_exists=True,
                lifetime="detached",
            )
            .remote()
        )
    return _global_data_holder
