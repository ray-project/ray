from typing import Generic, TypeVar

T = TypeVar("T")


# TODO(ekl) this is a dummy generic ref type for documentation purposes only.
# We should try to make the Cython ray.ObjectRef properly generic.
class ObjectRef(Generic[T]):
    pass
