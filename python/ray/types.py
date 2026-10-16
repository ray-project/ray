from typing import Generic, TypeVar

from ray.util.annotations import PublicAPI

T = TypeVar("T")


# TODO(ekl) this is a dummy generic ref type for documentation purposes only.
# We should try to make the Cython ray.ObjectRef properly generic.
# NOTE(sang): Looks like using Generic in Cython is not currently possible.
@PublicAPI
class ObjectRef(Generic[T]):
    pass
