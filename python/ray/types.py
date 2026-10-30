from typing import Generic, TypeVar

from ray.util.annotations import PublicAPI

T = TypeVar("T")


# TODO(ekl) this is a dummy generic ref type for documentation purposes only.
# We should try to make the Cython ray.ObjectRef properly generic.
# NOTE(sang): Looks like using Generic in Cython is not currently possible.
# We should update Cython > 3.0 for this.
@PublicAPI
class ObjectRef(Generic[T]):
    """A generic placeholder for ``ray.ObjectRef`` used in type hints.

    ``ObjectRef[T]`` annotates a reference to an object of type ``T``. This
    class exists for documentation only. At runtime, object refs are instances
    of the Cython class ``ray.ObjectRef``, which isn't generic.
    """

    pass
