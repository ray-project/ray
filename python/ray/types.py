from typing import Generic, TypeVar

from ray.util.annotations import PublicAPI

T = TypeVar("T")


# TODO(ekl) this is a dummy generic ref type for documentation purposes only.
# We should try to make the Cython ray.ObjectRef properly generic.
# NOTE(sang): Looks like using Generic in Cython is not currently possible.
# We should update Cython > 3.0 for this.
@PublicAPI
class ObjectRef(Generic[T]):
    pass


@PublicAPI(stability="beta")
class StreamingObjectRefGenerator(Generic[T]):
    def __next__(self) -> ObjectRef[T]:
        pass

    # TODO(sang): Support typing for anext

    def completed(self) -> ObjectRef:
        pass

    def next_ready(self) -> bool:
        pass

    def is_finished(self) -> bool:
        pass
