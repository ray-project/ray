from typing import Generic, TypeVar, Awaitable

from ray.util.annotations import PublicAPI

T = TypeVar("T")


# TODO(ekl) this is a dummy generic ref type for documentation purposes only.
# We should try to make the Cython ray.ObjectRef properly generic.
@PublicAPI
class ObjectRef(Generic[T]):
    pass


@PublicAPI(stability="beta")
class StreamingObjectRefGeneratorType(Generic[T]):
    def __next__(self) -> ObjectRef[T]:
        pass

    # TODO(sang): Support typing for anext

    def completed(self) -> ObjectRef:
        pass

    def next_ready(self) -> bool:
        pass

    def is_finished(self) -> bool:
        pass
