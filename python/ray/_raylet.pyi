from typing import (
    AsyncGenerator,
    Generator,
    Generic,
    NoReturn,
    Optional,
    TypeVar,
    Union,
)

from ray._private.worker import Worker
from ray.includes.object_ref import ObjectRef, _set_future_helper
from ray.includes.unique_ids import (
    ActorClassID,
    ActorID,
    BaseID,
    ClusterID,
    FunctionID,
    JobID,
    NodeID,
    ObjectID,
    PlacementGroupID,
    TaskID,
    UniqueID,
    WorkerID,
    check_id,
)

__all__ = [
    # ray.includes.unique_ids
    "ActorClassID",
    "ActorID",
    "BaseID",
    "ClusterID",
    "FunctionID",
    "JobID",
    "NodeID",
    "ObjectID",
    "PlacementGroupID",
    "TaskID",
    "UniqueID",
    "WorkerID",
    "check_id",

    # ray.includes.object_ref
    "_set_future_helper",
    "ObjectRef",

    # raylet classes/functions
    "ObjectRefGenerator",
]


_R = TypeVar("_R") # for ObjectRefs

_ORG = TypeVar("_ORG",bound=ObjectRefGenerator)
class ObjectRefGenerator(Generic[_R],Generator[ObjectRef[_R],None,None],AsyncGenerator[ObjectRef[_R],None]):
    """A generator to obtain object references
    from a task in a streaming manner.

    The class is compatible with generator and
    async generator interface.

    The class is not thread-safe.

    Do not initialize the class and create an instance directly.
    The instance should be created by `.remote`.

    >>> gen = generator_task.remote()
    >>> next(gen)
    >>> await gen.__anext__()
    """

    _generator_ref: ObjectRef[None]
    _generator_task_exception: Optional[Exception]
    worker: Worker



    def __init__(self, generator_ref: ObjectRef[None], worker: "Worker") -> None: ...

    # Public APIs

    def __iter__(self:_ORG) -> _ORG: ...

    def __next__(self) -> ObjectRef[_R]:
        """Waits until a next ref is available and returns the object ref.

        Raises StopIteration if there's no more objects
        to generate.

        The object ref will contain an exception if the task fails.
        When the generator task returns N objects, it can return
        up to N + 1 objects (if there's a system failure, the
        last object will contain a system level exception).
        """
        ...

    def send(self, value) -> NoReturn: ...

    def throw(self, value, val=None, tb=None) -> NoReturn: ...

    def close(self) -> NoReturn: ...

    def __aiter__(self:_ORG) -> _ORG: ...

    async def __anext__(self) -> ObjectRef[_R]: ...

    async def asend(self, value) -> NoReturn: ...

    async def athrow(self, value, val=None, tb=None) -> NoReturn: ...

    async def aclose(self) -> NoReturn: ...

    def completed(self) -> ObjectRef[None]:
        """Returns an object ref that is ready when
        a generator task completes.

        If the task is failed unexpectedly (e.g., worker failure),
        the `ray.get(gen.completed())` raises an exception.

        The function returns immediately.

        >>> ray.get(gen.completed())
        """
        ...

    def next_ready(self) -> bool:
        """If True, it means the output of next(gen) is ready and
        ray.get(next(gen)) returns immediately. False otherwise.

        It returns False when next(gen) raises a StopIteration
        (this condition should be checked using is_finished).

        The function returns immediately.
        """
        ...


    def is_finished(self) -> bool:
        """If True, it means the generator is finished
        and all output is taken. False otherwise.

        When True, if next(gen) is called, it will raise StopIteration
        or StopAsyncIteration

        The function returns immediately.
        """
        ...

    # Private APIs

    def _get_next_ref(self) -> ObjectRef[_R]:
        """Return the next reference from a generator.

        Note that the ObjectID generated from a generator
        is always deterministic.
        """
        ...


    def _next_sync(
        self,
        timeout_s: Optional[Union[int, float]] = None
    ) -> ObjectRef[_R]: # ObjectRef could be nil
        """Waits for timeout_s and returns the object ref if available.

        If an object is not available within the given timeout, it
        returns a nil object reference.

        If -1 timeout is provided, it means it waits infinitely.

        Waiting is implemented as busy waiting.

        Raises StopIteration if there's no more objects
        to generate.

        The object ref will contain an exception if the task fails.
        When the generator task returns N objects, it can return
        up to N + 1 objects (if there's a system failure, the
        last object will contain a system level exception).

        Args:
            timeout_s: If the next object is not ready within
                this timeout, it returns the nil object ref.
        """
        ...

    async def _suppress_exceptions(self, ref: ObjectRef) -> None: ...

    async def _next_async(
            self,
            timeout_s: Optional[Union[int, float]] = None
    ) -> ObjectRef[_R]: # ObjectRef could be nil
        """Same API as _next_sync, but it is for async context."""
        ...

    def __del__(self) -> None: ...
        # if hasattr(self.worker, "core_worker"):
        #     # The stream is created when a task is first submitted.
        #     # NOTE: This can be called multiple times
        #     # because python doesn't guarantee __del__ is called
        #     # only once.
        #     self.worker.core_worker.async_delete_object_ref_stream(self._generator_ref)

    def __getstate__(self) -> NoReturn: ...
        # raise TypeError(
        #     "You cannot return or pass a generator to other task. "
        #     "Serializing a ObjectRefGenerator is not allowed.")

# update module. _raylet.pyx also updates the type name of ObjectRef using cython, but can't do that here
ObjectRefGenerator.__module__ = "ray"

# For backward compatibility
StreamingObjectRefGenerator = ObjectRefGenerator
