# source: object_ref.pxi
import asyncio
import concurrent.futures
from typing import Any, Awaitable, Callable, Generator, TypeVar, Union

from ray.includes.unique_ids import BaseID, JobID, TaskID

_T = TypeVar("_T")
def _set_future_helper(
    result: _T,
    *,
    py_future: Union[asyncio.Future[_T], concurrent.futures.Future[_T]],
) -> None: ...


_OR = TypeVar("_OR", bound=ObjectRef)
class ObjectRef(BaseID, Awaitable[_T]):


    def __init__(
            self, id: bytes, owner_addr: str = "", call_site_data: str = "",
            skip_adding_local_ref: bool = False, tensor_transport_val = 0) -> None: ...

    def __dealloc__(self) -> None: ...


    def task_id(self) -> TaskID: ...

    def job_id(self) -> JobID: ...

    def owner_address(self) -> str: ...

    def call_site(self) -> str: ...

    @classmethod
    def size(cls) -> int: ...

    def _set_id(self, id: bytes) -> None: ...

    @classmethod
    def nil(cls: type[_OR]) -> _OR: ...

    @classmethod
    def from_random(cls: type[_OR]) -> _OR: ...

    def future(self) -> concurrent.futures.Future[_T]:
        """Wrap ObjectRef with a concurrent.futures.Future

        Note that the future cancellation will not cancel the correspoding
        task when the ObjectRef representing return object of a task.
        Additionally, future.running() will always be ``False`` even if the
        underlying task is running.
        """
        ...

    def __await__(self) -> Generator[Any, None, _T]: ...

    def as_future(self, _internal=False) -> asyncio.Future[_T]:
        """Wrap ObjectRef with an asyncio.Future.

        Note that the future cancellation will not cancel the correspoding
        task when the ObjectRef representing return object of a task.
        """
        ...

    def _on_completed(self, py_callback: Callable[[_T], None]):
        """Register a callback that will be called after Object is ready.
        If the ObjectRef is already ready, the callback will be called soon.
        The callback should take the result as the only argument. The result
        can be an exception object in case of task error.
        """
        ...

    def tensor_transport(self) -> int: ...
