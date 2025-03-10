from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar
from ray.util.annotations import DeveloperAPI


T = TypeVar("T")


@DeveloperAPI
class DAGOperationFuture(ABC, Generic[T]):
    """
    A future representing the result of a DAG operation.

    This is an abstraction that is internal to each actor,
    and is not exposed to the DAG caller.
    """

    @abstractmethod
    def wait(self):
        """
        Wait for the future and return the result of the operation.
        """
        raise NotImplementedError


@DeveloperAPI
class ResolvedFuture(DAGOperationFuture):
    """
    A future that is already resolved. Calling `wait()` on this will
    immediately return the result without blocking.
    """

    def __init__(self, result):
        """
        Initialize a resolved future.

        Args:
            result: The result of the future.
        """
        self._result = result

    def wait(self):
        """
        Wait and immediately return the result. This operation will not block.
        """
        return self._result


@DeveloperAPI
class GPUFuture(DAGOperationFuture[Any]):
    """
    A future for a GPU event on a CUDA stream.

    This future wraps a buffer, and records an event on the given stream
    when it is created. When the future is waited on, it makes the current
    CUDA stream wait on the event, then returns the buffer.

    The buffer must be a GPU tensor produced by an earlier operation launched
    on the given stream, or it could be CPU data. Then the future guarantees
    that when the wait() returns, the buffer is ready on the current stream.

    The `wait()` does not block CPU.
    """

    def __init__(self, buf: Any, stream: Any = None):
        """
        Initialize a GPU future on the given stream.

        Args:
            buf: The buffer to return when the future is resolved.
            stream: The torch stream to record the event on, this event is waited
                on when the future is resolved. If None, the current stream is used.
        """
        from ray.air._internal.device_manager import (
            get_torch_device_manager_by_context,
        )

        if stream is None:
            stream = get_torch_device_manager_by_context().get_current_stream()

        self._buf = buf
        self._event = get_torch_device_manager_by_context().create_event()
        self._event.record(stream)

    def wait(self) -> Any:
        """
        Wait for the future on the current CUDA stream and return the result from
        the GPU operation. This operation does not block CPU.
        """
        from ray.air._internal.device_manager import (
            get_torch_device_manager_by_context,
        )

        stream = get_torch_device_manager_by_context().get_current_stream()

        stream.wait_event(self._event)
        return self._buf
