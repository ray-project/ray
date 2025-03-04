from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, Optional, TypeVar
from ray.util.annotations import DeveloperAPI


if TYPE_CHECKING:
    import cupy as cp

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

    def __init__(
        self, buf: Any, fut_id: int, stream: Optional["cp.cuda.Stream"] = None
    ):
        """
        Initialize a GPU future on the given stream.

        Args:
            buf: The buffer to return when the future is resolved.
            fut_id: The future ID to cache the future.
            stream: The CUDA stream to record the event on, this event is waited
                on when the future is resolved. If None, the current stream is used.
        """
        import cupy as cp
        from ray.experimental.channel.common import ChannelContext

        if stream is None:
            stream = cp.cuda.get_current_stream()

        self._buf = buf
        self._event = cp.cuda.Event()
        self._event.record(stream)
        self._fut_id = fut_id
        self._waited: bool = False

        # Cache the GPU future such that its CUDA event is properly destroyed.
        ctx = ChannelContext.get_current().serialization_context
        ctx.add_gpu_future(fut_id, self)

    def wait(self) -> Any:
        """
        Wait for the future on the current CUDA stream and return the result from
        the GPU operation. This operation does not block CPU.
        """
        import cupy as cp
        from ray.experimental.channel.common import ChannelContext

        current_stream = cp.cuda.get_current_stream()
        if not self._waited:
            self._waited = True
            current_stream.wait_event(self._event)
            # Destroy the CUDA event after it is waited on.
            ctx = ChannelContext.get_current().serialization_context
            ctx.remove_gpu_future(self._fut_id)

        return self._buf

    def destroy_event(self) -> None:
        """
        Destroy the CUDA event associated with this future.
        """
        import cupy as cp

        if self._event is None:
            return

        cp.cuda.runtime.eventDestroy(self._event.ptr)
        self._event.ptr = 0
        self._event = None
