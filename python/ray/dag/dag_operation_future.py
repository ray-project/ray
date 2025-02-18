from typing import TYPE_CHECKING, Any, Optional
from ray.util.annotations import DeveloperAPI


if TYPE_CHECKING:
    import cupy as cp


@DeveloperAPI
class GPUFuture:
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

    def __init__(self, buf: Any, stream: Optional["cp.cuda.Stream"] = None):
        """
        Initialize a GPU future on the given stream.

        Args:
            buf: The buffer to return when the future is resolved.
            stream: The CUDA stream to record the event on, this event is waited
                on when the future is resolved. If None, the current stream is used.
        """
        import cupy as cp

        if stream is None:
            stream = cp.cuda.get_current_stream()

        self._buf = buf
        self._event = cp.cuda.Event()
        self._event.record(stream)
        self._fut_id: Optional[int] = None

    def wait(self, blocking: bool = False) -> Any:
        """
        Wait for the future on the current CUDA stream. Future operations on the
        current CUDA stream will not begin until the GPU operation captured
        by this future finishes.

        Args:
            blocking: Whether this operation blocks CPU.

        Return:
            Result from the GPU operation. The returned result is immediately
            ready to use iff blocking.
        """
        import cupy as cp

        current_stream = cp.cuda.get_current_stream()
        current_stream.wait_event(self._event)
        if blocking:
            current_stream.synchronize()

        from ray.experimental.channel.common import ChannelContext

        ctx = ChannelContext.get_current().serialization_context
        # This GPU future is no longer needed. Destroy the CUDA event it contains.
        ctx.pop_gpu_future(self._fut_id)
        return self._buf

    def cache(self, fut_id: int) -> None:
        """
        Cache the future inside the actor's channel context so that the CUDA
        event it contains can be destroyed controllably.

        Args:
            fut_id: The id of this future, which is the corresponding task's index.
        """
        from ray.experimental.channel.common import ChannelContext

        ctx = ChannelContext.get_current().serialization_context
        ctx.set_gpu_future(fut_id, self)
        self._fut_id = fut_id

    def destroy_event(self) -> None:
        """
        Destroys the CUDA event contained in this future.
        """
        if self._event is None:
            return

        import cupy as cp

        cp.cuda.runtime.eventDestroy(self._event.ptr)
        self._event.ptr = 0
        self._event = None
