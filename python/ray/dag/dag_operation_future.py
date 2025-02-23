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

    The `wait()` optionally blocks CPU.
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

    def wait(self, blocking: bool = False) -> Any:
        """
        Wait on the CUDA event associated with this future. Future operations on the
        current CUDA stream are queued after the operation captured by this future.

        Args:
            blocking: Whether this operation blocks CPU. It is blocking when
                the future is sent across actors (e.g., by SharedMemoryChannel),
                in which case the future should be resolved before channel write.

        Return:
            The result buffer. It is only ready to read if blocking.
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

        if blocking:
            current_stream.synchronize()
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
