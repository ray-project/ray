from abc import ABC, abstractmethod
from typing import Generic, TypeVar


T = TypeVar("T")


class DAGOperationFuture(ABC, Generic[T]):
    @abstractmethod
    def wait(self):
        raise NotImplementedError


class ResolvedFuture(DAGOperationFuture):
    """
    A future that is already resolved. Calling `wait()` on this will
    immediately return the result without blocking.

    Args:
        result: The result of the future.
    """

    def __init__(self, result):
        self._result = result

    def wait(self):
        return self._result


class _GPUFuture(DAGOperationFuture["torch.Tensor"]):
    import torch
    import cupy as cp

    def __init__(self, buf: "torch.Tensor", stream: "cp.cuda.Stream"):
        """
        Initialize a GPU future.
        Args:
            buf: The buffer to return when the future is resolved.
            stream: The CUDA stream to record the event on.
        """
        import cupy as cp

        self._buf = buf
        self._event = cp.cuda.Event()
        self._event.record(stream)

    def wait(self) -> "torch.Tensor":
        """
        Wait for the future to resolve and return the buffer.
        """
        from python.ray.experimental.channel import ChannelContext

        if self._event is not None:
            current_stream = ChannelContext.get_current().current_stream
            current_stream.wait_event(self._event)
        return self._buf
