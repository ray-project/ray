from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, Optional, TypeVar
from ray.util.annotations import DeveloperAPI


if TYPE_CHECKING:
    import torch
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
class GPUFuture(DAGOperationFuture["torch.Tensor"]):
    """
    A future that represents a GPU operation.
    """

    def __init__(self, buf: "torch.Tensor", stream: Optional["cp.cuda.Stream"] = None):
        """
        Initialize a GPU future.

        Args:
            buf: The buffer to return when the future is resolved.
            stream: The CUDA stream to record the event on. If None, the current
                stream is used.
        """
        import cupy as cp

        if stream is None:
            stream = cp.cuda.get_current_stream()

        self._buf = buf
        self._event = cp.cuda.Event()
        self._event.record(stream)

    def wait(self) -> "torch.Tensor":
        """
        Wait for the future and return the result from the GPU operation.
        """
        import cupy as cp

        if self._event is not None:
            current_stream = cp.cuda.get_current_stream()
            current_stream.wait_event(self._event)
        return self._buf
