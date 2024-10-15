from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Generic, TypeVar
from ray.util.annotations import DeveloperAPI


if TYPE_CHECKING:
    import torch
    import cupy as cp

T = TypeVar("T")


@DeveloperAPI
class DAGOperationFuture(ABC, Generic[T]):
    """
    A future representing the result of a DAG operation.
    """

    @abstractmethod
    def wait(self):
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
        return self._result


@DeveloperAPI
class _GPUFuture(DAGOperationFuture["torch.Tensor"]):
    """
    A future that represents a GPU operation.
    """

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
        Wait for the future and return the result from the GPU operation.
        """
        import cupy as cp

        if self._event is not None:
            current_stream = cp.cuda.get_current_stream()
            current_stream.wait_event(self._event)
        return self._buf
