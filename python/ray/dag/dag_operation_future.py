from abc import ABC, abstractmethod
from typing import Any, Callable, Generic, List, Optional, TypeVar, Union

T = TypeVar("T")


class DAGOperationFuture(ABC, Generic[T]):
    @abstractmethod
    def wait(self, waiter: Optional[Any] = None):
        raise NotImplementedError


class ReadyFuture(DAGOperationFuture):
    def __init__(self, result):
        self._result = result

    def wait(self, waiter: Optional[Any] = None):
        return self._result


class ListFuture(DAGOperationFuture):
    def __init__(self, futures: List[DAGOperationFuture]):
        self._futures = futures

    def wait(self, waiter: Optional[Any] = None):
        results = []
        for future in self._futures:
            results.append(future.wait(waiter))
        return results


class WrappedFuture(DAGOperationFuture):
    def __init__(
        self, inner: DAGOperationFuture, method: Optional[Callable[[Any], Any]] = None
    ):
        assert isinstance(inner, DAGOperationFuture), (
            "Only DAGOperationFuture can be wrapped inside a WrappedFuture, "
            f"however, a {type(inner)} type was provided."
        )
        self._inner = inner
        self._method = method

    def wait(self, waiter: Optional[Any] = None):
        data = self._inner.wait(waiter)
        return self._method(data) if self._method is not None else data


class _GPUFuture(DAGOperationFuture["torch.Tensor"]):
    import torch
    import cupy as cp

    def __init__(self, buf: "torch.Tensor"):
        """
        Initialize a GPU future.
        Args:
            buf: The buffer to return when the future is resolved.
            event: The CUDA event to wait on before returning the buffer.
                If None, the buffer is immediately returned.
        """
        import cupy as cp

        self._buf = buf
        self._event = cp.cuda.Event()

    def record_event(self, stream: "cp.cuda.Stream") -> None:
        """
        Record the event on the provided stream.
        """
        self._event.record(stream)

    def wait(
        self,
        waiter: Optional[Union["cp.cuda.Stream", "cp.cuda.ExternalStream"]] = None,
    ) -> "torch.Tensor":
        """
        Wait for the future to resolve and return the buffer.

        Args:
            waiter: The stream to wait on before returning the buffer.
                If None, the current stream is used.
        """
        import cupy as cp

        if self._event is not None:
            if waiter is None:
                waiter = cp.cuda.get_current_stream()
            waiter.wait_event(self._event)
        return self._buf
