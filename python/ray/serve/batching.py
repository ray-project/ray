import asyncio
import inspect
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union, TypeVar, Generic

from ray.util.annotations import DeveloperAPI
import ray
from ray import serve

logger = logging.getLogger("ray.serve")

@DeveloperAPI
class _BatchQueue:
    def __init__(
        self,
        max_batch_size: int,
        batch_wait_timeout_s: float,
        max_concurrent_batches: int = 10,
        handle_batch_func: Optional[Callable] = None,
        batch_size_fn: Optional[Callable[[List[Any]], int]] = None,
    ):
        self.max_batch_size = max_batch_size
        self.batch_wait_timeout_s = batch_wait_timeout_s
        self.max_concurrent_batches = max_concurrent_batches
        self.handle_batch_func = handle_batch_func
        self.batch_size_fn = batch_size_fn
        self.semaphore = asyncio.Semaphore(max_concurrent_batches)
        self._holding_tasks = []

    def _warn_if_max_batch_size_exceeds_max_ongoing_requests(self):
        pass

    def set_max_batch_size(self, new_max_batch_size: int) -> None:
        self.max_batch_size = new_max_batch_size

    def set_batch_wait_timeout_s(self, new_batch_wait_timeout_s: float) -> None:
        self.batch_wait_timeout_s = new_batch_wait_timeout_s

    def set_max_concurrent_batches(self, new_max_concurrent_batches: int) -> None:
        self.max_concurrent_batches = new_max_concurrent_batches
        self.semaphore = asyncio.Semaphore(new_max_concurrent_batches)
        self._holding_tasks = []
        self._warn_if_max_batch_size_exceeds_max_ongoing_requests()

    def get_max_concurrent_batches(self) -> int:
        return self.max_concurrent_batches

@DeveloperAPI
def batch(
    _func: Optional[Callable[..., Any]] = None,
    *,
    max_batch_size: int = 10,
    batch_wait_timeout_s: float = 0.01,
    max_concurrent_batches: int = 1,
    batch_size_fn: Optional[Callable[[List[Any]], int]] = None,
) -> Any:
    if callable(_func):
        queue = _BatchQueue(
            max_batch_size=10,
            batch_wait_timeout_s=0.01,
            max_concurrent_batches=1,
            handle_batch_func=_func,
        )
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return _func(*args, **kwargs)
        wrapper.set_max_batch_size = queue.set_max_batch_size
        wrapper.set_batch_wait_timeout_s = queue.set_batch_wait_timeout_s
        wrapper.set_max_concurrent_batches = queue.set_max_concurrent_batches
        wrapper.get_max_concurrent_batches = queue.get_max_concurrent_batches
        return wrapper

    def decorator(func: Callable[..., Any]) -> Any:
        queue = _BatchQueue(
            max_batch_size=max_batch_size,
            batch_wait_timeout_s=batch_wait_timeout_s,
            max_concurrent_batches=max_concurrent_batches,
            handle_batch_func=func,
            batch_size_fn=batch_size_fn,
        )
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)
        wrapper.set_max_batch_size = queue.set_max_batch_size
        wrapper.set_batch_wait_timeout_s = queue.set_batch_wait_timeout_s
        wrapper.set_max_concurrent_batches = queue.set_max_concurrent_batches
        wrapper.get_max_concurrent_batches = queue.get_max_concurrent_batches
        return wrapper

    return decorator
