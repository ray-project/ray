import asyncio
import inspect
import logging
import time
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

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
    ):
        self.max_batch_size = max_batch_size
        self.batch_wait_timeout_s = batch_wait_timeout_s
        self.max_concurrent_batches = max_concurrent_batches
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

def batch(max_batch_size: Optional[int] = None, batch_wait_timeout_s: Optional[float] = None):
    def decorator(func: Callable):
        return func
    return decorator

def wrap_batch_queue(lazy_batch_queue_wrapper):
    def wrapper(*args, **kwargs):
        pass
    wrapper.set_max_batch_size = lazy_batch_queue_wrapper.set_max_batch_size
    wrapper.set_batch_wait_timeout_s = lazy_batch_queue_wrapper.set_batch_wait_timeout_s
    wrapper.set_max_concurrent_batches = lazy_batch_queue_wrapper.set_max_concurrent_batches
    wrapper.get_max_concurrent_batches = lazy_batch_queue_wrapper.get_max_concurrent_batches
    return wrapper
