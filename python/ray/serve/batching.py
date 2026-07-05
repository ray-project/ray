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
    def __init__(self, max_batch_size: int, batch_wait_timeout_s: float, max_concurrent_batches: int = 10):
        self.max_batch_size = max_batch_size
        self.batch_wait_timeout_s = batch_wait_timeout_s
        self.max_concurrent_batches = max_concurrent_batches
        self.semaphore = asyncio.Semaphore(max_concurrent_batches)
        self._holding_tasks = []

    def set_max_concurrent_batches(self, new_max_concurrent_batches: int) -> None:
        """Safely updates queue's max_concurrent_batches and modifies semaphore limits."""
        old_max = self.max_concurrent_batches
        self.max_concurrent_batches = new_max_concurrent_batches
        
        delta = new_max_concurrent_batches - old_max
        if delta > 0:
            while delta > 0 and self._holding_tasks:
                task = self._holding_tasks.pop()
                task.cancel()
                delta -= 1
        elif delta < 0:
            async def hold_permit(sem):
                try:
                    await sem.acquire()
                    try:
                        await asyncio.Event().wait()
                    finally:
                        sem.release()
                except asyncio.CancelledError:
                    pass

            for _ in range(abs(delta)):
                task = asyncio.create_task(hold_permit(self.semaphore))
                self._holding_tasks.append(task)
