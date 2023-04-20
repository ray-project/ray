from collections import deque
from threading import Semaphore

import ray
import torch


def get_device_name():
    gpu_ids = ray.get_gpu_ids()
    if len(gpu_ids) == 0:
        return "cpu"
    assert len(gpu_ids) == 1
    return f"cuda:{gpu_ids[0]}"


class BlockBatchLoader(object):
    def __init__(self, capacity: int = 100) -> None:
        self._capacity = capacity
        self._push_semaphore = Semaphore(self._capacity)
        self._pop_semaphore = Semaphore(0)
        self._queue = deque()
        self._device_name = get_device_name()
        self._counter = 0

    def push_batch(self, batch: torch.Tensor, labels: torch.Tensor = None) -> int:
        self._push_semaphore.acquire()
        counter = self._counter
        self._counter += 1
        batch = batch.to(self._device_name) if batch is not None else None
        labels = labels.to(self._device_name) if labels is not None else None
        self._queue.append((counter, batch, labels))
        self._pop_semaphore.release()
        return counter

    def pop_batch(self):
        self._pop_semaphore.acquire()
        (counter, batch, labels) = self._queue.popleft()
        self._push_semaphore.release()
        return counter, batch, labels

    def __iter__(self):
        while True:
            _, batch, labels = self.pop_batch()
            yield batch, labels
