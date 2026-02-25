from typing import Any, List

import ray
from ray import cloudpickle

_ray_initialized = False


class SizeEstimator:
    """Efficiently estimates the Ray serialized size of a stream of items.

    For efficiency, this only samples a fraction of the added items for real
    Ray-serialization.
    """

    def __init__(self):
        self._running_mean = RunningMean()
        self._count = 0

    def add(self, item: Any) -> None:
        self._count += 1
        if self._count <= 10:
            self._running_mean.add(self._real_size(item), weight=1)
        elif self._count <= 100:
            if self._count % 10 == 0:
                self._running_mean.add(self._real_size(item), weight=10)
        elif self._count % 100 == 0:
            self._running_mean.add(self._real_size(item), weight=100)

    def add_block(self, block: List[Any]) -> None:
        if self._count < 10:
            for i in range(min(10 - self._count, len(block))):
                self._running_mean.add(self._real_size(block[i]), weight=1)
        if self._count < 100:
            for i in range(
                10 - (self._count % 10), min(100 - self._count, len(block)), 10
            ):
                self._running_mean.add(self._real_size(block[i]), weight=10)
        if (len(block) + (self._count % 100)) // 100 > 1:
            for i in range(100 - (self._count % 100), len(block), 100):
                self._running_mean.add(self._real_size(block[i]), weight=100)
        self._count += len(block)

    def size_bytes(self) -> int:
        return int(self._running_mean.mean * self._count)

    def _real_size(self, item: Any) -> int:
        is_client = ray.util.client.ray.is_connected()
        # In client mode, fallback to using Ray cloudpickle instead of the
        # real serializer.
        if is_client:
            return len(cloudpickle.dumps(item))

        # We're using an internal Ray API, and have to ensure it's
        # initialized # by calling a public API.
        global _ray_initialized
        if not _ray_initialized:
            _ray_initialized = True
            ray.put(None)
        return (
            ray._private.worker.global_worker.get_serialization_context()
            .serialize(item)
            .total_bytes
        )


# Adapted from the RLlib MeanStdFilter.
class RunningMean:
    def __init__(self):
        self._weight = 0
        self._mean = 0

    def add(self, x: int, weight: int = 1) -> None:
        if weight == 0:
            return
        n1 = self._weight
        n2 = weight
        n = n1 + n2
        M = (n1 * self._mean + n2 * x) / n
        self._weight = n
        self._mean = M

    @property
    def n(self) -> int:
        return self._weight

    @property
    def mean(self) -> float:
        return self._mean

    def __repr__(self):
        return "(n={}, mean={})".format(self.n, self.mean)
