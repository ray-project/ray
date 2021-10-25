from typing import Any

import ray


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

    def size_bytes(self) -> int:
        return int(self._running_mean.mean * self._count)

    def _real_size(self, item: Any) -> int:
        return ray.worker.global_worker.get_serialization_context().serialize(
            item).total_bytes


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
