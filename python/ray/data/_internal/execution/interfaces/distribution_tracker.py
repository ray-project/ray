import copy
import math
from typing import Dict, Optional, Union

try:
    from datasketches import kll_doubles_sketch

    _DATASKETCHES_AVAILABLE = True
except ImportError:
    _DATASKETCHES_AVAILABLE = False


class DistributionTracker:
    """Tracks the running mean, variance, min, max, and approximate percentiles of a
    stream of values using Welford's algorithm for moments and a KLL sketch for
    quantiles.

    More on Welford's algorithm:
    https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
    """

    def __init__(self):
        self._count = 0
        self._mean = 0.0
        self._m2 = 0.0
        self._min = float("inf")
        self._max = float("-inf")
        self._sketch = kll_doubles_sketch(200) if _DATASKETCHES_AVAILABLE else None

    def add_sample(self, value: float) -> None:
        self._count += 1

        delta = value - self._mean
        self._mean += delta / self._count
        delta2 = value - self._mean
        self._m2 += delta * delta2

        if value < self._min:
            self._min = value
        if value > self._max:
            self._max = value

        if self._sketch is not None:
            self._sketch.update(value)

    def merge(self, other: "DistributionTracker") -> None:
        """Merge another tracker into this one (associative, commutative).

        Uses Chan's parallel variant of Welford's algorithm for moments.
        See: https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford:~:text=Parallel%20algorithm%5Bedit%5D
        """
        if other is self:
            # Merging an accumulator into itself would double its samples
            # (count, m2, and the sketch), so treat it as a no-op.
            return
        if other._count == 0:
            return
        if self._count == 0:
            self._count = other._count
            self._mean = other._mean
            self._m2 = other._m2
            self._min = other._min
            self._max = other._max
        else:
            delta = other._mean - self._mean
            total = self._count + other._count
            self._m2 += other._m2 + (delta**2) * self._count * other._count / total
            self._mean = (self._count * self._mean + other._count * other._mean) / total
            self._count = total
            self._min = min(self._min, other._min)
            self._max = max(self._max, other._max)
        if self._sketch is not None and other._sketch is not None:
            try:
                self._sketch.merge(other._sketch)
            except Exception:
                self._sketch = None
        elif self._sketch is None and other._sketch is not None:
            # self tracked data but has no sketch; adopt a copy of other's
            # sketch.
            self._sketch = copy.copy(other._sketch)

    @property
    def num_samples(self) -> int:
        return self._count

    @property
    def mean(self) -> float:
        return self._mean

    @property
    def variance(self) -> float:
        if self._count < 2:
            return 0.0
        return self._m2 / (self._count - 1)

    @property
    def stddev(self) -> float:
        return math.sqrt(self.variance)

    @property
    def min(self) -> Optional[float]:
        if self._count == 0:
            return None
        return self._min

    @property
    def max(self) -> Optional[float]:
        if self._count == 0:
            return None
        return self._max

    def _quantile(self, q: float) -> Optional[float]:
        if self._sketch is None or self._count == 0:
            return None
        return self._sketch.get_quantiles([q])[0]

    @property
    def p25(self) -> Optional[float]:
        return self._quantile(0.25)

    @property
    def p50(self) -> Optional[float]:
        return self._quantile(0.5)

    @property
    def p75(self) -> Optional[float]:
        return self._quantile(0.75)

    @property
    def p90(self) -> Optional[float]:
        return self._quantile(0.9)

    @property
    def p95(self) -> Optional[float]:
        return self._quantile(0.95)

    @property
    def p99(self) -> Optional[float]:
        return self._quantile(0.99)

    def as_dict(self) -> Dict[str, Optional[Union[int, float]]]:
        return {
            "num_samples": self.num_samples,
            "mean": self.mean,
            "variance": self.variance,
            "min": self.min,
            "max": self.max,
            "p25": self.p25,
            "p50": self.p50,
            "p75": self.p75,
            "p90": self.p90,
            "p95": self.p95,
            "p99": self.p99,
        }

    # ``kll_doubles_sketch`` is a C++-backed object that does not
    # pickle natively. DistributionTracker rides on DatasetStats
    # (via Timer), which is cloudpickled when Datasets cross actor /
    # process boundaries — without these hooks any such transfer
    # raises ``TypeError: cannot pickle 'kll_doubles_sketch' object``.
    # The sketch exposes its own byte serialization, so we round-trip
    # through that.
    def __getstate__(self):
        state = self.__dict__.copy()
        if self._sketch is not None:
            state["_sketch"] = self._sketch.serialize()
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        # If the source had datasketches but this side doesn't, drop
        # the sketch (percentiles will return None — same fallback as a
        # default construction without datasketches installed).
        if self._sketch is not None and not _DATASKETCHES_AVAILABLE:
            self._sketch = None
        elif self._sketch is not None and not isinstance(
            self._sketch, kll_doubles_sketch
        ):
            self._sketch = kll_doubles_sketch.deserialize(self._sketch)
