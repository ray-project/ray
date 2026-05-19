import math
from typing import Any, Dict, Optional

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
    def p50(self) -> Optional[float]:
        return self._quantile(0.5)

    @property
    def p90(self) -> Optional[float]:
        return self._quantile(0.9)

    @property
    def p95(self) -> Optional[float]:
        return self._quantile(0.95)

    @property
    def p99(self) -> Optional[float]:
        return self._quantile(0.99)

    def as_dict(self) -> Dict[str, Any]:
        return {
            "num_samples": self.num_samples,
            "mean": self.mean,
            "variance": self.variance,
            "min": self.min,
            "max": self.max,
            "p50": self.p50,
            "p90": self.p90,
            "p95": self.p95,
            "p99": self.p99,
        }
