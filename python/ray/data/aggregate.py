import math
from typing import Callable, Optional, List, TYPE_CHECKING

from ray.util.annotations import PublicAPI
from ray.data.block import T, U, KeyType, AggType, KeyFn, _validate_key_fn

if TYPE_CHECKING:
    from ray.data import Dataset


@PublicAPI(stability="beta")
class AggregateFn(object):
    def __init__(self,
                 init: Callable[[KeyType], AggType],
                 accumulate: Callable[[AggType, T], AggType],
                 merge: Callable[[AggType, AggType], AggType],
                 finalize: Callable[[AggType], U] = lambda a: a,
                 name: Optional[str] = None):
        """Defines an aggregate function in the accumulator style.

        Aggregates a collection of inputs of type T into
        a single output value of type U.
        See https://www.sigops.org/s/conferences/sosp/2009/papers/yu-sosp09.pdf
        for more details about accumulator-based aggregation.

        Args:
            init: This is called once for each group
                to return the empty accumulator.
                For example, an empty accumulator for a sum would be 0.
            accumulate: This is called once per row of the same group.
                This combines the accumulator and the row,
                returns the updated accumulator.
            merge: This may be called multiple times, each time to merge
                two accumulators into one.
            finalize: This is called once to compute the final aggregation
                result from the fully merged accumulator.
            name: The name of the aggregation. This will be used as the output
                column name in the case of Arrow dataset.
        """
        self.init = init
        self.accumulate = accumulate
        self.merge = merge
        self.finalize = finalize
        self.name = name

    def _validate(self, ds: "Dataset") -> None:
        """Raise an error if this cannot be applied to the given dataset."""
        pass


class _AggregateOnKeyBase(AggregateFn):
    def _set_key_fn(self, on: KeyFn):
        self._key_fn = on

    def _validate(self, ds: "Dataset") -> None:
        _validate_key_fn(ds, self._key_fn)


@PublicAPI(stability="beta")
class Count(AggregateFn):
    """Defines count aggregation."""

    def __init__(self):
        super().__init__(
            init=lambda k: 0,
            accumulate=lambda a, r: a + 1,
            merge=lambda a1, a2: a1 + a2,
            name="count()")


@PublicAPI(stability="beta")
class Sum(_AggregateOnKeyBase):
    """Defines sum aggregation."""

    def __init__(self, on: Optional[KeyFn] = None):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)
        super().__init__(
            init=lambda k: 0,
            accumulate=lambda a, r: a + on_fn(r),
            merge=lambda a1, a2: a1 + a2,
            name=(f"sum({str(on)})"))


@PublicAPI(stability="beta")
class Min(_AggregateOnKeyBase):
    """Defines min aggregation."""

    def __init__(self, on: Optional[KeyFn] = None):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)
        super().__init__(
            init=lambda k: None,
            accumulate=(
                lambda a, r: (on_fn(r) if a is None else min(a, on_fn(r)))),
            merge=lambda a1, a2: min(a1, a2),
            name=(f"min({str(on)})"))


@PublicAPI(stability="beta")
class Max(_AggregateOnKeyBase):
    """Defines max aggregation."""

    def __init__(self, on: Optional[KeyFn] = None):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)
        super().__init__(
            init=lambda k: None,
            accumulate=(
                lambda a, r: (on_fn(r) if a is None else max(a, on_fn(r)))),
            merge=lambda a1, a2: max(a1, a2),
            name=(f"max({str(on)})"))


@PublicAPI(stability="beta")
class Mean(_AggregateOnKeyBase):
    """Defines mean aggregation."""

    def __init__(self, on: Optional[KeyFn] = None):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)
        super().__init__(
            init=lambda k: [0, 0],
            accumulate=lambda a, r: [a[0] + on_fn(r), a[1] + 1],
            merge=lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]],
            finalize=lambda a: a[0] / a[1],
            name=(f"mean({str(on)})"))


@PublicAPI(stability="beta")
class Std(_AggregateOnKeyBase):
    """Defines standard deviation aggregation.

    Uses Welford's online method for an accumulator-style computation of the
    standard deviation. This method was chosen due to it's numerical
    stability, and it being computable in a single pass.
    This may give different (but more accurate) results than NumPy, Pandas,
    and sklearn, which use a less numerically stable two-pass algorithm.
    See
    https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm
    """

    def __init__(self, on: Optional[KeyFn] = None, ddof: int = 1):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        def accumulate(a: List[float], r: float):
            # Accumulates the current count, the current mean, and the sum of
            # squared differences from the current mean (M2).
            M2, mean, count = a
            # Select the data on which we want to calculate the standard
            # deviation.
            val = on_fn(r)

            count += 1
            delta = val - mean
            mean += delta / count
            delta2 = val - mean
            M2 += delta * delta2
            return [M2, mean, count]

        def merge(a: List[float], b: List[float]):
            # Merges two accumulations into one.
            # See
            # https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
            M2_a, mean_a, count_a = a
            M2_b, mean_b, count_b = b
            delta = mean_b - mean_a
            count = count_a + count_b
            # NOTE: We use this mean calculation since it's more numerically
            # stable than mean_a + delta * count_b / count, which actually
            # deviates from Pandas in the ~15th decimal place and causes our
            # exact comparison tests to fail.
            mean = (mean_a * count_a + mean_b * count_b) / count
            # Update the sum of squared differences.
            M2 = M2_a + M2_b + (delta**2) * count_a * count_b / count
            return [M2, mean, count]

        def finalize(a: List[float]):
            # Compute the final standard deviation from the accumulated
            # sum of squared differences from current mean and the count.
            M2, mean, count = a
            if count < 2:
                return 0.0
            return math.sqrt(M2 / (count - ddof))

        super().__init__(
            init=lambda k: [0, 0, 0],
            accumulate=accumulate,
            merge=merge,
            finalize=finalize,
            name=(f"std({str(on)})"))


def _to_on_fn(on: Optional[KeyFn]):
    if on is None:
        return lambda r: r
    elif isinstance(on, str):
        return lambda r: r[on]
    else:
        return on
