import functools
import math
from typing import Callable, Optional, List, TYPE_CHECKING, Any

from ray.util.annotations import PublicAPI
from ray.data.block import T, U, KeyType, AggType, KeyFn, _validate_key_fn

if TYPE_CHECKING:
    from ray.data import Dataset

_INIT_SENTINEL = float("inf")


@PublicAPI(stability="beta")
class AggregateFn(object):
    def __init__(
        self,
        init: Callable[[KeyType], AggType],
        accumulate: Callable[[AggType, T], AggType],
        merge: Callable[[AggType, AggType], AggType],
        finalize: Callable[[AggType], U] = lambda a: a,
        name: Optional[str] = None,
    ):
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
            name="count()",
        )


def _null_accumulate(
    ignore_nulls: bool,
    on_fn: Callable[[T], T],
    accum: Callable[[AggType, T], AggType],
    first: Callable[[AggType], AggType],
    a: AggType,
    r: T,
) -> AggType:
    """
    Accumulate while handling nulls.

    This performs an accumulation subject to the following null rules:
    1. If r is null and ignore_nulls=False, return null.
    2. If r is null and ignore_nulls=True, return a.
    3. If r is non-null and a is null, return null.
    4. If r is non-null and its the first row, return first(r).
    5. If r is non-null and a is non-null, return accum(a, r).

    Args:
        ignore_nulls: Whether nulls should be ignored or cause a null result.
        on_fn: Function selecting a subset of the row to apply the aggregation.
        accum: The core accumulator function.
        first: Function processing the first row in the accumulation.
        a: Accumulated so far.
        r: This row.

    Returns:
        Accumulated result including the provided row
    """
    r = on_fn(r)
    if _is_null(r):
        if ignore_nulls:
            return a
        else:
            return None
    else:
        if a == _INIT_SENTINEL:
            return first(r)
        elif a is None:
            return None
        else:
            return accum(a, r)


def _null_merge(
    ignore_nulls: bool,
    merge: Callable[[AggType, AggType], AggType],
    a1: AggType,
    a2: AggType,
) -> AggType:
    """
    Merge two accumulations while handling nulls.

    This merges two accumulations subject to the following null rules:
    1. If a1 is empty and a2 is empty, return empty sentinel.
    2. If a1 (a2) is empty and a2 (a1) is null, return null.
    3. If a1 (a2) is empty and a2 (a1) is non-null, return a2 (a1).
    4. If a1 (a2) is null, return a2 (a1) if ignoring nulls, null otherwise.
    5. If a1 and a2 are both non-null, return merge(a1, a2).

    Args:
        ignore_nulls: Whether nulls should be ignored or cause a null result.
        merge: The core merge function.
        a1: One of the intermediate accumulations to merge.
        a2: One of the intermediate accumulations to merge.

    Returns:
        Accumulation of the two provided accumulations.
    """
    if a1 == _INIT_SENTINEL:
        # If one operand is the init value, propagate the other.
        # No matter whether a2 is a real value, the init value, or None,
        # propagating each of these is correct if a1 is the init value.
        return a2
    if a1 is None:
        # If we're ignoring nulls, propagate a2; otherwise, propagate null.
        return a2 if ignore_nulls else None
    if a2 == _INIT_SENTINEL:
        # If one operand is the init value, propagate the other.
        return a1
    if a2 is None:
        # If we're ignoring nulls, propagate a1; otherwise, propagate null.
        return a1 if ignore_nulls else None
    return merge(a1, a2)


def _null_finalize(finalize: Callable[[AggType], AggType], a: AggType) -> AggType:
    """
    Finalize an accumulation while handling nulls.

    If the accumulation is empty or null, this returns null.

    Args:
        finalize: The core finalizing function.
        a: Accumulation result to finalize.

    Returns:
        Finalized accumulation result.
    """
    return finalize(a) if a != _INIT_SENTINEL and a is not None else None


@PublicAPI(stability="beta")
class Sum(_AggregateOnKeyBase):
    """Defines sum aggregation."""

    def __init__(self, on: KeyFn = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        super().__init__(
            init=lambda k: _INIT_SENTINEL,
            accumulate=functools.partial(
                _null_accumulate,
                ignore_nulls,
                on_fn,
                lambda a, r: a + r,
                lambda r: r,
            ),
            merge=functools.partial(_null_merge, ignore_nulls, lambda a1, a2: a1 + a2),
            finalize=functools.partial(_null_finalize, lambda a: a),
            name=(f"sum({str(on)})"),
        )


@PublicAPI(stability="beta")
class Min(_AggregateOnKeyBase):
    """Defines min aggregation."""

    def __init__(self, on: KeyFn = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        super().__init__(
            init=lambda k: _INIT_SENTINEL,
            accumulate=functools.partial(
                _null_accumulate, ignore_nulls, on_fn, min, lambda r: r
            ),
            merge=functools.partial(_null_merge, ignore_nulls, min),
            finalize=functools.partial(_null_finalize, lambda a: a),
            name=(f"min({str(on)})"),
        )


@PublicAPI(stability="beta")
class Max(_AggregateOnKeyBase):
    """Defines max aggregation."""

    def __init__(self, on: KeyFn = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        super().__init__(
            init=lambda k: _INIT_SENTINEL,
            accumulate=functools.partial(
                _null_accumulate, ignore_nulls, on_fn, max, lambda r: r
            ),
            merge=functools.partial(_null_merge, ignore_nulls, max),
            finalize=functools.partial(_null_finalize, lambda a: a),
            name=(f"max({str(on)})"),
        )


@PublicAPI(stability="beta")
class Mean(_AggregateOnKeyBase):
    """Defines mean aggregation."""

    def __init__(self, on: KeyFn = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        super().__init__(
            init=lambda k: _INIT_SENTINEL,
            accumulate=functools.partial(
                _null_accumulate,
                ignore_nulls,
                on_fn,
                lambda a, r: [a[0] + r, a[1] + 1],
                lambda r: [r, 1],
            ),
            merge=functools.partial(
                _null_merge, ignore_nulls, lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]]
            ),
            finalize=functools.partial(_null_finalize, lambda a: a[0] / a[1]),
            name=(f"mean({str(on)})"),
        )


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

    def __init__(
        self,
        on: KeyFn = None,
        ddof: int = 1,
        ignore_nulls: bool = True,
    ):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        def accumulate(a: List[float], r: float):
            # Accumulates the current count, the current mean, and the sum of
            # squared differences from the current mean (M2).
            M2, mean, count = a

            count += 1
            delta = r - mean
            mean += delta / count
            delta2 = r - mean
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
            M2 = M2_a + M2_b + (delta ** 2) * count_a * count_b / count
            return [M2, mean, count]

        def finalize(a: List[float]):
            # Compute the final standard deviation from the accumulated
            # sum of squared differences from current mean and the count.
            M2, mean, count = a
            if count < 2:
                return 0.0
            return math.sqrt(M2 / (count - ddof))

        super().__init__(
            init=lambda k: _INIT_SENTINEL,
            accumulate=functools.partial(
                _null_accumulate, ignore_nulls, on_fn, accumulate, lambda r: [0, r, 1]
            ),
            merge=functools.partial(_null_merge, ignore_nulls, merge),
            finalize=functools.partial(_null_finalize, finalize),
            name=(f"std({str(on)})"),
        )


def _to_on_fn(on: KeyFn):
    if on is None:
        return lambda r: r
    elif isinstance(on, str):
        return lambda r: r[on]
    else:
        return on


def _is_null(r: Any):
    try:
        import pandas as pd

        return pd.isnull(r)
    except ModuleNotFoundError:
        import numpy as np

        try:
            return np.isnan(r)
        except TypeError:
            return r is None
