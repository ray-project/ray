import math
from typing import Callable, Optional, List, TYPE_CHECKING

import numpy as np

from ray.util.annotations import PublicAPI
from ray.data.block import T, U, KeyType, AggType, KeyFn, _validate_key_fn
from ray.data.impl.table_block import TableBlockAccessor
from ray.data.impl.null_aggregate import (
    _null_wrap_init,
    _null_wrap_accumulate,
    _null_wrap_merge,
    _null_wrap_finalize,
)

if TYPE_CHECKING:
    from ray.data import Dataset


@PublicAPI
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


@PublicAPI
class Count(AggregateFn):
    """Defines count aggregation."""

    def __init__(self):
        super().__init__(
            init=lambda k: 0,
            accumulate=lambda a, r: a + 1,
            merge=lambda a1, a2: a1 + a2,
            name="count()",
        )


@PublicAPI
class Sum(_AggregateOnKeyBase):
    """Defines sum aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        super().__init__(
            init=_null_wrap_init(lambda k: 0),
            accumulate=_null_wrap_accumulate(ignore_nulls, on_fn, lambda a, r: a + r),
            merge=_null_wrap_merge(ignore_nulls, lambda a1, a2: a1 + a2),
            finalize=_null_wrap_finalize(lambda a: a),
            name=(f"sum({str(on)})"),
        )


@PublicAPI
class Min(_AggregateOnKeyBase):
    """Defines min aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        super().__init__(
            init=_null_wrap_init(lambda k: float("inf")),
            accumulate=_null_wrap_accumulate(ignore_nulls, on_fn, min),
            merge=_null_wrap_merge(ignore_nulls, min),
            finalize=_null_wrap_finalize(lambda a: a),
            name=(f"min({str(on)})"),
        )


@PublicAPI
class Max(_AggregateOnKeyBase):
    """Defines max aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        super().__init__(
            init=_null_wrap_init(lambda k: float("-inf")),
            accumulate=_null_wrap_accumulate(ignore_nulls, on_fn, max),
            merge=_null_wrap_merge(ignore_nulls, max),
            finalize=_null_wrap_finalize(lambda a: a),
            name=(f"max({str(on)})"),
        )


@PublicAPI
class Mean(_AggregateOnKeyBase):
    """Defines mean aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        super().__init__(
            init=_null_wrap_init(lambda k: [0, 0]),
            accumulate=_null_wrap_accumulate(
                ignore_nulls, on_fn, lambda a, r: [a[0] + r, a[1] + 1]
            ),
            merge=_null_wrap_merge(
                ignore_nulls, lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]]
            ),
            finalize=_null_wrap_finalize(lambda a: a[0] / a[1]),
            name=(f"mean({str(on)})"),
        )


@PublicAPI
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
        on: Optional[KeyFn] = None,
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
            init=_null_wrap_init(lambda k: [0, 0, 0]),
            accumulate=_null_wrap_accumulate(ignore_nulls, on_fn, accumulate),
            merge=_null_wrap_merge(ignore_nulls, merge),
            finalize=_null_wrap_finalize(finalize),
            name=(f"std({str(on)})"),
        )


@PublicAPI
class VectorizedAggregation:
    """Defines a vectorized aggregation for tabular datasets, where both per-block
    aggregations and cross-block aggregation merges are vectorized.
    """

    def __init__(
        self,
        aggregate_block: Callable[[TableBlockAccessor, str], U],
        merge_block_aggs: Callable[[np.ndarray], np.ndarray],
        name: str,
    ):
        """
        Build the vectorized aggregation.

        Args:
            aggregate_block: Callable performing vectorized aggregation of a tabular
                block.
            merge_block_aggs: Callable performing vectorized merging of block
                aggregations.
            name: The name of this aggregation. This will be displayed in the progress
                bar.
        """
        self.aggregate_block = aggregate_block
        self.merge_block_aggs = merge_block_aggs
        self.name = name


class VectorizedSum(VectorizedAggregation):
    """Defines a vectorized sum aggregation."""

    def __init__(self, on: KeyFn, ignore_nulls: bool = True):
        def _aggregate_block(block_acc):
            result = block_acc.sum(on, ignore_nulls)
            return result if result is not None else np.nan

        def _merge_block_aggs(block_aggs):
            block_aggs = np.array(block_aggs)
            if ignore_nulls:
                if np.all(np.isnan(block_aggs)):
                    result = np.nan
                else:
                    result = np.nansum(block_aggs)
            else:
                result = np.sum(block_aggs)
            return result

        super().__init__(
            aggregate_block=_aggregate_block,
            merge_block_aggs=_merge_block_aggs,
            name="Sum",
        )


class VectorizedMin(VectorizedAggregation):
    """Defines a vectorized min aggregation."""

    def __init__(self, on: KeyFn, ignore_nulls: bool = True):
        def _aggregate_block(block_acc):
            result = block_acc.min(on, ignore_nulls)
            return result if result is not None else np.nan

        def _merge_block_aggs(block_aggs):
            block_aggs = np.array(block_aggs)
            if ignore_nulls:
                if np.all(np.isnan(block_aggs)):
                    result = np.nan
                else:
                    result = np.nanmin(block_aggs)
            else:
                result = np.min(block_aggs)
            return result

        super().__init__(
            aggregate_block=_aggregate_block,
            merge_block_aggs=_merge_block_aggs,
            name="Min",
        )


class VectorizedMax(VectorizedAggregation):
    """Defines a vectorized max aggregation."""

    def __init__(self, on: KeyFn, ignore_nulls: bool = True):
        def _aggregate_block(block_acc):
            result = block_acc.max(on, ignore_nulls)
            return result if result is not None else np.nan

        def _merge_block_aggs(block_aggs):
            block_aggs = np.array(block_aggs)
            if ignore_nulls:
                if np.all(np.isnan(block_aggs)):
                    result = np.nan
                else:
                    result = np.nanmax(block_aggs)
            else:
                result = np.max(block_aggs)
            return result

        super().__init__(
            aggregate_block=_aggregate_block,
            merge_block_aggs=_merge_block_aggs,
            name="Max",
        )


class VectorizedMean(VectorizedAggregation):
    """Defines a vectorized mean aggregation."""

    def __init__(self, on: KeyFn, ignore_nulls: bool = True):
        def _aggregate_block(block_acc):
            sum_ = block_acc.sum(on, ignore_nulls)
            if sum_ is None:
                sum_ = np.nan
            count = block_acc.count(on, ignore_nulls)
            return (sum_, count)

        def _merge_block_aggs(block_aggs):
            block_sums, block_counts = zip(*block_aggs)
            block_sums, block_counts = np.array(block_sums), np.array(block_counts)
            if ignore_nulls:
                if np.all(block_counts == 0):
                    sum_ = np.nan
                else:
                    sum_ = np.nansum(block_sums)
            else:
                sum_ = np.sum(block_sums)
            count = np.sum(block_counts)
            if count == 0:
                return None
            else:
                return sum_ / count

        super().__init__(
            aggregate_block=_aggregate_block,
            merge_block_aggs=_merge_block_aggs,
            name="Max",
        )


class VectorizedStd(VectorizedAggregation):
    """Defines a vectorized mean aggregation."""

    def __init__(self, on: KeyFn, ignore_nulls: bool = True, ddof: int = 1):
        def _aggregate_block(block_acc):
            count = block_acc.count(on, ignore_nulls)
            if count == 0:
                return 0, 0, 0
            sum_ = block_acc.sum(on, ignore_nulls)
            if sum_ is None:
                sum_ = np.nan
            mean = sum_ / count
            M2 = block_acc.sum_of_squared_diffs_from_mean(on, ignore_nulls, mean)
            return M2, mean, count

        def _merge_block_aggs(block_aggs):
            block_M2s, block_means, block_counts = zip(*block_aggs)
            block_M2s, block_means, block_counts = (
                np.array(block_M2s),
                np.array(block_means),
                np.array(block_counts),
            )
            count = np.sum(block_counts)
            if count == 0:
                return None
            mean = np.sum(np.dot(block_means, block_counts)) / count
            # TODO(Clark): Generalize pairwise sum of squared diffs from mean update to
            # an N-way update. Below is not correct.
            M2 = np.sum(block_M2s) + (np.prod(block_counts) / count) * np.sum(
                (block_means - mean) ** 2
            )
            if count < 2:
                return 0.0
            return np.sqrt(M2 / (count - ddof))

        super().__init__(
            aggregate_block=_aggregate_block,
            merge_block_aggs=_merge_block_aggs,
            name="Std",
        )


def _to_on_fn(on: Optional[KeyFn]):
    if on is None:
        return lambda r: r
    elif isinstance(on, str):
        return lambda r: r[on]
    else:
        return on
