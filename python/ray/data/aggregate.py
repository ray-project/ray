import math
from typing import Callable, Optional, List, TYPE_CHECKING

from ray.util.annotations import PublicAPI
from ray.data.block import (
    T,
    U,
    Block,
    BlockAccessor,
    KeyType,
    AggType,
    KeyFn,
    _validate_key_fn,
)
from ray.data.impl.table_block import TableBlockAccessor
from ray.data.impl.null_aggregate import (
    _null_wrap_init,
    _null_wrap_merge,
    _null_wrap_accumulate,
    _null_wrap_vectorized_aggregate,
    _null_wrap_finalize,
)

if TYPE_CHECKING:
    from ray.data import Dataset


@PublicAPI
class AggregateFn(object):
    def __init__(
        self,
        init: Callable[[KeyType], AggType],
        merge: Callable[[AggType, AggType], AggType],
        accumulate: Callable[[AggType, T], AggType] = None,
        vectorized_aggregate: Callable[[AggType, Block[T]], AggType] = None,
        finalize: Callable[[AggType], U] = lambda a: a,
        name: Optional[str] = None,
    ):
        """Defines an aggregate function in the accumulator style.

        Aggregates a collection of inputs of type T into
        a single output value of type U.
        See https://www.sigops.org/s/conferences/sosp/2009/papers/yu-sosp09.pdf
        for more details about accumulator-based aggregation.

        Args:
            init: This is called once for each group to return the empty accumulator.
                For example, an empty accumulator for a sum would be 0.
            merge: This may be called multiple times, each time to merge
                two accumulators into one.
            accumulate: This is called once per row of the same group.
                This combines the accumulator and the row, returns the updated
                accumulator.
            vectorized_aggregate: This is used to calculate the aggregation for a single
                block, and is vectorized alternative to accumulate. This will be given
                the empty accumulator and the entire block, allowing for vectorized
                aggregation of the block. This will only be used if
                agg_fn.can_vectorize_for_block() returns True for the provided block.
            finalize: This is called once to compute the final aggregation
                result from the fully merged accumulator.
            name: The name of the aggregation. This will be used as the output
                column name in the case of Arrow dataset.
        """
        if accumulate is None and vectorized_aggregate is None:
            raise ValueError(
                "At least one of accumulate or vectorized_aggregate must be provided."
            )
        self.init = init
        self.merge = merge
        self.accumulate = accumulate
        self.vectorized_aggregate = vectorized_aggregate
        self.finalize = finalize
        self.name = name

    def can_vectorize_for_block(self, block_acc: BlockAccessor[T]) -> bool:
        """Whether this aggregation can apply a vectorized aggregation to the provided
        block.
        """
        # Aggregation functions must opt-in to vectorization, default is the accumulator
        # style.
        return False

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


class _VectorizedAggregateBase(_AggregateOnKeyBase):
    """Mixin for supporting vectorized aggregation on tabular datasets."""

    def can_vectorize_for_block(self, block_acc: BlockAccessor[T]) -> bool:
        # Support vectorization for tabular blocks.
        return isinstance(block_acc, TableBlockAccessor)


@PublicAPI
class Sum(_VectorizedAggregateBase):
    """Defines sum aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        super().__init__(
            init=_null_wrap_init(lambda k: 0),
            merge=_null_wrap_merge(ignore_nulls, lambda a1, a2: a1 + a2),
            accumulate=_null_wrap_accumulate(
                ignore_nulls,
                on_fn,
                lambda a, r: a + r,
            ),
            vectorized_aggregate=_null_wrap_vectorized_aggregate(
                ignore_nulls,
                lambda block_acc: block_acc.sum(on, ignore_nulls),
            ),
            finalize=_null_wrap_finalize(lambda a: a),
            name=(f"sum({str(on)})"),
        )


@PublicAPI
class Min(_VectorizedAggregateBase):
    """Defines min aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        super().__init__(
            init=_null_wrap_init(lambda k: float("inf")),
            merge=_null_wrap_merge(ignore_nulls, min),
            accumulate=_null_wrap_accumulate(ignore_nulls, on_fn, min),
            vectorized_aggregate=_null_wrap_vectorized_aggregate(
                ignore_nulls,
                lambda block_acc: block_acc.min(on, ignore_nulls),
            ),
            finalize=_null_wrap_finalize(lambda a: a),
            name=(f"min({str(on)})"),
        )


@PublicAPI
class Max(_VectorizedAggregateBase):
    """Defines max aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        super().__init__(
            init=_null_wrap_init(lambda k: float("-inf")),
            merge=_null_wrap_merge(ignore_nulls, max),
            accumulate=_null_wrap_accumulate(ignore_nulls, on_fn, max),
            vectorized_aggregate=_null_wrap_vectorized_aggregate(
                ignore_nulls,
                lambda block_acc: block_acc.max(on, ignore_nulls),
            ),
            finalize=_null_wrap_finalize(lambda a: a),
            name=(f"max({str(on)})"),
        )


@PublicAPI
class Mean(_VectorizedAggregateBase):
    """Defines mean aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        def vectorized_mean(block_acc: BlockAccessor[T]) -> AggType:
            sum_ = block_acc.sum(on, ignore_nulls)
            if sum_ is None:
                return None
            count = block_acc.count(on, ignore_nulls)
            return [sum_, count]

        super().__init__(
            init=_null_wrap_init(lambda k: [0, 0]),
            merge=_null_wrap_merge(
                ignore_nulls, lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]]
            ),
            accumulate=_null_wrap_accumulate(
                ignore_nulls, on_fn, lambda a, r: [a[0] + r, a[1] + 1]
            ),
            vectorized_aggregate=_null_wrap_vectorized_aggregate(
                ignore_nulls,
                vectorized_mean,
            ),
            finalize=_null_wrap_finalize(lambda a: a[0] / a[1]),
            name=(f"mean({str(on)})"),
        )


@PublicAPI
class Std(_VectorizedAggregateBase):
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

        def vectorized_agg(block_acc: BlockAccessor[T]) -> AggType:
            count = block_acc.count(on, ignore_nulls)
            if count == 0:
                return None
            sum_ = block_acc.sum(on, ignore_nulls)
            if sum_ is None:
                return None
            mean = sum_ / count
            M2 = block_acc.sum_of_squared_diffs_from_mean(on, ignore_nulls, mean)
            return [M2, mean, count]

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
            merge=_null_wrap_merge(ignore_nulls, merge),
            accumulate=_null_wrap_accumulate(ignore_nulls, on_fn, accumulate),
            vectorized_aggregate=_null_wrap_vectorized_aggregate(
                ignore_nulls,
                vectorized_agg,
            ),
            finalize=_null_wrap_finalize(finalize),
            name=(f"std({str(on)})"),
        )


def _to_on_fn(on: Optional[KeyFn]):
    if on is None:
        return lambda r: r
    elif isinstance(on, str):
        return lambda r: r[on]
    else:
        return on
