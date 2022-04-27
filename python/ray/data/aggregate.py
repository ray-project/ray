import functools
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
from ray.data._internal.null_aggregate import (
    _null_wrap_init,
    _null_wrap_merge,
    _null_wrap_accumulate_block,
    _null_wrap_finalize,
    _null_wrap_accumulate_row,
)

if TYPE_CHECKING:
    import polars
    from ray.data import Dataset


@PublicAPI
class AggregateFn(object):
    def __init__(
        self,
        init: Callable[[KeyType], AggType],
        merge: Callable[[AggType, AggType], AggType],
        accumulate_row: Callable[[AggType, T], AggType] = None,
        accumulate_block: Callable[[AggType, Block[T]], AggType] = None,
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
            accumulate_row: This is called once per row of the same group.
                This combines the accumulator and the row, returns the updated
                accumulator. Exactly one of accumulate_row and accumulate_block must
                be provided.
            accumulate_block: This is used to calculate the aggregation for a
                single block, and is vectorized alternative to accumulate_row. This will
                be given a base accumulator and the entire block, allowing for
                vectorized accumulation of the block. Exactly one of accumulate_row and
                accumulate_block must be provided.
            finalize: This is called once to compute the final aggregation
                result from the fully merged accumulator.
            name: The name of the aggregation. This will be used as the output
                column name in the case of Arrow dataset.
        """
        if (accumulate_row is None and accumulate_block is None) or (
            accumulate_row is not None and accumulate_block is not None
        ):
            raise ValueError(
                "Exactly one of accumulate_row or accumulate_block must be provided."
            )
        if accumulate_block is None:

            def accumulate_block(a: AggType, block: Block[T]) -> AggType:
                block_acc = BlockAccessor.for_block(block)
                for r in block_acc.iter_rows():
                    a = accumulate_row(a, r)
                return a

        self.init = init
        self.merge = merge
        self.accumulate_block = accumulate_block
        self.finalize = finalize
        self.name = name

    def _validate(self, ds: "Dataset") -> None:
        """Raise an error if this cannot be applied to the given dataset."""
        pass


@PublicAPI(stability="alpha")
class PolarsAggregation:
    """A Polars aggregation, expressed via mapper and reducer Polars expressions."""

    def __init__(
        self,
        map_expression: "polars.Expr",
        reduce_expression: "polars.Expr",
    ):
        self.map_expression = map_expression
        self.reduce_expression = reduce_expression


@PublicAPI(stability="alpha")
class WithPolars:
    """An aggregation that can also be used as a Polars aggregation."""

    def as_polars(self) -> PolarsAggregation:
        raise NotImplementedError


class _AggregateOnKeyBase(AggregateFn, WithPolars):
    def _set_key_fn(self, on: KeyFn):
        self._key_fn = on

    def _validate(self, ds: "Dataset") -> None:
        _validate_key_fn(ds, self._key_fn)


@PublicAPI
class Count(AggregateFn, WithPolars):
    """Defines count aggregation."""

    def __init__(self):
        super().__init__(
            init=lambda k: 0,
            accumulate_block=(
                lambda a, block: a + BlockAccessor.for_block(block).num_rows()
            ),
            merge=lambda a1, a2: a1 + a2,
            name="count()",
        )

    def as_polars(self) -> PolarsAggregation:
        import polars as pl

        return PolarsAggregation(pl.count(), pl.sum("count").suffix("()"))


@PublicAPI
class Sum(_AggregateOnKeyBase):
    """Defines sum aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)

        null_merge = _null_wrap_merge(ignore_nulls, lambda a1, a2: a1 + a2)

        super().__init__(
            init=_null_wrap_init(lambda k: 0),
            merge=null_merge,
            accumulate_block=_null_wrap_accumulate_block(
                ignore_nulls,
                lambda block: BlockAccessor.for_block(block).sum(on, ignore_nulls),
                null_merge,
            ),
            finalize=_null_wrap_finalize(lambda a: a),
            name=(f"sum({str(on)})"),
        )

        self.on = on
        self.ignore_nulls = ignore_nulls

    def as_polars(self) -> PolarsAggregation:
        import polars as pl

        sum_col = f"sum({self.on})"

        map_expr = (
            pl.when(self.ignore_nulls or pl.col(self.on).null_count() == 0)
            .then(pl.sum(self.on))
            .otherwise(None)
            .alias(sum_col)
        )
        reduce_expr = (
            pl.when(self.ignore_nulls or pl.col(sum_col).null_count() == 0)
            .then(pl.sum(sum_col))
            .otherwise(None)
            .alias(sum_col)
        )
        return PolarsAggregation(map_expr, reduce_expr)


@PublicAPI
class Min(_AggregateOnKeyBase):
    """Defines min aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)

        null_merge = _null_wrap_merge(ignore_nulls, min)

        super().__init__(
            init=_null_wrap_init(lambda k: float("inf")),
            merge=null_merge,
            accumulate_block=_null_wrap_accumulate_block(
                ignore_nulls,
                lambda block: BlockAccessor.for_block(block).min(on, ignore_nulls),
                null_merge,
            ),
            finalize=_null_wrap_finalize(lambda a: a),
            name=(f"min({str(on)})"),
        )

        self.on = on
        self.ignore_nulls = ignore_nulls

    def as_polars(self) -> PolarsAggregation:
        import polars as pl

        temp_col = f"__{self.on}_MIN__"

        map_expr = (
            pl.when(self.ignore_nulls or pl.col(self.on).null_count() == 0)
            .then(pl.min(self.on))
            .otherwise(None)
            .alias(temp_col)
        )
        reduce_expr = (
            pl.when(self.ignore_nulls or pl.col(temp_col).null_count() == 0)
            .then(pl.min(temp_col))
            .otherwise(None)
            .alias(f"min({str(self.on)})")
        )
        return PolarsAggregation(map_expr, reduce_expr)


@PublicAPI
class Max(_AggregateOnKeyBase):
    """Defines max aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)

        null_merge = _null_wrap_merge(ignore_nulls, max)

        super().__init__(
            init=_null_wrap_init(lambda k: float("-inf")),
            merge=null_merge,
            accumulate_block=_null_wrap_accumulate_block(
                ignore_nulls,
                lambda block: BlockAccessor.for_block(block).max(on, ignore_nulls),
                null_merge,
            ),
            finalize=_null_wrap_finalize(lambda a: a),
            name=(f"max({str(on)})"),
        )

        self.on = on
        self.ignore_nulls = ignore_nulls

    def as_polars(self) -> PolarsAggregation:
        import polars as pl

        temp_col = f"__{self.on}_MAX__"

        map_expr = (
            pl.when(self.ignore_nulls or pl.col(self.on).null_count() == 0)
            .then(pl.max(self.on))
            .otherwise(None)
            .alias(temp_col)
        )
        reduce_expr = (
            pl.when(self.ignore_nulls or pl.col(temp_col).null_count() == 0)
            .then(pl.max(temp_col))
            .otherwise(None)
            .alias(f"max({str(self.on)})")
        )
        return PolarsAggregation(map_expr, reduce_expr)


@PublicAPI
class Mean(_AggregateOnKeyBase):
    """Defines mean aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)

        null_merge = _null_wrap_merge(
            ignore_nulls, lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]]
        )

        def vectorized_mean(block: Block[T]) -> AggType:
            block_acc = BlockAccessor.for_block(block)
            count = block_acc.count(on)
            if count == 0 or count is None:
                # Empty or all null.
                return None
            sum_ = block_acc.sum(on, ignore_nulls)
            if sum_ is None:
                # ignore_nulls=False and at least one null.
                return None
            return [sum_, count]

        super().__init__(
            init=_null_wrap_init(lambda k: [0, 0]),
            merge=null_merge,
            accumulate_block=_null_wrap_accumulate_block(
                ignore_nulls,
                vectorized_mean,
                null_merge,
            ),
            finalize=_null_wrap_finalize(lambda a: a[0] / a[1]),
            name=(f"mean({str(on)})"),
        )

        self.on = on
        self.ignore_nulls = ignore_nulls

    def as_polars(self) -> PolarsAggregation:
        import polars as pl

        temp_col = f"__{self.on}_MEAN__"

        map_expr = (
            pl.when(self.ignore_nulls or pl.col(self.on).null_count() == 0)
            .then(
                pl.struct(
                    [
                        pl.sum(self.on).alias("sum"),
                        (pl.count() - pl.col(self.on).null_count()).alias("count"),
                    ]
                ),
            )
            .otherwise(None)
            .alias(temp_col)
        )
        reduce_expr = (
            pl.when(self.ignore_nulls or pl.col(temp_col).null_count() == 0)
            .then(
                pl.col(temp_col).struct.field("sum").sum()
                / pl.col(temp_col).struct.field("count").sum()
            )
            .otherwise(None)
            .alias(f"mean({str(self.on)})")
        )
        return PolarsAggregation(map_expr, reduce_expr)


@PublicAPI
class Std(_AggregateOnKeyBase):
    """Defines standard deviation aggregation.

    Uses Welford's online method for an accumulator-style computation of the
    standard deviation. This method was chosen due to its numerical
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

        null_merge = _null_wrap_merge(ignore_nulls, merge)

        def vectorized_std(block: Block[T]) -> AggType:
            block_acc = BlockAccessor.for_block(block)
            count = block_acc.count(on)
            if count == 0 or count is None:
                # Empty or all null.
                return None
            sum_ = block_acc.sum(on, ignore_nulls)
            if sum_ is None:
                # ignore_nulls=False and at least one null.
                return None
            mean = sum_ / count
            M2 = block_acc.sum_of_squared_diffs_from_mean(on, ignore_nulls, mean)
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
            merge=null_merge,
            accumulate_block=_null_wrap_accumulate_block(
                ignore_nulls,
                vectorized_std,
                null_merge,
            ),
            finalize=_null_wrap_finalize(finalize),
            name=(f"std({str(on)})"),
        )

        self.on = on
        self.ignore_nulls = ignore_nulls
        self.ddof = ddof

    def as_polars(self) -> PolarsAggregation:
        import polars as pl

        temp_col = f"__{self.on}_STD__"

        map_expr = (
            pl.when(self.ignore_nulls or pl.col(self.on).null_count() == 0)
            .then(
                pl.struct(
                    [
                        ((pl.col(self.on) - pl.mean(self.on)) ** 2).sum().alias("M2"),
                        pl.mean(self.on).alias("mean"),
                        (pl.count() - pl.col(self.on).null_count()).alias("count"),
                    ]
                )
            )
            .otherwise(None)
            .alias(temp_col)
        )

        # TODO(Clark): Generalize Chan's method for merging Welford accumulations to be
        # vectorized over N accumulations, rather than using binary fold/accumulation
        # implementation.
        def fold(acc, x):
            if x is None:
                return acc
            # Merges two accumulations into one.
            # See
            # https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm
            M2_acc, mean_acc, count_acc = acc["M2"], acc["mean"], acc["count"]
            M2_x, mean_x, count_x = x["M2"], x["mean"], x["count"]
            if count_x is None or count_x == 0:
                return acc
            delta = mean_x - mean_acc
            count = count_acc + count_x
            # NOTE: We use this mean calculation since it's more numerically
            # stable than mean_acc + delta * count_x / count, which actually
            # deviates from Pandas in the ~15th decimal place and causes our
            # exact comparison tests to fail.
            mean = (mean_acc * count_acc + mean_x * count_x) / count
            # Update the sum of squared differences.
            M2 = M2_acc + M2_x + (delta ** 2) * count_acc * count_x / count
            return {"M2": M2, "mean": mean, "count": count}

        def finalize(a: dict):
            # Compute the final standard deviation from the accumulated
            # sum of squared differences from current mean and the count.
            if a is None:
                return 0.0
            M2, _, count = a["M2"], a["mean"], a["count"]
            if count is None or count < 2:
                return 0.0
            return math.sqrt(M2 / (count - self.ddof))

        reduce_expr = (
            pl.when(self.ignore_nulls or pl.col(temp_col).null_count() == 0)
            .then(
                pl.col(temp_col).apply(
                    lambda s: finalize(functools.reduce(fold, s)),
                    return_dtype=pl.datatypes.Float64(),
                )
            )
            .otherwise(None)
            .alias(f"std({self.on})")
        )
        return PolarsAggregation(map_expr, reduce_expr)


@PublicAPI
class AbsMax(_AggregateOnKeyBase):
    """Defines absolute max aggregation."""

    def __init__(self, on: Optional[KeyFn] = None, ignore_nulls: bool = True):
        self._set_key_fn(on)
        on_fn = _to_on_fn(on)

        super().__init__(
            init=_null_wrap_init(lambda k: 0),
            merge=_null_wrap_merge(ignore_nulls, max),
            accumulate_row=_null_wrap_accumulate_row(
                ignore_nulls, on_fn, lambda a, r: max(a, abs(r))
            ),
            finalize=_null_wrap_finalize(lambda a: a),
            name=(f"abs_max({str(on)})"),
        )


def _to_on_fn(on: Optional[KeyFn]):
    if on is None:
        return lambda r: r
    elif isinstance(on, str):
        return lambda r: r[on]
    else:
        return on
