import abc
from typing import Optional

from ray.data._internal.arrow_ops.transform_pyarrow import (
    MIN_PYARROW_VERSION_TYPE_PROMOTION,
)
from ray.data.aggregate import (
    AggregateFnV2,
    Count,
    Sum,
    Min,
    Max,
    AbsMax,
    Quantile,
    Unique,
)
from ray.data.block import AggType, BlockColumn, BlockColumnAccessor, Block, U


MIN_PYARROW_VERSION_VECTORIZED_AGGREGATIONS = MIN_PYARROW_VERSION_TYPE_PROMOTION


# TODO move to anyscale package
class VectorizedAggregateFnV2(AggregateFnV2, abc.ABC):
    """Base class for fully vectorized aggregations"""

    def combine(self, current_accumulator: AggType, new: AggType) -> AggType:
        raise NotImplementedError(
            "this method should not be invoked for vectorized aggregations!"
        )

    @abc.abstractmethod
    def _combine_column(self, accumulator_col: BlockColumn) -> AggType:
        ...


class CountVectorized(VectorizedAggregateFnV2, Count):
    def _combine_column(self, accumulator_col: BlockColumn) -> AggType:
        return BlockColumnAccessor.for_column(accumulator_col).sum(
            ignore_nulls=self._ignore_nulls
        )


class SumVectorized(VectorizedAggregateFnV2, Sum):
    def _combine_column(self, accumulator_col: BlockColumn) -> AggType:
        return BlockColumnAccessor.for_column(accumulator_col).sum(
            ignore_nulls=self._ignore_nulls
        )


class MinVectorized(VectorizedAggregateFnV2, Min):
    def _combine_column(self, accumulator_col: BlockColumn) -> AggType:
        return BlockColumnAccessor.for_column(accumulator_col).min(
            ignore_nulls=self._ignore_nulls
        )


class MaxVectorized(VectorizedAggregateFnV2, Max):
    def _combine_column(self, accumulator_col: BlockColumn) -> AggType:
        return BlockColumnAccessor.for_column(accumulator_col).max(
            ignore_nulls=self._ignore_nulls
        )


class AbsMaxVectorized(VectorizedAggregateFnV2, AbsMax):
    def _combine_column(self, accumulator_col: BlockColumn) -> AggType:
        return BlockColumnAccessor.for_column(accumulator_col).max(
            ignore_nulls=self._ignore_nulls
        )


class QuantileVectorized(VectorizedAggregateFnV2, Quantile):
    def aggregate_block(self, block: Block) -> AggType:
        accessor = BlockColumnAccessor.for_column(block[self._target_col_name])
        # Return whole column as is
        #
        # NOTE: We have to make sure that the column is represented in an
        #       Arrow-compatible format (either ``pyarrow.Array`` or Python's ``list``)
        #       to make sure it's not converted into a tensor downstream
        return accessor._as_arrow_compatible()

    def _combine_column(self, accumulator_col: BlockColumn) -> AggType:
        # Combine all the lists in a column into a single value (ie flatten it)
        return BlockColumnAccessor.for_column(accumulator_col).flatten()

    def _finalize(self, accumulator: AggType) -> Optional[U]:
        accessor = BlockColumnAccessor.for_column(accumulator)

        return accessor.quantile(q=self._q, ignore_nulls=self._ignore_nulls)


class UniqueVectorized(VectorizedAggregateFnV2, Unique):
    def aggregate_block(self, block: Block) -> AggType:
        column = block[self._target_col_name]

        unique = BlockColumnAccessor.for_column(column).unique()
        # Return column of unique values as is
        #
        # NOTE: We have to make sure that the column is represented in an
        #       Arrow-compatible format (either ``pyarrow.Array`` or Python's ``list``)
        #       to make sure it's not converted into a tensor downstream
        return BlockColumnAccessor.for_column(unique)._as_arrow_compatible()

    def _combine_column(self, accumulator_col: BlockColumn) -> AggType:
        # Combine all the lists in a column into a single value (ie flatten it)
        flattened = BlockColumnAccessor.for_column(accumulator_col).flatten()
        return BlockColumnAccessor.for_column(flattened).unique()
