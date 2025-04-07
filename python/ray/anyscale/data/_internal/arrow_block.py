from typing import List

import numpy as np

from ray.anyscale.data._internal.block import OptimizedTableBlockMixin

_INTERNAL_NUM_ROWS_COUNTER_COLUMN_NAME = "__rd_internal_num_rows"


class ArrowBlockMixin(OptimizedTableBlockMixin):
    """Mixin extending ``ArrowBlockAccessor`` providing optimized
    implementations for some common operations
    """

    def _get_group_boundaries_sorted(self, keys: List[str]) -> np.ndarray:
        import pyarrow as pa
        import pyarrow.compute as pac

        if self.num_rows() == 0:
            return np.array([], dtype=np.int32)
        elif not keys:
            # If no keys are specified, whole block is considered a single group
            return np.array([0, self.num_rows()])

        # This method computes offsets for individual groups with a
        # following algorithm:
        #
        #   - Column with single int value of 1 (for every row) is appended
        ones = np.ones(self._table.num_rows, dtype=np.int32)

        extended_table = self._table.append_column(
            _INTERNAL_NUM_ROWS_COUNTER_COLUMN_NAME, pa.array(ones)
        )

        #   - Block is aggregated based on the target group-key, where
        #       newly added column is summed up (computing the size of the group)
        aggregated_extended_table = (
            extended_table.group_by(keys).aggregate(
                [(_INTERNAL_NUM_ROWS_COUNTER_COLUMN_NAME, "sum")]
            )
            # NOTE: Arrow performs hash-based aggregations and hence returned
            #       table could be out of order
            .sort_by([(k, "ascending") for k in keys])
        )

        group_size_column = aggregated_extended_table[
            f"{_INTERNAL_NUM_ROWS_COUNTER_COLUMN_NAME}_sum"
        ]

        #   - Column with respective sizes of the group is transformed into
        #       an array of offsets (by running cumulative sum on it)
        offsets_col = pac.cumulative_sum(group_size_column)

        return np.concatenate([[0], offsets_col.to_numpy()])
