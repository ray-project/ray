import logging
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Tuple, Type

from ray._private.arrow_utils import get_pyarrow_version
from ray.air.util.transform_pyarrow import _is_pa_extension_type
from ray.data._internal.arrow_block import ArrowBlockAccessor, ArrowBlockBuilder
from ray.data._internal.arrow_ops.transform_pyarrow import (
    MIN_PYARROW_VERSION_RUN_END_ENCODED_TYPES,
    MIN_PYARROW_VERSION_VIEW_TYPES,
)
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.hash_shuffle import (
    HashShufflingOperatorBase,
    StatefulShuffleAggregation,
)
from ray.data._internal.logical.operators.join_operator import JoinType
from ray.data._internal.util import GiB, MiB
from ray.data.block import Block
from ray.data.context import DataContext

if TYPE_CHECKING:
    import pyarrow as pa


@dataclass(frozen=True)
class _DatasetPreprocessingResult:
    """Result of join preprocessing containing split tables.

    Separates tables into supported (directly joinable) and unsupported
    (requires indexing) column projections.
    """

    supported_projection: "pa.Table"
    unsupported_projection: "pa.Table"


_JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP = {
    JoinType.INNER: "inner",
    JoinType.LEFT_OUTER: "left outer",
    JoinType.RIGHT_OUTER: "right outer",
    JoinType.FULL_OUTER: "full outer",
    JoinType.LEFT_SEMI: "left semi",
    JoinType.RIGHT_SEMI: "right semi",
    JoinType.LEFT_ANTI: "left anti",
    JoinType.RIGHT_ANTI: "right anti",
}


logger = logging.getLogger(__name__)


class JoiningShuffleAggregation(StatefulShuffleAggregation):
    """Aggregation performing distributed joining of the 2 sequences,
    by utilising hash-based shuffling.

    Hash-based shuffling applied to 2 input sequences and employing the same
    partitioning scheme allows to

        - Accumulate identical keys from both sequences into the same
        (numerical) partition. In other words, all keys such that

            hash(key) % num_partitions = partition_id

        - Perform join on individual partitions independently (from other partitions)

    For actual joining Pyarrow native joining functionality is utilised, providing
    incredible performance while allowing keep the data from being deserialized.
    """

    def __init__(
        self,
        *,
        aggregator_id: int,
        join_type: JoinType,
        left_key_col_names: Tuple[str],
        right_key_col_names: Tuple[str],
        target_partition_ids: List[int],
        data_context: DataContext,
        left_columns_suffix: Optional[str] = None,
        right_columns_suffix: Optional[str] = None,
    ):
        super().__init__(aggregator_id)

        assert (
            len(left_key_col_names) > 0
        ), "At least 1 column to join on has to be provided"
        assert len(right_key_col_names) == len(
            left_key_col_names
        ), "Number of column for both left and right join operands has to match"

        assert join_type in _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP, (
            f"Join type is not currently supported (got: {join_type}; "  # noqa: C416
            f"supported: {[jt for jt in JoinType]})"  # noqa: C416
        )

        self._left_key_col_names: Tuple[str] = left_key_col_names
        self._right_key_col_names: Tuple[str] = right_key_col_names
        self._join_type: JoinType = join_type

        self._left_columns_suffix: Optional[str] = left_columns_suffix
        self._right_columns_suffix: Optional[str] = right_columns_suffix

        # Partition builders for the partition corresponding to
        # left and right input sequences respectively
        self._left_input_seq_partition_builders: Dict[int, ArrowBlockBuilder] = {
            partition_id: ArrowBlockBuilder() for partition_id in target_partition_ids
        }

        self._right_input_seq_partition_builders: Dict[int, ArrowBlockBuilder] = {
            partition_id: ArrowBlockBuilder() for partition_id in target_partition_ids
        }
        self.data_context = data_context

    def accept(self, input_seq_id: int, partition_id: int, partition_shard: Block):
        assert 0 <= input_seq_id < 2

        partition_builder = self._get_partition_builder(
            input_seq_id=input_seq_id,
            partition_id=partition_id,
        )

        partition_builder.add_block(partition_shard)

    def finalize(self, partition_id: int) -> Block:

        left_on, right_on = list(self._left_key_col_names), list(
            self._right_key_col_names
        )

        preprocess_result_l, preprocess_result_r = self._preprocess(
            left_on, right_on, partition_id
        )

        # Perform the join on supported columns
        arrow_join_type = _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP[self._join_type]

        # Perform the join on supported columns
        supported = preprocess_result_l.supported_projection.join(
            preprocess_result_r.supported_projection,
            join_type=arrow_join_type,
            keys=left_on,
            right_keys=right_on,
            left_suffix=self._left_columns_suffix,
            right_suffix=self._right_columns_suffix,
        )

        # Add back unsupported columns (join type logic is in should_index_* variables)
        supported = self._postprocess(
            supported,
            preprocess_result_l.unsupported_projection,
            preprocess_result_r.unsupported_projection,
        )

        return supported

    def _preprocess(
        self,
        left_on: List[str],
        right_on: List[str],
        partition_id: int,
    ) -> Tuple[_DatasetPreprocessingResult, _DatasetPreprocessingResult]:
        import pyarrow as pa

        left_seq_partition: pa.Table = self._get_partition_builder(
            input_seq_id=0, partition_id=partition_id
        ).build()

        right_seq_partition: pa.Table = self._get_partition_builder(
            input_seq_id=1, partition_id=partition_id
        ).build()

        # Get supported columns
        supported_l, unsupported_l = self._split_unsupported_columns(left_seq_partition)
        supported_r, unsupported_r = self._split_unsupported_columns(
            right_seq_partition
        )

        # Handle joins on unsupported columns
        conflicting_columns: Set[str] = set(unsupported_l.column_names) & set(left_on)
        if conflicting_columns:
            raise ValueError(
                f"Cannot join on columns with unjoinable types. "
                f"Left join key columns {conflicting_columns} have unjoinable types "
                f"(map, union, list, struct, etc.) which cannot be used for join operations."
            )

        conflicting_columns: Set[str] = set(unsupported_r.column_names) & set(right_on)
        if conflicting_columns:
            raise ValueError(
                f"Cannot join on columns with unjoinable types. "
                f"Right join key columns {conflicting_columns} have unjoinable types "
                f"(map, union, list, struct, etc.) which cannot be used for join operations."
            )

        # Index if we have unsupported columns
        should_index_l = self._should_index_side("left", supported_l, unsupported_l)
        should_index_r = self._should_index_side("right", supported_r, unsupported_r)

        # Add index columns for back-referencing if we have unsupported columns
        if should_index_l:
            supported_l = self._append_index_column(
                table=supported_l, col_name=self._index_name("left")
            )
        if should_index_r:
            supported_r = self._append_index_column(
                table=supported_r, col_name=self._index_name("right")
            )

        left = _DatasetPreprocessingResult(
            supported_projection=supported_l,
            unsupported_projection=unsupported_l,
        )
        right = _DatasetPreprocessingResult(
            supported_projection=supported_r,
            unsupported_projection=unsupported_r,
        )
        return left, right

    def _postprocess(
        self,
        supported: "pa.Table",
        unsupported_l: "pa.Table",
        unsupported_r: "pa.Table",
    ) -> "pa.Table":
        # Index if we have unsupported columns
        should_index_l = self._index_name("left") in supported.schema.names
        should_index_r = self._index_name("right") in supported.schema.names

        # Add back unsupported columns (join type logic is in should_index_* variables)
        if should_index_l:
            supported = self._add_back_unsupported_columns(
                joined_table=supported,
                unsupported_table=unsupported_l,
                index_col_name=self._index_name("left"),
            )

        if should_index_r:
            supported = self._add_back_unsupported_columns(
                joined_table=supported,
                unsupported_table=unsupported_r,
                index_col_name=self._index_name("right"),
            )

        return supported

    def _index_name(self, suffix: str) -> str:
        return f"__rd_index_level_{suffix}__"

    def clear(self, partition_id: int):
        self._left_input_seq_partition_builders.pop(partition_id)
        self._right_input_seq_partition_builders.pop(partition_id)

    def _get_partition_builder(self, *, input_seq_id: int, partition_id: int):
        if input_seq_id == 0:
            partition_builder = self._left_input_seq_partition_builders[partition_id]
        elif input_seq_id == 1:
            partition_builder = self._right_input_seq_partition_builders[partition_id]
        else:
            raise ValueError(
                f"Unexpected inpt sequence id of '{input_seq_id}' (expected 0 or 1)"
            )
        return partition_builder

    def _should_index_side(
        self, side: str, supported_table: "pa.Table", unsupported_table: "pa.Table"
    ) -> bool:
        """
        Determine whether to create an index column for a given side of the join.

        A column is "supported" if it is "joinable", and "unsupported" otherwise.
        A supported_table is a table with only "supported" columns. Index columns are
        needed when we have both supported and unsupported columns in a table, and
        that table's columns will appear in the final result.

        Args:
            side: "left" or "right" to indicate which side of the join
            supported_table: Table containing ONLY joinable columns
            unsupported_table: Table containing ONLY unjoinable columns

        Returns:
            True if an index column should be created for this side
        """
        # Must have both supported and unsupported columns to need indexing.
        # We cannot rely on row_count because it can return a non-zero row count
        # for an empty-schema.
        if not supported_table.schema or not unsupported_table.schema:
            return False

        # For semi/anti joins, only index the side that appears in the result
        if side == "left":
            # Left side appears in result for all joins except right_semi/right_anti
            return self._join_type not in [JoinType.RIGHT_SEMI, JoinType.RIGHT_ANTI]
        else:  # side == "right"
            # Right side appears in result for all joins except left_semi/left_anti
            return self._join_type not in [JoinType.LEFT_SEMI, JoinType.LEFT_ANTI]

    def _split_unsupported_columns(
        self, table: "pa.Table"
    ) -> Tuple["pa.Table", "pa.Table"]:
        """
        Split a PyArrow table into two tables based on column joinability.

        Separates columns into supported types and unsupported types that cannot be
        directly joined on but should be preserved in results.

        Args:
            table: Input PyArrow table to split

        Returns:
            Tuple of (supported_table, unsupported_table) where:
            - supported_table contains columns with primitive/joinable types
            - unsupported_table contains columns with complex/unjoinable types
        """
        supported, unsupported = [], []
        for idx in range(len(table.columns)):
            col: "pa.ChunkedArray" = table.column(idx)
            col_type: "pa.DataType" = col.type

            if _is_pa_extension_type(col_type) or self._is_pa_join_not_supported(
                col_type
            ):
                unsupported.append(idx)
            else:
                supported.append(idx)

        return table.select(supported), table.select(unsupported)

    def _add_back_unsupported_columns(
        self,
        joined_table: "pa.Table",
        unsupported_table: "pa.Table",
        index_col_name: str,
    ) -> "pa.Table":
        # Extract the index column array and drop the column from the joined table
        i = joined_table.schema.get_field_index(index_col_name)
        indices = joined_table.column(i)
        joined_table = joined_table.remove_column(i)

        # Project the unsupported columns using the indices and combine with joined table
        projected = ArrowBlockAccessor(unsupported_table).take(indices)
        return ArrowBlockAccessor(joined_table).hstack(projected)

    def _append_index_column(self, table: "pa.Table", col_name: str) -> "pa.Table":
        import numpy as np
        import pyarrow as pa

        index_col = pa.array(np.arange(table.num_rows))
        return table.append_column(col_name, index_col)

    def _is_pa_join_not_supported(self, type: "pa.DataType") -> bool:
        """
        The latest pyarrow versions do not support joins where the
        tables contain the following types below (lists,
        structs, maps, unions, extension types, etc.)

        Args:
            type: The input type of column.

        Returns:
            True if the type cannot be present (non join-key) during joins.
            False if the type can be present.
        """
        import pyarrow as pa

        pyarrow_version = get_pyarrow_version()
        is_v12 = pyarrow_version >= MIN_PYARROW_VERSION_RUN_END_ENCODED_TYPES
        is_v16 = pyarrow_version >= MIN_PYARROW_VERSION_VIEW_TYPES

        return (
            pa.types.is_map(type)
            or pa.types.is_union(type)
            or pa.types.is_list(type)
            or pa.types.is_struct(type)
            or pa.types.is_null(type)
            or pa.types.is_large_list(type)
            or pa.types.is_fixed_size_list(type)
            or (is_v12 and pa.types.is_run_end_encoded(type))
            or (
                is_v16
                and (
                    pa.types.is_binary_view(type)
                    or pa.types.is_string_view(type)
                    or pa.types.is_list_view(type)
                )
            )
        )


class JoinOperator(HashShufflingOperatorBase):
    def __init__(
        self,
        data_context: DataContext,
        left_input_op: PhysicalOperator,
        right_input_op: PhysicalOperator,
        left_key_columns: Tuple[str],
        right_key_columns: Tuple[str],
        join_type: JoinType,
        *,
        num_partitions: Optional[int] = None,
        left_columns_suffix: Optional[str] = None,
        right_columns_suffix: Optional[str] = None,
        partition_size_hint: Optional[int] = None,
        aggregator_ray_remote_args_override: Optional[Dict[str, Any]] = None,
        shuffle_aggregation_type: Optional[Type[StatefulShuffleAggregation]] = None,
    ):
        if shuffle_aggregation_type is not None:
            if not issubclass(shuffle_aggregation_type, StatefulShuffleAggregation):
                raise TypeError(
                    f"shuffle_aggregation_type must be a subclass of StatefulShuffleAggregation, "
                    f"got {shuffle_aggregation_type}"
                )

        aggregation_class = shuffle_aggregation_type or JoiningShuffleAggregation
        super().__init__(
            name_factory=(
                lambda num_partitions: f"Join(num_partitions={num_partitions})"
            ),
            input_ops=[left_input_op, right_input_op],
            data_context=data_context,
            key_columns=[left_key_columns, right_key_columns],
            num_partitions=num_partitions,
            partition_size_hint=partition_size_hint,
            partition_aggregation_factory=(
                lambda aggregator_id, target_partition_ids: aggregation_class(
                    aggregator_id=aggregator_id,
                    join_type=join_type,
                    left_key_col_names=left_key_columns,
                    right_key_col_names=right_key_columns,
                    target_partition_ids=target_partition_ids,
                    data_context=data_context,
                    left_columns_suffix=left_columns_suffix,
                    right_columns_suffix=right_columns_suffix,
                )
            ),
            aggregator_ray_remote_args_override=aggregator_ray_remote_args_override,
            shuffle_progress_bar_name="Shuffle",
            finalize_progress_bar_name="Join",
        )

    def _get_operator_num_cpus_override(self) -> float:
        return self.data_context.join_operator_actor_num_cpus_override

    @classmethod
    def _estimate_aggregator_memory_allocation(
        cls,
        *,
        num_aggregators: int,
        num_partitions: int,
        estimated_dataset_bytes: int,
    ) -> int:
        partition_byte_size_estimate = math.ceil(
            estimated_dataset_bytes / num_partitions
        )

        # Estimate of object store memory required to accommodate all partitions
        # handled by a single aggregator
        aggregator_shuffle_object_store_memory_required: int = math.ceil(
            estimated_dataset_bytes / num_aggregators
        )
        # Estimate of memory required to perform actual (in-memory) join
        # operation (inclusive of 50% overhead allocated for Pyarrow join
        # implementation)
        #
        # NOTE:
        #   - 2x due to budgeted 100% overhead of Arrow's in-memory join
        join_memory_required: int = math.ceil(partition_byte_size_estimate * 2)
        # Estimate of memory required to accommodate single partition as an output
        # (inside Object Store)
        #
        # NOTE: x2 due to 2 sequences involved in joins
        output_object_store_memory_required: int = partition_byte_size_estimate

        aggregator_total_memory_required: int = (
            # Inputs (object store)
            aggregator_shuffle_object_store_memory_required
            +
            # Join (heap)
            join_memory_required
            +
            # Output (object store)
            output_object_store_memory_required
        )

        logger.info(
            f"Estimated memory requirement for joining aggregator "
            f"(partitions={num_partitions}, "
            f"aggregators={num_aggregators}, "
            f"dataset (estimate)={estimated_dataset_bytes / GiB:.1f}GiB): "
            f"shuffle={aggregator_shuffle_object_store_memory_required / MiB:.1f}MiB, "
            f"joining={join_memory_required / MiB:.1f}MiB, "
            f"output={output_object_store_memory_required / MiB:.1f}MiB, "
            f"total={aggregator_total_memory_required / MiB:.1f}MiB, "
        )

        return aggregator_total_memory_required
