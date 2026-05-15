import logging
import math
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Set, Tuple, Type

from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.arrow_ops.transform_pyarrow import (
    MIN_PYARROW_VERSION_RUN_END_ENCODED_TYPES,
    MIN_PYARROW_VERSION_VIEW_TYPES,
)
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.hash_shuffle import (
    HashShufflingOperatorBase,
    ShuffleAggregation,
    _combine,
)
from ray.data._internal.logical.operators import JoinType
from ray.data._internal.util import GiB, MiB
from ray.data._internal.utils.arrow_utils import get_pyarrow_version
from ray.data._internal.utils.transform_pyarrow import _is_pa_extension_type
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


class JoiningAggregation(ShuffleAggregation):
    """Stateless aggregation for distributed joining of 2 sequences.

    This implementation performs hash-based distributed joining by:
        - Accumulating identical keys from both sequences into the same partition
        - Performing join on individual partitions independently

    For actual joining, Pyarrow native joining functionality is utilised.
    """

    def __init__(
        self,
        *,
        join_type: JoinType,
        left_key_col_names: Tuple[str, ...],
        right_key_col_names: Tuple[str, ...],
        left_columns_suffix: Optional[str] = None,
        right_columns_suffix: Optional[str] = None,
        data_context: DataContext,
    ):
        assert (
            len(left_key_col_names) > 0
        ), "At least 1 column to join on has to be provided"
        assert len(right_key_col_names) == len(
            left_key_col_names
        ), "Number of columns for both left and right join operands has to match"

        assert join_type in _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP, (
            f"Join type is not currently supported (got: {join_type}; "  # noqa: C416
            f"supported: {[jt for jt in JoinType]})"  # noqa: C416
        )

        self._left_key_col_names: Tuple[str, ...] = left_key_col_names
        self._right_key_col_names: Tuple[str, ...] = right_key_col_names
        self._join_type: JoinType = join_type

        self._left_columns_suffix: Optional[str] = left_columns_suffix
        self._right_columns_suffix: Optional[str] = right_columns_suffix

    def finalize(self, partition_shards_map: Dict[int, List[Block]]) -> Iterator[Block]:
        """Performs join on blocks from left (seq 0) and right (seq 1) sequences."""

        assert (
            len(partition_shards_map) == 2
        ), f"Two input-sequences are expected (got {len(partition_shards_map)})"

        left_partition_shards = partition_shards_map[0]
        right_partition_shards = partition_shards_map[1]

        left_table = _combine(left_partition_shards)
        right_table = _combine(right_partition_shards)

        yield join_tables(
            left_table,
            right_table,
            join_type=self._join_type,
            left_key_col_names=self._left_key_col_names,
            right_key_col_names=self._right_key_col_names,
            left_columns_suffix=self._left_columns_suffix,
            right_columns_suffix=self._right_columns_suffix,
        )


def join_tables(
    left_table: "pa.Table",
    right_table: "pa.Table",
    *,
    join_type: JoinType,
    left_key_col_names: Tuple[str, ...],
    right_key_col_names: Tuple[str, ...],
    left_columns_suffix: Optional[str] = None,
    right_columns_suffix: Optional[str] = None,
) -> "pa.Table":
    """Apply preprocess -> ``pa.Table.join`` -> postprocess to two input tables.

    Shared between the physical executor (``JoiningAggregation.finalize``)
    and plan-time schema inference (``Join.infer_schema``), which calls
    this with empty tables built from the input schemas. Plan-time and
    runtime schemas therefore agree by construction.
    """
    left_on = list(left_key_col_names)
    right_on = list(right_key_col_names)

    # Eagerly validate suffix conflicts so callers get a clear error instead
    # of the opaque PyArrow schema-merge error ('Field X exists 2 times').
    # Skip for semi/anti joins: only one side's columns appear in the result,
    # so overlapping non-key names between left and right are harmless.
    if join_type not in (
        JoinType.LEFT_SEMI,
        JoinType.LEFT_ANTI,
        JoinType.RIGHT_SEMI,
        JoinType.RIGHT_ANTI,
    ):
        left_cols = set(left_table.schema.names)
        # PyArrow drops right key columns from output (coalescing them into
        # the left keys), so only right non-key columns can collide with
        # left columns. Subtracting only right_on (not left_on) correctly
        # handles asymmetric key names (left_on != right_on).
        right_output_cols = set(right_table.schema.names) - set(right_on)
        collisions = left_cols & right_output_cols
        if left_columns_suffix is None and right_columns_suffix is None and collisions:
            raise ValueError(
                "Left and right columns suffixes cannot be both None "
                f"(overlapping columns: {sorted(collisions)})"
            )

    # Preprocess: split unsupported columns and add index columns if needed
    preprocess_result_l, preprocess_result_r = _preprocess_for_join(
        left_table, right_table, left_on, right_on, join_type
    )

    # Perform the join on supported columns
    arrow_join_type = _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP[join_type]

    supported = preprocess_result_l.supported_projection.join(
        preprocess_result_r.supported_projection,
        join_type=arrow_join_type,
        keys=left_on,
        right_keys=right_on,
        left_suffix=left_columns_suffix,
        right_suffix=right_columns_suffix,
    )

    # Add back unsupported columns
    return _postprocess_join_result(
        supported,
        preprocess_result_l.unsupported_projection,
        preprocess_result_r.unsupported_projection,
    )


def _preprocess_for_join(
    left_table: "pa.Table",
    right_table: "pa.Table",
    left_on: List[str],
    right_on: List[str],
    join_type: JoinType,
) -> Tuple[_DatasetPreprocessingResult, _DatasetPreprocessingResult]:
    """Split inputs into supported/unsupported columns and add indices."""
    supported_l, unsupported_l = _split_unsupported_columns(left_table)
    supported_r, unsupported_r = _split_unsupported_columns(right_table)

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
    should_index_l = _should_index_side("left", supported_l, unsupported_l, join_type)
    should_index_r = _should_index_side("right", supported_r, unsupported_r, join_type)

    # Add index columns for back-referencing if we have unsupported columns
    if should_index_l:
        supported_l = _append_index_column(
            table=supported_l, col_name=_index_name("left")
        )
    if should_index_r:
        supported_r = _append_index_column(
            table=supported_r, col_name=_index_name("right")
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


def _postprocess_join_result(
    supported: "pa.Table",
    unsupported_l: "pa.Table",
    unsupported_r: "pa.Table",
) -> "pa.Table":
    """Re-attach unsupported columns to the joined table via the index column."""
    should_index_l = _index_name("left") in supported.schema.names
    should_index_r = _index_name("right") in supported.schema.names

    if should_index_l:
        supported = _add_back_unsupported_columns(
            joined_table=supported,
            unsupported_table=unsupported_l,
            index_col_name=_index_name("left"),
        )

    if should_index_r:
        supported = _add_back_unsupported_columns(
            joined_table=supported,
            unsupported_table=unsupported_r,
            index_col_name=_index_name("right"),
        )

    return supported


def _index_name(suffix: str) -> str:
    return f"__rd_index_level_{suffix}__"


def _should_index_side(
    side: str,
    supported_table: "pa.Table",
    unsupported_table: "pa.Table",
    join_type: JoinType,
) -> bool:
    """Determine whether to create an index column for a given side of the join."""
    # Must have both supported and unsupported columns to need indexing.
    # We cannot rely on row_count because it can return a non-zero row count
    # for an empty-schema.
    if not supported_table.schema or not unsupported_table.schema:
        return False

    # For semi/anti joins, only index the side that appears in the result
    if side == "left":
        # Left side appears in result for all joins except right_semi/right_anti
        return join_type not in [JoinType.RIGHT_SEMI, JoinType.RIGHT_ANTI]
    else:  # side == "right"
        # Right side appears in result for all joins except left_semi/left_anti
        return join_type not in [JoinType.LEFT_SEMI, JoinType.LEFT_ANTI]


def _split_unsupported_columns(
    table: "pa.Table",
) -> Tuple["pa.Table", "pa.Table"]:
    """Split a PyArrow table into supported / unsupported (for joins) columns."""
    supported, unsupported = [], []
    for idx in range(len(table.columns)):
        col: "pa.ChunkedArray" = table.column(idx)
        col_type: "pa.DataType" = col.type

        if _is_pa_extension_type(col_type) or _is_pa_join_not_supported(col_type):
            unsupported.append(idx)
        else:
            supported.append(idx)

    return table.select(supported), table.select(unsupported)


def _add_back_unsupported_columns(
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


def _append_index_column(table: "pa.Table", col_name: str) -> "pa.Table":
    import numpy as np
    import pyarrow as pa

    index_col = pa.array(np.arange(table.num_rows))
    return table.append_column(col_name, index_col)


def _is_pa_join_not_supported(type: "pa.DataType") -> bool:
    """The latest pyarrow versions do not support joins on certain types
    (lists, structs, maps, unions, extension types, etc.)."""
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
        shuffle_aggregation_type: Optional[Type[ShuffleAggregation]] = None,
    ):
        # Use new stateless JoiningAggregation factory
        def _create_joining_aggregation() -> JoiningAggregation:
            if shuffle_aggregation_type is not None:
                if not issubclass(shuffle_aggregation_type, ShuffleAggregation):
                    raise TypeError(
                        f"shuffle_aggregation_type must be a subclass of {ShuffleAggregation}, "
                        f"got {shuffle_aggregation_type}"
                    )

            aggregation_class = shuffle_aggregation_type or JoiningAggregation

            return aggregation_class(
                join_type=join_type,
                left_key_col_names=left_key_columns,
                right_key_col_names=right_key_columns,
                left_columns_suffix=left_columns_suffix,
                right_columns_suffix=right_columns_suffix,
                data_context=data_context,
            )

        super().__init__(
            name_factory=(
                lambda num_partitions: f"Join(num_partitions={num_partitions})"
            ),
            input_ops=[left_input_op, right_input_op],
            data_context=data_context,
            key_columns=[left_key_columns, right_key_columns],
            num_input_seqs=2,
            num_partitions=num_partitions,
            partition_size_hint=partition_size_hint,
            partition_aggregation_factory=_create_joining_aggregation,
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
