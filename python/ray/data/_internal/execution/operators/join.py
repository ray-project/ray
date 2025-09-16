import logging
import math
from typing import Any, Dict, List, Optional, Tuple, Type

from ray.data import DataContext
from ray.data._internal.arrow_block import ArrowBlockBuilder
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.hash_shuffle import (
    HashShufflingOperatorBase,
    StatefulShuffleAggregation,
)
from ray.data._internal.logical.operators.join_operator import JoinType
from ray.data._internal.util import GiB
from ray.data.block import Block

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
        import pyarrow as pa

        left_seq_partition: pa.Table = self._get_partition_builder(
            input_seq_id=0, partition_id=partition_id
        ).build()
        right_seq_partition: pa.Table = self._get_partition_builder(
            input_seq_id=1, partition_id=partition_id
        ).build()

        left_on, right_on = list(self._left_key_col_names), list(
            self._right_key_col_names
        )

        arrow_join_type = _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP[self._join_type]
        joined = left_seq_partition.join(
            right_seq_partition,
            join_type=arrow_join_type,
            keys=left_on,
            right_keys=right_on,
            left_suffix=self._left_columns_suffix,
            right_suffix=self._right_columns_suffix,
        )

        return joined

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
        num_partitions: int,
        left_columns_suffix: Optional[str] = None,
        right_columns_suffix: Optional[str] = None,
        partition_size_hint: Optional[int] = None,
        aggregator_ray_remote_args_override: Optional[Dict[str, Any]] = None,
        shuffle_aggregation_type: Optional[Type[StatefulShuffleAggregation]] = None,
    ):
        # Runtime validation (still recommended even with type hints)
        if shuffle_aggregation_type is not None:
            if not issubclass(shuffle_aggregation_type, StatefulShuffleAggregation):
                raise TypeError(
                    f"shuffle_aggregation_type must be a subclass of StatefulShuffleAggregation, "
                    f"got {shuffle_aggregation_type}"
                )

        aggregation_class = shuffle_aggregation_type or JoiningShuffleAggregation

        super().__init__(
            name=f"Join(num_partitions={num_partitions})",
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

    def _get_default_num_cpus_per_partition(self) -> int:
        """
        CPU allocation for aggregating actors of Join operator is calculated as:
        num_cpus (per partition) = CPU budget / # partitions

        Assuming:
        - Default number of partitions: 64
        - Total operator's CPU budget with default settings: 8 cores
        - Number of CPUs per partition: 8 / 64 = 0.125

        These CPU budgets are derived such that Ray Data pipeline could run on a
        single node (using the default settings).
        """
        return 0.125

    def _get_operator_num_cpus_per_partition_override(self) -> int:
        return self.data_context.join_operator_actor_num_cpus_per_partition_override

    @classmethod
    def _estimate_aggregator_memory_allocation(
        cls,
        *,
        num_aggregators: int,
        num_partitions: int,
        partition_byte_size_estimate: int,
    ) -> int:
        dataset_size = num_partitions * partition_byte_size_estimate
        # Estimate of object store memory required to accommodate all partitions
        # handled by a single aggregator
        #
        # NOTE: x2 due to 2 sequences involved in joins
        aggregator_shuffle_object_store_memory_required: int = math.ceil(
            2 * dataset_size / num_aggregators
        )
        # Estimate of memory required to perform actual (in-memory) join
        # operation (inclusive of 50% overhead allocated for Pyarrow join
        # implementation)
        #
        # NOTE:
        #   - x2 due to 2 partitions (from left/right sequences)
        #   - x1.5 due to 50% overhead of in-memory join
        join_memory_required: int = math.ceil(partition_byte_size_estimate * 3)
        # Estimate of memory required to accommodate single partition as an output
        # (inside Object Store)
        #
        # NOTE: x2 due to 2 sequences involved in joins
        output_object_store_memory_required: int = 2 * partition_byte_size_estimate

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

        logger.debug(
            f"Estimated memory requirement for joining aggregator "
            f"(partitions={num_partitions}, aggregators={num_aggregators}): "
            f"shuffle={aggregator_shuffle_object_store_memory_required / GiB:.2f}GiB, "
            f"joining={join_memory_required / GiB:.2f}GiB, "
            f"output={output_object_store_memory_required / GiB:.2f}GiB, "
            f"total={aggregator_total_memory_required / GiB:.2f}GiB, "
        )

        return aggregator_total_memory_required
