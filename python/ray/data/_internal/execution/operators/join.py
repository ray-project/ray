"""Execution operators for join operations in Ray Data.

This module provides the physical execution operators for join operations, including
the JoiningShuffleAggregation class which handles distributed joining using hash-based
shuffling and PyArrow's native join functionality.
"""

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
    """Aggregation performing distributed joining of two sequences using hash-based
    shuffling.

    This class implements distributed join operations by utilizing hash-based shuffling
    to ensure that identical keys from both sequences are accumulated into the same
    partition. This allows join operations to be performed independently on each
    partition.

    Hash-based shuffling applied to two input sequences and employing the same
    partitioning scheme allows:

        - Accumulate identical keys from both sequences into the same
        (numerical) partition. In other words, all keys such that

            hash(key) % num_partitions = partition_id

        - Perform join on individual partitions independently (from other partitions)

    For actual joining, PyArrow's native joining functionality is utilized, providing
    incredible performance while allowing the data to remain deserialized.

    Examples:
        .. testcode::

            # Create a joining shuffle aggregation
            aggregation = JoiningShuffleAggregation(
                aggregator_id=0,
                join_type=JoinType.INNER,
                left_key_col_names=("id",),
                right_key_col_names=("id",),
                target_partition_ids=[0, 1, 2],
                data_context=DataContext.get_current()
            )
    """

    def __init__(
        self,
        *,
        aggregator_id: int,
        join_type: JoinType,
        left_key_col_names: Tuple[str, ...],
        right_key_col_names: Tuple[str, ...],
        target_partition_ids: List[int],
        data_context: DataContext,
        left_columns_suffix: Optional[str] = None,
        right_columns_suffix: Optional[str] = None,
    ):
        """Initialize the JoiningShuffleAggregation.

        Args:
            aggregator_id: Unique identifier for this aggregator instance.
            join_type: Type of join to perform (inner, outer, semi, anti).
            left_key_col_names: Column names to use as join keys from the left
                dataset.
            right_key_col_names: Column names to use as join keys from the right
                dataset.
            target_partition_ids: List of partition IDs to target for this aggregation.
            data_context: Ray Data context for configuration and settings.
            left_columns_suffix: Optional suffix for left dataset columns to avoid
                conflicts.
            right_columns_suffix: Optional suffix for right dataset columns to avoid
                conflicts.

        Raises:
            AssertionError: If invalid parameters are provided.
        """
        super().__init__(aggregator_id)

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
        """Accept a partition shard from one of the input sequences.

        Args:
            input_seq_id: Identifier for the input sequence (0 for left, 1 for right).
            partition_id: Identifier for the target partition.
            partition_shard: Block of data to be added to the partition.

        Raises:
            AssertionError: If input_seq_id is not 0 or 1.
        """
        assert 0 <= input_seq_id < 2

        partition_builder = self._get_partition_builder(
            input_seq_id=input_seq_id,
            partition_id=partition_id,
        )

        partition_builder.add_block(partition_shard)

    def finalize(self, partition_id: int) -> Block:
        """Finalize the join operation for a specific partition.

        This method builds the final joined result for the specified partition by
        performing a PyArrow join between the accumulated left and right partition data.

        Args:
            partition_id: Identifier for the partition to finalize.

        Returns:
            Block containing the joined data for the specified partition.
        """
        import pyarrow as pa

        left_seq_partition: pa.Table = self._get_partition_builder(
            input_seq_id=0, partition_id=partition_id
        ).build()
        right_seq_partition: pa.Table = self._get_partition_builder(
            input_seq_id=1, partition_id=partition_id
        ).build()

        left_on, right_on = (
            list(self._left_key_col_names),
            list(self._right_key_col_names),
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
        """Clear the partition builders for a specific partition.

        This method removes the partition builders for the specified partition,
        freeing up memory after the join operation is complete.

        Args:
            partition_id: Identifier for the partition to clear.
        """
        self._left_input_seq_partition_builders.pop(partition_id)
        self._right_input_seq_partition_builders.pop(partition_id)

    def _get_partition_builder(self, *, input_seq_id: int, partition_id: int):
        """Get the appropriate partition builder for the given input sequence and
        partition.

        Args:
            input_seq_id: Identifier for the input sequence (0 for left, 1 for right).
            partition_id: Identifier for the target partition.

        Returns:
            ArrowBlockBuilder for the specified input sequence and partition.

        Raises:
            ValueError: If input_seq_id is not 0 or 1.
        """
        if input_seq_id == 0:
            partition_builder = self._left_input_seq_partition_builders[partition_id]
        elif input_seq_id == 1:
            partition_builder = self._right_input_seq_partition_builders[partition_id]
        else:
            raise ValueError(
                f"Unexpected input sequence id of '{input_seq_id}' (expected 0 or 1)"
            )
        return partition_builder


class JoinOperator(HashShufflingOperatorBase):
    """Physical operator for executing join operations in Ray Data.

    This operator handles the physical execution of join operations by coordinating
    hash-based shuffling and aggregation to perform distributed joins between datasets.
    It supports various join types including inner, outer, semi, and anti joins.

    Examples:
        .. testcode::

            # Create sample datasets for demonstration
            import ray
            from ray.data import from_items
            from ray.data._internal.logical.operators.join_operator import JoinType

            # Create left dataset
            left_data = [{"id": i, "value": f"left_{i}"} for i in range(5)]
            left_ds = from_items(left_data)

            # Create right dataset
            right_data = [{"id": i, "value": f"right_{i}"} for i in range(3, 8)]
            right_ds = from_items(right_data)

            # Get physical operators from the datasets
            left_physical_op = left_ds._physical_plan.dag
            right_physical_op = right_ds._physical_plan.dag

            # Create a join operator
            join_op = JoinOperator(
                data_context=DataContext.get_current(),
                left_input_op=left_physical_op,
                right_input_op=right_physical_op,
                left_key_columns=("id",),
                right_key_columns=("id",),
                join_type=JoinType.INNER,
                num_partitions=4
            )
    """

    def __init__(
        self,
        data_context: DataContext,
        left_input_op: PhysicalOperator,
        right_input_op: PhysicalOperator,
        left_key_columns: Tuple[str, ...],
        right_key_columns: Tuple[str, ...],
        join_type: JoinType,
        *,
        num_partitions: int,
        left_columns_suffix: Optional[str] = None,
        right_columns_suffix: Optional[str] = None,
        partition_size_hint: Optional[int] = None,
        aggregator_ray_remote_args_override: Optional[Dict[str, Any]] = None,
        shuffle_aggregation_type: Optional[Type[StatefulShuffleAggregation]] = None,
    ):
        """Initialize the JoinOperator.

        Args:
            data_context: Ray Data context for configuration and settings.
            left_input_op: Physical operator for the left input dataset.
            right_input_op: Physical operator for the right input dataset.
            left_key_columns: Column names to use as join keys from the left dataset.
            right_key_columns: Column names to use as join keys from the right dataset.
            join_type: Type of join to perform.
            num_partitions: Number of output partitions to generate.
            left_columns_suffix: Optional suffix for left dataset columns to avoid
                conflicts.
            right_columns_suffix: Optional suffix for right dataset columns to avoid
                conflicts.
            partition_size_hint: Hint about the estimated size of resulting partitions.
            aggregator_ray_remote_args_override: Optional Ray remote arguments override.
            shuffle_aggregation_type: Optional custom shuffle aggregation class.

        Raises:
            TypeError: If shuffle_aggregation_type is not a valid subclass.
        """
        # Runtime validation (still recommended even with type hints)
        if shuffle_aggregation_type is not None:
            if not issubclass(shuffle_aggregation_type, StatefulShuffleAggregation):
                raise TypeError(
                    f"shuffle_aggregation_type must be a subclass of "
                    f"StatefulShuffleAggregation, got {shuffle_aggregation_type}"
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
        """Get the default number of CPUs to allocate per partition.

        This method provides a reasonable default for CPU allocation based on
        the join operation characteristics.

        Returns:
            Default number of CPUs per partition.
        """
        return 1

    def _get_operator_num_cpus_per_partition_override(self) -> int:
        """Get the operator-specific CPU allocation override per partition.

        Returns:
            Number of CPUs per partition override from the data context.
        """
        return self.data_context.join_operator_actor_num_cpus_per_partition_override

    @classmethod
    def _estimate_aggregator_memory_allocation(
        cls,
        *,
        num_aggregators: int,
        num_partitions: int,
        partition_byte_size_estimate: int,
    ) -> int:
        """Estimate memory allocation required for join aggregators.

        This method calculates the estimated memory requirements for join aggregators
        based on the number of partitions, partition sizes, and join operation
        overhead.

        Args:
            num_aggregators: Number of aggregator actors to distribute the work.
            num_partitions: Total number of partitions to process.
            partition_byte_size_estimate: Estimated size of each partition in bytes.

        Returns:
            Estimated total memory requirement in bytes for a single aggregator.

        Note:
            The memory estimation includes:
            - Shuffle memory for input partitions (2x due to left/right sequences)
            - Join operation memory (3x partition size due to PyArrow overhead)
            - Output memory for joined results (2x due to left/right sequences)
        """
        dataset_size = num_partitions * partition_byte_size_estimate

        # Estimate of object store memory required to accommodate all partitions
        # handled by a single aggregator
        #
        # NOTE: x2 due to 2 sequences involved in joins
        aggregator_shuffle_object_store_memory_required: int = math.ceil(
            2 * dataset_size / num_aggregators
        )

        # Estimate of memory required to perform actual (in-memory) join
        # operation (inclusive of 50% overhead allocated for PyArrow join
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
