import logging
from typing import Any, Dict, List, Optional, Tuple

from ray.anyscale.data._internal.execution.operators.hash_shuffle import (
    HashShufflingOperatorBase,
    StatefulShuffleAggregation,
)
from ray.anyscale.data._internal.logical.operators.join_operator import JoinType
from ray.data import DataContext
from ray.data._internal.arrow_block import ArrowBlockBuilder
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data.block import Block

_JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP = {
    JoinType.INNER: "inner",
    JoinType.LEFT_OUTER: "left outer",
    JoinType.RIGHT_OUTER: "right outer",
    JoinType.FULL_OUTER: "full outer",
}


logger = logging.getLogger(__name__)


class JoiningShuffleAggregation(StatefulShuffleAggregation):
    def __init__(
        self,
        *,
        aggregator_id: int,
        join_type: JoinType,
        left_key_col_names: Tuple[str],
        right_key_col_names: Tuple[str],
        target_partition_ids: List[int],
    ):
        super().__init__(aggregator_id)

        assert (
            len(left_key_col_names) > 0
        ), "At least 1 column to join on has to be provided"
        assert len(right_key_col_names) == len(
            left_key_col_names
        ), "Number of column for both left and right join operands has to match"

        assert _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP[join_type], (
            f"Join type is not currently supported (got: {join_type}; "  # noqa: C416
            f"supported: {[jt for jt in JoinType]})"  # noqa: C416
        )

        self._left_key_col_names: Tuple[str] = left_key_col_names
        self._right_key_col_names: Tuple[str] = right_key_col_names
        self._join_type: JoinType = join_type

        # Partition builders for the partition corresponding to
        # left and right input sequences respectively
        self._left_input_seq_partition_builders: Dict[int, ArrowBlockBuilder] = {
            partition_id: ArrowBlockBuilder() for partition_id in target_partition_ids
        }

        self._right_input_seq_partition_builders: Dict[int, ArrowBlockBuilder] = {
            partition_id: ArrowBlockBuilder() for partition_id in target_partition_ids
        }

    def accept(self, input_seq_id: int, partition_id: int, partition_shard: Block):
        assert 0 <= input_seq_id < 2

        # TODO clean up
        logger.debug(
            f">>> [DBG] Accepting partition shard input_seq={input_seq_id}, "
            f"partition={partition_id} (rows={partition_shard.num_rows})"
        )

        partition_builder = self._get_partition_builder(
            input_seq_id=input_seq_id,
            partition_id=partition_id,
        )

        partition_builder.add_block(partition_shard)

        # TODO clean up
        logger.debug(
            f">>> [DBG] Accepted partition shard input_seq={input_seq_id}, "
            f"partition={partition_id} (rows={partition_shard.num_rows})"
        )

    def finalize(self, partition_id: int) -> Block:
        import pyarrow as pa

        # TODO clean up
        logger.debug(f">>> [DBG] Finalizing partition for partition={partition_id}")

        left_seq_partition: pa.Table = self._get_partition_builder(
            input_seq_id=0, partition_id=partition_id
        ).build()
        right_seq_partition: pa.Table = self._get_partition_builder(
            input_seq_id=1, partition_id=partition_id
        ).build()

        arrow_join_type = _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP[self._join_type]

        # TODO use threads?
        joined = left_seq_partition.join(
            right_seq_partition,
            join_type=arrow_join_type,
            keys=list(self._left_key_col_names),
            right_keys=(list(self._right_key_col_names)),
        )

        # TODO clean up
        logger.debug(
            f">>> [DBG] Finalized partition {partition_id} (rows={joined.num_rows})"
        )

        return joined

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
        aggregator_ray_remote_args_override: Optional[Dict[str, Any]] = None,
    ):
        super().__init__(
            name=f"Join(num_partitions={num_partitions})",
            input_ops=[left_input_op, right_input_op],
            data_context=data_context,
            key_columns=[left_key_columns, right_key_columns],
            num_partitions=num_partitions,
            partition_aggregation_factory=(
                lambda aggregator_id, target_partition_ids: JoiningShuffleAggregation(
                    aggregator_id=aggregator_id,
                    left_key_col_names=left_key_columns,
                    right_key_col_names=right_key_columns,
                    join_type=join_type,
                    target_partition_ids=target_partition_ids,
                )
            ),
            aggregator_ray_remote_args_override=aggregator_ray_remote_args_override,
        )

    @classmethod
    def _get_default_aggregator_ray_remote_args(
        cls, *, num_partitions: int, num_aggregators: int
    ):
        shuffle_base_remote_args = super()._get_default_aggregator_ray_remote_args(
            num_partitions=num_partitions,
            num_aggregators=num_aggregators,
        )

        return {
            **shuffle_base_remote_args,
            # TODO elaborate (avoid co-locating large number of aggregators onto the
            #      same node to prevent spilling/OOMs/OODs)
            "scheduling_strategy": "SPREAD",
        }
