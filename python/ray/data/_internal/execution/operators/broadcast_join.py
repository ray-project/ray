import logging
from typing import Optional, Tuple

import ray
from ray.data._internal.arrow_block import ArrowBlockAccessor
from ray.data._internal.execution.interfaces import (
    ExecutionResources,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.operators.join_operator import JoinType
from ray.data.block import DataBatch

_JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP = {
    JoinType.INNER: "inner",
    JoinType.LEFT_OUTER: "left outer",
    JoinType.RIGHT_OUTER: "right outer",
    JoinType.FULL_OUTER: "full outer",
}

logger = logging.getLogger(__name__)


class BroadcastJoinFunction:
    """A callable class that performs broadcast joins using PyArrow.

    The right dataset is materialized and broadcasted on first use.
    Each call performs a PyArrow join on a batch from the left dataset.
    """

    def __init__(
        self,
        right_input_op: PhysicalOperator,
        data_context,
        join_type: JoinType,
        left_key_columns: Tuple[str],
        right_key_columns: Tuple[str],
        left_columns_suffix: Optional[str] = None,
        right_columns_suffix: Optional[str] = None,
    ):
        """Initialize the broadcast join function.

        Args:
            right_input_op: Physical operator for the right dataset
            data_context: Ray Data context
            join_type: Type of join to perform
            left_key_columns: Join keys for left dataset
            right_key_columns: Join keys for right dataset
            left_columns_suffix: Suffix for left columns
            right_columns_suffix: Suffix for right columns
        """
        self.join_type = join_type
        self.left_key_columns = left_key_columns
        self.right_key_columns = right_key_columns
        self.left_columns_suffix = left_columns_suffix
        self.right_columns_suffix = right_columns_suffix

        # Materialize the right table immediately and broadcast it
        logger.info("Materializing right dataset for broadcast join")

        # Execute the right input operator to get all its ref bundles
        right_ref_bundles = list(right_input_op.get_work_refs())

        # Collect all the blocks
        right_blocks = [
            ray.get(block_ref)
            for ref_bundle in right_ref_bundles
            for block_ref, _ in ref_bundle.blocks
        ]

        # Combine all blocks into a single PyArrow table
        import pyarrow as pa

        right_tables = [
            ArrowBlockAccessor.for_block(block).to_arrow() for block in right_blocks
        ]

        if right_tables:
            right_table = pa.concat_tables(right_tables)
        else:
            # Empty right table - create empty table with schema if possible
            schema = right_input_op.schema()
            right_table = (
                pa.Table.from_pydict({}, schema=schema) if schema else pa.table({})
            )

        self.right_table = right_table

    def __call__(self, batch: DataBatch) -> DataBatch:
        """Perform PyArrow join on a batch from the left dataset.

        Args:
            batch: Batch from left dataset

        Returns:
            Joined batch
        """
        import pyarrow as pa

        # Convert left batch to PyArrow table
        if isinstance(batch, dict):
            left_table = pa.table(batch)
        else:
            left_table = batch

        # Perform the join
        arrow_join_type = _JOIN_TYPE_TO_ARROW_JOIN_VERB_MAP[self.join_type]
        joined_table = left_table.join(
            self.right_table,
            join_type=arrow_join_type,
            keys=list(self.left_key_columns),
            right_keys=list(self.right_key_columns),
            left_suffix=self.left_columns_suffix,
            right_suffix=self.right_columns_suffix,
        )

        return joined_table


def BroadcastJoinOperator(
    data_context,
    left_input_op: PhysicalOperator,
    right_input_op: PhysicalOperator,
    left_key_columns: Tuple[str],
    right_key_columns: Tuple[str],
    join_type: JoinType,
    num_partitions: int,
    left_columns_suffix: Optional[str] = None,
    right_columns_suffix: Optional[str] = None,
):
    """Factory function that creates a broadcast join operator.

    The approach is to:
    1. Execute the right input operator to get its data
    2. Convert the right data to PyArrow and broadcast it
    3. Use MapOperator on the left input with the broadcast join function
    """
    from ray.data._internal.compute import ActorPoolStrategy
    from ray.data._internal.execution.operators.map_transformer import (
        BatchMapTransformFn,
        BlocksToBatchesMapTransformFn,
        BuildOutputBlocksMapTransformFn,
        MapTransformCallable,
        MapTransformer,
    )
    from ray.data._internal.execution.interfaces.task_context import TaskContext
    from ray.data.block import DataBatch

    # Create the broadcast join function that materializes the right dataset immediately
    join_fn = BroadcastJoinFunction(
        right_input_op=right_input_op,
        data_context=data_context,
        join_type=join_type,
        left_key_columns=left_key_columns,
        right_key_columns=right_key_columns,
        left_columns_suffix=left_columns_suffix,
        right_columns_suffix=right_columns_suffix,
    )

    # Create a composite transform function that applies the join function to batches
    # following the same pattern as in plan_udf_map_op.py
    def batch_join_fn(
        batches: "Iterable[DataBatch]", _: TaskContext
    ) -> "Iterable[DataBatch]":
        for batch in batches:
            yield join_fn(batch)

    # Use the same pattern as _create_map_transformer_for_map_batches_op
    transform_fns = [
        # Convert input blocks to batches.
        BlocksToBatchesMapTransformFn(
            batch_size=None,  # Use entire blocks as batches
            batch_format="pyarrow",
            zero_copy_batch=False,
        ),
        # Apply the UDF.
        BatchMapTransformFn(batch_join_fn, is_udf=True),
        # Convert output batches to blocks.
        BuildOutputBlocksMapTransformFn.for_batches(),
    ]
    map_transformer = MapTransformer(transform_fns)

    # Use ActorPoolStrategy with concurrency equal to num_partitions
    compute_strategy = ActorPoolStrategy(size=num_partitions)

    # Create and return the MapOperator
    return MapOperator.create(
        map_transformer=map_transformer,
        input_op=left_input_op,
        data_context=data_context,
        target_max_block_size=None,
        name=f"BroadcastJoin(num_partitions={num_partitions})",
        compute_strategy=compute_strategy,
        min_rows_per_bundle=None,
        supports_fusion=False,
        map_task_kwargs=None,
        ray_remote_args_fn=None,
        ray_remote_args={},
    )
