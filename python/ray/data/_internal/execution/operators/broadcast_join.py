import logging
from typing import Optional, Tuple

import ray
from ray.data._internal.execution.interfaces import PhysicalOperator
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
        from ray.data._internal.arrow_block import ArrowBlockAccessor

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


class BroadcastJoinOperator(MapOperator):
    """Physical operator for broadcast joins using map_batches."""

    def __init__(
        self,
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
        """Initialize the broadcast join operator.

        The approach is to:
        1. Execute the right input operator to get its data
        2. Convert the right data to PyArrow and broadcast it
        3. Use MapOperator on the left input with the broadcast join function
        """
        from ray.data._internal.compute import ActorPoolStrategy
        from ray.data._internal.execution.operators.map_transformer import (
            MapTransformer,
            BatchUDFMapTransformFn,
        )

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

        # Use ActorPoolStrategy with concurrency equal to num_partitions
        compute_strategy = ActorPoolStrategy(size=num_partitions)

        # Create batch UDF transform function
        batch_fn = BatchUDFMapTransformFn(
            fn=join_fn,
            batch_size=None,  # Use entire blocks as batches
            batch_format="pyarrow",
            zero_copy_batch=False,
            fn_args=None,
            fn_kwargs=None,
            fn_constructor_args=None,
            fn_constructor_kwargs=None,
        )

        map_transformer = MapTransformer(
            [batch_fn],
            compute_strategy,
            data_context.min_parallelism,
            ray_remote_args={},
            ray_remote_args_fn=None,
        )

        # Initialize as a MapOperator
        super().__init__(
            map_transformer=map_transformer,
            data_context=data_context,
            input_op=left_input_op,
            name=f"BroadcastJoin(num_partitions={num_partitions})",
            target_max_block_size=None,
            min_rows_per_bundled_input=None,
            ray_remote_args={},
        )
