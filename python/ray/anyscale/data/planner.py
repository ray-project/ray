from typing import List

from ray.anyscale.data._internal.execution.operators.join import JoinOperator
from ray.anyscale.data._internal.execution.operators.streaming_hash_aggregate import (
    StreamingHashAggregate,
)
from ray.anyscale.data._internal.logical.operators.join_operator import Join
from ray.anyscale.data._internal.logical.operators.list_files_operator import ListFiles
from ray.anyscale.data._internal.logical.operators.partition_files_operator import (
    PartitionFiles,
)
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.anyscale.data._internal.logical.operators.streaming_aggregate import (
    StreamingAggregate,
)
from ray.anyscale.data._internal.planner.plan_list_files_op import plan_list_files_op
from ray.anyscale.data._internal.planner.plan_partition_files_op import (
    plan_partition_files_op,
)
from ray.anyscale.data._internal.planner.plan_read_files_op import plan_read_files_op
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.planner.planner import register_plan_logical_op_fn
from ray.data.context import DataContext


def _register_anyscale_plan_logical_op_fns():
    def plan_streaming_aggregate(
        logical_op: StreamingAggregate,
        physical_children: List[PhysicalOperator],
        data_context: DataContext,
    ) -> PhysicalOperator:
        assert len(physical_children) == 1
        return StreamingHashAggregate(
            input_op=physical_children[0],
            data_context=data_context,
            key=logical_op.key,
            agg_fn=logical_op.agg_fn,
            num_aggregators=logical_op.num_aggregators,
        )

    register_plan_logical_op_fn(StreamingAggregate, plan_streaming_aggregate)
    register_plan_logical_op_fn(ListFiles, plan_list_files_op)
    register_plan_logical_op_fn(PartitionFiles, plan_partition_files_op)
    register_plan_logical_op_fn(ReadFiles, plan_read_files_op)

    def plan_join_op(
        logical_op: Join,
        physical_children: List[PhysicalOperator],
        data_context: DataContext,
    ) -> PhysicalOperator:
        assert len(physical_children) == 2
        assert logical_op._num_outputs is not None

        return JoinOperator(
            data_context=data_context,
            left_input_op=physical_children[0],
            right_input_op=physical_children[1],
            join_type=logical_op._join_type,
            left_key_columns=logical_op._left_key_columns,
            right_key_columns=logical_op._right_key_columns,
            left_columns_suffix=logical_op._left_columns_suffix,
            right_columns_suffix=logical_op._right_columns_suffix,
            num_partitions=logical_op._num_outputs,
            partition_size_hint=logical_op._partition_size_hint,
            aggregator_ray_remote_args_override=logical_op._aggregator_ray_remote_args,
        )

    register_plan_logical_op_fn(Join, plan_join_op)
