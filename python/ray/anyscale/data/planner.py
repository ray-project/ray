from typing import List

from ray.anyscale.data._internal.execution.operators.streaming_hash_aggregate import (
    StreamingHashAggregate,
)
from ray.anyscale.data._internal.logical.operators.expand_paths_operator import (
    ExpandPaths,
)
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.anyscale.data._internal.logical.operators.streaming_aggregate import (
    StreamingAggregate,
)
from ray.anyscale.data._internal.planner.plan_expand_paths_op import (
    plan_expand_paths_op,
)
from ray.anyscale.data._internal.planner.plan_read_files_op import plan_read_files_op
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.planner.planner import register_plan_logical_op_fn


def _register_anyscale_plan_logical_op_fns():
    def plan_streaming_aggregate(
        logical_op: StreamingAggregate, physical_children: List[PhysicalOperator]
    ) -> PhysicalOperator:
        assert len(physical_children) == 1
        return StreamingHashAggregate(
            input_op=physical_children[0],
            key=logical_op.key,
            agg_fn=logical_op.agg_fn,
            num_aggregators=logical_op.num_aggregators,
        )

    register_plan_logical_op_fn(StreamingAggregate, plan_streaming_aggregate)
    register_plan_logical_op_fn(ExpandPaths, plan_expand_paths_op)
    register_plan_logical_op_fn(ReadFiles, plan_read_files_op)
