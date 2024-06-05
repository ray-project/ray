from typing import List

from ray.anyscale.data.logical_operators.streaming_aggregate import StreamingAggregate
from ray.anyscale.data.physical_operators.streaming_hash_aggregate import (
    StreamingHashAggregate,
)
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
