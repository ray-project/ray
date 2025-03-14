from ray.anyscale.data.api.streaming_aggregate import StreamingAggFn
from ray.data._internal.logical.interfaces.logical_operator import LogicalOperator


class StreamingAggregate(LogicalOperator):
    """Logical operator for streaming aggregate operation."""

    def __init__(
        self,
        input_op: LogicalOperator,
        key: str,
        agg_fn: StreamingAggFn,
        num_aggregators: int,
    ):
        super().__init__("StreamingAggregate", [input_op], 1)
        self.key = key
        self.agg_fn = agg_fn
        self.num_aggregators = num_aggregators
