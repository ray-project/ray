import typing
from typing import Protocol

from ray.anyscale.data.api.streaming_aggregate import StreamingAggFn
from ray.anyscale.data.logical_operators.streaming_aggregate import StreamingAggregate
from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan
from ray.data._internal.plan import ExecutionPlan

if typing.TYPE_CHECKING:
    from ray.data import Dataset


class DatasetProtocol(Protocol):
    _plan: "ExecutionPlan"
    _logical_plan: "LogicalPlan"


class DatasetMixin:
    """A mix-in class that allows adding Anyscale proprietary methods to
    :class:`~ray.data.Dataset`
    """

    def streaming_aggregate(
        self: DatasetProtocol,
        key: str,
        agg_fn: StreamingAggFn,
        num_aggregators: int,
    ) -> "Dataset":
        """Apply a streaming aggregation operation to the dataset.

        This operation groups the dataset by the given key and applies the
        user-defined aggregate function to each group in a streaming way.

        Examples:

        .. testcode::

            num_ids = 3
            num_rows_per_id = 3
            ds = ray.data.from_items(
                [
                    {"id": i, "value": i * j}
                    for i in range(1, num_ids + 1)
                    for j in range(1, num_rows_per_id + 1)
                ]
            )

            class SumAggFn(StreamingAggFn):
                def init_state(self, key):
                    return {"id": key, "sum": 0, "count": 0}

                def aggregate_row(self, key, state, row):
                    assert key == state["id"]
                    state["sum"] += row["value"]
                    state["count"] += 1
                    if state["count"] == num_rows_per_id:
                        return {"id": state["id"], "sum": state["sum"]}, True
                    return state, False

            ds = ds.streaming_aggregate("id", SumAggFn(), 2)
            print(sorted(ds.take_all(), key=lambda item: item["id"]))

        .. testoutput::

            [{'id': 1, 'sum': 6}, {'id': 2, 'sum': 12}, {'id': 3, 'sum': 18}]

        Args:
            key: The key to group by.
            agg_fn: The user-defined aggregate function.
            num_aggregators: The number of aggregators to use.
        """

        from ray.data import Dataset

        plan = self._plan.copy()
        agg_op = StreamingAggregate(
            self._logical_plan.dag,
            key=key,
            agg_fn=agg_fn,
            num_aggregators=num_aggregators,
        )
        logical_plan = LogicalPlan(agg_op)
        return Dataset(plan, logical_plan)
