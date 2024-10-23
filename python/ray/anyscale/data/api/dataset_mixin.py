import functools
from typing import Any, Dict, List, Optional, Protocol

from ray.anyscale.data._internal.logical.operators.partition_files_operator import (
    PartitionFiles,
)
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.anyscale.data._internal.logical.operators.streaming_aggregate import (
    StreamingAggregate,
)
from ray.anyscale.data.api.streaming_aggregate import StreamingAggFn
from ray.anyscale.data.datasource.snowflake_datasink import SnowflakeDatasink
from ray.data import Dataset
from ray.data._internal.logical.interfaces.logical_plan import LogicalPlan
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.stats import DatasetStats


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

    @functools.wraps(Dataset.input_files)
    def input_files(self) -> List[str]:
        if isinstance(self._logical_plan.dag, ReadFiles):
            input_dependencies = self._logical_plan.dag.input_dependencies
            assert len(input_dependencies) == 1
            partition_files_op = input_dependencies[0]
            assert isinstance(partition_files_op, PartitionFiles)
            execution_plan = ExecutionPlan(DatasetStats(metadata={}, parent=None))
            logical_plan = LogicalPlan(partition_files_op)
            dataset = Dataset(execution_plan, logical_plan)
            return list(
                {row[PartitionFiles.PATH_COLUMN_NAME] for row in dataset.take_all()}
            )
        else:
            return self._plan.input_files() or []

    def write_snowflake(
        self,
        table: str,
        connection_parameters: str,
        *,
        ray_remote_args: Dict[str, Any] = None,
        concurrency: Optional[int] = None,
    ):
        """Write this ``Dataset`` to a Snowflake table.

        Example:

            .. testcode::
                :skipif: True

                import ray

                connection_parameters = dict(
                    user=...,
                    account="ABCDEFG-ABC12345",
                    password=...,
                    database="SNOWFLAKE_SAMPLE_DATA",
                    schema="TPCDS_SF100TCL"
                )
                ds = ray.data.read_parquet("s3://anonymous@ray-example-data/iris.parquet")
                ds.write_snowflake("MY_DATABASE.MY_SCHEMA.IRIS"", connection_parameters)

        Args:
            table: The name of the table to write to.
            connection_parameters: Keyword arguments to pass to
                ``snowflake.connector.connect``. To view supported parameters, read
                https://docs.snowflake.com/developer-guide/python-connector/python-connector-api#functions.
        """  # noqa: E501
        return self.write_datasink(
            SnowflakeDatasink(table, connection_parameters),
            ray_remote_args=ray_remote_args,
            concurrency=concurrency,
        )
