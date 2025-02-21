import functools
from typing import Any, Dict, List, Optional, Protocol, Tuple

from ray.anyscale.data._internal.logical.operators.join_operator import Join
from ray.anyscale.data._internal.logical.operators.list_files_operator import (
    PATH_COLUMN_NAME,
)
from ray.anyscale.data._internal.logical.operators.partition_files_operator import (
    PartitionFiles,
)
from ray.anyscale.data._internal.logical.operators.read_files_operator import ReadFiles
from ray.anyscale.data._internal.logical.operators.streaming_aggregate import (
    StreamingAggregate,
)
from ray.anyscale.data.api.streaming_aggregate import StreamingAggFn
from ray.anyscale.data.datasource.snowflake_datasink import SnowflakeDatasink
from ray.data import Dataset, Schema
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
        logical_plan = LogicalPlan(agg_op, self.context)
        return Dataset(plan, logical_plan)

    def join(
        self: DatasetProtocol,
        ds: "Dataset",
        join_type: str,
        num_partitions: int,
        on: Tuple[str] = ("id",),
        right_on: Optional[Tuple[str]] = None,
        left_suffix: Optional[str] = None,
        right_suffix: Optional[str] = None,
        *,
        partition_size_hint: Optional[int] = None,
        aggregator_ray_remote_args: Optional[Dict[str, Any]] = None,
        validate_schemas: bool = False,
    ) -> "Dataset":
        """Join :class:`Datasets <ray.data.Dataset>` on join keys.

        Args:
            ds: Other dataset to join against
            join_type: The kind of join that should be performed, one of ("inner",
                "left_outer", "right_outer", "full_outer")
            num_partitions: Total number of "partitions" input sequences will be split
                into with each partition being joined independently. Increasing number
                of partitions allows to reduce individual partition size, hence reducing
                memory requirements when individual partitions are being joined. Note
                that, consequently, this will also be a total number of blocks that will
                be produced as a result of executing join.
            on: The columns from the left operand that will be used as
                keys for the join operation.
            right_on: The columns from the right operand that will be
                used as keys for the join operation. When none, `on` will
                be assumed to be a list of columns to be used for the right dataset
                as well.
            left_suffix: (Optional) Suffix to be appended for columns of the left
                operand.
            right_suffix: (Optional) Suffix to be appended for columns of the right
                operand.
            partition_size_hint: (Optional) Hint to joining operator about the estimated
                avg expected size of the individual partition (in bytes).
                This is used in estimating the total dataset size and allow to tune
                memory requirement of the individual joining workers to prevent OOMs
                when joining very large datasets.
            aggregator_ray_remote_args: (Optional) Parameter overriding `ray.remote`
                args passed when constructing joining (aggregator) workers.
            validate_schemas: (Optional) Controls whether validation of provided
                configuration against input schemas will be performed (defaults to
                false, since obtaining schemas could be prohibitively expensive).

        Returns:
            A :class:`Dataset` that holds join of input left Dataset with the right
              Dataset based on join type and keys.

        Examples:

        .. testcode::

            doubles_ds = ray.data.range(3).map(
                lambda row: {"id": row["id"], "double": int(row["id"]) * 2}
            )

            squares_ds = ray.data.range(3).map(
                lambda row: {"id": row["id"], "square": int(row["id"]) ** 2}
            )

            joined_ds = doubles.join(
                squares,
                join_type="inner",
                num_partitions=16,
                on=("id",),
            )

            print(sorted(joined_ds.take_all(), key=lambda item: item["id"]))

        .. testoutput::

            [
                {'id': 1, 'double': 2, 'square': 1},
                {'id': 2, 'double': 4, 'square': 4},
                {'id': 3, 'double': 6, 'square': 9}
            ]
        """

        if not isinstance(on, (tuple, list)):
            raise ValueError(
                f"Expected tuple or list as `on` (got {type(on).__name__})"
            )

        if right_on and not isinstance(right_on, (tuple, list)):
            raise ValueError(
                f"Expected tuple or list as `right_on` (got {type(right_on).__name__})"
            )

        # NOTE: If no separate keys provided for the right side, assume just the left
        #       side ones
        right_on = right_on or on

        # NOTE: By default validating schemas are disabled as it could be arbitrarily
        #       expensive (potentially executing whole pipeline to completion) to fetch
        #       one currently
        if validate_schemas:
            left_op_schema: Optional["Schema"] = self.schema()
            right_op_schema: Optional["Schema"] = ds.schema()

            Join._validate_schemas(left_op_schema, right_op_schema, on, right_on)

        plan = self._plan.copy()
        op = Join(
            left_input_op=self._logical_plan.dag,
            right_input_op=ds._logical_plan.dag,
            left_key_columns=on,
            right_key_columns=right_on,
            join_type=join_type,
            num_partitions=num_partitions,
            left_columns_suffix=left_suffix,
            right_columns_suffix=right_suffix,
            partition_size_hint=partition_size_hint,
            aggregator_ray_remote_args=aggregator_ray_remote_args,
        )

        return Dataset(plan, LogicalPlan(op, self.context))

    @functools.wraps(Dataset.input_files)
    def input_files(self) -> List[str]:
        if isinstance(self._logical_plan.dag, ReadFiles):
            input_dependencies = self._logical_plan.dag.input_dependencies
            assert len(input_dependencies) == 1
            partition_files_op = input_dependencies[0]
            assert isinstance(partition_files_op, PartitionFiles)
            execution_plan = ExecutionPlan(DatasetStats(metadata={}, parent=None))
            logical_plan = LogicalPlan(partition_files_op, self.context)
            dataset = Dataset(execution_plan, logical_plan)
            return list({row[PATH_COLUMN_NAME] for row in dataset.take_all()})
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
