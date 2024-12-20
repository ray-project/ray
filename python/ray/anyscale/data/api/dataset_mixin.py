import functools
from typing import Any, Dict, List, Optional, Protocol, Sequence, Tuple

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
        num_outputs: int,
        key_column_names: Tuple[str] = ("id",),
        right_key_column_names: Optional[Tuple[str]] = None,
        *,
        aggregator_ray_remote_args: Optional[Dict[str, Any]] = None,
    ) -> "Dataset":
        """Join :class:`Datasets <ray.data.Dataset>` on join keys

        Args:
            ds: Other dataset to join against
            join_type: The kind of join that should be performed, one of (“inner”,
              “left_outer”, “right_outer”, “full_outer”)
            num_outputs: Total number of blocks to be produced by this operation (see
              py-doc for `Dataset.repartition` operation for more context)
            key_column_names: The columns from the left operand that will be used as
              keys for the join operation.
            right_key_column_names: The columns from the right operand that will be
              used as keys for the join operation. When none, `key_column_names` will
              be assumed to be a list of columns to be used for the right dataset
              as well.


        Returns:
            A :class:`Dataset` that holds join of input left Dataset with the right
              Dataset based on join type and keys.
        """

        left_op_schema: Optional["Schema"] = self.schema()
        right_op_schema: Optional["Schema"] = ds.schema()

        # NOTE: If no separate keys provided for the right side, assume just the left
        #       side ones
        right_key_column_names = right_key_column_names or key_column_names

        _validate_join_op(
            left_op_schema, right_op_schema, key_column_names, right_key_column_names
        )

        plan = self._plan.copy()
        op = Join(
            left_input_op=self._logical_plan.dag,
            right_input_op=ds._logical_plan.dag,
            left_key_columns=key_column_names,
            right_key_columns=right_key_column_names,
            join_type=join_type,
            num_outputs=num_outputs,
            aggregator_ray_remote_args=aggregator_ray_remote_args,
        )
        logical_plan = LogicalPlan(op, self.context)
        return Dataset(plan, logical_plan)

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


def _validate_join_op(
    left_op_schema: "Schema",
    right_op_schema: "Schema",
    left_key_column_names: Tuple[str],
    right_key_column_names: Tuple[str],
):
    def _col_names_as_str(keys: Sequence[str]):
        keys_joined = ", ".join(map(lambda k: f"'{k}'", keys))
        return f"[{keys_joined}]"

    if len(left_key_column_names) < 1:
        raise ValueError(
            f"At least 1 column name to join on has to be provided (got "
            f"{_col_names_as_str(left_key_column_names)})"
        )

    if len(left_key_column_names) != len(right_key_column_names):
        raise ValueError(
            f"Number of columns provided for left and right datasets has to match "
            f"(got {_col_names_as_str(left_key_column_names)} and "
            f"{_col_names_as_str(right_key_column_names)})"
        )

    def _get_key_column_types(schema: "Schema", keys: Tuple[str]):
        return (
            [_type for name, _type in zip(schema.names, schema.types) if name in keys]
            if schema
            else None
        )

    right_op_key_cols = _get_key_column_types(right_op_schema, left_key_column_names)
    left_op_key_cols = _get_key_column_types(left_op_schema, right_key_column_names)

    if left_op_key_cols != right_op_key_cols:
        raise ValueError(
            f"Key columns are expected to be present and have the same types "
            "in both left and right operands of the join operation: "
            f"left has {left_op_schema}, but right has {right_op_schema}"
        )
