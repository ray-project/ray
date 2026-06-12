"""Unit tests for GPU hash aggregation with ShuffleStrategy.GPU_SHUFFLE.

The planning tests do not require GPUs. Tests marked ``gpu`` exercise cuDF and
RAPIDS MPF paths on actual GPU hardware.
"""

from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

import ray
import ray.data._internal.gpu_shuffle.hash_aggregate as hash_aggregate
from ray.data._internal.execution.interfaces import ExecutionResources, PhysicalOperator
from ray.data._internal.gpu_shuffle.hash_aggregate import (
    GPUAggregateFn,
    GPUAggregationPlan,
    GPUHashAggregateActor,
    GPUHashAggregateOperator,
    build_gpu_aggregation_plan,
)
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators import Aggregate
from ray.data._internal.planner.plan_all_to_all_op import plan_all_to_all_op
from ray.data.aggregate import AggregateFnV2, AsList, Count, Max, Mean, Min, Sum
from ray.data.block import BlockMetadata
from ray.data.context import DataContext, ShuffleStrategy

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_input_op_mock(num_blocks=None, size_bytes=None):
    """Return a minimal PhysicalOperator mock compatible with HashShufflingOperatorBase."""
    logical_mock = MagicMock(LogicalOperator)
    logical_mock.infer_metadata.return_value = BlockMetadata(
        num_rows=None,
        size_bytes=size_bytes,
        exec_stats=None,
        input_files=None,
    )
    logical_mock.estimated_num_outputs.return_value = num_blocks

    op_mock = MagicMock(PhysicalOperator)
    op_mock._output_dependencies = []
    op_mock._logical_operators = [logical_mock]
    op_mock.num_output_splits.return_value = 1
    return op_mock


# ---------------------------------------------------------------------------
# GPU hash aggregate planning
# ---------------------------------------------------------------------------


class TestGPUHashAggregatePlanning:
    def _make_aggregate_op(
        self,
        aggs,
        key="user_id",
        num_partitions=8,
        input_schema=pa.schema([("user_id", pa.int64()), ("value", pa.int64())]),
    ):
        input_op = MagicMock(LogicalOperator)
        input_op.infer_schema.return_value = input_schema
        return Aggregate(
            key=key,
            aggs=list(aggs),
            input_dependencies=[input_op],
            num_partitions=num_partitions,
        )

    def test_builtin_aggregation_plan_supported(self):
        schema = pa.schema([("user_id", pa.int64()), ("value", pa.int64())])
        plan = build_gpu_aggregation_plan(
            ("user_id",),
            (
                Count(),
                Count(on="value", ignore_nulls=True),
                Sum("value"),
                Min("value"),
                Max("value"),
                Mean("value"),
            ),
            input_schema=schema,
        )
        assert isinstance(plan, GPUAggregationPlan), plan
        assert plan.shuffle_key_columns == ("user_id",)
        assert plan.output_names == (
            "count()",
            "count(value)",
            "sum(value)",
            "min(value)",
            "max(value)",
            "mean(value)",
        )
        assert tuple(type(agg).__name__ for agg in plan._gpu_aggregates) == (
            "GPUCount",
            "GPUCount",
            "GPUSum",
            "GPUMin",
            "GPUMax",
            "GPUMean",
        )
        for gpu_agg in plan._gpu_aggregates:
            assert not isinstance(gpu_agg, AggregateFnV2)
            assert not hasattr(gpu_agg, "aggregate_block")
            assert not hasattr(gpu_agg, "combine")
            assert not hasattr(gpu_agg, "finalize")
        assert "user_id" in plan.required_columns
        assert "value" in plan.required_columns

    def test_unsupported_aggregation_plan_rejected(self):
        schema = pa.schema([("user_id", pa.int64())])
        unsupported_result = build_gpu_aggregation_plan(
            ("user_id",), (AsList("value"),), input_schema=schema
        )
        assert unsupported_result == "AsList is not supported by GPU aggregation."

        missing_schema_result = build_gpu_aggregation_plan(
            ("user_id",), (Sum("value"),)
        )
        assert missing_schema_result == (
            "missing input schema for key column(s): user_id."
        )

    def test_custom_gpu_aggregation_plan_supported_without_aggregate_fn_v2(self):
        class _CustomGPUAggregate(GPUAggregateFn):
            def __init__(self) -> None:
                super().__init__(
                    "custom(value)",
                    on="value",
                    ignore_nulls=True,
                )

            def gpu_accumulator_columns(
                self, accumulator_prefix: str
            ) -> Tuple[str, ...]:
                return (f"{accumulator_prefix}_acc",)

            def gpu_partial_aggregate(
                self,
                df: Any,
                key_columns: Tuple[str, ...],
                *,
                output_name: str,
                accumulator_prefix: str,
                input_schema: Any = None,
            ) -> Any:
                raise NotImplementedError

            def gpu_final_aggregate(
                self,
                df: Any,
                key_columns: Tuple[str, ...],
                *,
                output_name: str,
                accumulator_prefix: str,
            ) -> Any:
                raise NotImplementedError

        gpu_agg = _CustomGPUAggregate()
        plan = build_gpu_aggregation_plan(
            ("user_id",),
            (gpu_agg,),
            input_schema=pa.schema([("user_id", pa.int64()), ("value", pa.int64())]),
        )

        assert isinstance(plan, GPUAggregationPlan), plan
        assert plan._gpu_aggregates == (gpu_agg,)
        assert plan.output_names == ("custom(value)",)
        assert plan.required_columns == ("user_id", "value")
        assert not isinstance(gpu_agg, AggregateFnV2)
        assert not hasattr(gpu_agg, "aggregate_block")
        assert not hasattr(gpu_agg, "combine")
        assert not hasattr(gpu_agg, "finalize")

    def test_gpu_shuffle_routes_supported_aggregate_to_gpu_operator(self):
        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = 4
        ctx._shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE

        logical_op = self._make_aggregate_op([Count(), Sum("value")])
        input_physical_op = _make_input_op_mock()
        original_build_plan = hash_aggregate.build_gpu_aggregation_plan
        built_plans: List[GPUAggregationPlan] = []

        def _build_plan_once(*args: Any, **kwargs: Any) -> GPUAggregationPlan:
            result = original_build_plan(*args, **kwargs)
            assert isinstance(result, GPUAggregationPlan), result
            built_plans.append(result)
            return result

        with patch(
            "ray.data._internal.gpu_shuffle.hash_aggregate.build_gpu_aggregation_plan",
            side_effect=_build_plan_once,
        ) as mock_build_plan:
            op = plan_all_to_all_op(logical_op, [input_physical_op], ctx)

        assert isinstance(op, GPUHashAggregateOperator)
        assert op._num_partitions == 8
        assert "GPUHashAggregate" in op.name
        mock_build_plan.assert_called_once()
        assert op._aggregation_plan is built_plans[0]

    def test_gpu_hash_aggregate_operator_uses_prebuilt_plan(self):
        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = 4
        ctx._shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE
        input_physical_op = _make_input_op_mock()
        schema = pa.schema([("user_id", pa.int64()), ("value", pa.int64())])
        aggregation_plan = build_gpu_aggregation_plan(
            ("user_id",), (Count(), Sum("value")), input_schema=schema
        )
        assert isinstance(aggregation_plan, GPUAggregationPlan), aggregation_plan

        with patch(
            "ray.data._internal.gpu_shuffle.hash_shuffle.GPURankPool"
        ) as mock_default_pool:
            op = GPUHashAggregateOperator(
                ctx,
                input_physical_op,
                key_columns=("user_id",),
                aggregation_plan=aggregation_plan,
                num_partitions=8,
            )

        mock_default_pool.assert_not_called()
        assert op._aggregation_plan is aggregation_plan
        assert op._rank_pool.nranks == 4

    def test_gpu_shuffle_unsupported_aggregate_falls_back_to_cpu_hash_aggregate(self):
        from ray.data._internal.execution.operators.hash_aggregate import (
            HashAggregateOperator,
        )

        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = 4
        ctx._shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE

        logical_op = self._make_aggregate_op([AsList("value")])
        input_physical_op = _make_input_op_mock(num_blocks=8, size_bytes=1024)

        with patch(
            "ray.data._internal.execution.operators.hash_shuffle"
            "._get_total_cluster_resources",
            return_value=ExecutionResources(cpu=4, memory=1024 * 1024 * 1024),
        ), patch(
            "ray.data._internal.execution.operators.hash_shuffle.ray.put",
            return_value=MagicMock(),
        ):
            op = plan_all_to_all_op(logical_op, [input_physical_op], ctx)

        assert isinstance(op, HashAggregateOperator)

    def test_gpu_shuffle_missing_key_schema_falls_back_to_cpu_hash_aggregate(self):
        from ray.data._internal.execution.operators.hash_aggregate import (
            HashAggregateOperator,
        )

        ctx = DataContext()
        ctx.gpu_shuffle_num_actors = 4
        ctx._shuffle_strategy = ShuffleStrategy.GPU_SHUFFLE

        logical_op = self._make_aggregate_op([Sum("value")], input_schema=None)
        input_physical_op = _make_input_op_mock(num_blocks=8, size_bytes=1024)

        with patch(
            "ray.data._internal.execution.operators.hash_shuffle"
            "._get_total_cluster_resources",
            return_value=ExecutionResources(cpu=4, memory=1024 * 1024 * 1024),
        ), patch(
            "ray.data._internal.execution.operators.hash_shuffle.ray.put",
            return_value=MagicMock(),
        ):
            op = plan_all_to_all_op(logical_op, [input_physical_op], ctx)

        assert isinstance(op, HashAggregateOperator)

    def test_global_aggregate_uses_synthetic_shuffle_key(self):
        plan = build_gpu_aggregation_plan(tuple(), (Count(), Sum("value")))
        assert isinstance(plan, GPUAggregationPlan), plan
        assert plan.shuffle_key_columns == (hash_aggregate._GLOBAL_AGGREGATE_KEY,)

    def test_required_columns_excludes_unused_input_columns(self):
        table = pa.table(
            {
                "user_id": pa.array([0, 0, 1], type=pa.int64()),
                "small": pa.array([1, 2, 3], type=pa.int64()),
                "unused": pa.array([[1], [2], [3]], type=pa.list_(pa.int64())),
            }
        )
        plan = build_gpu_aggregation_plan(
            ("user_id",), (Count(), Sum("small")), input_schema=table.schema
        )
        assert isinstance(plan, GPUAggregationPlan), plan
        assert plan.required_columns == ("user_id", "small")

        projected = table.select(list(plan.required_columns))
        assert projected.column_names == ["user_id", "small"]

    def test_global_count_required_columns_empty(self):
        table = pa.table({"unused": pa.array([10, 20, 30], type=pa.int64())})
        plan = build_gpu_aggregation_plan(
            tuple(), (Count(),), input_schema=table.schema
        )
        assert isinstance(plan, GPUAggregationPlan), plan
        assert plan.required_columns == ()

    def test_merge_input_schema_unifies_value_dtype_across_blocks(self):
        block1 = pa.table({"value": pa.array([1], type=pa.int8())})
        block2 = pa.table({"value": pa.array([2], type=pa.int32())})
        plan = build_gpu_aggregation_plan(tuple(), (Sum("value"),))
        assert isinstance(plan, GPUAggregationPlan), plan

        runtime_schema = None
        runtime_schema = plan.merge_input_schema(runtime_schema, block1.schema)
        runtime_schema = plan.merge_input_schema(runtime_schema, block2.schema)

        assert runtime_schema.field("value").type == pa.int32()

    def test_merge_input_schema_uses_logical_schema_for_shuffle_keys(self):
        logical_schema = pa.schema(
            [
                ("user_id", pa.int32()),
                ("value", pa.int64()),
            ]
        )
        plan = build_gpu_aggregation_plan(
            ("user_id",), (Sum("value"),), input_schema=logical_schema
        )
        assert isinstance(plan, GPUAggregationPlan), plan

        runtime_schema = plan.merge_input_schema(
            None, pa.schema([("user_id", pa.int8()), ("value", pa.int64())])
        )
        runtime_schema = plan.merge_input_schema(
            runtime_schema,
            pa.schema([("user_id", pa.int32()), ("value", pa.int64())]),
        )

        assert runtime_schema.field("user_id").type == pa.int32()
        assert runtime_schema.field("value").type == pa.int64()

    def test_normalize_output_arrow_null_schema(self):
        null_schema = pa.schema(
            [
                ("user_id", pa.int64()),
                ("value", pa.null()),
            ]
        )
        null_plan = build_gpu_aggregation_plan(
            ("user_id",), (Sum("value"),), input_schema=null_schema
        )
        assert isinstance(null_plan, GPUAggregationPlan), null_plan
        output_table = pa.table(
            {
                "user_id": pa.array([0, 1, 2], type=pa.int64()),
                "sum(value)": pa.array([None, None, None], type=pa.int64()),
            }
        )

        assert null_plan.normalize_output_arrow(output_table).schema == pa.schema(
            [
                ("user_id", pa.int64()),
                ("sum(value)", pa.null()),
            ]
        )

    def test_normalize_output_arrow_preserves_values_when_runtime_schema_upgrades(
        self,
    ):
        null_schema = pa.schema(
            [
                ("user_id", pa.int64()),
                ("value", pa.null()),
            ]
        )
        plan = build_gpu_aggregation_plan(
            ("user_id",), (Sum("value"),), input_schema=null_schema
        )
        assert isinstance(plan, GPUAggregationPlan), plan

        runtime_schema = plan.merge_input_schema(
            null_schema,
            pa.schema([("user_id", pa.int64()), ("value", pa.int64())]),
        )
        output_table = pa.table(
            {
                "user_id": pa.array([0, 1], type=pa.int64()),
                "sum(value)": pa.array([10, 20], type=pa.int64()),
            }
        )

        normalized = plan.normalize_output_arrow(
            output_table, input_schema=runtime_schema
        )
        assert normalized.column("sum(value)").to_pylist() == [10, 20]
        assert normalized.schema.field("sum(value)").type == pa.int64()


# ---------------------------------------------------------------------------
# GPU fixtures — shared by real-GPU aggregate tests below
# ---------------------------------------------------------------------------


def _num_cluster_gpus() -> int:
    """Return the number of GPUs in the Ray cluster (0 if Ray not initialised)."""
    if not ray.is_initialized():
        return 0
    return int(ray.cluster_resources().get("GPU", 0))


@pytest.fixture(scope="module")
def ray_with_gpu():
    """Skip the test if GPU packages or GPU hardware are absent."""
    pytest.importorskip("cudf", reason="cudf (GPU DataFrame library) not installed")
    pytest.importorskip("rapidsmpf", reason="rapidsmpf not installed")

    if not ray.is_initialized():
        ray.init()

    num_gpus = _num_cluster_gpus()
    if num_gpus < 1:
        pytest.skip("No GPU resources found in the Ray cluster")

    yield num_gpus


# ---------------------------------------------------------------------------
# GPUAggregationPlan — real cuDF execution (conditional)
# ---------------------------------------------------------------------------


@pytest.mark.gpu
class TestGPUAggregationPlanReal:
    """Exercises aggregation plan methods with real cuDF on GPU hardware."""

    def test_sum_and_mean_schema_source_dtypes_use_cudf_accumulators(
        self, ray_with_gpu
    ):
        import cudf

        schema = pa.schema(
            [
                ("user_id", pa.int64()),
                ("small", pa.int8()),
                ("flag", pa.bool_()),
                ("ratio", pa.float32()),
            ]
        )
        plan = build_gpu_aggregation_plan(
            ("user_id",),
            (Sum("small"), Mean("flag"), Sum("ratio")),
            input_schema=schema,
        )
        assert isinstance(plan, GPUAggregationPlan), plan

        df = cudf.DataFrame(
            {
                "user_id": [0, 0, 1],
                "small": np.array([100, 100, 1], dtype=np.int8),
                "flag": [True, True, False],
                "ratio": np.array([1.5, 2.5, 3.5], dtype=np.float32),
            }
        )
        partial = plan.partial_aggregate(df, input_schema=schema)

        sum_col = plan.accumulator_columns[0]
        mean_sum_col = plan.accumulator_columns[1]
        float_sum_col = plan.accumulator_columns[4]
        group_zero = partial[partial["user_id"] == 0].iloc[0]

        assert str(partial[sum_col].dtype) == "int64"
        assert str(partial[mean_sum_col].dtype) == "int64"
        assert str(partial[float_sum_col].dtype) == "float64"
        assert group_zero[sum_col] == 200
        assert group_zero[mean_sum_col] == 2
        assert group_zero[float_sum_col] == pytest.approx(4.0)

    def test_empty_final_aggregate_preserves_output_dtypes(self, ray_with_gpu):
        import cudf

        schema = pa.schema(
            [
                ("user_id", pa.int64()),
                ("value", pa.int8()),
            ]
        )
        plan = build_gpu_aggregation_plan(
            ("user_id",),
            (Count(), Sum("value"), Min("value"), Mean("value")),
            input_schema=schema,
        )
        assert isinstance(plan, GPUAggregationPlan), plan

        result = plan.final_aggregate(cudf.DataFrame())
        result_table = result.to_arrow(preserve_index=False)
        expected_schema = pa.schema(
            [
                ("user_id", pa.int64()),
                ("count()", pa.int64()),
                ("sum(value)", pa.int64()),
                ("min(value)", pa.int64()),
                ("mean(value)", pa.float64()),
            ]
        )
        assert result_table.schema.equals(expected_schema, check_metadata=False)

        global_plan = build_gpu_aggregation_plan(
            tuple(), (Count(),), input_schema=schema
        )
        assert isinstance(global_plan, GPUAggregationPlan), global_plan

        global_result = global_plan.final_aggregate(cudf.DataFrame())
        global_table = global_result.to_arrow(preserve_index=False)
        assert global_table.schema.equals(
            pa.schema([("count()", pa.int64())]), check_metadata=False
        )

        runtime_plan = build_gpu_aggregation_plan(
            ("user_id",),
            (Count(), Sum("value")),
            input_schema=pa.schema(
                [
                    ("user_id", pa.int64()),
                    ("value", pa.int8()),
                ]
            ),
        )
        assert isinstance(runtime_plan, GPUAggregationPlan), runtime_plan

        runtime_result = runtime_plan.final_aggregate(
            cudf.DataFrame(),
            input_schema=pa.schema(
                [
                    ("user_id", pa.int64()),
                    ("value", pa.int8()),
                ]
            ),
        )
        runtime_table = runtime_result.to_arrow(preserve_index=False)
        assert runtime_table.schema.equals(
            pa.schema(
                [
                    ("user_id", pa.int64()),
                    ("count()", pa.int64()),
                    ("sum(value)", pa.int64()),
                ]
            ),
            check_metadata=False,
        )

    def test_custom_gpu_aggregate_fn_receives_input_schema(self, ray_with_gpu):
        import cudf

        class _CustomGPUAggregate(GPUAggregateFn):
            def __init__(self) -> None:
                super().__init__(
                    "custom(value)",
                    on="value",
                    ignore_nulls=True,
                )
                self.seen_schema: Optional[pa.Schema] = None

            def gpu_accumulator_columns(
                self, accumulator_prefix: str
            ) -> Tuple[str, ...]:
                return (f"{accumulator_prefix}_acc",)

            def gpu_partial_aggregate(
                self,
                df: cudf.DataFrame,
                key_columns: Tuple[str, ...],
                *,
                output_name: str,
                accumulator_prefix: str,
                input_schema: Any = None,
            ) -> cudf.DataFrame:
                self.seen_schema = input_schema
                acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
                return df[[key_columns[0], "value"]].rename(columns={"value": acc_col})

            def gpu_final_aggregate(
                self,
                df: cudf.DataFrame,
                key_columns: Tuple[str, ...],
                *,
                output_name: str,
                accumulator_prefix: str,
            ) -> cudf.DataFrame:
                acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
                return df[[key_columns[0], acc_col]].rename(
                    columns={acc_col: output_name}
                )

            def gpu_partial_accumulator_dtypes(
                self,
                df: cudf.DataFrame,
                accumulator_prefix: str,
                *,
                input_schema: Any = None,
            ) -> Dict[str, Any]:
                acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
                return {acc_col: df["value"].dtype}

            def gpu_final_output_dtypes(
                self,
                df: cudf.DataFrame,
                output_name: str,
                accumulator_prefix: str,
                *,
                input_schema: Any = None,
            ) -> Dict[str, Any]:
                acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
                return {output_name: df[acc_col].dtype}

        schema = pa.schema([("user_id", pa.int64()), ("value", pa.int32())])
        gpu_agg = _CustomGPUAggregate()
        plan = build_gpu_aggregation_plan(("user_id",), (gpu_agg,), input_schema=schema)
        assert isinstance(plan, GPUAggregationPlan), plan

        partial = plan.partial_aggregate(
            cudf.DataFrame({"user_id": [1], "value": np.array([2], dtype=np.int32)}),
            input_schema=schema,
        )
        result = plan.final_aggregate(partial, input_schema=schema)

        assert gpu_agg.seen_schema is schema
        assert partial.to_pandas().to_dict("list") == {
            "user_id": [1],
            plan.accumulator_columns[0]: [2],
        }
        assert str(partial[plan.accumulator_columns[0]].dtype) == "int32"
        assert result.to_pandas().to_dict("list") == {
            "user_id": [1],
            "custom(value)": [2],
        }
        assert str(result["custom(value)"].dtype) == "int32"

    def test_null_reductions_preserve_groups_and_accumulator_dtypes(
        self, ray_with_gpu, monkeypatch
    ):
        import cudf

        original_group_with_optional_reduction = (
            hash_aggregate._BuiltinGPUAggregateFn._group_with_optional_reduction
        )
        original_gpu_partial_aggregate = hash_aggregate.GPUCount.gpu_partial_aggregate

        def _drop_all_null_group_with_optional_reduction(
            self,
            df,
            key_columns,
            *,
            value_column,
            size_col,
            count_col,
            aggregate_name,
            output_column,
            output_dtype,
        ):
            result, count_dtype = original_group_with_optional_reduction(
                self,
                df,
                key_columns,
                value_column=value_column,
                size_col=size_col,
                count_col=count_col,
                aggregate_name=aggregate_name,
                output_column=output_column,
                output_dtype=output_dtype,
            )
            if hash_aggregate._all_counts_zero(result, count_col):
                return result, count_dtype
            grouped = df.groupby(list(key_columns), dropna=False)
            out = grouped[value_column].count().reset_index()
            counts = out.rename(columns={out.columns[-1]: "__count"})
            non_null_keys = counts[counts["__count"] > 0][list(key_columns)]
            result = result.merge(non_null_keys, on=list(key_columns), how="inner")
            if output_column in result.columns:
                result[output_column] = result[output_column].astype("float64")
            return result, count_dtype

        def _gpu_partial_aggregate_drop_zero_counts(
            self,
            df,
            key_columns,
            *,
            output_name,
            accumulator_prefix,
            input_schema=None,
        ):
            acc_col = self.gpu_accumulator_columns(accumulator_prefix)[0]
            if self.target_column is None or not self.ignore_nulls:
                return original_gpu_partial_aggregate(
                    self,
                    df,
                    key_columns,
                    output_name=output_name,
                    accumulator_prefix=accumulator_prefix,
                    input_schema=input_schema,
                )

            grouped = df.groupby(list(key_columns), dropna=False)
            result = grouped.agg(
                **{acc_col: (self.target_column, "count")}
            ).reset_index()
            count_dtype = hash_aggregate._cudf_column_dtype(result, acc_col)
            counts = result[result[acc_col] > 0]
            result = result[list(key_columns)].merge(
                counts, on=list(key_columns), how="left"
            )
            hash_aggregate._fill_missing_count(result, acc_col, count_dtype)
            return result[list(key_columns) + [acc_col]]

        monkeypatch.setattr(
            hash_aggregate._BuiltinGPUAggregateFn,
            "_group_with_optional_reduction",
            _drop_all_null_group_with_optional_reduction,
        )
        monkeypatch.setattr(
            hash_aggregate.GPUCount,
            "gpu_partial_aggregate",
            _gpu_partial_aggregate_drop_zero_counts,
        )

        schema = pa.schema([("user_id", pa.int64())])
        plan = build_gpu_aggregation_plan(
            ("user_id",), (Sum("value"),), input_schema=schema
        )
        assert isinstance(plan, GPUAggregationPlan), plan
        acc_col = plan.accumulator_columns[0]

        df = cudf.DataFrame(
            {
                "user_id": [0, 1, 2, 0],
                "value": cudf.Series([None, None, None, None], dtype="int64"),
            }
        )
        partial = plan.partial_aggregate(df)
        assert str(partial[acc_col].dtype) == "int64"

        result = (
            plan.final_aggregate(partial)
            .to_pandas()
            .sort_values("user_id")
            .reset_index(drop=True)
        )
        assert result["user_id"].tolist() == [0, 1, 2]
        assert result["sum(value)"].isna().all()

        count_plan = build_gpu_aggregation_plan(
            ("user_id",),
            (Count("value", ignore_nulls=True),),
            input_schema=schema,
        )
        assert isinstance(count_plan, GPUAggregationPlan), count_plan

        count_partial = count_plan.partial_aggregate(df)
        count_result = (
            count_plan.final_aggregate(count_partial)
            .to_pandas()
            .sort_values("user_id")
            .reset_index(drop=True)
        )
        assert count_result.to_dict("records") == [
            {"user_id": 0, "count(value)": 0},
            {"user_id": 1, "count(value)": 0},
            {"user_id": 2, "count(value)": 0},
        ]

    def test_partial_aggregate_widens_shuffle_key_to_logical_schema(self, ray_with_gpu):
        import cudf

        logical_schema = pa.schema(
            [
                ("user_id", pa.int32()),
                ("value", pa.int64()),
            ]
        )
        plan = build_gpu_aggregation_plan(
            ("user_id",), (Sum("value"),), input_schema=logical_schema
        )
        assert isinstance(plan, GPUAggregationPlan), plan

        df = cudf.DataFrame(
            {
                "user_id": np.array([1], dtype=np.int8),
                "value": np.array([1], dtype=np.int64),
            }
        )
        partial = plan.partial_aggregate(
            df,
            input_schema=pa.schema([("user_id", pa.int8()), ("value", pa.int64())]),
        )

        assert partial["user_id"].iloc[0] == 1
        assert str(partial["user_id"].dtype) == "int32"

    def test_global_count_partial_aggregate(self, ray_with_gpu):
        import cudf

        plan = build_gpu_aggregation_plan(tuple(), (Count(),))
        assert isinstance(plan, GPUAggregationPlan), plan

        df = cudf.DataFrame(index=range(3))
        partial = plan.partial_aggregate(df)
        assert partial[plan.accumulator_columns[0]].iloc[0] == 3

    def test_normalize_partial_output_null_key_column(self, ray_with_gpu):
        import cudf

        nan_key_plan = build_gpu_aggregation_plan(
            ("item",), (Count(),), input_schema=pa.schema([("item", pa.null())])
        )
        assert isinstance(nan_key_plan, GPUAggregationPlan), nan_key_plan
        nan_key_partial = cudf.DataFrame(
            {
                "item": cudf.Series([None], dtype="float64"),
                nan_key_plan.accumulator_columns[0]: np.array([1], dtype=np.int64),
            }
        )
        normalized_nan_key_partial = nan_key_plan._normalize_partial_output(
            nan_key_partial,
            nan_key_partial,
            ("item",),
            input_schema=pa.schema([("item", pa.null())]),
        )

        assert str(normalized_nan_key_partial["item"].dtype) == "float64"
        assert (
            str(normalized_nan_key_partial[nan_key_plan.accumulator_columns[0]].dtype)
            == "int64"
        )

    def test_normalize_partial_output_unknown_schema_widening(self, ray_with_gpu):
        import cudf

        unknown_schema_plan = build_gpu_aggregation_plan(tuple(), (Sum("B"),))
        assert isinstance(unknown_schema_plan, GPUAggregationPlan), unknown_schema_plan
        unknown_schema_acc_col = unknown_schema_plan.accumulator_columns[0]
        int_input = cudf.DataFrame({"A": [0], "B": np.array([1], dtype=np.int64)})
        int_partial = cudf.DataFrame(
            {
                "A": np.array([0], dtype=np.int64),
                unknown_schema_acc_col: np.array([1], dtype=np.int64),
            }
        )
        double_input = cudf.DataFrame(
            {"A": [0], "B": np.array([1.0], dtype=np.float64)}
        )
        double_partial = cudf.DataFrame(
            {
                "A": np.array([0], dtype=np.int64),
                unknown_schema_acc_col: np.array([1.0], dtype=np.float64),
            }
        )

        normalized_int_partial = unknown_schema_plan._normalize_partial_output(
            int_partial,
            int_input,
            ("A",),
            input_schema=pa.schema([("A", pa.int64()), ("B", pa.int64())]),
        )
        normalized_double_partial = unknown_schema_plan._normalize_partial_output(
            double_partial,
            double_input,
            ("A",),
            input_schema=pa.schema([("A", pa.int64()), ("B", pa.float64())]),
        )

        assert str(normalized_int_partial[unknown_schema_acc_col].dtype) == "int64"
        assert str(normalized_double_partial[unknown_schema_acc_col].dtype) == "float64"

    def test_unsupported_cudf_dtype_cast_is_noop(self, monkeypatch):
        cudf = pytest.importorskip("cudf")

        df = cudf.DataFrame({"value": np.array([1, 2], dtype=np.int64)})

        def raise_type_error(_self):
            raise TypeError("unsupported cuDF dtype")

        monkeypatch.setattr(hash_aggregate.DataType, "to_cudf_type", raise_type_error)

        hash_aggregate._cast_column_to_dtype(
            df, "value", hash_aggregate.DataType.from_numpy("int64")
        )

        assert df["value"].to_pandas().tolist() == [1, 2]
        assert str(df["value"].dtype) == "int64"


# ---------------------------------------------------------------------------
# GPUHashAggregateActor — real GPU paths (conditional)
# ---------------------------------------------------------------------------


@pytest.mark.gpu
class TestGPUHashAggregateActorReal:
    """Exercises GPU aggregate methods on actual hardware."""

    def _make_setup_actor(self, aggregation_plan, total_nparts: int = 2):
        actor = GPUHashAggregateActor.options(num_gpus=1).remote(
            nranks=1,
            total_nparts=total_nparts,
            aggregation_plan=aggregation_plan,
        )
        _, root_address = ray.get(actor.setup_root.remote())
        ray.get(actor.setup_worker.remote(root_address))
        return actor

    @staticmethod
    def _collect_tables(actor) -> List[pa.Table]:
        gen = actor.finish_and_extract.options(num_returns="streaming").remote()
        return [
            item for ref in gen for item in [ray.get(ref)] if isinstance(item, pa.Table)
        ]

    @staticmethod
    def _collect_frame(actor) -> pd.DataFrame:
        tables = TestGPUHashAggregateActorReal._collect_tables(actor)
        frames = [table.to_pandas() for table in tables if table.num_rows > 0]
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    def test_grouped_builtin_aggregates_real_gpu(self, ray_with_gpu):
        table = pa.table(
            {
                "group": pa.array([0, 0, 1, 1, 1, 2], type=pa.int64()),
                "value": pa.array([1, None, 2, 3, None, 10], type=pa.int64()),
            }
        )
        plan = build_gpu_aggregation_plan(
            ("group",),
            (
                Count(),
                Count("value", ignore_nulls=True),
                Sum("value"),
                Min("value"),
                Max("value"),
                Mean("value"),
            ),
            input_schema=table.schema,
        )
        assert isinstance(plan, GPUAggregationPlan), plan

        actor = self._make_setup_actor(plan)
        try:
            assert ray.get(actor.insert_batch.remote(table)) == table.num_rows
            result = (
                self._collect_frame(actor).sort_values("group").reset_index(drop=True)
            )
        finally:
            ray.kill(actor)

        assert result["group"].tolist() == [0, 1, 2]
        assert result["count()"].tolist() == [2, 3, 1]
        assert result["count(value)"].tolist() == [1, 2, 1]
        assert result["sum(value)"].tolist() == [1, 5, 10]
        assert result["min(value)"].tolist() == [1, 2, 10]
        assert result["max(value)"].tolist() == [1, 3, 10]
        assert result["mean(value)"].tolist() == pytest.approx([1.0, 2.5, 10.0])

    def test_grouped_aggregate_output_has_unique_join_keys(self, ray_with_gpu):
        import cudf

        schema = pa.schema(
            [
                ("key1", pa.int64()),
                ("key2", pa.int64()),
                ("value", pa.int64()),
            ]
        )
        plan = build_gpu_aggregation_plan(
            ("key1", "key2"),
            (
                Min(on="value", alias_name="min_value"),
                Count(alias_name="row_count"),
            ),
            input_schema=schema,
        )
        assert isinstance(plan, GPUAggregationPlan), plan

        df = cudf.DataFrame(
            {
                "key1": [0, 0, 1],
                "key2": [10, 10, 20],
                "value": [5, 6, 7],
            }
        )
        partial = plan.partial_aggregate(df, input_schema=schema)
        result = plan.final_aggregate(partial, input_schema=schema)
        result_table = result.to_arrow(preserve_index=False)

        assert result_table.column_names == ["key1", "key2", "min_value", "row_count"]
        assert len(result_table.column_names) == len(set(result_table.column_names))

        key_table = pa.table(
            {
                "key1": pa.array([0, 1], type=pa.int64()),
                "key2": pa.array([10, 20], type=pa.int64()),
            }
        )
        joined = key_table.join(
            result_table,
            keys=["key1", "key2"],
            join_type="inner",
        )
        assert joined.num_rows == 2

    def test_global_builtin_aggregates_real_gpu(self, ray_with_gpu):
        plan = build_gpu_aggregation_plan(
            tuple(),
            (
                Count(),
                Count("value", ignore_nulls=True),
                Sum("value"),
                Mean("value"),
            ),
        )
        assert isinstance(plan, GPUAggregationPlan), plan

        table = pa.table({"value": pa.array([1, None, 2, 5], type=pa.int64())})
        actor = self._make_setup_actor(plan, total_nparts=1)
        try:
            assert ray.get(actor.insert_batch.remote(table)) == table.num_rows
            result = self._collect_frame(actor)
        finally:
            ray.kill(actor)

        assert result.columns.tolist() == [
            "count()",
            "count(value)",
            "sum(value)",
            "mean(value)",
        ]
        assert len(result) == 1
        row = result.iloc[0].to_dict()
        assert row["count()"] == 4
        assert row["count(value)"] == 3
        assert row["sum(value)"] == 8
        assert row["mean(value)"] == pytest.approx(8 / 3)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
