import sys
from typing import TYPE_CHECKING, List, Optional

import numpy as np
import pandas as pd
import pytest

import ray

if TYPE_CHECKING:
    from ray.data.context import DataContext
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.from_operators import (
    FromArrow,
    FromItems,
    FromNumpy,
    FromPandas,
)
from ray.data._internal.logical.operators.map_operator import (
    Filter,
    FlatMap,
    MapBatches,
    MapRows,
    Project,
)
from ray.data._internal.logical.optimizers import PhysicalOptimizer
from ray.data._internal.planner import create_planner
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource
from ray.data.datasource.datasource import ReadTask
from ray.data.expressions import col
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import _check_usage_record, get_parquet_read_logical_op
from ray.data.tests.util import column_udf, extract_values, named_values
from ray.tests.conftest import *  # noqa


def test_read_operator(ray_start_regular_shared_2_cpus):
    ctx = DataContext.get_current()
    planner = create_planner()
    op = get_parquet_read_logical_op()
    plan = LogicalPlan(op, ctx)
    physical_op = planner.plan(plan).dag

    assert op.name == "ReadParquet"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)
    # Check that the linked logical operator is the same the input op.
    assert physical_op._logical_operators == [op]
    assert physical_op.input_dependencies[0]._logical_operators == [op]


def test_read_operator_emits_warning_for_large_read_tasks():
    class StubDatasource(Datasource):
        def estimate_inmemory_data_size(self) -> Optional[int]:
            return None

        def get_read_tasks(
            self,
            parallelism: int,
            per_task_row_limit: Optional[int] = None,
            data_context: Optional["DataContext"] = None,
        ) -> List[ReadTask]:
            large_object = np.zeros((128, 1024, 1024), dtype=np.uint8)  # 128 MiB

            def read_fn():
                _ = large_object
                yield pd.DataFrame({"column": [0]})

            return [
                ReadTask(
                    read_fn,
                    BlockMetadata(1, None, None, None),
                    per_task_row_limit=per_task_row_limit,
                )
            ]

    with pytest.warns(UserWarning):
        ray.data.read_datasource(StubDatasource()).materialize()


def test_split_blocks_operator(ray_start_regular_shared_2_cpus):
    ctx = DataContext.get_current()

    planner = create_planner()
    op = get_parquet_read_logical_op(parallelism=10)
    logical_plan = LogicalPlan(op, ctx)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert physical_op.name == "ReadParquet->SplitBlocks(10)"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)
    assert physical_op._additional_split_factor == 10

    # Test that split blocks prevents fusion.
    op = MapBatches(
        op,
        lambda x: x,
    )
    logical_plan = LogicalPlan(op, ctx)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag
    assert physical_op.name == "MapBatches(<lambda>)"
    assert len(physical_op.input_dependencies) == 1
    up_physical_op = physical_op.input_dependencies[0]
    assert isinstance(up_physical_op, MapOperator)
    assert up_physical_op.name == "ReadParquet->SplitBlocks(10)"


def test_from_operators(ray_start_regular_shared_2_cpus):
    ctx = DataContext.get_current()

    op_classes = [
        FromArrow,
        FromItems,
        FromNumpy,
        FromPandas,
    ]
    for op_cls in op_classes:
        planner = create_planner()
        op = op_cls([], [])
        plan = LogicalPlan(op, ctx)
        physical_op = planner.plan(plan).dag

        assert op.name == op_cls.__name__
        assert isinstance(physical_op, InputDataBuffer)
        assert len(physical_op.input_dependencies) == 0

        # Check that the linked logical operator is the same the input op.
        assert physical_op._logical_operators == [op]


def test_from_items_e2e(ray_start_regular_shared_2_cpus):
    data = ["Hello", "World"]
    ds = ray.data.from_items(data)
    assert ds.take_all() == named_values("item", data), ds

    # Check that metadata fetch is included in stats.
    assert "FromItems" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromItems"
    _check_usage_record(["FromItems"])


def test_map_operator_udf_name(ray_start_regular_shared_2_cpus):
    # Test the name of the Map operator with different types of UDF.
    def normal_function(x):
        return x

    lambda_function = lambda x: x  # noqa: E731

    class CallableClass:
        def __call__(self, x):
            return x

    class NormalClass:
        def method(self, x):
            return x

    udf_list = [
        # A nomral function.
        normal_function,
        # A lambda function
        lambda_function,
        # A callable class.
        CallableClass,
        # An instance of a callable class.
        CallableClass(),
        # A normal class method.
        NormalClass().method,
    ]

    expected_names = [
        "normal_function",
        "<lambda>",
        "CallableClass",
        "CallableClass",
        "NormalClass.method",
    ]

    for udf, expected_name in zip(udf_list, expected_names):
        op = MapRows(
            get_parquet_read_logical_op(),
            udf,
        )
        assert op.name == f"Map({expected_name})"


def test_map_batches_operator(ray_start_regular_shared_2_cpus):
    ctx = DataContext.get_current()

    planner = create_planner()
    read_op = get_parquet_read_logical_op()
    op = MapBatches(
        read_op,
        lambda x: x,
    )
    plan = LogicalPlan(op, ctx)
    physical_op = planner.plan(plan).dag

    assert op.name == "MapBatches(<lambda>)"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)

    # Check that the linked logical operator is the same the input op.
    assert physical_op._logical_operators == [op]


def test_map_batches_e2e(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(5)
    ds = ds.map_batches(column_udf("id", lambda x: x))
    assert sorted(extract_values("id", ds.take_all())) == list(range(5)), ds
    _check_usage_record(["ReadRange", "MapBatches"])


def test_map_rows_operator(ray_start_regular_shared_2_cpus):
    ctx = DataContext.get_current()

    planner = create_planner()
    read_op = get_parquet_read_logical_op()
    op = MapRows(
        read_op,
        lambda x: x,
    )
    plan = LogicalPlan(op, ctx)
    physical_op = planner.plan(plan).dag

    assert op.name == "Map(<lambda>)"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_map_rows_e2e(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(5)
    ds = ds.map(column_udf("id", lambda x: x + 1))
    expected = [1, 2, 3, 4, 5]
    actual = sorted(extract_values("id", ds.take_all()))
    assert actual == expected, f"Expected {expected}, but got {actual}"
    _check_usage_record(["ReadRange", "Map"])


def test_filter_operator(ray_start_regular_shared_2_cpus):
    ctx = DataContext.get_current()

    planner = create_planner()
    read_op = get_parquet_read_logical_op()
    op = Filter(
        read_op,
        fn=lambda x: x,
    )
    plan = LogicalPlan(op, ctx)
    physical_op = planner.plan(plan).dag

    assert op.name == "Filter(<lambda>)"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_filter_e2e(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(5)
    ds = ds.filter(fn=lambda x: x["id"] % 2 == 0)
    assert sorted(extract_values("id", ds.take_all())) == [0, 2, 4], ds
    _check_usage_record(["ReadRange", "Filter"])


def test_project_operator_select(ray_start_regular_shared_2_cpus):
    """
    Checks that the physical plan is properly generated for the Project operator from
    select columns.
    """
    path = "example://iris.parquet"
    ds = ray.data.read_parquet(path)
    ds = ds.map_batches(lambda d: d)
    cols = ["sepal.length", "petal.width"]
    ds = ds.select_columns(cols)

    logical_plan = ds._plan._logical_plan
    op = logical_plan.dag
    assert isinstance(op, Project), op.name
    assert op.exprs == [col("sepal.length"), col("petal.width")]

    physical_plan = create_planner().plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag
    assert isinstance(physical_op, TaskPoolMapOperator)
    assert isinstance(physical_op.input_dependency, TaskPoolMapOperator)


def test_project_operator_rename(ray_start_regular_shared_2_cpus):
    """
    Checks that the physical plan is properly generated for the Project operator from
    rename columns.
    """
    from ray.data.expressions import star

    path = "example://iris.parquet"
    ds = ray.data.read_parquet(path)
    ds = ds.map_batches(lambda d: d)
    cols_rename = {"sepal.length": "sepal_length", "petal.width": "pedal_width"}
    ds = ds.rename_columns(cols_rename)

    logical_plan = ds._plan._logical_plan
    op = logical_plan.dag
    assert isinstance(op, Project), op.name
    assert op.exprs == [
        star(),
        col("sepal.length").alias("sepal_length"),
        col("petal.width").alias("pedal_width"),
    ]
    physical_plan = create_planner().plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag
    assert isinstance(physical_op, TaskPoolMapOperator)
    assert isinstance(physical_op.input_dependency, TaskPoolMapOperator)


def test_flat_map(ray_start_regular_shared_2_cpus):
    ctx = DataContext.get_current()

    planner = create_planner()
    read_op = get_parquet_read_logical_op()
    op = FlatMap(
        read_op,
        lambda x: x,
    )
    plan = LogicalPlan(op, ctx)
    physical_op = planner.plan(plan).dag

    assert op.name == "FlatMap(<lambda>)"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_flat_map_e2e(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(2)
    ds = ds.flat_map(fn=lambda x: [{"id": x["id"]}, {"id": x["id"]}])
    assert extract_values("id", ds.take_all()) == [0, 0, 1, 1], ds
    _check_usage_record(["ReadRange", "FlatMap"])


def test_column_ops_e2e(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(2)
    ds = ds.add_column(fn=lambda df: df.iloc[:, 0], col="new_col")
    assert ds.take_all() == [{"id": 0, "new_col": 0}, {"id": 1, "new_col": 1}], ds
    _check_usage_record(["ReadRange", "MapBatches"])

    select_ds = ds.select_columns(cols=["new_col"])
    assert select_ds.take_all() == [{"new_col": 0}, {"new_col": 1}]
    _check_usage_record(["ReadRange", "MapBatches"])

    ds = ds.drop_columns(cols=["new_col"])
    assert ds.take_all() == [{"id": 0}, {"id": 1}], ds
    _check_usage_record(["ReadRange", "MapBatches"])


def test_random_sample_e2e(ray_start_regular_shared_2_cpus):
    import math

    def ensure_sample_size_close(dataset, sample_percent=0.5):
        r1 = ds.random_sample(sample_percent)
        assert math.isclose(
            r1.count(), int(ds.count() * sample_percent), rel_tol=2, abs_tol=2
        )

    ds = ray.data.range(10, override_num_blocks=2)
    ensure_sample_size_close(ds)

    ds = ray.data.range(10, override_num_blocks=2)
    ensure_sample_size_close(ds)

    ds = ray.data.range_tensor(5, override_num_blocks=2, shape=(2, 2))
    ensure_sample_size_close(ds)

    _check_usage_record(["ReadRange", "MapBatches"])


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
