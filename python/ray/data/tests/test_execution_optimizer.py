import itertools
import sys
from typing import List, Optional
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from packaging.version import parse as parse_version

import ray
from ray._private.arrow_utils import get_pyarrow_version
from ray.data._internal.datasource.parquet_datasink import ParquetDatasink
from ray.data._internal.execution.interfaces.op_runtime_metrics import OpRuntimeMetrics
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.execution.operators.zip_operator import ZipOperator
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.interfaces.physical_plan import PhysicalPlan
from ray.data._internal.logical.operators.all_to_all_operator import (
    RandomShuffle,
    Repartition,
    Sort,
)
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
from ray.data._internal.logical.operators.n_ary_operator import Zip
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.logical.optimizers import PhysicalOptimizer
from ray.data._internal.logical.rules.configure_map_task_memory import (
    ConfigureMapTaskMemoryUsingOutputSize,
)
from ray.data._internal.planner import create_planner
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.stats import DatasetStats
from ray.data.block import BlockMetadata
from ray.data.context import DataContext
from ray.data.datasource import Datasource
from ray.data.datasource.datasource import ReadTask
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import _check_usage_record, get_parquet_read_logical_op
from ray.data.tests.util import column_udf, extract_values, named_values
from ray.tests.conftest import *  # noqa


def _should_skip_huggingface_test():
    """Check if we should skip the HuggingFace test due to version incompatibility."""
    pyarrow_version = get_pyarrow_version()
    if pyarrow_version is None:
        return False

    try:
        datasets_version = __import__("datasets").__version__
        if datasets_version is None:
            return False

        return pyarrow_version < parse_version("12.0.0") and parse_version(
            datasets_version
        ) >= parse_version("3.0.0")
    except (ImportError, AttributeError):
        return False


def _check_valid_plan_and_result(
    ds,
    expected_plan,
    expected_result,
    expected_physical_plan_ops=None,
):
    assert ds.take_all() == expected_result
    assert ds._plan._logical_plan.dag.dag_str == expected_plan

    expected_physical_plan_ops = expected_physical_plan_ops or []
    for op in expected_physical_plan_ops:
        assert op in ds.stats(), f"Operator {op} not found: {ds.stats()}"


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
            self, parallelism: int, per_task_row_limit: Optional[int] = None
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
    assert extract_values("id", ds.take_all()) == list(range(5)), ds
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
    assert op.cols == cols

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
    path = "example://iris.parquet"
    ds = ray.data.read_parquet(path)
    ds = ds.map_batches(lambda d: d)
    cols_rename = {"sepal.length": "sepal_length", "petal.width": "pedal_width"}
    ds = ds.rename_columns(cols_rename)

    logical_plan = ds._plan._logical_plan
    op = logical_plan.dag
    assert isinstance(op, Project), op.name
    assert not op.cols
    assert op.cols_rename == cols_rename

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


def test_random_shuffle_operator(ray_start_regular_shared_2_cpus):
    ctx = DataContext.get_current()

    planner = create_planner()
    read_op = get_parquet_read_logical_op()
    op = RandomShuffle(
        read_op,
        seed=0,
    )
    plan = LogicalPlan(op, ctx)
    physical_op = planner.plan(plan).dag

    assert op.name == "RandomShuffle"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)

    # Check that the linked logical operator is the same the input op.
    assert physical_op._logical_operators == [op]


def test_random_shuffle_e2e(ray_start_regular_shared_2_cpus, configure_shuffle_method):
    ds = ray.data.range(12, override_num_blocks=4)
    r1 = extract_values("id", ds.random_shuffle(seed=0).take_all())
    r2 = extract_values("id", ds.random_shuffle(seed=1024).take_all())
    assert r1 != r2, (r1, r2)
    assert sorted(r1) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], r1
    assert sorted(r2) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], r2
    _check_usage_record(["ReadRange", "RandomShuffle"])


@pytest.mark.parametrize(
    "shuffle",
    [True, False],
)
def test_repartition_operator(ray_start_regular_shared_2_cpus, shuffle):
    ctx = DataContext.get_current()

    planner = create_planner()
    read_op = get_parquet_read_logical_op()
    op = Repartition(read_op, num_outputs=5, shuffle=shuffle)
    plan = LogicalPlan(op, ctx)
    physical_op = planner.plan(plan).dag

    assert op.name == "Repartition"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)

    # Check that the linked logical operator is the same the input op.
    assert physical_op._logical_operators == [op]


@pytest.mark.parametrize(
    "shuffle",
    [True, False],
)
def test_repartition_e2e(
    ray_start_regular_shared_2_cpus, configure_shuffle_method, shuffle
):
    def _check_repartition_usage_and_stats(ds):
        _check_usage_record(["ReadRange", "Repartition"])
        ds_stats: DatasetStats = ds._plan.stats()
        if shuffle:
            assert ds_stats.base_name == "ReadRange->Repartition"
            assert "ReadRange->RepartitionMap" in ds_stats.metadata
        else:
            assert ds_stats.base_name == "Repartition"
            assert "RepartitionSplit" in ds_stats.metadata
        assert "RepartitionReduce" in ds_stats.metadata

    ds = ray.data.range(10000, override_num_blocks=10).repartition(20, shuffle=shuffle)
    assert ds._plan.initial_num_blocks() == 20, ds._plan.initial_num_blocks()
    assert ds.sum() == sum(range(10000))
    assert ds._block_num_rows() == [500] * 20, ds._block_num_rows()
    _check_repartition_usage_and_stats(ds)

    # Test num_output_blocks > num_rows to trigger empty block handling.
    ds = ray.data.range(20, override_num_blocks=10).repartition(40, shuffle=shuffle)
    assert ds._plan.initial_num_blocks() == 40, ds._plan.initial_num_blocks()
    assert ds.sum() == sum(range(20))
    if shuffle:
        assert ds._block_num_rows() == [10] * 2 + [0] * (40 - 2), ds._block_num_rows()
    else:
        assert ds._block_num_rows() == [1] * 20 + [0] * 20, ds._block_num_rows()
    _check_repartition_usage_and_stats(ds)

    # Test case where number of rows does not divide equally into num_output_blocks.
    ds = ray.data.range(22).repartition(4, shuffle=shuffle)
    assert ds._plan.initial_num_blocks() == 4, ds._plan.initial_num_blocks()
    assert ds.sum() == sum(range(22))
    if shuffle:
        assert ds._block_num_rows() == [9, 9, 4, 0], ds._block_num_rows()
    else:
        assert ds._block_num_rows() == [5, 6, 5, 6], ds._block_num_rows()
    _check_repartition_usage_and_stats(ds)

    # Test case where we do not split on repartitioning.
    ds = ray.data.range(10, override_num_blocks=1).repartition(1, shuffle=shuffle)
    assert ds._plan.initial_num_blocks() == 1, ds._plan.initial_num_blocks()
    assert ds.sum() == sum(range(10))
    assert ds._block_num_rows() == [10], ds._block_num_rows()
    _check_repartition_usage_and_stats(ds)


def test_write_operator(ray_start_regular_shared_2_cpus, tmp_path):
    ctx = DataContext.get_current()

    concurrency = 2
    planner = create_planner()
    datasink = ParquetDatasink(tmp_path)
    read_op = get_parquet_read_logical_op()
    op = Write(
        read_op,
        datasink,
        concurrency=concurrency,
    )
    plan = LogicalPlan(op, ctx)
    physical_op = planner.plan(plan).dag

    assert op.name == "Write"
    assert isinstance(physical_op, TaskPoolMapOperator)
    assert physical_op._concurrency == concurrency
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)

    # Check that the linked logical operator is the same the input op.
    assert physical_op._logical_operators == [op]


def test_sort_operator(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()

    planner = create_planner()
    read_op = get_parquet_read_logical_op()
    op = Sort(
        read_op,
        sort_key=SortKey("col1"),
    )
    plan = LogicalPlan(op, ctx)
    physical_op = planner.plan(plan).dag

    assert op.name == "Sort"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_sort_e2e(ray_start_regular_shared_2_cpus, configure_shuffle_method, tmp_path):
    ds = ray.data.range(100, override_num_blocks=4)
    ds = ds.random_shuffle()
    ds = ds.sort("id")
    assert extract_values("id", ds.take_all()) == list(range(100))
    _check_usage_record(["ReadRange", "RandomShuffle", "Sort"])

    df = pd.DataFrame({"one": list(range(100)), "two": ["a"] * 100})
    ds = ray.data.from_pandas([df])
    ds.write_parquet(tmp_path)

    ds = ray.data.read_parquet(tmp_path)
    ds = ds.random_shuffle()
    ds1 = ds.sort("one")
    ds2 = ds.sort("one", descending=True)
    r1 = ds1.select_columns(["one"]).take_all()
    r2 = ds2.select_columns(["one"]).take_all()
    assert [d["one"] for d in r1] == list(range(100))
    assert [d["one"] for d in r2] == list(reversed(range(100)))


def test_sort_validate_keys(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(10)
    assert extract_values("id", ds.sort("id").take_all()) == list(range(10))

    invalid_col_name = "invalid_column"
    with pytest.raises(ValueError, match="there's no such column in the dataset"):
        ds.sort(invalid_col_name).take_all()

    ds_named = ray.data.from_items(
        [
            {"col1": 1, "col2": 2},
            {"col1": 3, "col2": 4},
            {"col1": 5, "col2": 6},
            {"col1": 7, "col2": 8},
        ]
    )

    ds_sorted_col1 = ds_named.sort("col1", descending=True)
    r1 = ds_sorted_col1.select_columns(["col1"]).take_all()
    r2 = ds_sorted_col1.select_columns(["col2"]).take_all()
    assert [d["col1"] for d in r1] == [7, 5, 3, 1]
    assert [d["col2"] for d in r2] == [8, 6, 4, 2]

    with pytest.raises(ValueError, match="there's no such column in the dataset"):
        ds_named.sort(invalid_col_name).take_all()


def test_inherit_batch_format_rule():
    from ray.data._internal.logical.rules.inherit_batch_format import (
        InheritBatchFormatRule,
    )

    ctx = DataContext.get_current()

    operator1 = get_parquet_read_logical_op()
    operator2 = MapBatches(operator1, fn=lambda g: g, batch_format="pandas")
    sort_key = SortKey("number", descending=True)
    operator3 = Sort(operator2, sort_key)
    original_plan = LogicalPlan(dag=operator3, context=ctx)

    rule = InheritBatchFormatRule()
    optimized_plan = rule.apply(original_plan)
    assert optimized_plan.dag._batch_format == "pandas"


def test_batch_format_on_sort(ray_start_regular_shared_2_cpus):
    """Checks that the Sort op can inherit batch_format from upstream ops correctly."""
    ds = ray.data.from_items(
        [
            {"col1": 1, "col2": 2},
            {"col1": 1, "col2": 4},
            {"col1": 5, "col2": 6},
            {"col1": 7, "col2": 8},
        ]
    )
    df_expected = pd.DataFrame(
        {
            "col1": [7, 5, 1, 1],
            "col2": [8, 6, 4, 2],
        }
    )
    df_actual = (
        ds.groupby("col1")
        .map_groups(lambda g: g, batch_format="pandas")
        .sort("col2", descending=True)
        .to_pandas()
    )
    pd.testing.assert_frame_equal(df_actual, df_expected)


def test_batch_format_on_aggregate(ray_start_regular_shared_2_cpus):
    """Checks that the Aggregate op can inherit batch_format
    from upstream ops correctly."""
    from ray.data.aggregate import AggregateFn

    ds = ray.data.from_items(
        [
            {"col1": 1, "col2": 2},
            {"col1": 1, "col2": 4},
            {"col1": 5, "col2": 6},
            {"col1": 7, "col2": 8},
        ]
    )
    aggregation = AggregateFn(
        init=lambda column: 1,
        accumulate_row=lambda a, row: a * row["col2"],
        merge=lambda a1, a2: a1 * a2,
        name="prod",
    )
    assert (
        ds.groupby("col1")
        .map_groups(lambda g: g, batch_format="pandas")
        .aggregate(aggregation)
    ) == {"prod": 384}


def test_aggregate_e2e(ray_start_regular_shared_2_cpus, configure_shuffle_method):
    ds = ray.data.range(100, override_num_blocks=4)
    ds = ds.groupby("id").count()
    assert ds.count() == 100
    for idx, row in enumerate(ds.sort("id").iter_rows()):
        assert row == {"id": idx, "count()": 1}
    _check_usage_record(["ReadRange", "Aggregate"])


def test_aggregate_validate_keys(ray_start_regular_shared_2_cpus):
    ds = ray.data.range(10)
    invalid_col_name = "invalid_column"
    with pytest.raises(ValueError):
        ds.groupby(invalid_col_name).count()

    ds_named = ray.data.from_items(
        [
            {"col1": 1, "col2": "a"},
            {"col1": 1, "col2": "b"},
            {"col1": 2, "col2": "c"},
            {"col1": 3, "col2": "c"},
        ]
    )

    ds_groupby_col1 = ds_named.groupby("col1").count()
    assert ds_groupby_col1.sort("col1").take_all() == [
        {"col1": 1, "count()": 2},
        {"col1": 2, "count()": 1},
        {"col1": 3, "count()": 1},
    ]
    ds_groupby_col2 = ds_named.groupby("col2").count()
    assert ds_groupby_col2.sort("col2").take_all() == [
        {"col2": "a", "count()": 1},
        {"col2": "b", "count()": 1},
        {"col2": "c", "count()": 2},
    ]

    with pytest.raises(
        ValueError,
        match="there's no such column in the dataset",
    ):
        ds_named.groupby(invalid_col_name).count()


def test_zip_operator(ray_start_regular_shared_2_cpus):
    ctx = DataContext.get_current()

    planner = create_planner()
    read_op1 = get_parquet_read_logical_op()
    read_op2 = get_parquet_read_logical_op()
    op = Zip(read_op1, read_op2)
    plan = LogicalPlan(op, ctx)
    physical_op = planner.plan(plan).dag

    assert op.name == "Zip"
    assert isinstance(physical_op, ZipOperator)
    assert len(physical_op.input_dependencies) == 2
    assert isinstance(physical_op.input_dependencies[0], MapOperator)
    assert isinstance(physical_op.input_dependencies[1], MapOperator)

    # Check that the linked logical operator is the same the input op.
    assert physical_op._logical_operators == [op]


@pytest.mark.parametrize(
    "num_blocks1,num_blocks2,num_blocks3",
    list(itertools.combinations_with_replacement(range(1, 12), 3)),
)
def test_zip_e2e(
    ray_start_regular_shared_2_cpus, num_blocks1, num_blocks2, num_blocks3
):
    n = 12
    ds1 = ray.data.range(n, override_num_blocks=num_blocks1)
    ds2 = ray.data.range(n, override_num_blocks=num_blocks2).map(
        column_udf("id", lambda x: x + 1)
    )
    ds3 = ray.data.range(n, override_num_blocks=num_blocks3).map(
        column_udf("id", lambda x: x + 2)
    )
    ds = ds1.zip(ds2, ds3)
    assert ds.take() == named_values(
        ["id", "id_1", "id_2"], zip(range(n), range(1, n + 1), range(2, n + 2))
    )
    _check_usage_record(["ReadRange", "Zip"])


def test_from_modin_e2e(ray_start_regular_shared_2_cpus):
    import modin.pandas as mopd

    df = pd.DataFrame(
        {"one": list(range(100)), "two": list(range(100))},
    )
    modf = mopd.DataFrame(df)
    ds = ray.data.from_modin(modf)
    # `ds.take_all()` triggers execution with new backend, which is
    # needed for checking operator usage below.
    assert len(ds.take_all()) == len(df)
    # `ds.to_pandas()` does not use the new backend.
    dfds = ds.to_pandas()

    assert df.equals(dfds)
    # Check that metadata fetch is included in stats. This is `FromPandas`
    # instead of `FromModin` because `from_modin` reduces to `from_pandas_refs`.
    assert "FromPandas" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromPandas"
    _check_usage_record(["FromPandas"])


@pytest.mark.parametrize("enable_pandas_block", [False, True])
def test_from_pandas_refs_e2e(ray_start_regular_shared_2_cpus, enable_pandas_block):
    ctx = ray.data.context.DataContext.get_current()
    old_enable_pandas_block = ctx.enable_pandas_block
    ctx.enable_pandas_block = enable_pandas_block

    try:
        df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
        df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})

        ds = ray.data.from_pandas_refs([ray.put(df1), ray.put(df2)])
        values = [(r["one"], r["two"]) for r in ds.take(6)]
        rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "FromPandas" in ds.stats()
        assert ds._plan._logical_plan.dag.name == "FromPandas"

        # Test chaining multiple operations
        ds2 = ds.map_batches(lambda x: x)
        values = [(r["one"], r["two"]) for r in ds2.take(6)]
        assert values == rows
        assert "MapBatches" in ds2.stats()
        assert "FromPandas" in ds2.stats()
        assert ds2._plan._logical_plan.dag.name == "MapBatches(<lambda>)"

        # test from single pandas dataframe
        ds = ray.data.from_pandas_refs(ray.put(df1))
        values = [(r["one"], r["two"]) for r in ds.take(3)]
        rows = [(r.one, r.two) for _, r in df1.iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "FromPandas" in ds.stats()
        assert ds._plan._logical_plan.dag.name == "FromPandas"
        _check_usage_record(["FromPandas"])
    finally:
        ctx.enable_pandas_block = old_enable_pandas_block


def test_from_numpy_refs_e2e(ray_start_regular_shared_2_cpus):
    import numpy as np

    arr1 = np.expand_dims(np.arange(0, 4), axis=1)
    arr2 = np.expand_dims(np.arange(4, 8), axis=1)

    ds = ray.data.from_numpy_refs([ray.put(arr1), ray.put(arr2)])
    values = np.stack(extract_values("data", ds.take(8)))
    np.testing.assert_array_equal(values, np.concatenate((arr1, arr2)))
    # Check that conversion task is included in stats.
    assert "FromNumpy" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromNumpy"
    _check_usage_record(["FromNumpy"])

    # Test chaining multiple operations
    ds2 = ds.map_batches(lambda x: x)
    values = np.stack(extract_values("data", ds2.take(8)))
    np.testing.assert_array_equal(values, np.concatenate((arr1, arr2)))
    assert "MapBatches" in ds2.stats()
    assert "FromNumpy" in ds2.stats()
    assert ds2._plan._logical_plan.dag.name == "MapBatches(<lambda>)"
    _check_usage_record(["FromNumpy", "MapBatches"])

    # Test from single NumPy ndarray.
    ds = ray.data.from_numpy_refs(ray.put(arr1))
    values = np.stack(extract_values("data", ds.take(4)))
    np.testing.assert_array_equal(values, arr1)
    # Check that conversion task is included in stats.
    assert "FromNumpy" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromNumpy"
    _check_usage_record(["FromNumpy"])


def test_from_arrow_refs_e2e(ray_start_regular_shared_2_cpus):
    import pyarrow as pa

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_arrow_refs(
        [ray.put(pa.Table.from_pandas(df1)), ray.put(pa.Table.from_pandas(df2))]
    )

    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "FromArrow" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromArrow"
    _check_usage_record(["FromArrow"])

    # test from single pyarrow table ref
    ds = ray.data.from_arrow_refs(ray.put(pa.Table.from_pandas(df1)))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows
    # Check that conversion task is included in stats.
    assert "FromArrow" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromArrow"
    _check_usage_record(["FromArrow"])


@pytest.mark.skipif(
    _should_skip_huggingface_test,
    reason="Skip due to HuggingFace datasets >= 3.0.0 requiring pyarrow >= 12.0.0",
)
def test_from_huggingface_e2e(ray_start_regular_shared_2_cpus):
    import datasets

    from ray.data.tests.test_huggingface import hfds_assert_equals

    data = datasets.load_dataset("tweet_eval", "emotion")
    assert isinstance(data, datasets.DatasetDict)
    ray_datasets = {
        "train": ray.data.from_huggingface(data["train"]),
        "validation": ray.data.from_huggingface(data["validation"]),
        "test": ray.data.from_huggingface(data["test"]),
    }

    for ds_key, ds in ray_datasets.items():
        assert isinstance(ds, ray.data.Dataset)
        # `ds.take_all()` triggers execution with new backend, which is
        # needed for checking operator usage below.
        assert len(ds.take_all()) > 0
        # Check that metadata fetch is included in stats;
        # the underlying implementation uses the `ReadParquet` operator
        # as this is an un-transformed public dataset.
        assert "ReadParquet" in ds.stats() or "FromArrow" in ds.stats()
        assert (
            ds._plan._logical_plan.dag.name == "ReadParquet"
            or ds._plan._logical_plan.dag.name == "FromArrow"
        )
        # use sort by 'text' to match order of rows
        hfds_assert_equals(data[ds_key], ds)
        try:
            _check_usage_record(["ReadParquet"])
        except AssertionError:
            _check_usage_record(["FromArrow"])

    # test transformed public dataset for fallback behavior
    base_hf_dataset = data["train"]
    hf_dataset_split = base_hf_dataset.train_test_split(test_size=0.2)
    ray_dataset_split_train = ray.data.from_huggingface(hf_dataset_split["train"])
    assert isinstance(ray_dataset_split_train, ray.data.Dataset)
    # `ds.take_all()` triggers execution with new backend, which is
    # needed for checking operator usage below.
    assert len(ray_dataset_split_train.take_all()) > 0
    # Check that metadata fetch is included in stats;
    # the underlying implementation uses the `FromArrow` operator.
    assert "FromArrow" in ray_dataset_split_train.stats()
    assert ray_dataset_split_train._plan._logical_plan.dag.name == "FromArrow"
    assert ray_dataset_split_train.count() == hf_dataset_split["train"].num_rows
    _check_usage_record(["FromArrow"])


@pytest.mark.skipif(
    sys.version_info >= (3, 12),
    reason="Skip due to incompatibility tensorflow with Python 3.12+",
)
def test_from_tf_e2e(ray_start_regular_shared_2_cpus):
    import tensorflow as tf
    import tensorflow_datasets as tfds

    tf_dataset = tfds.load("mnist", split=["train"], as_supervised=True)[0]
    tf_dataset = tf_dataset.take(8)  # Use subset to make test run faster.

    ray_dataset = ray.data.from_tf(tf_dataset)

    actual_data = extract_values("item", ray_dataset.take_all())
    expected_data = list(tf_dataset)
    assert len(actual_data) == len(expected_data)
    for (expected_features, expected_label), (actual_features, actual_label) in zip(
        expected_data, actual_data
    ):
        tf.debugging.assert_equal(expected_features, actual_features)
        tf.debugging.assert_equal(expected_label, actual_label)

    # Check that metadata fetch is included in stats.
    assert "FromItems" in ray_dataset.stats()
    # Underlying implementation uses `FromItems` operator
    assert ray_dataset._plan._logical_plan.dag.name == "FromItems"
    _check_usage_record(["FromItems"])


def test_from_torch_e2e(ray_start_regular_shared_2_cpus, tmp_path):
    import torchvision

    torch_dataset = torchvision.datasets.FashionMNIST(tmp_path, download=True)

    ray_dataset = ray.data.from_torch(torch_dataset)

    expected_data = list(torch_dataset)
    actual_data = list(ray_dataset.take_all())
    assert extract_values("item", actual_data) == expected_data

    # Check that metadata fetch is included in stats.
    assert "ReadTorch" in ray_dataset.stats()

    # Underlying implementation uses `FromItems` operator
    assert ray_dataset._plan._logical_plan.dag.name == "ReadTorch"
    _check_usage_record(["ReadTorch"])


def test_limit_pushdown_conservative(ray_start_regular_shared_2_cpus):
    """Test limit pushdown behavior - pushes through safe operations."""

    def f1(x):
        return x

    def f2(x):
        return x

    # Test 1: Basic Limit -> Limit fusion (should still work)
    ds = ray.data.range(100).limit(5).limit(100)
    _check_valid_plan_and_result(
        ds, "Read[ReadRange] -> Limit[limit=5]", [{"id": i} for i in range(5)]
    )

    ds = ray.data.range(100).limit(100).limit(5)
    _check_valid_plan_and_result(
        ds, "Read[ReadRange] -> Limit[limit=5]", [{"id": i} for i in range(5)]
    )

    ds = ray.data.range(100).limit(50).limit(80).limit(5).limit(20)
    _check_valid_plan_and_result(
        ds, "Read[ReadRange] -> Limit[limit=5]", [{"id": i} for i in range(5)]
    )

    # Test 2: Limit should push through MapRows operations (safe)
    ds = ray.data.range(100, override_num_blocks=100).map(f1).limit(1)
    _check_valid_plan_and_result(
        ds, "Read[ReadRange] -> Limit[limit=1] -> MapRows[Map(f1)]", [{"id": 0}]
    )

    # Test 3: Limit should not push through MapBatches operations
    ds = ray.data.range(100, override_num_blocks=100).map_batches(f2).limit(1)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> MapBatches[MapBatches(f2)] -> Limit[limit=1]",
        [{"id": 0}],
    )

    # Test 4: Limit should NOT push through Filter operations (conservative)
    ds = (
        ray.data.range(100, override_num_blocks=100)
        .filter(lambda x: x["id"] < 50)
        .limit(1)
    )
    _check_valid_plan_and_result(
        ds, "Read[ReadRange] -> Filter[Filter(<lambda>)] -> Limit[limit=1]", [{"id": 0}]
    )

    # Test 5: Limit should push through Project operations (safe)
    ds = ray.data.range(100, override_num_blocks=100).select_columns(["id"]).limit(5)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> Limit[limit=5] -> Project[Project]",
        [{"id": i} for i in range(5)],
    )

    # Test 6: Limit should stop at Sort operations (AllToAll)
    ds = ray.data.range(100).sort("id").limit(5)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> Sort[Sort] -> Limit[limit=5]",
        [{"id": i} for i in range(5)],
    )

    # Test 7: More complex interweaved case.
    ds = ray.data.range(100).sort("id").map(f1).limit(20).sort("id").map(f2).limit(5)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> Sort[Sort] -> Limit[limit=20] -> MapRows[Map(f1)] -> "
        "Sort[Sort] -> Limit[limit=5] -> MapRows[Map(f2)]",
        [{"id": i} for i in range(5)],
    )

    # Test 8: Test limit pushdown between two Map operators.
    ds = ray.data.range(100, override_num_blocks=100).map(f1).limit(1).map(f2)
    _check_valid_plan_and_result(
        ds,
        "Read[ReadRange] -> Limit[limit=1] -> MapRows[Map(f1)] -> MapRows[Map(f2)]",
        [{"id": 0}],
    )


def test_limit_pushdown_correctness(ray_start_regular_shared_2_cpus):
    """Test that limit pushdown produces correct results in various scenarios."""

    # Test 1: Simple project + limit
    ds = ray.data.range(100).select_columns(["id"]).limit(10)
    result = ds.take_all()
    expected = [{"id": i} for i in range(10)]
    assert result == expected

    # Test 2: Multiple operations + limit (with MapRows pushdown)
    ds = (
        ray.data.range(100)
        .map(lambda x: {"id": x["id"], "squared": x["id"] ** 2})
        .select_columns(["id"])
        .limit(5)
    )
    result = ds.take_all()
    expected = [{"id": i} for i in range(5)]
    assert result == expected

    # Test 3: MapRows operations should get limit pushed (safe)
    ds = ray.data.range(100).map(lambda x: {"id": x["id"] * 2}).limit(5)
    result = ds.take_all()
    expected = [{"id": i * 2} for i in range(5)]
    assert result == expected

    # Test 4: MapBatches operations should not get limit pushed
    ds = ray.data.range(100).map_batches(lambda batch: {"id": batch["id"] * 2}).limit(5)
    result = ds.take_all()
    expected = [{"id": i * 2} for i in range(5)]
    assert result == expected

    # Test 5: Filter operations should not get limit pushed (conservative)
    ds = ray.data.range(100).filter(lambda x: x["id"] % 2 == 0).limit(3)
    result = ds.take_all()
    expected = [{"id": i} for i in [0, 2, 4]]
    assert result == expected

    # Test 6: Complex chain with both safe operations (should all get limit pushed)
    ds = (
        ray.data.range(100)
        .select_columns(["id"])  # Project - could be safe if it was the immediate input
        .map(lambda x: {"id": x["id"] + 1})  # MapRows - NOT safe, stops pushdown
        .limit(3)
    )
    result = ds.take_all()
    expected = [{"id": i + 1} for i in range(3)]
    assert result == expected

    # The plan should show all operations after the limit
    plan_str = ds._plan._logical_plan.dag.dag_str
    assert (
        "Read[ReadRange] -> Limit[limit=3] -> Project[Project] -> MapRows[Map(<lambda>)]"
        == plan_str
    )


def test_limit_pushdown_scan_efficiency(ray_start_regular_shared_2_cpus):
    """Test that limit pushdown scans fewer rows from the data source."""

    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self, amount=1):
            self.value += amount
            return self.value

        def get(self):
            return self.value

        def reset(self):
            self.value = 0

    # Create a custom datasource that tracks how many rows it produces
    class CountingDatasource(Datasource):
        def __init__(self):
            self.counter = Counter.remote()

        def prepare_read(self, parallelism, n_per_block=10):
            def read_fn(block_idx):
                # Each block produces n_per_block rows
                ray.get(self.counter.increment.remote(n_per_block))
                return [
                    pd.DataFrame(
                        {
                            "id": range(
                                block_idx * n_per_block, (block_idx + 1) * n_per_block
                            )
                        }
                    )
                ]

            return [
                ReadTask(
                    lambda i=i: read_fn(i),
                    BlockMetadata(
                        num_rows=n_per_block,
                        size_bytes=n_per_block * 8,  # rough estimate
                        input_files=None,
                        exec_stats=None,
                    ),
                )
                for i in range(parallelism)
            ]

        def get_rows_produced(self):
            return ray.get(self.counter.get.remote())

    # Test 1: Project + Limit should scan fewer rows due to pushdown
    source = CountingDatasource()
    ds = ray.data.read_datasource(source, override_num_blocks=20, n_per_block=10)
    ds = ds.select_columns(["id"]).limit(5)
    result = ds.take_all()

    # Should get correct results
    assert len(result) == 5
    assert result == [{"id": i} for i in range(5)]

    # Should have scanned significantly fewer than all 200 rows (20 blocks * 10 rows)
    # Due to pushdown, we should scan much less
    rows_produced_1 = source.get_rows_produced()
    assert rows_produced_1 < 200  # Should be much less than total

    # Test 2: MapRows + Limit should also scan fewer rows due to pushdown
    source2 = CountingDatasource()
    ds2 = ray.data.read_datasource(source2, override_num_blocks=20, n_per_block=10)
    ds2 = ds2.map(lambda x: x).limit(5)
    result2 = ds2.take_all()

    # Should get correct results
    assert len(result2) == 5
    assert result2 == [{"id": i} for i in range(5)]

    # Should also scan fewer than total due to pushdown
    rows_produced_2 = source2.get_rows_produced()
    assert rows_produced_2 < 200

    # Both should be efficient with pushdown
    assert rows_produced_1 < 100  # Should be much less than total
    assert rows_produced_2 < 100  # Should be much less than total

    # Test 3: Filter + Limit should scan fewer due to early termination, but not pushdown
    source3 = CountingDatasource()
    ds3 = ray.data.read_datasource(source3, override_num_blocks=20, n_per_block=10)
    ds3 = ds3.filter(lambda x: x["id"] % 2 == 0).limit(3)
    result3 = ds3.take_all()

    # Should get correct results
    assert len(result3) == 3
    assert result3 == [{"id": i} for i in [0, 2, 4]]

    # Should still scan fewer than total due to early termination
    rows_produced_3 = source3.get_rows_produced()
    assert rows_produced_3 < 200


def test_limit_pushdown_union(ray_start_regular_shared_2_cpus):
    """Test limit pushdown behavior with Union operations."""

    # Create two datasets and union with limit
    ds1 = ray.data.range(100, override_num_blocks=10)
    ds2 = ray.data.range(200, override_num_blocks=10)
    ds = ds1.union(ds2).limit(5)

    expected_plan = "Read[ReadRange] -> Limit[limit=5], Read[ReadRange] -> Limit[limit=5] -> Union[Union] -> Limit[limit=5]"
    _check_valid_plan_and_result(ds, expected_plan, [{"id": i} for i in range(5)])


def test_limit_pushdown_union_with_maprows(ray_start_regular_shared_2_cpus):
    """Limit after Union + MapRows: limit should be pushed before the MapRows
    and inside each Union branch."""
    ds1 = ray.data.range(100, override_num_blocks=10)
    ds2 = ray.data.range(200, override_num_blocks=10)
    ds = ds1.union(ds2).map(lambda x: x).limit(5)

    expected_plan = (
        "Read[ReadRange] -> Limit[limit=5], "
        "Read[ReadRange] -> Limit[limit=5] -> Union[Union] -> "
        "Limit[limit=5] -> MapRows[Map(<lambda>)]"
    )
    _check_valid_plan_and_result(ds, expected_plan, [{"id": i} for i in range(5)])


def test_limit_pushdown_union_with_sort(ray_start_regular_shared_2_cpus):
    """Limit after Union + Sort: limit must NOT push through the Sort."""
    ds1 = ray.data.range(100, override_num_blocks=4)
    ds2 = ray.data.range(50, override_num_blocks=4).map(
        lambda x: {"id": x["id"] + 1000}
    )
    ds = ds1.union(ds2).sort("id").limit(5)

    expected_plan = (
        "Read[ReadRange], "
        "Read[ReadRange] -> MapRows[Map(<lambda>)] -> "
        "Union[Union] -> Sort[Sort] -> Limit[limit=5]"
    )
    _check_valid_plan_and_result(ds, expected_plan, [{"id": i} for i in range(5)])


def test_limit_pushdown_multiple_unions(ray_start_regular_shared_2_cpus):
    """Outer limit over nested unions should create a branch-local limit
    for every leaf plus the global one."""
    ds = (
        ray.data.range(100)
        .union(ray.data.range(100, override_num_blocks=5))
        .union(ray.data.range(50))
        .limit(5)
    )

    expected_plan = (
        "Read[ReadRange] -> Limit[limit=5], "
        "Read[ReadRange] -> Limit[limit=5] -> Union[Union] -> Limit[limit=5], "
        "Read[ReadRange] -> Limit[limit=5] -> Union[Union] -> Limit[limit=5]"
    )
    _check_valid_plan_and_result(ds, expected_plan, [{"id": i} for i in range(5)])


def test_limit_pushdown_union_with_groupby(ray_start_regular_shared_2_cpus):
    """Limit after Union + Aggregate: limit should stay after Aggregate."""
    ds1 = ray.data.range(100)
    ds2 = ray.data.range(100).map(lambda x: {"id": x["id"] + 1000})
    ds = ds1.union(ds2).groupby("id").count().limit(5)
    # Result should contain 5 distinct ids with count == 1.
    res = ds.take_all()
    # Plan suffix check (no branch limits past Aggregate).
    assert ds._plan._logical_plan.dag.dag_str.endswith(
        "Union[Union] -> Aggregate[Aggregate] -> Limit[limit=5]"
    )
    assert len(res) == 5 and all(r["count()"] == 1 for r in res)


def test_limit_pushdown_complex_chain(ray_start_regular_shared_2_cpus):
    """
    Complex end-to-end case:
      1. Two branches each with a branch-local Limit pushed to Read.
         • left  : Project
         • right : MapRows
      2. Union of the two branches.
      3. Global Aggregate (groupby/count).
      4. Sort (descending id) – pushes stop here.
      5. Final Limit.
    Verifies both plan rewrite and result correctness.
    """
    # ── left branch ────────────────────────────────────────────────
    left = ray.data.range(50).select_columns(["id"]).limit(10)

    # ── right branch ───────────────────────────────────────────────
    right = ray.data.range(50).map(lambda x: {"id": x["id"] + 1000}).limit(10)

    # ── union → aggregate → sort → limit ──────────────────────────
    ds = left.union(right).groupby("id").count().sort("id", descending=True).limit(3)

    # Expected logical-plan string.
    expected_plan = (
        "Read[ReadRange] -> Limit[limit=10] -> Project[Project], "
        "Read[ReadRange] -> Limit[limit=10] -> MapRows[Map(<lambda>)] "
        "-> Union[Union] -> Aggregate[Aggregate] -> Sort[Sort] -> Limit[limit=3]"
    )

    # Top-3 ids are the three largest (1009, 1008, 1007) with count()==1.
    expected_result = [
        {"id": 1009, "count()": 1},
        {"id": 1008, "count()": 1},
        {"id": 1007, "count()": 1},
    ]

    _check_valid_plan_and_result(ds, expected_plan, expected_result)


def test_limit_pushdown_union_maps_projects(ray_start_regular_shared_2_cpus):
    r"""
    Read -> MapBatches -> MapRows -> Project
         \                               /
          --------   Union   -------------   → Limit
    The limit should be pushed in front of each branch
    (past MapRows, Project) while the original
    global Limit is preserved after the Union.
    """
    # Left branch.
    left = (
        ray.data.range(30)
        .map_batches(lambda b: b)
        .map(lambda r: {"id": r["id"]})
        .select_columns(["id"])
    )

    # Right branch with shifted ids.
    right = (
        ray.data.range(30)
        .map_batches(lambda b: b)
        .map(lambda r: {"id": r["id"] + 100})
        .select_columns(["id"])
    )

    ds = left.union(right).limit(3)

    expected_plan = (
        "Read[ReadRange] -> "
        "MapBatches[MapBatches(<lambda>)] -> Limit[limit=3] -> MapRows[Map(<lambda>)] -> "
        "Project[Project], "
        "Read[ReadRange] -> "
        "MapBatches[MapBatches(<lambda>)] -> Limit[limit=3] -> MapRows[Map(<lambda>)] -> "
        "Project[Project] -> Union[Union] -> Limit[limit=3]"
    )

    expected_result = [{"id": i} for i in range(3)]  # First 3 rows from left branch.

    _check_valid_plan_and_result(ds, expected_plan, expected_result)


def test_execute_to_legacy_block_list(
    ray_start_regular_shared_2_cpus,
):
    ds = ray.data.range(10)
    # Stats not initialized until `ds.iter_rows()` is called
    assert ds._plan._snapshot_stats is None

    for i, row in enumerate(ds.iter_rows()):
        assert row["id"] == i

    assert ds._plan._snapshot_stats is not None
    assert "ReadRange" in ds._plan._snapshot_stats.metadata
    assert ds._plan._snapshot_stats.time_total_s > 0


def test_streaming_executor(
    ray_start_regular_shared_2_cpus,
):
    ds = ray.data.range(100, override_num_blocks=4)
    ds = ds.map_batches(lambda x: x)
    ds = ds.filter(lambda x: x["id"] > 0)
    ds = ds.random_shuffle()
    ds = ds.map_batches(lambda x: x)

    result = []
    for batch in ds.iter_batches(batch_size=3):
        batch = batch["id"]
        assert len(batch) == 3, batch
        result.extend(batch)
    assert sorted(result) == list(range(1, 100)), result
    _check_usage_record(["ReadRange", "MapBatches", "Filter", "RandomShuffle"])


def test_schema_partial_execution(
    ray_start_regular_shared_2_cpus,
):
    fields = [
        ("sepal.length", pa.float64()),
        ("sepal.width", pa.float64()),
        ("petal.length", pa.float64()),
        ("petal.width", pa.float64()),
        ("variety", pa.string()),
    ]
    ds = ray.data.read_parquet(
        "example://iris.parquet",
        schema=pa.schema(fields),
        override_num_blocks=2,
    ).map_batches(lambda x: x)

    iris_schema = ds.schema()
    assert iris_schema == ray.data.dataset.Schema(pa.schema(fields))
    # Verify that ds.schema() executes only the first block, and not the
    # entire Dataset.
    assert not ds._plan.has_computed_output()
    assert ds._plan._logical_plan.dag.dag_str == (
        "Read[ReadParquet] -> MapBatches[MapBatches(<lambda>)]"
    )


@pytest.mark.parametrize(
    "average_bytes_per_output, ray_remote_args, ray_remote_args_fn, data_context, expected_memory",
    [
        # The user hasn't set memory, so the rule should configure it.
        (1, None, None, DataContext(), 1),
        # The user has set memory, so the rule shouldn't change it.
        (1, {"memory": 2}, None, DataContext(), 2),
        (1, None, lambda: {"memory": 2}, DataContext(), 2),
        # An estimate isn't available, so the rule shouldn't configure memory.
        (None, None, None, DataContext(), None),
    ],
)
def test_configure_map_task_memory_rule(
    average_bytes_per_output,
    ray_remote_args,
    ray_remote_args_fn,
    data_context,
    expected_memory,
):
    input_op = InputDataBuffer(MagicMock(), [])
    map_op = MapOperator.create(
        MagicMock(),
        input_op=input_op,
        data_context=data_context,
        ray_remote_args=ray_remote_args,
        ray_remote_args_fn=ray_remote_args_fn,
    )
    map_op._metrics = MagicMock(
        spec=OpRuntimeMetrics, average_bytes_per_output=average_bytes_per_output
    )
    plan = PhysicalPlan(map_op, op_map=MagicMock(), context=data_context)
    rule = ConfigureMapTaskMemoryUsingOutputSize()

    new_plan = rule.apply(plan)

    remote_args = new_plan.dag._get_runtime_ray_remote_args()
    assert remote_args.get("memory") == expected_memory


def test_limit_pushdown_map_per_block_limit_applied(ray_start_regular_shared_2_cpus):
    """Test that per-block limits are actually applied during map execution."""

    # Create a global counter using Ray
    @ray.remote
    class Counter:
        def __init__(self):
            self.value = 0

        def increment(self):
            self.value += 1
            return self.value

        def get(self):
            return self.value

    counter = Counter.remote()

    def track_processing(row):
        # Record that this row was processed
        ray.get(counter.increment.remote())
        return row

    # Create dataset with limit pushed through map
    ds = ray.data.range(1000, override_num_blocks=10).map(track_processing).limit(50)

    # Execute and get results
    result = ds.take_all()

    # Verify correct results
    expected = [{"id": i} for i in range(50)]
    assert result == expected

    # Check how many rows were actually processed
    processed_count = ray.get(counter.get.remote())

    # With per-block limits, we should process fewer rows than the total dataset
    # but at least the number we need for the final result
    assert (
        processed_count >= 50
    ), f"Expected at least 50 rows processed, got {processed_count}"
    assert (
        processed_count < 1000
    ), f"Expected fewer than 1000 rows processed, got {processed_count}"

    print(f"Processed {processed_count} rows to get {len(result)} results")


def test_limit_pushdown_preserves_map_behavior(ray_start_regular_shared_2_cpus):
    """Test that adding per-block limits doesn't change the logical result."""

    def add_one(row):
        row["id"] += 1
        return row

    # Compare with and without limit pushdown
    ds_with_limit = ray.data.range(100).map(add_one).limit(10)
    ds_without_limit = ray.data.range(100).limit(10).map(add_one)

    result_with = ds_with_limit.take_all()
    result_without = ds_without_limit.take_all()

    # Results should be identical
    assert result_with == result_without

    # Both should have the expected transformation applied
    expected = [{"id": i + 1} for i in range(10)]
    assert result_with == expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
