import itertools
import sys
from unittest.mock import MagicMock

import pandas as pd
import pyarrow as pa
import pytest

import ray
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
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data._internal.logical.operators.n_ary_operator import Zip
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.logical.rules.configure_map_task_memory import (
    ConfigureMapTaskMemoryUsingOutputSize,
)
from ray.data._internal.planner import create_planner
from ray.data._internal.planner.exchange.sort_task_spec import SortKey
from ray.data._internal.stats import DatasetStats
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import _check_usage_record, get_parquet_read_logical_op
from ray.data.tests.util import column_udf, extract_values, named_values
from ray.tests.conftest import *  # noqa


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
    list(itertools.combinations_with_replacement(range(1, 4), 3)),
)
def test_zip_e2e(
    ray_start_regular_shared_2_cpus, num_blocks1, num_blocks2, num_blocks3
):
    n = 4
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

    remote_args = new_plan.dag._get_dynamic_ray_remote_args()
    assert remote_args.get("memory") == expected_memory


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
