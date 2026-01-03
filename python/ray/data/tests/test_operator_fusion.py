from unittest.mock import MagicMock

import numpy as np
import pytest

import ray
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    BatchMapTransformFn,
    BlockMapTransformFn,
)
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.input_data_operator import InputData
from ray.data._internal.logical.operators.map_operator import (
    Filter,
    FlatMap,
    MapBatches,
    MapRows,
    Project,
)
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.optimizers import PhysicalOptimizer, get_execution_plan
from ray.data._internal.plan import ExecutionPlan
from ray.data._internal.planner import create_planner
from ray.data._internal.stats import DatasetStats
from ray.data.context import DataContext
from ray.data.dataset import Dataset
from ray.data.expressions import star
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.test_util import _check_usage_record, get_parquet_read_logical_op
from ray.data.tests.util import column_udf, extract_values
from ray.tests.conftest import *  # noqa


def test_read_map_batches_operator_fusion(ray_start_regular_shared_2_cpus):
    ctx = DataContext.get_current()

    # Test that Read is fused with MapBatches.
    planner = create_planner()
    read_op = get_parquet_read_logical_op(parallelism=1)
    op = MapBatches(
        read_op,
        lambda x: x,
    )
    logical_plan = LogicalPlan(op, ctx)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches(<lambda>)"
    assert physical_op.name == "ReadParquet->MapBatches(<lambda>)"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    input = physical_op.input_dependencies[0]
    assert isinstance(input, InputDataBuffer)
    assert physical_op in input.output_dependencies, input.output_dependencies
    assert physical_op._logical_operators == [read_op, op]


def test_read_map_chain_operator_fusion(ray_start_regular_shared_2_cpus):
    ctx = DataContext.get_current()

    # Test that a chain of different map operators are fused.
    planner = create_planner()
    read_op = get_parquet_read_logical_op(parallelism=1)
    map1 = MapRows(read_op, lambda x: x)
    map2 = MapBatches(map1, lambda x: x)
    map3 = FlatMap(map2, lambda x: x)
    map4 = Filter(map3, fn=lambda x: x)
    logical_plan = LogicalPlan(map4, ctx)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert map4.name == "Filter(<lambda>)"
    assert (
        physical_op.name == "ReadParquet->Map(<lambda>)->MapBatches(<lambda>)"
        "->FlatMap(<lambda>)->Filter(<lambda>)"
    )
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)
    assert physical_op._logical_operators == [read_op, map1, map2, map3, map4]


def test_read_map_batches_operator_fusion_compatible_remote_args(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()

    # Test that map operators are stilled fused when remote args are compatible.
    compatiple_remote_args_pairs = [
        # Empty remote args are compatible.
        ({}, {}),
        # Test `num_cpus` and `num_gpus`.
        ({"num_cpus": 2}, {"num_cpus": 2}),
        ({"num_gpus": 2}, {"num_gpus": 2}),
        # `num_cpus` defaults to 1, `num_gpus` defaults to 0.
        # The following 2 should be compatible.
        ({"num_cpus": 1}, {}),
        ({}, {"num_gpus": 0}),
        # Test specifying custom resources.
        ({"resources": {"custom": 1}}, {"resources": {"custom": 1}}),
        ({"resources": {"custom": 0}}, {"resources": {}}),
        # If the downstream op doesn't have `scheduling_strategy`, it will
        # inherit from the upstream op.
        ({"scheduling_strategy": "SPREAD"}, {}),
    ]
    for up_remote_args, down_remote_args in compatiple_remote_args_pairs:
        planner = create_planner()
        read_op = get_parquet_read_logical_op(
            ray_remote_args={"resources": {"non-existent": 1}},
            parallelism=1,
        )
        op = MapBatches(read_op, lambda x: x, ray_remote_args=up_remote_args)
        op = MapBatches(op, lambda x: x, ray_remote_args=down_remote_args)
        logical_plan = LogicalPlan(op, ctx)

        physical_plan = planner.plan(logical_plan)
        optimized_physical_plan = PhysicalOptimizer().optimize(physical_plan)
        physical_op = optimized_physical_plan.dag

        assert op.name == "MapBatches(<lambda>)", (up_remote_args, down_remote_args)
        assert physical_op.name == "MapBatches(<lambda>)->MapBatches(<lambda>)", (
            up_remote_args,
            down_remote_args,
        )
        assert isinstance(physical_op, MapOperator), (up_remote_args, down_remote_args)
        assert len(physical_op.input_dependencies) == 1, (
            up_remote_args,
            down_remote_args,
        )
        assert physical_op.input_dependencies[0].name == "ReadParquet", (
            up_remote_args,
            down_remote_args,
        )


def test_read_map_batches_operator_fusion_incompatible_remote_args(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()

    # Test that map operators won't get fused if the remote args are incompatible.
    incompatible_remote_args_pairs = [
        # Use different resources.
        ({"num_cpus": 2}, {"num_gpus": 2}),
        # Same resource, but different values.
        ({"num_cpus": 3}, {"num_cpus": 2}),
        # Incompatible custom resources.
        ({"resources": {"custom": 2}}, {"resources": {"custom": 1}}),
        ({"resources": {"custom1": 1}}, {"resources": {"custom2": 1}}),
        # Different scheduling strategies.
        ({"scheduling_strategy": "SPREAD"}, {"scheduling_strategy": "PACK"}),
    ]
    for up_remote_args, down_remote_args in incompatible_remote_args_pairs:
        planner = create_planner()
        read_op = get_parquet_read_logical_op(
            ray_remote_args={"resources": {"non-existent": 1}}
        )
        op = MapBatches(read_op, lambda x: x, ray_remote_args=up_remote_args)
        op = MapBatches(op, lambda x: x, ray_remote_args=down_remote_args)
        logical_plan = LogicalPlan(op, ctx)
        physical_plan = planner.plan(logical_plan)
        physical_plan = PhysicalOptimizer().optimize(physical_plan)
        physical_op = physical_plan.dag

        assert op.name == "MapBatches(<lambda>)", (up_remote_args, down_remote_args)
        assert physical_op.name == "MapBatches(<lambda>)", (
            up_remote_args,
            down_remote_args,
        )
        assert isinstance(physical_op, MapOperator), (up_remote_args, down_remote_args)
        assert len(physical_op.input_dependencies) == 1, (
            up_remote_args,
            down_remote_args,
        )
        assert physical_op.input_dependencies[0].name == "MapBatches(<lambda>)", (
            up_remote_args,
            down_remote_args,
        )


def test_read_map_batches_operator_fusion_compute_tasks_to_actors(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()

    # Test that a task-based map operator is fused into an actor-based map operator when
    # the former comes before the latter.
    planner = create_planner()
    read_op = get_parquet_read_logical_op(parallelism=1)
    op = MapBatches(read_op, lambda x: x)
    op = MapBatches(op, lambda x: x, compute=ray.data.ActorPoolStrategy())
    logical_plan = LogicalPlan(op, ctx)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches(<lambda>)"
    assert physical_op.name == "ReadParquet->MapBatches(<lambda>)->MapBatches(<lambda>)"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_read_map_batches_operator_fusion_compute_read_to_actors(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()

    # Test that reads fuse into an actor-based map operator.
    planner = create_planner()
    read_op = get_parquet_read_logical_op(parallelism=1)
    op = MapBatches(read_op, lambda x: x, compute=ray.data.ActorPoolStrategy())
    logical_plan = LogicalPlan(op, ctx)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches(<lambda>)"
    assert physical_op.name == "ReadParquet->MapBatches(<lambda>)"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_read_map_batches_operator_fusion_incompatible_compute(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()

    # Test that map operators are not fused when compute strategies are incompatible.
    planner = create_planner()
    read_op = get_parquet_read_logical_op(parallelism=1)
    op = MapBatches(read_op, lambda x: x, compute=ray.data.ActorPoolStrategy())
    op = MapBatches(op, lambda x: x)
    logical_plan = LogicalPlan(op, ctx)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches(<lambda>)"
    assert physical_op.name == "MapBatches(<lambda>)"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    upstream_physical_op = physical_op.input_dependencies[0]
    assert isinstance(upstream_physical_op, MapOperator)
    # Reads should fuse into actor compute.
    assert upstream_physical_op.name == "ReadParquet->MapBatches(<lambda>)"


def test_read_with_map_batches_fused_successfully(
    ray_start_regular_shared_2_cpus, temp_dir
):
    """Since MapBatches does NOT specify `batch_size`, successfully fused with
    ReadParquet"""

    # Test that fusion of map operators merges their block sizes in the expected way
    # (taking the max).
    n = 10
    ds = ray.data.range(n)

    mapped_ds = ds.map_batches(lambda x: x).map_batches(lambda x: x)

    physical_plan = get_execution_plan(mapped_ds._logical_plan)

    physical_op = physical_plan.dag
    assert isinstance(physical_op, MapOperator)

    actual_plan_str = physical_op.dag_str

    # All Map ops are fused with Read
    assert (
        "InputDataBuffer[Input] -> "
        "TaskPoolMapOperator[ReadRange->MapBatches(<lambda>)->MapBatches(<lambda>)]"
        == actual_plan_str
    )

    # # Target min-rows requirement is not set
    assert physical_op._block_ref_bundler._min_rows_per_bundle is None


@pytest.mark.parametrize(
    "input_op,fused",
    [
        (
            # No fusion (could drastically expand dataset)
            Read(
                datasource=MagicMock(name="Parquet"),
                datasource_or_legacy_reader=MagicMock(
                    get_read_tasks=lambda _: [MagicMock()]
                ),
                parallelism=1,
            ),
            False,
        ),
        (
            # No fusion (could drastically reduce dataset)
            Filter(InputData([]), fn=lambda x: False),
            False,
        ),
        (
            # No fusion (could drastically expand/reduce dataset)
            FlatMap(InputData([]), lambda x: x),
            False,
        ),
        (
            # Fusion
            MapBatches(InputData([]), lambda x: x),
            True,
        ),
        (
            # Fusion
            MapRows(InputData([]), lambda x: x),
            True,
        ),
        (
            # Fusion
            Project(InputData([]), exprs=[star()]),
            True,
        ),
    ],
)
def test_map_batches_batch_size_fusion(
    ray_start_regular_shared_2_cpus, input_op, fused
):
    """Since MapBatches specifies `batch_size` there's no fusion with ReadParquet"""

    context = DataContext.get_current()

    # Test that fusion of map operators merges their block sizes in the expected way
    # (taking the max).
    ds = Dataset(
        ExecutionPlan(DatasetStats(metadata={}, parent=None), context),
        LogicalPlan(input_op, context),
    )

    mapped_ds = ds.map_batches(lambda x: x, batch_size=2).map_batches(
        lambda x: x,
        batch_size=5,
    )

    physical_plan = get_execution_plan(mapped_ds._logical_plan)

    physical_op = physical_plan.dag

    assert isinstance(physical_op, MapOperator)

    actual_plan_str = physical_op.dag_str

    if fused:
        assert (
            f"InputDataBuffer[Input] -> TaskPoolMapOperator[{input_op.name}->"
            f"MapBatches(<lambda>)->MapBatches(<lambda>)]" == actual_plan_str
        )
    else:
        assert (
            f"InputDataBuffer[Input] -> TaskPoolMapOperator[{input_op.name}] -> "
            "TaskPoolMapOperator[MapBatches(<lambda>)->MapBatches(<lambda>)]"
            == actual_plan_str
        )

    # Target min-rows requirement is set to max of upstream and downstream
    assert physical_op._block_ref_bundler._min_rows_per_bundle == 5
    assert len(physical_op.input_dependencies) == 1


@pytest.mark.parametrize("upstream_batch_size", [None, 1, 2])
@pytest.mark.parametrize("downstream_batch_size", [None, 1, 2])
def test_map_batches_with_batch_size_specified_fusion(
    ray_start_regular_shared_2_cpus,
    temp_dir,
    upstream_batch_size,
    downstream_batch_size,
):
    # Test that fusion of map operators merges their block sizes in the expected way
    # (taking the max).
    n = 10
    ds = ray.data.range(n)

    mapped_ds = ds.map_batches(
        lambda x: x,
        batch_size=upstream_batch_size,
    ).map_batches(
        lambda x: x,
        batch_size=downstream_batch_size,
    )

    physical_plan = get_execution_plan(mapped_ds._logical_plan)

    root_op = physical_plan.dag
    assert isinstance(root_op, MapOperator)

    actual_plan_str = root_op.dag_str

    if upstream_batch_size is None and downstream_batch_size is None:
        expected_min_rows_per_bundle = None
        expected_plan_str = (
            "InputDataBuffer[Input] -> "
            "TaskPoolMapOperator[ReadRange->MapBatches(<lambda>)->MapBatches(<lambda>)]"
        )
    else:
        expected_min_rows_per_bundle = max(
            upstream_batch_size or 0, downstream_batch_size or 0
        )
        expected_plan_str = (
            "InputDataBuffer[Input] -> TaskPoolMapOperator[ReadRange] -> "
            "TaskPoolMapOperator[MapBatches(<lambda>)->MapBatches(<lambda>)]"
        )

    assert expected_plan_str == actual_plan_str

    # Target min-rows requirement is set to max of upstream and downstream
    assert (
        expected_min_rows_per_bundle == root_op._block_ref_bundler._min_rows_per_bundle
    )


def test_read_map_batches_operator_fusion_with_randomize_blocks_operator(
    ray_start_regular_shared_2_cpus,
):
    # Note: We currently do not fuse MapBatches->RandomizeBlocks.
    # This test is to ensure that we don't accidentally fuse them.
    def fn(batch):
        return {"id": [x + 1 for x in batch["id"]]}

    n = 10
    ds = ray.data.range(n)
    ds = ds.randomize_block_order()
    ds = ds.map_batches(fn, batch_size=None)
    assert set(extract_values("id", ds.take_all())) == set(range(1, n + 1))
    stats = ds.stats()
    # Ensure RandomizeBlockOrder and MapBatches are not fused.
    assert "RandomizeBlockOrder->MapBatches(fn)" not in stats
    assert "ReadRange" in stats
    assert "RandomizeBlockOrder" in stats
    assert "MapBatches(fn)" in stats
    # Regression tests ensuring RandomizeBlockOrder is never bypassed in the future
    assert "ReadRange->MapBatches(fn)->RandomizeBlockOrder" not in stats
    assert "ReadRange->MapBatches(fn)" not in stats
    # Ensure all three operators are also present in usage record
    _check_usage_record(["ReadRange", "MapBatches", "RandomizeBlockOrder"])


def test_read_map_batches_operator_fusion_with_random_shuffle_operator(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    # Note: we currently only support fusing MapOperator->AllToAllOperator.
    def fn(batch):
        return {"id": [x + 1 for x in batch["id"]]}

    n = 10
    ds = ray.data.range(n)
    ds = ds.map_batches(fn, batch_size=None)
    ds = ds.random_shuffle()
    assert set(extract_values("id", ds.take_all())) == set(range(1, n + 1))
    assert "ReadRange->MapBatches(fn)->RandomShuffle" in ds.stats()
    _check_usage_record(["ReadRange", "MapBatches", "RandomShuffle"])

    ds = ray.data.range(n)
    ds = ds.random_shuffle()
    ds = ds.map_batches(fn, batch_size=None)
    assert set(extract_values("id", ds.take_all())) == set(range(1, n + 1))
    # TODO(Scott): Update below assertion after supporting fusion in
    # the other direction (AllToAllOperator->MapOperator)
    assert "ReadRange->RandomShuffle->MapBatches(fn)" not in ds.stats()
    assert all(op in ds.stats() for op in ("ReadRange", "RandomShuffle", "MapBatches"))
    _check_usage_record(["ReadRange", "RandomShuffle", "MapBatches"])

    # Test fusing multiple `map_batches` with multiple `random_shuffle` operations.
    ds = ray.data.range(n)
    for _ in range(5):
        ds = ds.map_batches(fn, batch_size=None)
    ds = ds.random_shuffle()
    assert set(extract_values("id", ds.take_all())) == set(range(5, n + 5))
    assert f"ReadRange->{'MapBatches(fn)->' * 5}RandomShuffle" in ds.stats()

    # For interweaved map_batches and random_shuffle operations, we expect to fuse the
    # two pairs of MapBatches->RandomShuffle, but not the resulting
    # RandomShuffle operators.
    ds = ray.data.range(n)
    ds = ds.map_batches(fn, batch_size=None)
    ds = ds.random_shuffle()
    ds = ds.map_batches(fn, batch_size=None)
    ds = ds.random_shuffle()
    assert set(extract_values("id", ds.take_all())) == set(range(2, n + 2))
    assert "Operator 1 ReadRange->MapBatches(fn)->RandomShuffle" in ds.stats()
    assert "Operator 2 MapBatches(fn)->RandomShuffle" in ds.stats()
    _check_usage_record(["ReadRange", "RandomShuffle", "MapBatches"])

    # Check the case where the upstream map function returns multiple blocks.
    ctx = ray.data.DataContext.get_current()
    old_target_max_block_size = ctx.target_max_block_size
    ctx.target_max_block_size = 100

    def fn(_):
        return {"data": np.zeros((100, 100))}

    ds = ray.data.range(10)
    ds = ds.repartition(2).map(fn).random_shuffle().materialize()
    assert "Operator 1 ReadRange" in ds.stats()
    assert "Operator 2 Repartition" in ds.stats()
    assert "Operator 3 Map(fn)->RandomShuffle" in ds.stats()
    _check_usage_record(["ReadRange", "RandomShuffle", "Map"])

    ctx.target_max_block_size = old_target_max_block_size


@pytest.mark.parametrize("shuffle", (True, False))
def test_read_map_batches_operator_fusion_with_repartition_operator(
    ray_start_regular_shared_2_cpus, shuffle, configure_shuffle_method
):
    def fn(batch):
        return {"id": [x + 1 for x in batch["id"]]}

    n = 10
    ds = ray.data.range(n)
    ds = ds.map_batches(fn, batch_size=None)
    ds = ds.repartition(2, shuffle=shuffle)
    assert set(extract_values("id", ds.take_all())) == set(range(1, n + 1))

    # Operator fusion is only supported for shuffle repartition.
    if shuffle:
        assert "ReadRange->MapBatches(fn)->Repartition" in ds.stats()
    else:
        assert "ReadRange->MapBatches(fn)->Repartition" not in ds.stats()
        assert "ReadRange->MapBatches(fn)" in ds.stats()
        assert "Repartition" in ds.stats()
    _check_usage_record(["ReadRange", "MapBatches", "Repartition"])


def test_read_map_batches_operator_fusion_with_sort_operator(
    ray_start_regular_shared_2_cpus,
):
    # Note: We currently do not fuse MapBatches->Sort.
    # This test is to ensure that we don't accidentally fuse them, until
    # we implement it later.
    def fn(batch):
        return {"id": [x + 1 for x in batch["id"]]}

    n = 10
    ds = ray.data.range(n)
    ds = ds.map_batches(fn, batch_size=None)
    ds = ds.sort("id")
    assert extract_values("id", ds.take_all()) == list(range(1, n + 1))
    # TODO(Scott): update the below assertions after we support fusion.
    assert "ReadRange->MapBatches->Sort" not in ds.stats()
    assert "ReadRange->MapBatches" in ds.stats()
    assert "Sort" in ds.stats()
    _check_usage_record(["ReadRange", "MapBatches", "Sort"])


def test_read_map_batches_operator_fusion_with_aggregate_operator(
    ray_start_regular_shared_2_cpus, configure_shuffle_method
):
    from ray.data.aggregate import AggregateFn

    # Note: We currently do not fuse MapBatches->Aggregate.
    # This test is to ensure that we don't accidentally fuse them, until
    # we implement it later.
    def fn(batch):
        return {"id": [x % 2 for x in batch["id"]]}

    n = 100
    grouped_ds = ray.data.range(n).map_batches(fn, batch_size=None).groupby("id")
    agg_ds = grouped_ds.aggregate(
        AggregateFn(
            init=lambda k: [0, 0],
            accumulate_row=lambda a, r: [a[0] + r["id"], a[1] + 1],
            merge=lambda a1, a2: [a1[0] + a2[0], a1[1] + a2[1]],
            finalize=lambda a: a[0] / a[1],
            name="foo",
        ),
    )
    agg_ds.take_all() == [{"id": 0, "foo": 0.0}, {"id": 1, "foo": 1.0}]
    # TODO(Scott): update the below assertions after we support fusion.
    assert "ReadRange->MapBatches->Aggregate" not in agg_ds.stats()
    assert "ReadRange->MapBatches" in agg_ds.stats()
    assert "Aggregate" in agg_ds.stats()
    _check_usage_record(["ReadRange", "MapBatches", "Aggregate"])


def test_read_map_chain_operator_fusion_e2e(
    ray_start_regular_shared_2_cpus,
):
    ds = ray.data.range(10, override_num_blocks=2)
    ds = ds.filter(fn=lambda x: x["id"] % 2 == 0)
    ds = ds.map(column_udf("id", lambda x: x + 1))
    ds = ds.map_batches(
        lambda batch: {"id": [2 * x for x in batch["id"]]}, batch_size=None
    )
    ds = ds.flat_map(lambda x: [{"id": -x["id"]}, {"id": x["id"]}])
    assert extract_values("id", ds.take_all()) == [
        -2,
        2,
        -6,
        6,
        -10,
        10,
        -14,
        14,
        -18,
        18,
    ]
    name = (
        "ReadRange->Filter(<lambda>)->Map(<lambda>)"
        "->MapBatches(<lambda>)->FlatMap(<lambda>):"
    )
    assert name in ds.stats()
    _check_usage_record(["ReadRange", "Filter", "Map", "MapBatches", "FlatMap"])


def test_write_fusion(ray_start_regular_shared_2_cpus, tmp_path):
    ds = ray.data.range(10, override_num_blocks=2)
    ds.write_csv(tmp_path)
    assert "ReadRange->Write" in ds._write_ds.stats()
    _check_usage_record(["ReadRange", "WriteCSV"])


@pytest.mark.parametrize(
    "up_use_actor, up_concurrency, down_use_actor, down_concurrency, should_fuse",
    [
        # === Task->Task cases ===
        # Same concurrency set. Should fuse.
        (False, 1, False, 1, True),
        # Different concurrency set. Should not fuse.
        (False, 1, False, 2, False),
        # If one op has concurrency set, and the other doesn't, should not fuse.
        (False, None, False, 1, False),
        (False, 1, False, None, False),
        # === Task->Actor cases ===
        # When Task's concurrency is not set, should fuse.
        (False, None, True, 2, True),
        (False, None, True, (1, 2), True),
        # When max size matches, should fuse.
        (False, 2, True, 2, True),
        (False, 2, True, (1, 2), True),
        # When max size doesn't match, should not fuse.
        (False, 1, True, 2, False),
        (False, 1, True, (1, 2), False),
        # === Actor->Task cases ===
        # Should not fuse whatever concurrency is set.
        (True, 2, False, 2, False),
        # === Actor->Actor cases ===
        # Should not fuse whatever concurrency is set.
        (True, 2, True, 2, False),
    ],
)
def test_map_fusion_with_concurrency_arg(
    ray_start_regular_shared_2_cpus,
    up_use_actor,
    up_concurrency,
    down_use_actor,
    down_concurrency,
    should_fuse,
):
    """Test map operator fusion with different concurrency settings."""

    class Map:
        def __call__(self, row):
            return row

    def map(row):
        return row

    ds = ray.data.range(10, override_num_blocks=2)
    if not up_use_actor:
        ds = ds.map(map, num_cpus=0, concurrency=up_concurrency)
        up_name = "Map(map)"
    else:
        ds = ds.map(Map, num_cpus=0, concurrency=up_concurrency)
        up_name = "Map(Map)"

    if not down_use_actor:
        ds = ds.map(map, num_cpus=0, concurrency=down_concurrency)
        down_name = "Map(map)"
    else:
        ds = ds.map(Map, num_cpus=0, concurrency=down_concurrency)
        down_name = "Map(Map)"

    assert extract_values("id", ds.take_all()) == list(range(10))

    name = f"{up_name}->{down_name}"
    stats = ds.stats()
    if should_fuse:
        assert name in stats, stats
    else:
        assert name not in stats, stats


def check_transform_fns(op, expected_types):
    assert isinstance(op, MapOperator)
    transform_fns = op.get_map_transformer().get_transform_fns()
    assert len(transform_fns) == len(expected_types), transform_fns
    for i, transform_fn in enumerate(transform_fns):
        assert isinstance(transform_fn, expected_types[i]), transform_fn


@pytest.mark.skip("Needs zero-copy optimization for read->map_batches.")
def test_zero_copy_fusion_eliminate_build_output_blocks(
    ray_start_regular_shared_2_cpus,
):
    ctx = DataContext.get_current()

    # Test the EliminateBuildOutputBlocks optimization rule.
    planner = create_planner()
    read_op = get_parquet_read_logical_op()
    op = MapBatches(read_op, lambda x: x)
    logical_plan = LogicalPlan(op, ctx)
    physical_plan = planner.plan(logical_plan)

    # Before optimization, there should be a map op and and read op.
    # And they should have the following transform_fns.
    map_op = physical_plan.dag
    check_transform_fns(
        map_op,
        [
            BatchMapTransformFn,
        ],
    )
    read_op = map_op.input_dependencies[0]
    check_transform_fns(
        read_op,
        [
            BlockMapTransformFn,
        ],
    )

    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    fused_op = physical_plan.dag

    # After optimization, read and map ops should be fused as one op.
    # And the BuidlOutputBlocksMapTransformFn in the middle should be dropped.
    check_transform_fns(
        fused_op,
        [
            BlockMapTransformFn,
            BatchMapTransformFn,
        ],
    )


@pytest.mark.parametrize(
    "order,target_num_rows,batch_size,should_fuse",
    [
        # map_batches -> streaming_repartition: fuse when batch_size is a multiple of target_num_rows
        ("map_then_sr", 20, 20, True),
        ("map_then_sr", 20, 10, False),
        ("map_then_sr", 20, 40, True),
        ("map_then_sr", 20, None, False),
        # streaming_repartition -> map_batches: not fused
        ("sr_then_map", 20, 20, False),
    ],
)
def test_streaming_repartition_map_batches_fusion_order_and_params(
    ray_start_regular_shared_2_cpus,
    order,
    target_num_rows,
    batch_size,
    should_fuse,
):
    """Test fusion of streaming_repartition and map_batches with different orders
    and different target_num_rows/batch_size values."""
    n = 100
    ds = ray.data.range(n, override_num_blocks=2)

    if order == "map_then_sr":
        ds = ds.map_batches(lambda x: x, batch_size=batch_size)
        ds = ds.repartition(target_num_rows_per_block=target_num_rows)
        expected_fused_name = f"MapBatches(<lambda>)->StreamingRepartition[num_rows_per_block={target_num_rows}]"
    else:  # sr_then_map
        ds = ds.repartition(target_num_rows_per_block=target_num_rows)
        ds = ds.map_batches(lambda x: x, batch_size=batch_size)
        expected_fused_name = f"StreamingRepartition[num_rows_per_block={target_num_rows}]->MapBatches(<lambda>)"

    assert len(ds.take_all()) == n

    stats = ds.stats()
    if should_fuse:
        assert (
            expected_fused_name in stats
        ), f"Expected '{expected_fused_name}' in stats: {stats}"
    else:
        assert (
            expected_fused_name not in stats
        ), f"Did not expect '{expected_fused_name}' in stats: {stats}"


def test_streaming_repartition_no_further_fuse(
    ray_start_regular_shared_2_cpus,
):
    """Test that fused streaming_repartition operators don't fuse further.

    Case 1: map_batches -> map_batches -> streaming_repartition -> map_batches -> map_batches
            Result: map -> (map -> s_r)-> (map -> map)
            The fused (map -> s_r) doesn't fuse further with surrounding maps.
    """
    n = 100
    target_rows = 20

    # Case 1: map_batches -> map_batches -> streaming_repartition -> map_batches -> map_batches
    # Result: map -> (map -> s_r)-> (map -> map)
    ds1 = ray.data.range(n, override_num_blocks=2)
    ds1 = ds1.map_batches(lambda x: x, batch_size=target_rows)
    ds1 = ds1.map_batches(lambda x: x, batch_size=target_rows)
    ds1 = ds1.repartition(target_num_rows_per_block=target_rows)
    ds1 = ds1.map_batches(lambda x: x, batch_size=target_rows)
    ds1 = ds1.map_batches(lambda x: x, batch_size=target_rows)

    assert len(ds1.take_all()) == n
    stats1 = ds1.stats()

    assert (
        f"MapBatches(<lambda>)->StreamingRepartition[num_rows_per_block={target_rows}]"
        in stats1
    ), stats1
    assert "MapBatches(<lambda>)->MapBatches(<lambda>)" in stats1


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
