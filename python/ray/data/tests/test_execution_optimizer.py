from typing import List
import itertools
import pytest

import ray
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.execution.operators.zip_operator import ZipOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.optimizers import PhysicalOptimizer
from ray.data._internal.logical.operators.all_to_all_operator import (
    Aggregate,
    RandomShuffle,
    Repartition,
    Sort,
)
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.write_operator import Write
from ray.data._internal.logical.operators.map_operator import (
    MapRows,
    MapBatches,
    Filter,
    FlatMap,
)
from ray.data._internal.logical.operators.n_ary_operator import Zip
from ray.data._internal.logical.util import (
    _recorded_operators,
    _recorded_operators_lock,
    _op_name_white_list,
)
from ray.data._internal.planner.planner import Planner
from ray.data.aggregate import Count
from ray.data.datasource.parquet_datasource import ParquetDatasource

from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def _check_usage_record(op_names: List[str]):
    for op_name in op_names:
        assert op_name in _op_name_white_list
        with _recorded_operators_lock:
            assert _recorded_operators.get(op_name, 0) > 0


def test_read_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    op = Read(ParquetDatasource())
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "Read"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_map_batches_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = MapBatches(
        read_op,
        lambda x: x,
    )
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "MapBatches"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_map_batches_e2e(ray_start_regular_shared, enable_optimizer):
    ds = ray.data.range(5)
    ds = ds.map_batches(lambda x: x)
    assert ds.take_all() == list(range(5)), ds
    _check_usage_record(["ReadRange", "MapBatches"])


def test_map_rows_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = MapRows(
        read_op,
        lambda x: x,
    )
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "MapRows"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_map_rows_e2e(ray_start_regular_shared, enable_optimizer):
    ds = ray.data.range(5)
    ds = ds.map(lambda x: x + 1)
    assert ds.take_all() == [1, 2, 3, 4, 5], ds
    _check_usage_record(["ReadRange", "MapRows"])


def test_filter_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = Filter(
        read_op,
        lambda x: x,
    )
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "Filter"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_filter_e2e(ray_start_regular_shared, enable_optimizer):
    ds = ray.data.range(5)
    ds = ds.filter(fn=lambda x: x % 2 == 0)
    assert ds.take_all() == [0, 2, 4], ds
    _check_usage_record(["ReadRange", "Filter"])


def test_flat_map(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = FlatMap(
        read_op,
        lambda x: x,
    )
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "FlatMap"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_flat_map_e2e(ray_start_regular_shared, enable_optimizer):
    ds = ray.data.range(2)
    ds = ds.flat_map(fn=lambda x: [x, x])
    assert ds.take_all() == [0, 0, 1, 1], ds
    _check_usage_record(["ReadRange", "FlatMap"])


def test_column_ops_e2e(ray_start_regular_shared, enable_optimizer):
    ds = ray.data.range(2)
    ds = ds.add_column(fn=lambda df: df.iloc[:, 0], col="new_col")
    assert ds.take_all() == [{"value": 0, "new_col": 0}, {"value": 1, "new_col": 1}], ds
    _check_usage_record(["ReadRange", "MapBatches"])

    select_ds = ds.select_columns(cols=["new_col"])
    assert select_ds.take_all() == [{"new_col": 0}, {"new_col": 1}]
    _check_usage_record(["ReadRange", "MapBatches"])

    ds = ds.drop_columns(cols=["new_col"])
    assert ds.take_all() == [{"value": 0}, {"value": 1}], ds
    _check_usage_record(["ReadRange", "MapBatches"])


def test_random_sample_e2e(ray_start_regular_shared, enable_optimizer):
    import math

    def ensure_sample_size_close(dataset, sample_percent=0.5):
        r1 = ds.random_sample(sample_percent)
        assert math.isclose(
            r1.count(), int(ds.count() * sample_percent), rel_tol=2, abs_tol=2
        )

    ds = ray.data.range(10, parallelism=2)
    ensure_sample_size_close(ds)

    ds = ray.data.range_table(10, parallelism=2)
    ensure_sample_size_close(ds)

    ds = ray.data.range_tensor(5, parallelism=2, shape=(2, 2))
    ensure_sample_size_close(ds)

    _check_usage_record(["ReadRange", "MapBatches"])


def test_random_shuffle_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = RandomShuffle(
        read_op,
        seed=0,
    )
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "RandomShuffle"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_random_shuffle_e2e(
    ray_start_regular_shared, enable_optimizer, use_push_based_shuffle
):
    ds = ray.data.range(12, parallelism=4)
    r1 = ds.random_shuffle(seed=0).take_all()
    r2 = ds.random_shuffle(seed=1024).take_all()
    assert r1 != r2, (r1, r2)
    assert sorted(r1) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], r1
    assert sorted(r2) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], r2
    _check_usage_record(["ReadRange", "RandomShuffle"])


def test_repartition_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = Repartition(read_op, num_outputs=5, shuffle=True)
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "Repartition"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)

    # Check error is thrown for non-shuffle repartition.
    op = Repartition(read_op, num_outputs=5, shuffle=False)
    plan = LogicalPlan(op)
    with pytest.raises(AssertionError):
        planner.plan(plan)


def test_repartition_e2e(
    ray_start_regular_shared, enable_optimizer, use_push_based_shuffle
):
    ds = ray.data.range(10000, parallelism=10)
    ds1 = ds.repartition(20, shuffle=True)
    assert ds1._block_num_rows() == [500] * 20, ds

    # Check error is thrown for non-shuffle repartition.
    with pytest.raises(AssertionError):
        ds.repartition(20, shuffle=False).take_all()

    _check_usage_record(["ReadRange", "Repartition"])


def test_read_map_batches_operator_fusion(ray_start_regular_shared, enable_optimizer):
    # Test that Read is fused with MapBatches.
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = MapBatches(
        read_op,
        lambda x: x,
    )
    logical_plan = LogicalPlan(op)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches"
    assert physical_op.name == "DoRead->MapBatches"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_read_map_chain_operator_fusion(ray_start_regular_shared, enable_optimizer):
    # Test that a chain of different map operators are fused.
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = MapRows(read_op, lambda x: x)
    op = MapBatches(op, lambda x: x)
    op = FlatMap(op, lambda x: x)
    op = Filter(op, lambda x: x)
    logical_plan = LogicalPlan(op)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "Filter"
    assert physical_op.name == "DoRead->MapRows->MapBatches->FlatMap->Filter"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_read_map_batches_operator_fusion_compatible_remote_args(
    ray_start_regular_shared, enable_optimizer
):
    # Test that map operators are stilled fused when remote args are compatible.
    planner = Planner()
    read_op = Read(
        ParquetDatasource(),
        ray_remote_args={"num_cpus": 1, "scheduling_strategy": "SPREAD"},
    )
    op = MapBatches(read_op, lambda x: x, ray_remote_args={"num_cpus": 1})
    op = MapBatches(op, lambda x: x, ray_remote_args={"num_cpus": 1})
    logical_plan = LogicalPlan(op)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches"
    assert physical_op.name == "DoRead->MapBatches->MapBatches"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_read_map_batches_operator_fusion_incompatible_remote_args(
    ray_start_regular_shared, enable_optimizer
):
    # Test that map operators are not fused when remote args are incompatible.
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = MapBatches(read_op, lambda x: x, ray_remote_args={"num_cpus": 2})
    op = MapBatches(op, lambda x: x, ray_remote_args={"num_cpus": 3})
    logical_plan = LogicalPlan(op)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches"
    assert physical_op.name == "MapBatches"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    upstream_physical_op = physical_op.input_dependencies[0]
    assert isinstance(upstream_physical_op, MapOperator)
    # Read shouldn't fuse into first MapBatches either, due to the differing CPU
    # request.
    assert upstream_physical_op.name == "MapBatches"


def test_read_map_batches_operator_fusion_compute_tasks_to_actors(
    ray_start_regular_shared, enable_optimizer
):
    # Test that a task-based map operator is fused into an actor-based map operator when
    # the former comes before the latter.
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = MapBatches(read_op, lambda x: x, compute="tasks")
    op = MapBatches(op, lambda x: x, compute="actors")
    logical_plan = LogicalPlan(op)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches"
    assert physical_op.name == "DoRead->MapBatches->MapBatches"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_read_map_batches_operator_fusion_compute_read_to_actors(
    ray_start_regular_shared, enable_optimizer
):
    # Test that reads fuse into an actor-based map operator.
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = MapBatches(read_op, lambda x: x, compute="actors")
    logical_plan = LogicalPlan(op)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches"
    assert physical_op.name == "DoRead->MapBatches"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_read_map_batches_operator_fusion_incompatible_compute(
    ray_start_regular_shared, enable_optimizer
):
    # Test that map operators are not fused when compute strategies are incompatible.
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = MapBatches(read_op, lambda x: x, compute="actors")
    op = MapBatches(op, lambda x: x, compute="tasks")
    logical_plan = LogicalPlan(op)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches"
    assert physical_op.name == "MapBatches"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    upstream_physical_op = physical_op.input_dependencies[0]
    assert isinstance(upstream_physical_op, MapOperator)
    # Reads should fuse into actor compute.
    assert upstream_physical_op.name == "DoRead->MapBatches"


def test_read_map_batches_operator_fusion_target_block_size(
    ray_start_regular_shared, enable_optimizer
):
    # Test that fusion of map operators merges their block sizes in the expected way
    # (taking the max).
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = MapBatches(read_op, lambda x: x, target_block_size=2)
    op = MapBatches(op, lambda x: x, target_block_size=5)
    op = MapBatches(op, lambda x: x, target_block_size=3)
    logical_plan = LogicalPlan(op)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches"
    # Ops are still fused.
    assert physical_op.name == "DoRead->MapBatches->MapBatches->MapBatches"
    assert isinstance(physical_op, MapOperator)
    # Target block size is set to max.
    assert physical_op._block_ref_bundler._min_rows_per_bundle == 5
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_read_map_batches_operator_fusion_callable_classes(
    ray_start_regular_shared, enable_optimizer
):
    # Test that callable classes can still be fused if they're the same function.
    planner = Planner()
    read_op = Read(ParquetDatasource())

    class UDF:
        def __call__(self, x):
            return x

    op = MapBatches(read_op, UDF, compute="actors")
    op = MapBatches(op, UDF, compute="actors")
    logical_plan = LogicalPlan(op)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches"
    assert physical_op.name == "DoRead->MapBatches->MapBatches"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_read_map_batches_operator_fusion_incompatible_callable_classes(
    ray_start_regular_shared, enable_optimizer
):
    # Test that map operators are not fused when different callable classes are used.
    planner = Planner()
    read_op = Read(ParquetDatasource())

    class UDF:
        def __call__(self, x):
            return x

    class UDF2:
        def __call__(self, x):
            return x + 1

    op = MapBatches(read_op, UDF, compute="actors")
    op = MapBatches(op, UDF2, compute="actors")
    logical_plan = LogicalPlan(op)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches"
    assert physical_op.name == "MapBatches"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    upstream_physical_op = physical_op.input_dependencies[0]
    assert isinstance(upstream_physical_op, MapOperator)
    # Reads should still fuse with first map.
    assert upstream_physical_op.name == "DoRead->MapBatches"


def test_read_map_batches_operator_fusion_incompatible_constructor_args(
    ray_start_regular_shared, enable_optimizer
):
    # Test that map operators are not fused when callable classes have different
    # constructor args.
    planner = Planner()
    read_op = Read(ParquetDatasource())

    class UDF:
        def __init__(self, a):
            self._a

        def __call__(self, x):
            return x + self._a

    op = MapBatches(read_op, UDF, compute="actors", fn_constructor_args=(1,))
    op = MapBatches(op, UDF, compute="actors", fn_constructor_args=(2,))
    op = MapBatches(op, UDF, compute="actors", fn_constructor_kwargs={"a": 1})
    op = MapBatches(op, UDF, compute="actors", fn_constructor_kwargs={"a": 2})
    logical_plan = LogicalPlan(op)
    physical_plan = planner.plan(logical_plan)
    physical_plan = PhysicalOptimizer().optimize(physical_plan)
    physical_op = physical_plan.dag

    assert op.name == "MapBatches"
    # Last 3 physical map operators are unfused.
    for _ in range(3):
        assert isinstance(physical_op, MapOperator)
        assert physical_op.name == "MapBatches"
        assert len(physical_op.input_dependencies) == 1
        physical_op = physical_op.input_dependencies[0]
    # First physical map operator is fused with read.
    assert isinstance(physical_op, MapOperator)
    assert physical_op.name == "DoRead->MapBatches"
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_read_map_chain_operator_fusion_e2e(ray_start_regular_shared, enable_optimizer):
    ds = ray.data.range(10, parallelism=2)
    ds = ds.filter(lambda x: x % 2 == 0)
    ds = ds.map(lambda x: x + 1)
    ds = ds.map_batches(lambda batch: [2 * x for x in batch], batch_size=None)
    ds = ds.flat_map(lambda x: [-x, x])
    assert ds.take_all() == [-2, 2, -6, 6, -10, 10, -14, 14, -18, 18]
    name = "DoRead->Filter->MapRows->MapBatches->FlatMap:"
    assert name in ds.stats()
    _check_usage_record(["ReadRange", "Filter", "MapRows", "MapBatches", "FlatMap"])


def test_write_fusion(ray_start_regular_shared, enable_optimizer, tmp_path):
    ds = ray.data.range(10, parallelism=2)
    ds.write_csv(tmp_path)
    assert "DoRead->Write" in ds._write_ds.stats()
    _check_usage_record(["ReadRange", "WriteCSV"])


def test_write_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    datasource = ParquetDatasource()
    read_op = Read(datasource)
    op = Write(
        read_op,
        datasource,
    )
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "Write"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_sort_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = Sort(
        read_op,
        key="col1",
        descending=False,
    )
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "Sort"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_sort_e2e(
    ray_start_regular_shared, enable_optimizer, use_push_based_shuffle, local_path
):
    ds = ray.data.range(100, parallelism=4)
    ds = ds.random_shuffle()
    ds = ds.sort()
    assert ds.take_all() == list(range(100))
    _check_usage_record(["ReadRange", "RandomShuffle", "Sort"])

    # TODO: write_XXX and from_XXX are not supported yet in new execution plan.
    # Re-enable once supported.

    # df = pd.DataFrame({"one": list(range(100)), "two": ["a"] * 100})
    # ds = ray.data.from_pandas([df])
    # path = os.path.join(local_path, "test_parquet_dir")
    # os.mkdir(path)
    # ds.write_parquet(path)

    # ds = ray.data.read_parquet(path)
    # ds = ds.random_shuffle()
    # ds1 = ds.sort("one")
    # ds2 = ds.sort("one", descending=True)
    # r1 = ds1.select_columns(["one"]).take_all()
    # r2 = ds2.select_columns(["one"]).take_all()
    # assert [d["one"] for d in r1] == list(range(100))
    # assert [d["one"] for d in r2] == list(reversed(range(100)))


def test_aggregate_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = Aggregate(
        read_op,
        key="col1",
        aggs=[Count()],
    )
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "Aggregate"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_aggregate_e2e(
    ray_start_regular_shared,
    enable_optimizer,
    use_push_based_shuffle,
):
    ds = ray.data.range_table(100, parallelism=4)
    ds = ds.groupby("value").count()
    assert ds.count() == 100
    for idx, row in enumerate(ds.sort("value").iter_rows()):
        assert row.as_pydict() == {"value": idx, "count()": 1}
    _check_usage_record(["ReadRange", "Aggregate"])


def test_zip_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    read_op1 = Read(ParquetDatasource())
    read_op2 = Read(ParquetDatasource())
    op = Zip(read_op1, read_op2)
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "Zip"
    assert isinstance(physical_op, ZipOperator)
    assert len(physical_op.input_dependencies) == 2
    assert isinstance(physical_op.input_dependencies[0], MapOperator)
    assert isinstance(physical_op.input_dependencies[1], MapOperator)


@pytest.mark.parametrize(
    "num_blocks1,num_blocks2",
    list(itertools.combinations_with_replacement(range(1, 12), 2)),
)
def test_zip_e2e(ray_start_regular_shared, enable_optimizer, num_blocks1, num_blocks2):
    n = 12
    ds1 = ray.data.range(n, parallelism=num_blocks1)
    ds2 = ray.data.range(n, parallelism=num_blocks2).map(lambda x: x + 1)
    ds = ds1.zip(ds2)
    assert ds.take() == list(zip(range(n), range(1, n + 1)))
    _check_usage_record(["ReadRange", "Zip"])


def test_execute_to_legacy_block_list(
    ray_start_regular_shared,
    enable_optimizer,
    enable_streaming_executor,
):
    ds = ray.data.range(10)
    # Stats not initialized until `ds.iter_rows()` is called
    assert ds._plan._snapshot_stats is None

    for i, row in enumerate(ds.iter_rows()):
        assert row == i

    assert ds._plan._snapshot_stats is not None
    assert "DoRead" in ds._plan._snapshot_stats.stages
    assert ds._plan._snapshot_stats.time_total_s > 0


def test_execute_to_legacy_block_iterator(
    ray_start_regular_shared,
    enable_optimizer,
    enable_streaming_executor,
):
    ds = ray.data.range(10)
    assert ds._plan._snapshot_stats is None
    for batch in ds.iter_batches():
        assert batch is not None

    assert ds._plan._snapshot_stats is not None
    assert "DoRead" in ds._plan._snapshot_stats.stages
    assert ds._plan._snapshot_stats.time_total_s > 0


def test_streaming_executor(
    ray_start_regular_shared,
    enable_optimizer,
    enable_streaming_executor,
):
    ds = ray.data.range(100, parallelism=4)
    ds = ds.map_batches(lambda x: x)
    ds = ds.filter(lambda x: x > 0)
    ds = ds.random_shuffle()
    ds = ds.map_batches(lambda x: x)

    result = []
    for batch in ds.iter_batches(batch_size=3):
        assert len(batch) == 3, batch
        result.extend(batch)
    assert sorted(result) == list(range(1, 100)), result
    _check_usage_record(["ReadRange", "MapBatches", "Filter", "RandomShuffle"])


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
