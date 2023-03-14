import itertools
import pytest
import pandas as pd

import ray
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.execution.operators.zip_operator import ZipOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.from_arrow_operator import (
    FromArrow,
    FromArrowRefs,
    FromHuggingFace,
)
from ray.data._internal.logical.operators.from_spark_operator import FromSpark
from ray.data._internal.logical.operators.from_items_operator import (
    FromItems,
    FromTF,
    FromTorch,
)
from ray.data._internal.logical.operators.from_numpy_operator import (
    FromNumpy,
    FromNumpyRefs,
)
from ray.data._internal.logical.operators.from_pandas_operator import (
    FromDask,
    FromMARS,
    FromModin,
    FromPandas,
    FromPandasRefs,
)
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
from ray.data._internal.planner.planner import Planner
from ray.data.aggregate import Count
from ray.data.datasource.parquet_datasource import ParquetDatasource

from ray.data.tests.conftest import *  # noqa
from ray.tests.conftest import *  # noqa


def test_read_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    op = Read(ParquetDatasource())
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "Read"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_from_items_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    from_items_op = FromItems(["Hello", "World"])
    plan = LogicalPlan(from_items_op)
    physical_op = planner.plan(plan).dag

    assert from_items_op.name == "FromItems"
    assert isinstance(physical_op, InputDataBuffer)
    assert len(physical_op.input_dependencies) == 0


def test_from_items_e2e(ray_start_regular_shared, enable_optimizer):
    data = ["Hello", "World"]
    ds = ray.data.from_items(data)
    assert ds.take_all() == data, ds

    # Check that metadata fetch is included in stats.
    assert "from_items" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromItems"


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


def test_column_ops_e2e(ray_start_regular_shared, enable_optimizer):
    ds = ray.data.range(2)
    ds = ds.add_column(fn=lambda df: df.iloc[:, 0], col="new_col")
    assert ds.take_all() == [{"value": 0, "new_col": 0}, {"value": 1, "new_col": 1}], ds

    select_ds = ds.select_columns(cols=["new_col"])
    assert select_ds.take_all() == [{"new_col": 0}, {"new_col": 1}]

    ds = ds.drop_columns(cols=["new_col"])
    assert ds.take_all() == [{"value": 0}, {"value": 1}], ds


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


def test_write_fusion(ray_start_regular_shared, enable_optimizer, tmp_path):
    ds = ray.data.range(10, parallelism=2)
    ds.write_csv(tmp_path)
    assert "DoRead->Write" in ds._write_ds.stats()


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


def test_from_dask_operator(ray_start_regular_shared, enable_optimizer):
    import dask.dataframe as dd

    df = pd.DataFrame({"one": list(range(100)), "two": list(range(100))})
    ddf = dd.from_pandas(df, npartitions=10)

    planner = Planner()
    from_dask_op = FromDask(ddf)
    plan = LogicalPlan(from_dask_op)
    physical_op = planner.plan(plan).dag

    assert from_dask_op.name == "FromDask"
    assert isinstance(physical_op, InputDataBuffer)
    assert len(physical_op.input_dependencies) == 0


def test_from_dask_e2e(ray_start_regular_shared, enable_optimizer):
    import dask.dataframe as dd

    df = pd.DataFrame({"one": list(range(100)), "two": list(range(100))})
    ddf = dd.from_pandas(df, npartitions=10)
    ds = ray.data.from_dask(ddf)
    dfds = ds.to_pandas()
    assert df.equals(dfds)
    # Underlying implementation uses `FromPandasRefs` operator
    assert ds._plan._logical_plan.dag.name == "FromPandasRefs"


def test_from_mars_operator(ray_start_regular_shared, enable_optimizer):
    import mars
    import mars.dataframe as md

    cluster = mars.new_cluster_in_ray(worker_num=2, worker_cpu=1)

    n = 10000
    df = pd.DataFrame({"a": list(range(n)), "b": list(range(n, 2 * n))})
    mdf = md.DataFrame(df)

    planner = Planner()
    from_mars_op = FromMARS(mdf)
    plan = LogicalPlan(from_mars_op)
    physical_op = planner.plan(plan).dag

    assert from_mars_op.name == "FromMARS"
    assert isinstance(physical_op, InputDataBuffer)
    assert len(physical_op.input_dependencies) == 0

    cluster.stop()


def test_from_mars_e2e(ray_start_regular_shared, enable_optimizer):
    import mars
    import mars.dataframe as md
    import pyarrow as pa

    cluster = mars.new_cluster_in_ray(worker_num=2, worker_cpu=1)

    n = 10000
    df = pd.DataFrame({"a": list(range(n)), "b": list(range(n, 2 * n))})
    mdf = md.DataFrame(df)

    # Convert mars dataframe to ray dataset
    ds = ray.data.from_mars(mdf)
    pd.testing.assert_frame_equal(ds.to_pandas(), mdf.to_pandas())
    ds2 = ds.filter(lambda row: row["a"] % 2 == 0)
    assert ds2.take(5) == [{"a": 2 * i, "b": n + 2 * i} for i in range(5)]

    # Convert ray dataset to mars dataframe
    mdf2 = ds2.to_mars()
    pd.testing.assert_frame_equal(
        mdf2.head(5).to_pandas(),
        pd.DataFrame({"a": list(range(0, 10, 2)), "b": list(range(n, n + 10, 2))}),
    )

    # Test Arrow Dataset
    df2 = pd.DataFrame({c: range(5) for c in "abc"})
    ds3 = ray.data.from_arrow([pa.Table.from_pandas(df2) for _ in range(3)])
    df3 = ds3.to_mars()
    pd.testing.assert_frame_equal(
        df3.head(5).to_pandas(),
        df2,
    )

    # Test simple datasets
    with pytest.raises(NotImplementedError):
        ray.data.range(10).to_mars()

    # Underlying implementation uses `FromPandasRefs` operator
    assert ds._plan._logical_plan.dag.name == "FromPandasRefs"

    cluster.stop()


def test_from_modin_operator(ray_start_regular_shared, enable_optimizer):
    import modin.pandas as mopd

    df = pd.DataFrame(
        {"one": list(range(100)), "two": list(range(100))},
    )
    modf = mopd.DataFrame(df)

    planner = Planner()
    from_modin_op = FromModin(modf)
    plan = LogicalPlan(from_modin_op)
    physical_op = planner.plan(plan).dag

    assert from_modin_op.name == "FromModin"
    assert isinstance(physical_op, InputDataBuffer)
    assert len(physical_op.input_dependencies) == 0


def test_from_modin_e2e(ray_start_regular_shared, enable_optimizer):
    import modin.pandas as mopd

    df = pd.DataFrame(
        {"one": list(range(100)), "two": list(range(100))},
    )
    modf = mopd.DataFrame(df)
    ds = ray.data.from_modin(modf)
    dfds = ds.to_pandas()

    assert df.equals(dfds)
    # Underlying implementation uses `FromPandasRefs` operator
    assert ds._plan._logical_plan.dag.name == "FromPandasRefs"


@pytest.mark.parametrize("enable_pandas_block", [False, True])
def test_from_pandas_refs_operator(
    ray_start_regular_shared, enable_optimizer, enable_pandas_block
):
    ctx = ray.data.context.DatasetContext.get_current()
    old_enable_pandas_block = ctx.enable_pandas_block
    ctx.enable_pandas_block = enable_pandas_block
    try:
        df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
        df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})

        planner = Planner()
        from_pandas_ref_op = FromPandasRefs([df1, df2])
        plan = LogicalPlan(from_pandas_ref_op)
        physical_op = planner.plan(plan).dag

        assert from_pandas_ref_op.name == "FromPandasRefs"
        assert isinstance(physical_op, InputDataBuffer)
        assert len(physical_op.input_dependencies) == 0
    finally:
        ctx.enable_pandas_block = old_enable_pandas_block


@pytest.mark.parametrize("enable_pandas_block", [False, True])
def test_from_pandas_refs_e2e(
    ray_start_regular_shared, enable_optimizer, enable_pandas_block
):
    ctx = ray.data.context.DatasetContext.get_current()
    old_enable_pandas_block = ctx.enable_pandas_block
    ctx.enable_pandas_block = enable_pandas_block

    try:
        df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
        df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})

        ds = ray.data.from_pandas_refs([ray.put(df1), ray.put(df2)])
        assert ds.dataset_format() == "pandas" if enable_pandas_block else "arrow"
        values = [(r["one"], r["two"]) for r in ds.take(6)]
        rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "from_pandas_refs" in ds.stats()

        # test from single pandas dataframe
        ds = ray.data.from_pandas_refs(ray.put(df1))
        assert ds.dataset_format() == "pandas" if enable_pandas_block else "arrow"
        values = [(r["one"], r["two"]) for r in ds.take(3)]
        rows = [(r.one, r.two) for _, r in df1.iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "from_pandas_refs" in ds.stats()
        assert ds._plan._logical_plan.dag.name == "FromPandasRefs"
    finally:
        ctx.enable_pandas_block = old_enable_pandas_block


@pytest.mark.parametrize("enable_pandas_block", [False, True])
def test_from_pandas_operator(
    ray_start_regular_shared, enable_optimizer, enable_pandas_block
):
    ctx = ray.data.context.DatasetContext.get_current()
    old_enable_pandas_block = ctx.enable_pandas_block
    ctx.enable_pandas_block = enable_pandas_block
    try:
        df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
        df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})

        planner = Planner()
        from_pandas_op = FromPandas([df1, df2])
        plan = LogicalPlan(from_pandas_op)
        physical_op = planner.plan(plan).dag

        assert from_pandas_op.name == "FromPandas"
        assert isinstance(physical_op, InputDataBuffer)
        assert len(physical_op.input_dependencies) == 0
    finally:
        ctx.enable_pandas_block = old_enable_pandas_block


@pytest.mark.parametrize("enable_pandas_block", [False, True])
def test_from_pandas_e2e(
    ray_start_regular_shared, enable_optimizer, enable_pandas_block
):
    ctx = ray.data.context.DatasetContext.get_current()
    old_enable_pandas_block = ctx.enable_pandas_block
    ctx.enable_pandas_block = enable_pandas_block

    try:
        df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
        df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})

        ds = ray.data.from_pandas([df1, df2])
        assert ds.dataset_format() == "pandas" if enable_pandas_block else "arrow"
        values = [(r["one"], r["two"]) for r in ds.take(6)]
        rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "from_pandas_refs" in ds.stats()

        # test from single pandas dataframe
        ds = ray.data.from_pandas(df1)
        assert ds.dataset_format() == "pandas" if enable_pandas_block else "arrow"
        values = [(r["one"], r["two"]) for r in ds.take(3)]
        rows = [(r.one, r.two) for _, r in df1.iterrows()]
        assert values == rows
        # Check that metadata fetch is included in stats.
        assert "from_pandas_refs" in ds.stats()
        # Underlying implementation uses `FromPandasRefs` operator
        assert ds._plan._logical_plan.dag.name == "FromPandasRefs"
    finally:
        ctx.enable_pandas_block = old_enable_pandas_block


def test_from_numpy_refs_operator(
    ray_start_regular_shared,
    enable_optimizer,
):
    import numpy as np

    arr1 = np.expand_dims(np.arange(0, 4), axis=1)
    arr2 = np.expand_dims(np.arange(4, 8), axis=1)

    planner = Planner()
    from_numpy_ref_op = FromNumpyRefs([ray.put(arr1), ray.put(arr2)])
    plan = LogicalPlan(from_numpy_ref_op)
    physical_op = planner.plan(plan).dag

    assert from_numpy_ref_op.name == "FromNumpyRefs"
    assert isinstance(physical_op, InputDataBuffer)
    assert len(physical_op.input_dependencies) == 0


def test_from_numpy_refs_e2e(ray_start_regular_shared, enable_optimizer):
    import numpy as np

    arr1 = np.expand_dims(np.arange(0, 4), axis=1)
    arr2 = np.expand_dims(np.arange(4, 8), axis=1)

    ds = ray.data.from_numpy_refs([ray.put(arr1), ray.put(arr2)])
    values = np.stack(ds.take(8))
    np.testing.assert_array_equal(values, np.concatenate((arr1, arr2)))
    # Check that conversion task is included in stats.
    assert "from_numpy_refs" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromNumpyRefs"

    # Test from single NumPy ndarray.
    ds = ray.data.from_numpy_refs(ray.put(arr1))
    values = np.stack(ds.take(4))
    np.testing.assert_array_equal(values, arr1)
    # Check that conversion task is included in stats.
    assert "from_numpy_refs" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromNumpyRefs"


def test_from_numpy_operator(
    ray_start_regular_shared,
    enable_optimizer,
):
    import numpy as np

    arr1 = np.expand_dims(np.arange(0, 4), axis=1)
    arr2 = np.expand_dims(np.arange(4, 8), axis=1)

    planner = Planner()
    from_numpy_op = FromNumpy([arr1, arr2])
    plan = LogicalPlan(from_numpy_op)
    physical_op = planner.plan(plan).dag

    assert from_numpy_op.name == "FromNumpy"
    assert isinstance(physical_op, InputDataBuffer)
    assert len(physical_op.input_dependencies) == 0


def test_from_numpy_e2e(ray_start_regular_shared, enable_optimizer):
    import numpy as np

    arr1 = np.expand_dims(np.arange(0, 4), axis=1)
    arr2 = np.expand_dims(np.arange(4, 8), axis=1)

    ds = ray.data.from_numpy([arr1, arr2])
    values = np.stack(ds.take(8))
    np.testing.assert_array_equal(values, np.concatenate((arr1, arr2)))
    # Check that conversion task is included in stats.
    assert "from_numpy_refs" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromNumpyRefs"

    # Test from single NumPy ndarray.
    ds = ray.data.from_numpy(arr1)
    values = np.stack(ds.take(4))
    np.testing.assert_array_equal(values, arr1)
    # Check that conversion task is included in stats.
    assert "from_numpy_refs" in ds.stats()
    # Underlying implementation uses `FromNumpyRefs` operator
    assert ds._plan._logical_plan.dag.name == "FromNumpyRefs"


def test_from_arrow_refs_operator(
    ray_start_regular_shared,
    enable_optimizer,
):
    import pyarrow as pa

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})

    planner = Planner()
    from_arrow_refs_op = FromArrowRefs(
        [
            ray.put(pa.Table.from_pandas(df1)),
            ray.put(pa.Table.from_pandas(df2)),
        ]
    )
    plan = LogicalPlan(from_arrow_refs_op)
    physical_op = planner.plan(plan).dag

    assert from_arrow_refs_op.name == "FromArrowRefs"
    assert isinstance(physical_op, InputDataBuffer)
    assert len(physical_op.input_dependencies) == 0


def test_from_arrow_refs_e2e(ray_start_regular_shared, enable_optimizer):
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
    assert "from_arrow_refs" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromArrowRefs"

    # test from single pyarrow table ref
    ds = ray.data.from_arrow_refs(ray.put(pa.Table.from_pandas(df1)))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows
    # Check that conversion task is included in stats.
    assert "from_arrow_refs" in ds.stats()
    assert ds._plan._logical_plan.dag.name == "FromArrowRefs"


def test_from_arrow_operator(
    ray_start_regular_shared,
    enable_optimizer,
):
    import pyarrow as pa

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})

    planner = Planner()
    from_arrow_op = FromArrow(
        [
            pa.Table.from_pandas(df1),
            pa.Table.from_pandas(df2),
        ]
    )
    plan = LogicalPlan(from_arrow_op)
    physical_op = planner.plan(plan).dag

    assert from_arrow_op.name == "FromArrow"
    assert isinstance(physical_op, InputDataBuffer)
    assert len(physical_op.input_dependencies) == 0


def test_from_arrow_e2e(ray_start_regular_shared, enable_optimizer):
    import pyarrow as pa

    df1 = pd.DataFrame({"one": [1, 2, 3], "two": ["a", "b", "c"]})
    df2 = pd.DataFrame({"one": [4, 5, 6], "two": ["e", "f", "g"]})
    ds = ray.data.from_arrow([pa.Table.from_pandas(df1), pa.Table.from_pandas(df2)])

    values = [(r["one"], r["two"]) for r in ds.take(6)]
    rows = [(r.one, r.two) for _, r in pd.concat([df1, df2]).iterrows()]
    assert values == rows
    # Check that metadata fetch is included in stats.
    assert "from_arrow_refs" in ds.stats()
    # Underlying implementation uses `FromArrowRefs` operator
    assert ds._plan._logical_plan.dag.name == "FromArrowRefs"

    # test from single pyarrow table ref
    ds = ray.data.from_arrow(pa.Table.from_pandas(df1))
    values = [(r["one"], r["two"]) for r in ds.take(3)]
    rows = [(r.one, r.two) for _, r in df1.iterrows()]
    assert values == rows
    # Check that conversion task is included in stats.
    assert "from_arrow_refs" in ds.stats()
    # Underlying implementation uses `FromArrowRefs` operator
    assert ds._plan._logical_plan.dag.name == "FromArrowRefs"


def test_from_spark_operator(
    enable_optimizer,
    spark,
):
    spark_df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["one", "two"])

    planner = Planner()
    from_spark_op = FromSpark(spark_df)
    plan = LogicalPlan(from_spark_op)
    physical_op = planner.plan(plan).dag

    assert from_spark_op.name == "FromSpark"
    assert isinstance(physical_op, InputDataBuffer)
    assert len(physical_op.input_dependencies) == 0


def test_from_spark_e2e(enable_optimizer, spark):
    spark_df = spark.createDataFrame([(1, "a"), (2, "b"), (3, "c")], ["one", "two"])

    rows = [(r.one, r.two) for r in spark_df.take(3)]
    ds = ray.data.from_spark(spark_df)
    values = [(r["one"], r["two"]) for r in ds.take(6)]
    assert values == rows

    # Check that metadata fetch is included in stats.
    assert "from_arrow_refs" in ds.stats()
    # Underlying implementation uses `FromArrowRefs` operator
    assert ds._plan._logical_plan.dag.name == "FromArrowRefs"


def test_from_huggingface_operator(
    ray_start_regular_shared,
    enable_optimizer,
):
    import datasets

    data = datasets.load_dataset("tweet_eval", "emotion")
    assert isinstance(data, datasets.DatasetDict)

    planner = Planner()
    from_huggingface_op = FromHuggingFace(data)
    plan = LogicalPlan(from_huggingface_op)
    physical_op = planner.plan(plan).dag

    assert from_huggingface_op.name == "FromHuggingFace"
    assert isinstance(physical_op, InputDataBuffer)
    assert len(physical_op.input_dependencies) == 0


def test_from_huggingface_e2e(ray_start_regular_shared, enable_optimizer):
    import datasets

    data = datasets.load_dataset("tweet_eval", "emotion")
    assert isinstance(data, datasets.DatasetDict)

    ray_datasets = ray.data.from_huggingface(data)
    assert isinstance(ray_datasets, dict)
    assert ray.get(ray_datasets["train"].to_arrow_refs())[0].equals(
        data["train"].data.table
    )
    for ds in ray_datasets.values():
        assert isinstance(ds, ray.data.Dataset)
        ds.fully_executed()
        assert "from_arrow_refs" in ds.stats()
        assert ds._plan._logical_plan.dag.name == "FromArrowRefs"

    ray_dataset = ray.data.from_huggingface(data["train"]).fully_executed()
    assert isinstance(ray_dataset, ray.data.Dataset)
    assert ray.get(ray_dataset.to_arrow_refs())[0].equals(data["train"].data.table)

    # Check that metadata fetch is included in stats.
    assert "from_arrow_refs" in ray_dataset.stats()
    # Underlying implementation uses `FromArrowRefs` operator
    assert ray_dataset._plan._logical_plan.dag.name == "FromArrowRefs"


def test_from_tf_operator(ray_start_regular_shared, enable_optimizer):
    import tensorflow_datasets as tfds

    tf_dataset = tfds.load("mnist", split=["train"], as_supervised=True)[0]
    tf_dataset = tf_dataset.take(8)  # Use subset to make test run faster.

    planner = Planner()
    from_tf_op = FromTF(tf_dataset)
    plan = LogicalPlan(from_tf_op)
    physical_op = planner.plan(plan).dag

    assert from_tf_op.name == "FromTF"
    assert isinstance(physical_op, InputDataBuffer)
    assert len(physical_op.input_dependencies) == 0


def test_from_tf_e2e(ray_start_regular_shared, enable_optimizer):
    import tensorflow as tf
    import tensorflow_datasets as tfds

    tf_dataset = tfds.load("mnist", split=["train"], as_supervised=True)[0]
    tf_dataset = tf_dataset.take(8)  # Use subset to make test run faster.

    ray_dataset = ray.data.from_tf(tf_dataset)

    actual_data = ray_dataset.take_all()
    expected_data = list(tf_dataset)
    assert len(actual_data) == len(expected_data)
    for (expected_features, expected_label), (actual_features, actual_label) in zip(
        expected_data, actual_data
    ):
        tf.debugging.assert_equal(expected_features, actual_features)
        tf.debugging.assert_equal(expected_label, actual_label)

    # Check that metadata fetch is included in stats.
    assert "from_items" in ray_dataset.stats()
    # Underlying implementation uses `FromItems` operator
    assert ray_dataset._plan._logical_plan.dag.name == "FromItems"


def test_from_torch_operator(ray_start_regular_shared, enable_optimizer, tmp_path):
    import torchvision

    torch_dataset = torchvision.datasets.MNIST(tmp_path, download=True)

    planner = Planner()
    from_torch_op = FromTorch(torch_dataset)
    plan = LogicalPlan(from_torch_op)
    physical_op = planner.plan(plan).dag

    assert from_torch_op.name == "FromTorch"
    assert isinstance(physical_op, InputDataBuffer)
    assert len(physical_op.input_dependencies) == 0


def test_from_torch_e2e(ray_start_regular_shared, enable_optimizer, tmp_path):
    import torchvision

    torch_dataset = torchvision.datasets.MNIST(tmp_path, download=True)

    ray_dataset = ray.data.from_torch(torch_dataset)

    expected_data = list(torch_dataset)
    actual_data = list(ray_dataset.take_all())
    assert actual_data == expected_data

    # Check that metadata fetch is included in stats.
    assert "from_items" in ray_dataset.stats()
    # Underlying implementation uses `FromItems` operator
    assert ray_dataset._plan._logical_plan.dag.name == "FromItems"


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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
