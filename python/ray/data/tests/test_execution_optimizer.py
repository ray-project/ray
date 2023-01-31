import pytest

import ray
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.all_to_all_operator import (
    RandomShuffle,
    RandomizeBlocks,
    Repartition,
)
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.map_operator import (
    MapRows,
    MapBatches,
    Filter,
    FlatMap,
)
from ray.data._internal.planner.planner import Planner
from ray.data.datasource.parquet_datasource import ParquetDatasource

from ray.tests.conftest import *  # noqa


def test_read_operator(ray_start_cluster_enabled, enable_optimizer):
    planner = Planner()
    op = Read(ParquetDatasource())
    physical_op = planner.plan(op)

    assert op.name == "Read"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], InputDataBuffer)


def test_map_batches_operator(ray_start_cluster_enabled, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = MapBatches(
        read_op,
        lambda it: (x for x in it),
        lambda x: x,
    )
    physical_op = planner.plan(op)

    assert op.name == "MapBatches"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_map_batches_e2e(ray_start_cluster_enabled, enable_optimizer):
    ds = ray.data.range(5)
    ds = ds.map_batches(lambda x: x)
    assert ds.take_all() == list(range(5)), ds


def test_map_rows_operator(ray_start_cluster_enabled, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = MapRows(
        read_op,
        lambda x: x,
    )
    physical_op = planner.plan(op)

    assert op.name == "MapRows"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_map_rows_e2e(ray_start_cluster_enabled, enable_optimizer):
    ds = ray.data.range(5)
    ds = ds.map(lambda x: x + 1)
    assert ds.take_all() == [1, 2, 3, 4, 5], ds


def test_filter_operator(ray_start_cluster_enabled, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = Filter(
        read_op,
        lambda x: x,
    )
    physical_op = planner.plan(op)

    assert op.name == "Filter"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_filter_e2e(ray_start_cluster_enabled, enable_optimizer):
    ds = ray.data.range(5)
    ds = ds.filter(fn=lambda x: x % 2 == 0)
    assert ds.take_all() == [0, 2, 4], ds


def test_flat_map(ray_start_cluster_enabled, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = FlatMap(
        read_op,
        lambda x: x,
    )
    physical_op = planner.plan(op)

    assert op.name == "FlatMap"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_flat_map_e2e(ray_start_cluster_enabled, enable_optimizer):
    ds = ray.data.range(2)
    ds = ds.flat_map(fn=lambda x: [x, x])
    assert ds.take_all() == [0, 0, 1, 1], ds


def test_column_ops_e2e(ray_start_cluster_enabled, enable_optimizer):
    ds = ray.data.range(2)
    ds = ds.add_column(fn=lambda df: df.iloc[:, 0], col="new_col")
    assert ds.take_all() == [{"value": 0, "new_col": 0}, {"value": 1, "new_col": 1}], ds

    select_ds = ds.select_columns(cols=["new_col"])
    assert select_ds.take_all() == [{"new_col": 0}, {"new_col": 1}]

    ds = ds.drop_columns(cols=["new_col"])
    assert ds.take_all() == [{"value": 0}, {"value": 1}], ds


def test_random_sample_e2e(ray_start_cluster_enabled, enable_optimizer):
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


def test_randomize_blocks_operator(ray_start_cluster_enabled, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = RandomizeBlocks(
        read_op,
        seed=0,
    )
    physical_op = planner.plan(op)

    assert op.name == "RandomizeBlocks"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_randomize_blocks_e2e(ray_start_cluster_enabled, enable_optimizer):
    ds = ray.data.range(12, parallelism=4)
    ds = ds.randomize_block_order(seed=0)
    assert ds.take_all() == [6, 7, 8, 0, 1, 2, 3, 4, 5, 9, 10, 11], ds


def test_random_shuffle_operator(ray_start_cluster_enabled, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = RandomShuffle(
        read_op,
        seed=0,
    )
    physical_op = planner.plan(op)

    assert op.name == "RandomShuffle"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_random_shuffle_e2e(
    ray_start_cluster_enabled, enable_optimizer, use_push_based_shuffle
):
    ds = ray.data.range(12, parallelism=4)
    r1 = ds.random_shuffle(seed=0).take_all()
    r2 = ds.random_shuffle(seed=1024).take_all()
    assert r1 != r2, (r1, r2)
    assert sorted(r1) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], r1
    assert sorted(r2) == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], r2


def test_repartition_operator(ray_start_cluster_enabled, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = Repartition(
        read_op,
        num_outputs=5,
        shuffle=True,
    )
    physical_op = planner.plan(op)

    assert op.name == "Repartition"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)

    # Check error is thrown for non-shuffle repartition.
    with pytest.raises(AssertionError):
        planner.plan(
            Repartition(
                read_op,
                num_outputs=5,
                shuffle=False,
            )
        )


def test_repartition_e2e(
    ray_start_cluster_enabled, enable_optimizer, use_push_based_shuffle
):
    ds = ray.data.range(10000, parallelism=10)
    ds1 = ds.repartition(20, shuffle=True)
    assert ds1._block_num_rows() == [500] * 20, ds

    # Check error is thrown for non-shuffle repartition.
    with pytest.raises(AssertionError):
        ds.repartition(20, shuffle=False).take_all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
