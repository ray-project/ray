import pytest

import ray
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.map_operator import MapRows, MapBatches
from ray.data._internal.logical.planner import Planner
from ray.data.datasource.parquet_datasource import ParquetDatasource

from ray.tests.conftest import *  # noqa


def test_e2e_optimizer_sanity(ray_start_cluster_enabled, enable_optimizer):
    ds = ray.data.range(5)
    ds = ds.map_batches(lambda x: x)
    ds = ds.map(lambda x: x + 1)
    assert ds.take_all() == [1, 2, 3, 4, 5], ds


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


def test_map_rows_operator(ray_start_cluster_enabled, enable_optimizer):
    planner = Planner()
    read_op = Read(ParquetDatasource())
    op = MapRows(
        read_op,
        lambda it: (x for x in it),
        lambda x: x,
    )
    physical_op = planner.plan(op)

    assert op.name == "MapRows"
    assert isinstance(physical_op, MapOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
