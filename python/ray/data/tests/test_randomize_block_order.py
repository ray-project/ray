import pytest

from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.operators.all_to_all_operator import RandomizeBlocks
from ray.data._internal.planner import create_planner
from ray.data.context import DataContext
from ray.data.tests.test_util import get_parquet_read_logical_op


def test_randomize_blocks_operator(ray_start_regular_shared):
    ctx = DataContext.get_current()

    planner = create_planner()
    read_op = get_parquet_read_logical_op()
    op = RandomizeBlocks(
        read_op,
        seed=0,
    )
    plan = LogicalPlan(op, ctx)
    physical_op = planner.plan(plan).dag

    assert op.name == "RandomizeBlockOrder"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
