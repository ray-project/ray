import pytest

import ray
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.map_operator import AbstractUDFMap
from ray.data._internal.logical.operators.all_to_all_operator import (
    RandomizeBlocks,
    Repartition,
)
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data._internal.logical.rules.randomize_blocks import ReorderRandomizeBlocksRule
from ray.data._internal.logical.interfaces import LogicalPlan
from ray.data._internal.logical.optimizers import LogicalOptimizer
from ray.data._internal.planner.planner import Planner
from ray.data.tests.util import extract_values


def test_randomize_blocks_operator(ray_start_regular_shared, enable_optimizer):
    planner = Planner()
    read_op = Read(datasource=None)
    op = RandomizeBlocks(
        read_op,
        seed=0,
    )
    plan = LogicalPlan(op)
    physical_op = planner.plan(plan).dag

    assert op.name == "RandomizeBlocks"
    assert isinstance(physical_op, AllToAllOperator)
    assert len(physical_op.input_dependencies) == 1
    assert isinstance(physical_op.input_dependencies[0], MapOperator)


def test_randomize_block_order_rule():
    read = Read(datasource=None)
    operator1 = RandomizeBlocks(input_op=read, seed=None)
    operator2 = RandomizeBlocks(input_op=operator1, seed=None)
    operator3 = MapBatches(input_op=operator2, fn=lambda x: x)
    original_plan = LogicalPlan(dag=operator3)

    rule = ReorderRandomizeBlocksRule()
    optimized_plan = rule.apply(original_plan)

    # Check that RandomizeBlocks is the last operator in the DAG.
    assert isinstance(optimized_plan.dag, RandomizeBlocks)
    # Check that the seed is maintained.
    assert optimized_plan.dag._seed is None

    # Check that multiple RandomizeBlocks operators are deduped.
    operator_count = 0
    for _ in optimized_plan.dag.post_order_iter():
        operator_count += 1

    assert operator_count == 3


def test_randomize_block_order_rule_seed():
    read = Read(datasource=None)
    operator1 = RandomizeBlocks(input_op=read, seed=None)
    operator2 = RandomizeBlocks(input_op=operator1, seed=2)
    operator3 = MapBatches(input_op=operator2, fn=lambda x: x)
    original_plan = LogicalPlan(dag=operator3)

    rule = ReorderRandomizeBlocksRule()
    optimized_plan = rule.apply(original_plan)

    # Check that RandomizeBlocks is the last operator in the DAG.
    assert isinstance(optimized_plan.dag, RandomizeBlocks)
    # Check that the seed is maintained.
    assert optimized_plan.dag._seed == 2

    # Check that the two RandomizeBlocks operators are not collapsed since seeds are
    # provided.
    assert isinstance(optimized_plan.dag.input_dependencies[0], RandomizeBlocks)
    assert optimized_plan.dag.input_dependencies[0]._seed is None
    operator_count = 0
    for _ in optimized_plan.dag.post_order_iter():
        operator_count += 1

    # RandomizeBlocks operators should not be deduped.
    assert operator_count == 4


def test_randomize_block_order_after_repartition():
    read = Read(datasource=None)
    operator1 = RandomizeBlocks(input_op=read)
    operator2 = Repartition(input_op=operator1, num_outputs=1, shuffle=False)
    operator3 = RandomizeBlocks(input_op=operator2)
    operator4 = RandomizeBlocks(input_op=operator3)
    operator5 = MapBatches(input_op=operator4, fn=lambda x: x)
    operator6 = Repartition(input_op=operator5, num_outputs=1, shuffle=False)
    original_plan = LogicalPlan(dag=operator6)

    rule = ReorderRandomizeBlocksRule()
    optimized_plan = rule.apply(original_plan)

    assert isinstance(optimized_plan.dag, Repartition)
    assert isinstance(optimized_plan.dag.input_dependencies[0], RandomizeBlocks)

    # Check that multiple RandomizeBlocks operators are deduped within repartition
    # boundaries.
    operator_count = 0
    for _ in optimized_plan.dag.post_order_iter():
        operator_count += 1

    # Read -> RandomizeBlocks -> Repartition -> MapBatches -> RandomizeBlocks ->
    # Repartition
    assert operator_count == 6


def test_randomize_blocks_e2e(ray_start_regular_shared, enable_optimizer):
    ds = ray.data.range(12, parallelism=4)
    ds = ds.randomize_block_order(seed=0)
    assert extract_values("id", ds.take_all()) == [
        6,
        7,
        8,
        0,
        1,
        2,
        3,
        4,
        5,
        9,
        10,
        11,
    ], ds


def test_randomize_blocks_rule_e2e(ray_start_regular_shared, enable_optimizer):
    def dummy_map(x):
        return x

    ds = ray.data.range(10).randomize_block_order().map_batches(dummy_map)
    plan = ds._logical_plan
    optimized_plan = LogicalOptimizer().optimize(plan)

    inverse_order = iter([Read, AbstractUDFMap, RandomizeBlocks])
    for node in optimized_plan.dag.post_order_iter():
        assert isinstance(node, next(inverse_order))

    ds = (
        ray.data.range(10)
        .randomize_block_order()
        .repartition(10)
        .map_batches(dummy_map)
    )
    plan = ds._logical_plan
    optimized_plan = LogicalOptimizer().optimize(plan)

    inverse_order = iter([Read, RandomizeBlocks, Repartition, AbstractUDFMap])
    for node in optimized_plan.dag.post_order_iter():
        assert isinstance(node, next(inverse_order))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
