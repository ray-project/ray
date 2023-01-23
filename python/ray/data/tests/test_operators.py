import pytest
from typing import List, Iterable, Any
import time

import ray
from ray.data.block import Block
from ray.data._internal.compute import TaskPoolStrategy, ActorPoolStrategy
from ray.data._internal.execution.interfaces import (
    RefBundle,
    PhysicalOperator,
    ExecutionOptions,
)
from ray.data._internal.execution.operators.all_to_all_operator import AllToAllOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import make_ref_bundles
from ray.tests.conftest import *  # noqa
from ray._private.test_utils import wait_for_condition


def _get_blocks(bundle: RefBundle, output_list: List[Block]):
    for block, _ in bundle.blocks:
        output_list.append(ray.get(block))


def _mul2_transform(block_iter: Iterable[Block]) -> Iterable[Block]:
    for block in block_iter:
        yield [b * 2 for b in block]


def _take_outputs(op: PhysicalOperator) -> List[Any]:
    output = []
    while op.has_next():
        ref = op.get_next()
        assert ref.owns_blocks, ref
        _get_blocks(ref, output)
    return output


def test_input_data_buffer(ray_start_regular_shared):
    # Create with bundles.
    inputs = make_ref_bundles([[1, 2], [3], [4, 5]])
    op = InputDataBuffer(inputs)

    # Check we return all bundles in order.
    assert not op.completed()
    assert _take_outputs(op) == [[1, 2], [3], [4, 5]]
    assert op.completed()


def test_all_to_all_operator():
    def dummy_all_transform(bundles: List[RefBundle]):
        return make_ref_bundles([[1, 2], [3, 4]]), {"FooStats": []}

    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op = AllToAllOperator(
        dummy_all_transform, input_op=input_op, num_outputs=2, name="TestAll"
    )

    # Feed data.
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.inputs_done()

    # Check we return transformed bundles.
    assert not op.completed()
    assert _take_outputs(op) == [[1, 2], [3, 4]]
    stats = op.get_stats()
    assert "FooStats" in stats
    assert op.completed()


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_bulk(ray_start_regular_shared, use_actors):
    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
    )

    # Feed data and block on exec.
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.inputs_done()
    for work in op.get_work_refs():
        ray.get(work)
        op.notify_work_completed(work)
    if use_actors:
        assert op.progress_str() == "0 actors"
    else:
        assert op.progress_str() == ""

    # Check we return transformed bundles in order.
    assert not op.completed()
    assert _take_outputs(op) == [[i * 2] for i in range(100)]
    assert op.completed()

    # Check dataset stats.
    stats = op.get_stats()
    assert "TestMapper" in stats, stats
    assert len(stats["TestMapper"]) == 100, stats

    # Check memory stats.
    metrics = op.get_metrics()
    assert metrics["obj_store_mem_alloc"] == pytest.approx(8800, 0.5), metrics
    assert metrics["obj_store_mem_peak"] == pytest.approx(8800, 0.5), metrics
    assert metrics["obj_store_mem_freed"] == pytest.approx(6400, 0.5), metrics


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_streamed(ray_start_regular_shared, use_actors):
    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
    )

    # Feed data and implement streaming exec.
    output = []
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
        for work in op.get_work_refs():
            ray.get(work)
            op.notify_work_completed(work)
        assert op.has_next()
        while op.has_next():
            ref = op.get_next()
            assert ref.owns_blocks, ref
            _get_blocks(ref, output)

    # Check equivalent to bulk execution in order.
    assert output == [[i * 2] for i in range(100)]
    metrics = op.get_metrics()
    assert metrics["obj_store_mem_alloc"] == pytest.approx(8800, 0.5), metrics
    assert metrics["obj_store_mem_peak"] == pytest.approx(88, 0.5), metrics
    assert metrics["obj_store_mem_freed"] == pytest.approx(6400, 0.5), metrics
    assert not op.completed()


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_min_rows_per_bundle(ray_start_regular_shared, use_actors):
    # Simple sanity check of batching behavior.
    def _check_batch(block_iter: Iterable[Block]) -> Iterable[Block]:
        block_iter = list(block_iter)
        assert len(block_iter) == 5, block_iter
        for block in block_iter:
            yield block

    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator(
        _check_batch,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
        min_rows_per_bundle=5,
    )

    # Feed data and block on exec.
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.inputs_done()
    for work in op.get_work_refs():
        ray.get(work)
        op.notify_work_completed(work)

    # Check we return transformed bundles in order.
    assert _take_outputs(op) == [[i] for i in range(10)]
    assert op.completed()


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_ray_args(shutdown_only, use_actors):
    ray.shutdown()
    ray.init(num_cpus=0, num_gpus=1)
    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
        ray_remote_args={"num_cpus": 0, "num_gpus": 1},
    )

    # Feed data and block on exec.
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.inputs_done()
    for work in op.get_work_refs():
        ray.get(work)
        op.notify_work_completed(work)

    # Check we don't hang and complete with num_gpus=1.
    assert _take_outputs(op) == [[i * 2] for i in range(10)]
    assert op.completed()


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_shutdown(use_actors):
    ray.shutdown()
    ray.init(num_cpus=0, num_gpus=1)

    def _sleep(block_iter: Iterable[Block]) -> Iterable[Block]:
        time.sleep(999)

    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator(
        _sleep,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
        ray_remote_args={"num_cpus": 0, "num_gpus": 1},
    )

    # Start one task and then cancel.
    op.start(ExecutionOptions())
    op.add_input(input_op.get_next(), 0)
    assert len(op.get_work_refs()) == 1
    op.shutdown()

    # Tasks/actors should be cancelled/killed.
    wait_for_condition(lambda: (ray.available_resources().get("GPU", 0) == 1.0))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
