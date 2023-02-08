import pytest
import numpy as np
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
from ray.data._internal.execution.operators.map_operator import (
    MapOperator,
    _BlockRefBundler,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import make_ref_bundles
from ray.tests.conftest import *  # noqa
from ray._private.test_utils import wait_for_condition


def _get_blocks(bundle: RefBundle, output_list: List[Block]):
    for block, _ in bundle.blocks:
        output_list.append(ray.get(block))


def _mul2_transform(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
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
    def dummy_all_transform(bundles: List[RefBundle], ctx):
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


def test_num_outputs_total():
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op1 = MapOperator.create(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
    )
    assert op1.num_outputs_total() == 100

    def dummy_all_transform(bundles: List[RefBundle]):
        return make_ref_bundles([[1, 2], [3, 4]]), {"FooStats": []}

    op2 = AllToAllOperator(dummy_all_transform, input_op=op1, name="TestAll")
    assert op2.num_outputs_total() == 100


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_bulk(ray_start_regular_shared, use_actors):
    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    compute_strategy = (
        ActorPoolStrategy(max_size=1) if use_actors else TaskPoolStrategy()
    )
    op = MapOperator.create(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
    )

    # Feed data and block on exec.
    op.start(ExecutionOptions())
    if use_actors:
        # Actor will be pending after starting the operator.
        assert op.progress_str() == "0 actors (1 pending)"
    assert op.internal_queue_size() == 0
    i = 0
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
        i += 1
        if use_actors:
            assert op.internal_queue_size() == i
        else:
            assert op.internal_queue_size() == 0
    op.inputs_done()
    work_refs = op.get_work_refs()
    while work_refs:
        for work_ref in work_refs:
            ray.get(work_ref)
            op.notify_work_completed(work_ref)
        work_refs = op.get_work_refs()
        if use_actors and work_refs:
            # After actor is ready (first work ref resolved), actor will remain ready
            # while there is work to do.
            assert op.progress_str() == "1 actors"
    assert op.internal_queue_size() == 0
    if use_actors:
        # After all work is done, actor will have been killed to free up resources..
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
    op = MapOperator.create(
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
        while not op.has_next():
            work_refs = op.get_work_refs()
            ready, _ = ray.wait(work_refs, num_returns=1, fetch_local=False)
            op.notify_work_completed(ready[0])
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
    def _check_batch(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
        block_iter = list(block_iter)
        assert len(block_iter) == 5, block_iter
        for block in block_iter:
            yield block

    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator.create(
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
    work_refs = op.get_work_refs()
    while work_refs:
        for work_ref in work_refs:
            ray.get(work_ref)
            op.notify_work_completed(work_ref)
        work_refs = op.get_work_refs()

    # Check we return transformed bundles in order.
    assert _take_outputs(op) == [[i] for i in range(10)]
    assert op.completed()


@pytest.mark.parametrize("use_actors", [False, True])
@pytest.mark.parametrize("preserve_order", [False, True])
def test_map_operator_output_unbundling(
    ray_start_regular_shared, use_actors, preserve_order
):
    # Tests that the MapOperator's output queue unbundles the bundles returned from
    # tasks; this facilitates features such as dynamic block splitting.
    def noop(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
        for block in block_iter:
            yield block

    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator.create(
        noop,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
        # Send the everything in a single bundle of 10 blocks.
        min_rows_per_bundle=10,
    )

    # Feed data and block on exec.
    op.start(ExecutionOptions(preserve_order=preserve_order))
    inputs = []
    while input_op.has_next():
        inputs.append(input_op.get_next())
    # Sanity check: the op will get 10 input bundles.
    assert len(inputs) == 10
    for input_ in inputs:
        op.add_input(input_, 0)
    op.inputs_done()
    work_refs = op.get_work_refs()
    while work_refs:
        for work_ref in work_refs:
            ray.get(work_ref)
            op.notify_work_completed(work_ref)
        work_refs = op.get_work_refs()

    # Check that bundles are unbundled in the output queue.
    outputs = []
    while op.has_next():
        outputs.append(op.get_next())
    assert len(outputs) == 10
    assert op.completed()


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_ray_args(shutdown_only, use_actors):
    ray.shutdown()
    ray.init(num_cpus=0, num_gpus=1)
    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    compute_strategy = (
        ActorPoolStrategy(max_size=1) if use_actors else TaskPoolStrategy()
    )
    op = MapOperator.create(
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
    work_refs = op.get_work_refs()
    while work_refs:
        for work_ref in work_refs:
            ray.get(work_ref)
            op.notify_work_completed(work_ref)
        work_refs = op.get_work_refs()

    # Check we don't hang and complete with num_gpus=1.
    assert _take_outputs(op) == [[i * 2] for i in range(10)]
    assert op.completed()


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_shutdown(shutdown_only, use_actors):
    ray.shutdown()
    ray.init(num_cpus=0, num_gpus=1)

    def _sleep(block_iter: Iterable[Block]) -> Iterable[Block]:
        time.sleep(999)

    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator.create(
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


@pytest.mark.parametrize(
    "compute,expected",
    [
        (TaskPoolStrategy(), TaskPoolMapOperator),
        (ActorPoolStrategy(), ActorPoolMapOperator),
    ],
)
def test_map_operator_pool_delegation(compute, expected):
    # Test that the MapOperator factory delegates to the appropriate pool
    # implementation.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op = MapOperator.create(
        _mul2_transform,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute,
    )
    assert isinstance(op, expected)


def _get_bundles(bundle: RefBundle):
    output = []
    for block, _ in bundle.blocks:
        output.extend(ray.get(block))
    return output


@pytest.mark.parametrize(
    "target,in_bundles,expected_bundles",
    [
        (
            1,  # Unit target, should leave unchanged.
            [[1], [2], [3, 4], [5]],
            [[1], [2], [3, 4], [5]],
        ),
        (
            None,  # No target, should leave unchanged.
            [[1], [2], [3, 4], [5]],
            [[1], [2], [3, 4], [5]],
        ),
        (
            2,  # Empty blocks should be handled.
            [[1], [], [2, 3], []],
            [[1], [2, 3]],
        ),
        (
            2,  # Test bundling, finalizing, passing, leftovers, etc.
            [[1], [2], [3, 4, 5], [6], [7], [8], [9, 10], [11]],
            [[1, 2], [3, 4, 5], [6, 7], [8], [9, 10], [11]],
        ),
        (
            3,  # Test bundling, finalizing, passing, leftovers, etc.
            [[1], [2, 3], [4, 5, 6, 7], [8, 9], [10, 11]],
            [[1, 2, 3], [4, 5, 6, 7], [8, 9], [10, 11]],
        ),
    ],
)
def test_block_ref_bundler_basic(target, in_bundles, expected_bundles):
    # Test that the bundler creates the expected output bundles.
    bundler = _BlockRefBundler(target)
    bundles = make_ref_bundles(in_bundles)
    out_bundles = []
    for bundle in bundles:
        bundler.add_bundle(bundle)
        while bundler.has_bundle():
            out_bundle = _get_bundles(bundler.get_next_bundle())
            out_bundles.append(out_bundle)
    bundler.done_adding_bundles()
    if bundler.has_bundle():
        out_bundle = _get_bundles(bundler.get_next_bundle())
        out_bundles.append(out_bundle)
    assert len(out_bundles) == len(expected_bundles)
    for bundle, expected in zip(out_bundles, expected_bundles):
        assert bundle == expected


@pytest.mark.parametrize(
    "target,n,num_bundles,num_out_bundles,out_bundle_size",
    [
        (5, 20, 20, 4, 5),
        (5, 20, 10, 5, 4),
        (8, 16, 4, 2, 8),
    ],
)
def test_block_ref_bundler_uniform(
    target, n, num_bundles, num_out_bundles, out_bundle_size
):
    # Test that the bundler creates the expected number of bundles with the expected
    # size.
    bundler = _BlockRefBundler(target)
    data = np.arange(n)
    pre_bundles = [arr.tolist() for arr in np.array_split(data, num_bundles)]
    bundles = make_ref_bundles(pre_bundles)
    out_bundles = []
    for bundle in bundles:
        bundler.add_bundle(bundle)
        while bundler.has_bundle():
            out_bundles.append(bundler.get_next_bundle())
    bundler.done_adding_bundles()
    if bundler.has_bundle():
        out_bundles.append(bundler.get_next_bundle())
    assert len(out_bundles) == num_out_bundles
    for out_bundle in out_bundles:
        assert out_bundle.num_rows() == out_bundle_size
    flat_out = [
        i
        for bundle in out_bundles
        for block, _ in bundle.blocks
        for i in ray.get(block)
    ]
    assert flat_out == list(range(n))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
