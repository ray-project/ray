import collections
import random
import time
from typing import Any, Iterable, List
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

import ray
from ray._private.test_utils import wait_for_condition
from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.map_operator import (
    MapOperator,
    _BlockRefBundler,
)
from ray.data._internal.execution.operators.map_transformer import (
    create_map_transformer_from_block_fn,
)
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.execution.operators.union_operator import UnionOperator
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.block import Block
from ray.data.tests.util import run_one_op_task, run_op_tasks_sync
from ray.tests.conftest import *  # noqa


def _get_blocks(bundle: RefBundle, output_list: List[Block]):
    for block, _ in bundle.blocks:
        output_list.append(list(ray.get(block)["id"]))


def _mul2_transform(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
    for block in block_iter:
        yield pd.DataFrame({"id": [b * 2 for b in block["id"]]})


_mul2_map_data_prcessor = create_map_transformer_from_block_fn(_mul2_transform)


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
        assert len(ctx.sub_progress_bar_dict) == 2
        assert list(ctx.sub_progress_bar_dict.keys()) == ["Test1", "Test2"]
        return make_ref_bundles([[1, 2], [3, 4]]), {"FooStats": []}

    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op = AllToAllOperator(
        dummy_all_transform,
        input_op=input_op,
        num_outputs=2,
        sub_progress_bar_names=["Test1", "Test2"],
        name="TestAll",
    )

    # Initialize progress bar.
    num_bars = op.initialize_sub_progress_bars(0)
    assert num_bars == 2, num_bars

    # Feed data.
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()

    # Check we return transformed bundles.
    assert not op.completed()
    assert _take_outputs(op) == [[1, 2], [3, 4]]
    stats = op.get_stats()
    assert "FooStats" in stats
    assert op.completed()
    op.close_sub_progress_bars()


def test_num_outputs_total():
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(100)]))
    op1 = MapOperator.create(
        _mul2_map_data_prcessor,
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
    input_op = InputDataBuffer(
        make_ref_bundles([[np.ones(1024) * i] for i in range(100)])
    )
    compute_strategy = ActorPoolStrategy(size=1) if use_actors else TaskPoolStrategy()
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
    )

    # Feed data and block on exec.
    op.start(ExecutionOptions(actor_locality_enabled=False))
    if use_actors:
        # Actor will be pending after starting the operator.
        assert op.progress_str() == "0 actors (1 pending) [locality off]"
    assert op.internal_queue_size() == 0
    i = 0
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
        i += 1
        if use_actors:
            assert op.internal_queue_size() == i
        else:
            assert op.internal_queue_size() == 0
    op.all_inputs_done()

    tasks = op.get_active_tasks()
    while tasks:
        run_op_tasks_sync(op, only_existing=True)
        tasks = op.get_active_tasks()
        if use_actors and tasks:
            # After actor is ready (first work ref resolved), actor will remain ready
            # while there is work to do.
            assert op.progress_str() == "1 actors [locality off]"

    assert op.internal_queue_size() == 0
    if use_actors:
        # After all work is done, actor will have been killed to free up resources..
        assert op.progress_str() == "0 actors [locality off]"
    else:
        assert op.progress_str() == ""

    # Check we return transformed bundles in order.
    assert not op.completed()
    assert np.array_equal(
        _take_outputs(op), [[np.ones(1024) * i * 2] for i in range(100)]
    )
    assert op.completed()

    # Check dataset stats.
    stats = op.get_stats()
    assert "TestMapper" in stats, stats
    assert len(stats["TestMapper"]) == 100, stats

    # Check memory stats.
    metrics = op.get_metrics()
    assert metrics["obj_store_mem_alloc"] == pytest.approx(832200, 0.5), metrics
    assert metrics["obj_store_mem_peak"] == pytest.approx(1688000, 0.5), metrics
    assert metrics["obj_store_mem_freed"] == pytest.approx(832200, 0.5), metrics


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_streamed(ray_start_regular_shared, use_actors):
    # Create with inputs.
    input_op = InputDataBuffer(
        make_ref_bundles([[np.ones(1024) * i] for i in range(100)])
    )
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator.create(
        _mul2_map_data_prcessor,
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
            run_one_op_task(op)
        while op.has_next():
            ref = op.get_next()
            assert ref.owns_blocks, ref
            _get_blocks(ref, output)

    # Check equivalent to bulk execution in order.
    assert np.array_equal(output, [[np.ones(1024) * i * 2] for i in range(100)])
    metrics = op.get_metrics()
    assert metrics["obj_store_mem_alloc"] == pytest.approx(832200, 0.5), metrics
    assert metrics["obj_store_mem_peak"] == pytest.approx(16880, 0.5), metrics
    assert metrics["obj_store_mem_freed"] == pytest.approx(832200, 0.5), metrics
    if use_actors:
        assert "locality_hits" in metrics, metrics
        assert "locality_misses" in metrics, metrics
    else:
        assert "locality_hits" not in metrics, metrics
        assert "locality_misses" not in metrics, metrics
    assert not op.completed()


@pytest.mark.parametrize("equal", [False, True])
@pytest.mark.parametrize("chunk_size", [1, 10])
def test_split_operator(ray_start_regular_shared, equal, chunk_size):
    num_input_blocks = 100
    num_splits = 3
    # Add this many input blocks each time.
    # Make sure it is greater than num_splits * 2,
    # so we can test the output order of `OutputSplitter.get_next`.
    num_add_input_blocks = 10
    input_op = InputDataBuffer(
        make_ref_bundles([[i] * chunk_size for i in range(num_input_blocks)])
    )
    op = OutputSplitter(input_op, num_splits, equal=equal)

    # Feed data and implement streaming exec.
    output_splits = [[] for _ in range(num_splits)]
    op.start(ExecutionOptions())
    while input_op.has_next():
        for _ in range(num_add_input_blocks):
            if not input_op.has_next():
                break
            op.add_input(input_op.get_next(), 0)
        while op.has_next():
            ref = op.get_next()
            assert ref.owns_blocks, ref
            for block, _ in ref.blocks:
                assert ref.output_split_idx is not None
                output_splits[ref.output_split_idx].extend(list(ray.get(block)["id"]))
    op.all_inputs_done()

    expected_splits = [[] for _ in range(num_splits)]
    for i in range(num_splits):
        for j in range(i, num_input_blocks, num_splits):
            expected_splits[i].extend([j] * chunk_size)
    if equal:
        min_len = min(len(expected_splits[i]) for i in range(num_splits))
        for i in range(num_splits):
            expected_splits[i] = expected_splits[i][:min_len]
    for i in range(num_splits):
        assert output_splits[i] == expected_splits[i], (
            output_splits[i],
            expected_splits[i],
        )


@pytest.mark.parametrize("equal", [False, True])
@pytest.mark.parametrize("random_seed", [1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
def test_split_operator_random(ray_start_regular_shared, equal, random_seed):
    random.seed(random_seed)
    inputs = make_ref_bundles([[i] * random.randint(0, 10) for i in range(100)])
    num_inputs = sum(x.num_rows() for x in inputs)
    input_op = InputDataBuffer(inputs)
    op = OutputSplitter(input_op, 3, equal=equal)

    # Feed data and implement streaming exec.
    output_splits = collections.defaultdict(list)
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()
    while op.has_next():
        ref = op.get_next()
        assert ref.owns_blocks, ref
        for block, _ in ref.blocks:
            output_splits[ref.output_split_idx].extend(list(ray.get(block)["id"]))
    if equal:
        actual = [len(output_splits[i]) for i in range(3)]
        expected = [num_inputs // 3] * 3
        assert actual == expected
    else:
        assert sum(len(output_splits[i]) for i in range(3)) == num_inputs, output_splits


def test_split_operator_locality_hints(ray_start_regular_shared):
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    op = OutputSplitter(input_op, 2, equal=False, locality_hints=["node1", "node2"])

    def get_fake_loc(item):
        assert isinstance(item, int), item
        if item in [0, 1, 4, 5, 8]:
            return "node1"
        else:
            return "node2"

    def get_bundle_loc(bundle):
        block = ray.get(bundle.blocks[0][0])
        fval = list(block["id"])[0]
        return get_fake_loc(fval)

    op._get_location = get_bundle_loc

    # Feed data and implement streaming exec.
    output_splits = collections.defaultdict(list)
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()
    while op.has_next():
        ref = op.get_next()
        assert ref.owns_blocks, ref
        for block, _ in ref.blocks:
            output_splits[ref.output_split_idx].extend(list(ray.get(block)["id"]))

    total = 0
    for i in range(2):
        if i == 0:
            node = "node1"
        else:
            node = "node2"
        split = output_splits[i]
        for item in split:
            assert get_fake_loc(item) == node
            total += 1

    assert total == 10, total
    assert "all objects local" in op.progress_str()


def test_map_operator_actor_locality_stats(ray_start_regular_shared):
    # Create with inputs.
    input_op = InputDataBuffer(
        make_ref_bundles([[np.ones(100) * i] for i in range(100)])
    )
    compute_strategy = ActorPoolStrategy()
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
    )

    # Feed data and implement streaming exec.
    output = []
    options = ExecutionOptions()
    options.preserve_order = True
    options.actor_locality_enabled = True
    op.start(options)
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
        while not op.has_next():
            run_one_op_task(op)
        while op.has_next():
            ref = op.get_next()
            assert ref.owns_blocks, ref
            _get_blocks(ref, output)

    # Check equivalent to bulk execution in order.
    assert np.array_equal(output, [[np.ones(100) * i * 2] for i in range(100)])
    metrics = op.get_metrics()
    assert metrics["obj_store_mem_alloc"] == pytest.approx(92900, 0.5), metrics
    assert metrics["obj_store_mem_peak"] == pytest.approx(2096, 0.5), metrics
    assert metrics["obj_store_mem_freed"] == pytest.approx(92900, 0.5), metrics
    # Check e2e locality manager working.
    assert metrics["locality_hits"] == 100, metrics
    assert metrics["locality_misses"] == 0, metrics
    assert not op.completed()


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_min_rows_per_bundle(ray_start_regular_shared, use_actors):
    # Simple sanity check of batching behavior.
    def _check_batch(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
        block_iter = list(block_iter)
        assert len(block_iter) == 5, block_iter
        data = [block["id"][0] for block in block_iter]
        assert data == list(range(5)) or data == list(range(5, 10)), data
        for block in block_iter:
            yield block

    # Create with inputs.
    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator.create(
        create_map_transformer_from_block_fn(_check_batch),
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
        min_rows_per_bundle=5,
    )

    # Feed data and block on exec.
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()
    run_op_tasks_sync(op)

    _take_outputs(op)
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
        create_map_transformer_from_block_fn(noop),
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
    op.all_inputs_done()
    run_op_tasks_sync(op)

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
    compute_strategy = ActorPoolStrategy(size=1) if use_actors else TaskPoolStrategy()
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
        ray_remote_args={"num_cpus": 0, "num_gpus": 1},
    )

    # Feed data and block on exec.
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()
    run_op_tasks_sync(op)

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
        create_map_transformer_from_block_fn(_sleep),
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
        ray_remote_args={"num_cpus": 0, "num_gpus": 1},
    )

    # Start one task and then cancel.
    op.start(ExecutionOptions())
    op.add_input(input_op.get_next(), 0)
    assert op.num_active_tasks() == 1
    op.shutdown()

    # Tasks/actors should be cancelled/killed.
    wait_for_condition(lambda: (ray.available_resources().get("GPU", 0) == 1.0))


def test_actor_pool_map_operator_init(ray_start_regular_shared):
    """Tests that ActorPoolMapOperator runs init_fn on start."""

    from ray.exceptions import RayActorError

    def _sleep(block_iter: Iterable[Block]) -> Iterable[Block]:
        time.sleep(999)

    def _fail():
        raise ValueError("init_failed")

    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    compute_strategy = ActorPoolStrategy(min_size=1)

    op = MapOperator.create(
        create_map_transformer_from_block_fn(_sleep, init_fn=_fail),
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
    )

    with pytest.raises(RayActorError, match=r"init_failed"):
        op.start(ExecutionOptions())


def test_actor_pool_map_operator_should_add_input(ray_start_regular_shared):
    """Tests that ActorPoolMapOperator refuses input when actors are pending."""

    def _sleep(block_iter: Iterable[Block]) -> Iterable[Block]:
        time.sleep(999)

    input_op = InputDataBuffer(make_ref_bundles([[i] for i in range(10)]))
    compute_strategy = ActorPoolStrategy(size=1)

    op = MapOperator.create(
        create_map_transformer_from_block_fn(_sleep),
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute_strategy,
    )

    op.start(ExecutionOptions())

    # Cannot add input until actor has started.
    assert not op.should_add_input()
    run_op_tasks_sync(op)
    assert op.should_add_input()

    # Can accept up to four inputs per actor by default.
    for _ in range(4):
        assert op.should_add_input()
        op.add_input(input_op.get_next(), 0)
    assert not op.should_add_input()


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
        _mul2_map_data_prcessor,
        input_op=input_op,
        name="TestMapper",
        compute_strategy=compute,
    )
    assert isinstance(op, expected)


def test_limit_operator(ray_start_regular_shared):
    """Test basic functionalities of LimitOperator."""
    num_refs = 3
    num_rows_per_block = 3
    total_rows = num_refs * num_rows_per_block
    # Test limits with different values, from 0 to more than input size.
    limits = list(range(0, total_rows + 2))
    for limit in limits:
        refs = make_ref_bundles([[i] * num_rows_per_block for i in range(num_refs)])
        input_op = InputDataBuffer(refs)
        limit_op = LimitOperator(limit, input_op)
        limit_op.all_inputs_done = MagicMock(wraps=limit_op.all_inputs_done)
        if limit == 0:
            # If the limit is 0, the operator should be completed immediately.
            assert limit_op.completed()
            assert limit_op._limit_reached()
        else:
            # The number of output bundles is unknown until
            # inputs are completed.
            assert limit_op.num_outputs_total() is None, limit
        cur_rows = 0
        loop_count = 0
        while input_op.has_next() and not limit_op._limit_reached():
            loop_count += 1
            assert not limit_op.completed(), limit
            assert limit_op.need_more_inputs(), limit
            limit_op.add_input(input_op.get_next(), 0)
            while limit_op.has_next():
                # Drain the outputs. So the limit operator
                # will be completed when the limit is reached.
                limit_op.get_next()
            cur_rows += num_rows_per_block
            if cur_rows >= limit:
                assert limit_op.all_inputs_done.call_count == 1, limit
                assert limit_op.completed(), limit
                assert limit_op._limit_reached(), limit
                assert not limit_op.need_more_inputs(), limit
            else:
                assert limit_op.all_inputs_done.call_count == 0, limit
                assert not limit_op.completed(), limit
                assert not limit_op._limit_reached(), limit
                assert limit_op.need_more_inputs(), limit
        limit_op.all_inputs_done()
        # After inputs done, the number of output bundles
        # should be the same as the number of `add_input`s.
        assert limit_op.num_outputs_total() == loop_count, limit
        assert limit_op.completed(), limit


def _get_bundles(bundle: RefBundle):
    output = []
    for block, _ in bundle.blocks:
        output.extend(list(ray.get(block)["id"]))
    return output


@pytest.mark.parametrize("preserve_order", (True, False))
def test_union_operator(ray_start_regular_shared, preserve_order):
    """Test basic functionalities of UnionOperator."""
    execution_options = ExecutionOptions(preserve_order=preserve_order)
    ctx = ray.data.DataContext.get_current()
    ctx.execution_options = execution_options

    num_rows_per_block = 3
    data0 = make_ref_bundles([[i] * num_rows_per_block for i in range(3)])
    data1 = make_ref_bundles([[i] * num_rows_per_block for i in range(2)])
    data2 = make_ref_bundles([[i] * num_rows_per_block for i in range(1)])

    op0 = InputDataBuffer(data0)
    op1 = InputDataBuffer(data1)
    op2 = InputDataBuffer(data2)
    union_op = UnionOperator(op0, op1, op2)
    union_op.start(execution_options)

    assert not union_op.has_next()
    union_op.add_input(op0.get_next(), 0)
    assert union_op.has_next()

    assert union_op.get_next() == data0[0]
    assert not union_op.has_next()

    union_op.add_input(op0.get_next(), 0)
    union_op.add_input(op0.get_next(), 0)
    assert union_op.get_next() == data0[1]
    assert union_op.get_next() == data0[2]

    union_op.input_done(0)
    assert not union_op.completed()
    if preserve_order:
        assert union_op._input_idx_to_output == 1

    if preserve_order:
        union_op.add_input(op1.get_next(), 1)
        union_op.add_input(op2.get_next(), 2)
        assert union_op._input_idx_to_output == 1

        assert union_op.get_next() == data1[0]
        assert not union_op.has_next()

        # Check the case where an input op which is not the op
        # corresponding to _input_idx_to_output finishes first.
        union_op.input_done(2)
        assert union_op._input_idx_to_output == 1

        union_op.add_input(op1.get_next(), 1)
        assert union_op.has_next()
        assert union_op.get_next() == data1[1]
        assert not union_op.has_next()
        # Marking the current output buffer source op will
        # increment _input_idx_to_output to the next source.
        union_op.input_done(1)
        assert union_op._input_idx_to_output == 2
        assert union_op.has_next()
        assert union_op.get_next() == data2[0]
    else:
        union_op.add_input(op1.get_next(), 1)
        union_op.add_input(op2.get_next(), 2)
        union_op.add_input(op1.get_next(), 1)
        # The output will be in the same order as the inputs
        # were added with `add_input()`.
        assert union_op.get_next() == data1[0]
        assert union_op.get_next() == data2[0]
        assert union_op.get_next() == data1[1]

    assert all([len(b) == 0 for b in union_op._input_buffers])

    _take_outputs(union_op)
    union_op.all_inputs_done()
    assert union_op.completed()


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
        for i in list(ray.get(block)["id"])
    ]
    assert flat_out == list(range(n))


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
