import time
from typing import Iterable

import numpy as np
import pandas as pd
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
)
from ray.data._internal.execution.interfaces.task_context import TaskContext
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.map_operator import (
    MapOperator,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.stats import Timer
from ray.data.block import Block
from ray.data.context import (
    DataContext,
)
from ray.data.tests.util import (
    _get_blocks,
    _mul2_transform,
    _take_outputs,
    create_map_transformer_from_block_fn,
    run_one_op_task,
    run_op_tasks_sync,
)
from ray.tests.conftest import *  # noqa

_mul2_map_data_prcessor = create_map_transformer_from_block_fn(_mul2_transform)


def _run_map_operator_test(
    ray_start_regular_shared,
    use_actors,
    preserve_order,
    transform_fn,
    output_block_size_option,
    expected_blocks,
    test_name="TestMapper",
):
    """Shared test function for MapOperator output unbundling tests."""
    # Create with inputs.
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(10)])
    )
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()

    transformer = create_map_transformer_from_block_fn(
        transform_fn,
        output_block_size_option=output_block_size_option,
    )

    op = MapOperator.create(
        transformer,
        input_op=input_op,
        data_context=DataContext.get_current(),
        name=test_name,
        compute_strategy=compute_strategy,
        # Send everything in a single bundle of 10 blocks.
        min_rows_per_bundle=10,
    )

    # Feed data and block on exec.
    op.start(ExecutionOptions(preserve_order=preserve_order))
    if use_actors:
        # Wait for actors to be ready before adding inputs.
        run_op_tasks_sync(op, only_existing=True)

    while input_op.has_next():
        assert op.can_add_input()
        op.add_input(input_op.get_next(), 0)

    op.all_inputs_done()

    run_op_tasks_sync(op)

    # Check that bundles are unbundled in the output queue.
    outputs = []
    while op.has_next():
        outputs.append(op.get_next())
    assert len(outputs) == expected_blocks
    assert op.has_completed()


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_streamed(ray_start_regular_shared, use_actors):
    # Create with inputs.
    input_op = InputDataBuffer(
        DataContext.get_current(),
        make_ref_bundles([[np.ones(1024) * i] for i in range(100)]),
    )
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op,
        DataContext.get_current(),
        name="TestMapper",
        compute_strategy=compute_strategy,
    )

    # Feed data and implement streaming exec.
    output = []
    op.start(ExecutionOptions(actor_locality_enabled=True))

    if use_actors:
        # Wait for actors to be ready before adding inputs.
        run_op_tasks_sync(op, only_existing=True)

    while input_op.has_next():
        # If actor pool at capacity run 1 task and allow it to copmlete
        while not op.can_add_input():
            run_one_op_task(op)

        op.add_input(input_op.get_next(), 0)

    # Complete ingesting inputs
    op.all_inputs_done()
    run_op_tasks_sync(op)

    assert op.has_execution_finished()
    # NOTE: Op is not considered completed until its outputs are drained
    assert not op.has_completed()

    # Fetch all outputs
    while op.has_next():
        ref = op.get_next()
        assert ref.owns_blocks, ref
        _get_blocks(ref, output)

    assert op.has_completed()

    expected = [[np.ones(1024) * i * 2] for i in range(100)]
    output_sorted = sorted(output, key=lambda x: np.asarray(x[0]).flat[0])
    expected_sorted = sorted(expected, key=lambda x: np.asarray(x[0]).flat[0])
    assert np.array_equal(output_sorted, expected_sorted)
    metrics = op.metrics.as_dict()
    assert metrics["obj_store_mem_freed"] == pytest.approx(832200, 0.5), metrics
    if use_actors:
        assert "locality_hits" in metrics, metrics
        assert "locality_misses" in metrics, metrics
    else:
        assert "locality_hits" not in metrics, metrics
        assert "locality_misses" not in metrics, metrics


def test_map_operator_actor_locality_stats(ray_start_regular_shared):
    # Create with inputs.
    input_op = InputDataBuffer(
        DataContext.get_current(),
        make_ref_bundles([[np.ones(100) * i] for i in range(100)]),
    )
    compute_strategy = ActorPoolStrategy()
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        data_context=DataContext.get_current(),
        name="TestMapper",
        compute_strategy=compute_strategy,
        min_rows_per_bundle=None,
    )

    # Feed data and implement streaming exec.
    output = []
    options = ExecutionOptions()
    options.preserve_order = True
    options.actor_locality_enabled = True
    op.start(options)
    # Wait for actors to be ready before adding inputs.
    run_op_tasks_sync(op, only_existing=True)

    while input_op.has_next():
        # If actor pool at capacity run 1 task and allow it to copmlete
        while not op.can_add_input():
            run_one_op_task(op)

        op.add_input(input_op.get_next(), 0)

    # Complete ingesting inputs
    op.all_inputs_done()
    run_op_tasks_sync(op)

    assert op.has_execution_finished()
    # NOTE: Op is not considered completed until its outputs are drained
    assert not op.has_completed()

    # Fetch all outputs
    while op.has_next():
        ref = op.get_next()
        assert ref.owns_blocks, ref
        _get_blocks(ref, output)

    assert op.has_completed()

    # Check equivalent to bulk execution in order.
    assert np.array_equal(output, [[np.ones(100) * i * 2] for i in range(100)])
    metrics = op.metrics.as_dict()
    assert metrics["obj_store_mem_freed"] == pytest.approx(92900, 0.5), metrics
    # Check e2e locality manager working.
    assert metrics["locality_hits"] == 100, metrics
    assert metrics["locality_misses"] == 0, metrics


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
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(10)])
    )
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator.create(
        create_map_transformer_from_block_fn(_check_batch),
        input_op=input_op,
        data_context=DataContext.get_current(),
        name="TestMapper",
        compute_strategy=compute_strategy,
        min_rows_per_bundle=5,
    )

    # Feed data and block on exec.
    op.start(ExecutionOptions())
    if use_actors:
        # Wait for actors to be ready before adding inputs.
        run_op_tasks_sync(op, only_existing=True)

    while input_op.has_next():
        # Should be able to launch 2 tasks:
        #   - Input: 10 blocks of 1 row each
        #   - Bundled into 2 bundles (5 rows each)
        assert op.can_add_input()
        op.add_input(input_op.get_next(), 0)

    op.all_inputs_done()
    run_op_tasks_sync(op)

    _take_outputs(op)
    assert op.has_completed()


@pytest.mark.parametrize("use_actors", [False, True])
@pytest.mark.parametrize("preserve_order", [False, True])
@pytest.mark.parametrize(
    "target_max_block_size,num_expected_blocks", [(1, 10), (2**20, 1), (None, 1)]
)
def test_map_operator_output_unbundling(
    ray_start_regular_shared,
    use_actors,
    preserve_order,
    target_max_block_size,
    num_expected_blocks,
):
    """Test that MapOperator's output queue unbundles bundles from tasks."""

    def noop(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
        for block in block_iter:
            yield block

    _run_map_operator_test(
        ray_start_regular_shared,
        use_actors,
        preserve_order,
        noop,
        OutputBlockSizeOption.of(target_max_block_size=target_max_block_size),
        num_expected_blocks,
    )


@pytest.mark.parametrize("preserve_order", [False, True])
@pytest.mark.parametrize(
    "output_block_size_option,expected_blocks",
    [
        # Test target_max_block_size
        (OutputBlockSizeOption.of(target_max_block_size=1), 10),
        (OutputBlockSizeOption.of(target_max_block_size=2**20), 1),
        (OutputBlockSizeOption.of(target_max_block_size=None), 1),
        # Test target_num_rows_per_block
        (OutputBlockSizeOption.of(target_num_rows_per_block=1), 10),
        (OutputBlockSizeOption.of(target_num_rows_per_block=5), 2),
        (OutputBlockSizeOption.of(target_num_rows_per_block=10), 1),
        (OutputBlockSizeOption.of(target_num_rows_per_block=None), 1),
        # Test disable_block_shaping
        (OutputBlockSizeOption.of(disable_block_shaping=True), 10),
        (OutputBlockSizeOption.of(disable_block_shaping=False), 1),
        # Test combinations
        (
            OutputBlockSizeOption.of(
                target_max_block_size=1, target_num_rows_per_block=5
            ),
            10,
        ),
        (
            OutputBlockSizeOption.of(
                target_max_block_size=2**20, disable_block_shaping=True
            ),
            10,
        ),
        (
            OutputBlockSizeOption.of(
                target_num_rows_per_block=5, disable_block_shaping=True
            ),
            10,
        ),
    ],
)
def test_map_operator_output_block_size_options(
    ray_start_regular_shared,
    preserve_order,
    output_block_size_option,
    expected_blocks,
):
    """Test MapOperator with various OutputBlockSizeOption configurations."""

    def noop(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
        for block in block_iter:
            yield block

    _run_map_operator_test(
        ray_start_regular_shared,
        use_actors=False,
        preserve_order=preserve_order,
        transform_fn=noop,
        output_block_size_option=output_block_size_option,
        expected_blocks=expected_blocks,
    )


@pytest.mark.parametrize("preserve_order", [False, True])
def test_map_operator_disable_block_shaping_with_batches(
    ray_start_regular_shared,
    preserve_order,
):
    """Test MapOperator with disable_block_shaping=True using batch operations."""

    def batch_transform(batch_iter, ctx):
        for batch in batch_iter:
            # Simple transformation: add 1 to each value
            if hasattr(batch, "to_pandas"):
                df = batch.to_pandas()
                df = df + 1
                yield df
            else:
                yield batch

    _run_map_operator_test(
        ray_start_regular_shared,
        use_actors=False,
        preserve_order=preserve_order,
        transform_fn=batch_transform,
        output_block_size_option=OutputBlockSizeOption.of(disable_block_shaping=True),
        expected_blocks=10,  # With disable_block_shaping=True, we expect 10 blocks
        test_name="TestBatchMapper",
    )


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_ray_args(shutdown_only, use_actors):
    ray.shutdown()
    ray.init(num_cpus=0, num_gpus=1)
    # Create with inputs.
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(10)])
    )
    compute_strategy = ActorPoolStrategy(size=1) if use_actors else TaskPoolStrategy()
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        data_context=DataContext.get_current(),
        name="TestMapper",
        compute_strategy=compute_strategy,
        ray_remote_args={"num_cpus": 0, "num_gpus": 1},
    )

    # Feed data and block on exec.
    op.start(ExecutionOptions())
    if use_actors:
        # Wait for the actor to start.
        run_op_tasks_sync(op)

    while input_op.has_next():
        if use_actors:
            # For actors, we need to check capacity before adding input
            # and process tasks when the actor pool is at capacity.
            while not op.can_add_input():
                run_one_op_task(op)

        assert op.can_add_input()
        op.add_input(input_op.get_next(), 0)

    op.all_inputs_done()
    run_op_tasks_sync(op)

    # Check we don't hang and complete with num_gpus=1.
    outputs = _take_outputs(op)
    expected = [[i * 2] for i in range(10)]
    assert sorted(outputs) == expected, f"Expected {expected}, got {outputs}"
    assert op.has_completed()


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_operator_shutdown(shutdown_only, use_actors):
    ray.shutdown()
    ray.init(num_cpus=0, num_gpus=1)

    def _sleep(block_iter: Iterable[Block]) -> Iterable[Block]:
        time.sleep(999)

    # Create with inputs.
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(10)])
    )
    compute_strategy = ActorPoolStrategy(size=1) if use_actors else TaskPoolStrategy()
    op = MapOperator.create(
        create_map_transformer_from_block_fn(_sleep),
        input_op=input_op,
        data_context=DataContext.get_current(),
        name="TestMapper",
        compute_strategy=compute_strategy,
        ray_remote_args={"num_cpus": 0, "num_gpus": 1},
    )

    # Start one task and then cancel.
    op.start(ExecutionOptions())
    if use_actors:
        # Wait for the actor to start.
        run_op_tasks_sync(op)
    op.add_input(input_op.get_next(), 0)
    assert op.num_active_tasks() == 1
    # Regular Ray tasks can be interrupted/cancelled, so graceful shutdown works.
    # Actors running time.sleep() cannot be interrupted gracefully and need ray.kill() to release resources.
    # After proper shutdown, both should return the GPU to ray.available_resources().
    force_shutdown = use_actors
    op.shutdown(timer=Timer(), force=force_shutdown)

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
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(100)])
    )
    op = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op=input_op,
        data_context=DataContext.get_current(),
        name="TestMapper",
        compute_strategy=compute,
    )
    assert isinstance(op, expected)


@pytest.mark.parametrize("use_actors", [False, True])
def test_map_kwargs(ray_start_regular_shared, use_actors):
    """Test propagating additional kwargs to map tasks."""
    foo = 1
    bar = np.random.random(1024 * 1024)
    kwargs = {
        "foo": foo,  # Pass by value
        "bar": ray.put(bar),  # Pass by ObjectRef
    }

    def map_fn(block_iter: Iterable[Block], ctx: TaskContext) -> Iterable[Block]:
        nonlocal foo, bar
        assert ctx.kwargs["foo"] == foo
        # bar should be automatically deref'ed.
        assert np.array_equal(ctx.kwargs["bar"], bar)

        yield from block_iter

    input_op = InputDataBuffer(
        DataContext.get_current(),
        make_ref_bundles([[i] for i in range(10)]),
    )
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()
    op = MapOperator.create(
        create_map_transformer_from_block_fn(map_fn),
        input_op=input_op,
        data_context=DataContext.get_current(),
        name="TestMapper",
        compute_strategy=compute_strategy,
    )
    op.add_map_task_kwargs_fn(lambda: kwargs)
    op.start(ExecutionOptions())
    if use_actors:
        # Wait for the actor to start.
        run_op_tasks_sync(op)

    while input_op.has_next():
        if use_actors:
            # For actors, we need to check capacity before adding input
            # and process tasks when the actor pool is at capacity.
            while not op.can_add_input():
                run_one_op_task(op)

        assert op.can_add_input()
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()
    run_op_tasks_sync(op)

    _take_outputs(op)
    assert op.has_completed()


@pytest.mark.parametrize(
    "target_max_block_size, expected_num_outputs_per_task",
    [
        # 5 blocks (8b each) // 1 = 5 outputs / task
        [1, 5],
        # 5 blocks (8b each) // 1024 = 1 output / task
        [1024, 1],
        # All outputs combined in a single output
        [None, 1],
    ],
)
def test_map_estimated_num_output_bundles(
    target_max_block_size,
    expected_num_outputs_per_task,
):
    # Test map operator estimation
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(100)])
    )

    def yield_five(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
        for i in range(5):
            yield pd.DataFrame({"id": [i]})

    min_rows_per_bundle = 10
    # 100 inputs -> 100 / 10 = 10 tasks
    num_tasks = 10

    op = MapOperator.create(
        create_map_transformer_from_block_fn(
            yield_five,
            # Limit single block to hold no more than 1 byte
            output_block_size_option=OutputBlockSizeOption.of(
                target_max_block_size=target_max_block_size,
            ),
        ),
        input_op=input_op,
        data_context=DataContext.get_current(),
        name="TestEstimatedNumBlocks",
        min_rows_per_bundle=min_rows_per_bundle,
    )

    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
        if op.metrics.num_inputs_received % min_rows_per_bundle == 0:
            # enough inputs for a task bundle
            run_op_tasks_sync(op)
            assert (
                op._estimated_num_output_bundles
                == expected_num_outputs_per_task * num_tasks
            )

    op.all_inputs_done()

    assert op._estimated_num_output_bundles == expected_num_outputs_per_task * num_tasks


def test_map_estimated_blocks_split():
    # Test read output splitting

    min_rows_per_bundle = 10
    input_op = InputDataBuffer(
        DataContext.get_current(),
        make_ref_bundles(
            [[i, i + 1] for i in range(100)]
        ),  # create 2-row blocks so split_blocks can split into 2 blocks
    )

    def yield_five(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
        for i in range(5):
            yield pd.DataFrame({"id": [i]})

    op = MapOperator.create(
        create_map_transformer_from_block_fn(
            yield_five,
            # NOTE: Disable output block-shaping to keep blocks from being
            #       combined
            disable_block_shaping=True,
        ),
        input_op=input_op,
        data_context=DataContext.get_current(),
        name="TestEstimatedNumBlocksSplit",
        min_rows_per_bundle=min_rows_per_bundle,
    )
    op.set_additional_split_factor(2)

    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
        if op.metrics.num_inputs_received % min_rows_per_bundle == 0:
            # enough inputs for a task bundle
            run_op_tasks_sync(op)
            assert op._estimated_num_output_bundles == 100

    op.all_inputs_done()
    # Each output block is split in 2, so the number of blocks double.
    assert op._estimated_num_output_bundles == 100


def test_operator_metrics():
    NUM_INPUTS = 100
    NUM_BLOCKS_PER_TASK = 5
    MIN_ROWS_PER_BUNDLE = 10

    inputs = make_ref_bundles([[i] for i in range(NUM_INPUTS)])
    input_op = InputDataBuffer(DataContext.get_current(), inputs)

    def map_fn(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
        for i in range(NUM_BLOCKS_PER_TASK):
            yield pd.DataFrame({"id": [i]})

    op = MapOperator.create(
        create_map_transformer_from_block_fn(
            map_fn,
            output_block_size_option=OutputBlockSizeOption.of(
                target_max_block_size=1,
            ),
        ),
        input_op=input_op,
        data_context=DataContext.get_current(),
        name="TestEstimatedNumBlocks",
        min_rows_per_bundle=MIN_ROWS_PER_BUNDLE,
    )

    op.start(ExecutionOptions())
    num_outputs_taken = 0
    bytes_outputs_taken = 0
    for i in range(len(inputs)):
        # Add an input, run all tasks, and take all outputs.
        op.add_input(input_op.get_next(), 0)
        run_op_tasks_sync(op)
        while op.has_next():
            output = op.get_next()
            num_outputs_taken += 1
            bytes_outputs_taken += output.size_bytes()

        num_tasks_submitted = (i + 1) // MIN_ROWS_PER_BUNDLE

        metrics = op.metrics
        # Check input metrics
        assert metrics.num_inputs_received == i + 1, i
        assert metrics.bytes_inputs_received == sum(
            inputs[k].size_bytes() for k in range(i + 1)
        ), i
        assert (
            metrics.num_task_inputs_processed
            == num_tasks_submitted * MIN_ROWS_PER_BUNDLE
        ), i
        assert metrics.bytes_task_inputs_processed == sum(
            inputs[k].size_bytes()
            for k in range(num_tasks_submitted * MIN_ROWS_PER_BUNDLE)
        ), i

        # Check outputs metrics
        assert num_outputs_taken == num_tasks_submitted * NUM_BLOCKS_PER_TASK, i
        assert metrics.num_task_outputs_generated == num_outputs_taken, i
        assert metrics.bytes_task_outputs_generated == bytes_outputs_taken, i
        assert metrics.num_outputs_taken == num_outputs_taken, i
        assert metrics.bytes_outputs_taken == bytes_outputs_taken, i
        assert metrics.num_outputs_of_finished_tasks == num_outputs_taken, i
        assert metrics.bytes_outputs_of_finished_tasks == bytes_outputs_taken, i

        # Check task metrics
        assert metrics.num_tasks_submitted == num_tasks_submitted, i
        assert metrics.num_tasks_running == 0, i
        assert metrics.num_tasks_have_outputs == num_tasks_submitted, i
        assert metrics.num_tasks_finished == num_tasks_submitted, i

        # Check object store metrics
        assert metrics.obj_store_mem_freed == metrics.bytes_task_inputs_processed, i


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
