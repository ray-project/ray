import collections
import gc
import random
import time
from typing import Any, Callable, Iterable, List, Optional
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.data._internal.actor_autoscaler import ActorPoolScalingRequest
from ray.data._internal.compute import ActorPoolStrategy, TaskPoolStrategy
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    PhysicalOperator,
    RefBundle,
)
from ray.data._internal.execution.interfaces.task_context import TaskContext
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
    _per_block_limit_fn,
)
from ray.data._internal.execution.operators.map_transformer import (
    BlockMapTransformFn,
    MapTransformCallable,
    MapTransformer,
)
from ray.data._internal.execution.operators.output_splitter import OutputSplitter
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.execution.streaming_executor import StreamingExecutor
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.logical.optimizers import get_execution_plan
from ray.data._internal.output_buffer import OutputBlockSizeOption
from ray.data._internal.stats import Timer
from ray.data.block import Block, BlockAccessor
from ray.data.context import DataContext
from ray.data.tests.util import run_one_op_task, run_op_tasks_sync
from ray.tests.client_test_utils import create_remote_signal_actor
from ray.tests.conftest import *  # noqa


def _get_blocks(bundle: RefBundle, output_list: List[Block]):
    for block_ref in bundle.block_refs:
        output_list.append(list(ray.get(block_ref)["id"]))


def _mul2_transform(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
    for block in block_iter:
        yield pd.DataFrame({"id": [b * 2 for b in block["id"]]})


def create_map_transformer_from_block_fn(
    block_fn: MapTransformCallable[Block, Block],
    init_fn: Optional[Callable[[], None]] = None,
    output_block_size_option: Optional[OutputBlockSizeOption] = None,
    disable_block_shaping: bool = False,
):
    """Create a MapTransformer from a single block-based transform function.

    This method should only be used for testing and legacy compatibility.
    """
    return MapTransformer(
        [
            BlockMapTransformFn(
                block_fn,
                output_block_size_option=output_block_size_option,
                disable_block_shaping=disable_block_shaping,
            ),
        ],
        init_fn=init_fn,
    )


_mul2_map_data_prcessor = create_map_transformer_from_block_fn(_mul2_transform)


def _take_outputs(op: PhysicalOperator) -> List[Any]:
    output = []
    while op.has_next():
        ref = op.get_next()
        assert ref.owns_blocks, ref
        _get_blocks(ref, output)
    return output


def test_name_and_repr(ray_start_regular_shared):
    inputs = make_ref_bundles([[1, 2], [3], [4, 5]])
    input_op = InputDataBuffer(DataContext.get_current(), inputs)
    map_op1 = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op,
        DataContext.get_current(),
        name="map1",
    )

    assert map_op1.name == "map1"
    assert map_op1.dag_str == "InputDataBuffer[Input] -> TaskPoolMapOperator[map1]"
    assert str(map_op1) == "TaskPoolMapOperator[map1]"

    map_op2 = MapOperator.create(
        _mul2_map_data_prcessor,
        map_op1,
        DataContext.get_current(),
        name="map2",
    )
    assert map_op2.name == "map2"
    assert (
        map_op2.dag_str
        == "InputDataBuffer[Input] -> TaskPoolMapOperator[map1] -> TaskPoolMapOperator[map2]"
    )
    assert str(map_op2) == "TaskPoolMapOperator[map2]"


def test_input_data_buffer(ray_start_regular_shared):
    # Create with bundles.
    inputs = make_ref_bundles([[1, 2], [3], [4, 5]])
    op = InputDataBuffer(DataContext.get_current(), inputs)

    # Check we return all bundles in order.
    assert not op.completed()
    assert _take_outputs(op) == [[1, 2], [3], [4, 5]]
    assert op.completed()


def test_all_to_all_operator():
    def dummy_all_transform(bundles: List[RefBundle], ctx):
        assert len(ctx.sub_progress_bar_dict) == 2
        assert list(ctx.sub_progress_bar_dict.keys()) == ["Test1", "Test2"]
        return make_ref_bundles([[1, 2], [3, 4]]), {"FooStats": []}

    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(100)])
    )
    op = AllToAllOperator(
        dummy_all_transform,
        input_op,
        DataContext.get_current(),
        target_max_block_size_override=DataContext.get_current().target_max_block_size,
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
    outputs = _take_outputs(op)
    expected = [[1, 2], [3, 4]]
    assert sorted(outputs) == expected, f"Expected {expected}, got {outputs}"
    stats = op.get_stats()
    assert "FooStats" in stats
    assert op.completed()
    op.close_sub_progress_bars()


def test_num_outputs_total():
    # The number of outputs is always known for InputDataBuffer.
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(100)])
    )
    assert input_op.num_outputs_total() == 100

    # Prior to execution, the number of outputs is unknown
    # for Map/AllToAllOperator operators.
    op1 = MapOperator.create(
        _mul2_map_data_prcessor,
        input_op,
        DataContext.get_current(),
        name="TestMapper",
    )
    assert op1.num_outputs_total() is None

    def dummy_all_transform(bundles: List[RefBundle]):
        return make_ref_bundles([[1, 2], [3, 4]]), {"FooStats": []}

    op2 = AllToAllOperator(
        dummy_all_transform,
        input_op=op1,
        data_context=DataContext.get_current(),
        target_max_block_size_override=DataContext.get_current().target_max_block_size,
        name="TestAll",
    )
    assert op2.num_outputs_total() is None

    # Feed data and implement streaming exec.
    output = []
    op1.start(ExecutionOptions(actor_locality_enabled=True))
    while input_op.has_next():
        op1.add_input(input_op.get_next(), 0)
        while not op1.has_next():
            run_one_op_task(op1)
        while op1.has_next():
            ref = op1.get_next()
            assert ref.owns_blocks, ref
            _get_blocks(ref, output)
    # After op finishes, num_outputs_total is known.
    assert op1.num_outputs_total() == 100


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
    metrics = op.metrics.as_dict()
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
        DataContext.get_current(),
        make_ref_bundles([[i] * chunk_size for i in range(num_input_blocks)]),
    )
    op = OutputSplitter(
        input_op,
        num_splits,
        equal=equal,
        data_context=DataContext.get_current(),
    )

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
            for block_ref in ref.block_refs:
                assert ref.output_split_idx is not None
                output_splits[ref.output_split_idx].extend(
                    list(ray.get(block_ref)["id"])
                )
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
    input_op = InputDataBuffer(DataContext.get_current(), inputs)
    op = OutputSplitter(
        input_op, 3, equal=equal, data_context=DataContext.get_current()
    )

    # Feed data and implement streaming exec.
    output_splits = collections.defaultdict(list)
    op.start(ExecutionOptions())
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()
    while op.has_next():
        ref = op.get_next()
        assert ref.owns_blocks, ref
        for block_ref in ref.block_refs:
            output_splits[ref.output_split_idx].extend(list(ray.get(block_ref)["id"]))
    if equal:
        actual = [len(output_splits[i]) for i in range(3)]
        expected = [num_inputs // 3] * 3
        assert actual == expected
    else:
        assert sum(len(output_splits[i]) for i in range(3)) == num_inputs, output_splits


def test_split_operator_locality_hints(ray_start_regular_shared):
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(10)])
    )
    op = OutputSplitter(
        input_op,
        2,
        equal=False,
        data_context=DataContext.get_current(),
        locality_hints=["node1", "node2"],
    )

    def get_fake_loc(item):
        assert isinstance(item, int), item
        if item in [0, 1, 4, 5, 8]:
            return "node1"
        else:
            return "node2"

    def get_bundle_loc(bundle):
        block = ray.get(bundle.blocks[0][0])
        fval = list(block["id"])[0]
        return [get_fake_loc(fval)]

    op._get_locations = get_bundle_loc

    # Feed data and implement streaming exec.
    output_splits = collections.defaultdict(list)
    op.start(ExecutionOptions(actor_locality_enabled=True))
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()
    while op.has_next():
        ref = op.get_next()
        assert ref.owns_blocks, ref
        for block_ref in ref.block_refs:
            output_splits[ref.output_split_idx].extend(list(ray.get(block_ref)["id"]))

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
    metrics = op.metrics.as_dict()
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
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()
    run_op_tasks_sync(op)

    _take_outputs(op)
    assert op.completed()


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
    # Tests that the MapOperator's output queue unbundles the bundles returned from
    # tasks; this facilitates features such as dynamic block splitting.
    def noop(block_iter: Iterable[Block], ctx) -> Iterable[Block]:
        for block in block_iter:
            yield block

    # Create with inputs.
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(10)])
    )
    compute_strategy = ActorPoolStrategy() if use_actors else TaskPoolStrategy()

    transformer = create_map_transformer_from_block_fn(
        noop,
        output_block_size_option=OutputBlockSizeOption.of(
            target_max_block_size=target_max_block_size,
        ),
    )

    op = MapOperator.create(
        transformer,
        input_op=input_op,
        data_context=DataContext.get_current(),
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
    assert len(outputs) == num_expected_blocks
    assert op.completed()


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
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()
    run_op_tasks_sync(op)

    # Check we don't hang and complete with num_gpus=1.
    outputs = _take_outputs(op)
    expected = [[i * 2] for i in range(10)]
    assert sorted(outputs) == expected, f"Expected {expected}, got {outputs}"
    assert op.completed()


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


def test_actor_pool_map_operator_init(ray_start_regular_shared, data_context_override):
    """Tests that ActorPoolMapOperator runs init_fn on start."""

    from ray.exceptions import RayActorError

    # Override to block on actor pool provisioning at least min actors
    data_context_override.wait_for_min_actors_s = 60

    def _sleep(block_iter: Iterable[Block]) -> Iterable[Block]:
        time.sleep(999)

    def _fail():
        raise ValueError("init_failed")

    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(10)])
    )
    compute_strategy = ActorPoolStrategy(min_size=1)

    op = MapOperator.create(
        create_map_transformer_from_block_fn(_sleep, init_fn=_fail),
        input_op=input_op,
        data_context=DataContext.get_current(),
        name="TestMapper",
        compute_strategy=compute_strategy,
    )

    with pytest.raises(RayActorError, match=r"init_failed"):
        op.start(ExecutionOptions())


@pytest.mark.parametrize(
    "max_tasks_in_flight_strategy, max_tasks_in_flight_ctx, max_concurrency, expected_max_tasks_in_flight",
    [
        # Compute strategy takes precedence
        (3, 5, 4, 3),
        # DataContext.max_tasks_in_flight_per_actor takes precedence
        (None, 5, 4, 5),
        # Max tasks in-flight is derived as max_concurrency x 2
        (None, None, 4, 8),
    ],
)
def test_actor_pool_map_operator_should_add_input(
    ray_start_regular_shared,
    max_tasks_in_flight_strategy,
    max_tasks_in_flight_ctx,
    max_concurrency,
    expected_max_tasks_in_flight,
    restore_data_context,
):
    """Tests that ActorPoolMapOperator refuses input when actors are pending."""

    ctx = DataContext.get_current()
    ctx.max_tasks_in_flight_per_actor = max_tasks_in_flight_ctx

    input_op = InputDataBuffer(ctx, make_ref_bundles([[i] for i in range(10)]))

    compute_strategy = ActorPoolStrategy(
        size=1,
        max_tasks_in_flight_per_actor=max_tasks_in_flight_strategy,
    )

    def _failing_transform(
        block_iter: Iterable[Block], task_context: TaskContext
    ) -> Iterable[Block]:
        raise ValueError("expected failure")

    op = MapOperator.create(
        create_map_transformer_from_block_fn(_failing_transform),
        input_op=input_op,
        data_context=ctx,
        name="TestMapper",
        compute_strategy=compute_strategy,
        ray_remote_args={"max_concurrency": max_concurrency},
    )

    op.start(ExecutionOptions())

    # Cannot add input until actor has started.
    assert not op.should_add_input()
    run_op_tasks_sync(op)
    assert op.should_add_input()

    # Assert that single actor can accept up to N tasks
    for _ in range(expected_max_tasks_in_flight):
        assert op.should_add_input()
        op.add_input(input_op.get_next(), 0)
    assert not op.should_add_input()


def test_actor_pool_map_operator_num_active_tasks_and_completed(shutdown_only):
    """Tests ActorPoolMapOperator's num_active_tasks and completed methods."""
    num_actors = 2
    ray.shutdown()
    ray.init(num_cpus=num_actors)

    signal_actor = create_remote_signal_actor(ray).options(num_cpus=0).remote()

    def _map_transfom_fn(block_iter: Iterable[Block], _) -> Iterable[Block]:
        ray.get(signal_actor.wait.remote())
        yield from block_iter

    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(num_actors)])
    )
    compute_strategy = ActorPoolStrategy(min_size=num_actors, max_size=2 * num_actors)

    # Create an operator with [num_actors, 2 * num_actors] actors.
    # Resources are limited to num_actors, so the second half will be pending.
    op = MapOperator.create(
        create_map_transformer_from_block_fn(_map_transfom_fn),
        input_op=input_op,
        data_context=DataContext.get_current(),
        name="TestMapper",
        compute_strategy=compute_strategy,
    )
    actor_pool = op._actor_pool

    # Wait for the op to scale up to the min size.
    op.start(ExecutionOptions())
    run_op_tasks_sync(op)
    assert actor_pool.num_running_actors() == num_actors
    assert op.num_active_tasks() == 0

    # Scale up to the max size, the second half of the actors will be pending.
    actor_pool.scale(ActorPoolScalingRequest(delta=num_actors))
    assert actor_pool.num_pending_actors() == num_actors
    # `num_active_tasks` should exclude the metadata tasks for the pending actors.
    assert op.num_active_tasks() == 0

    # Add inputs.
    for _ in range(num_actors):
        assert op.should_add_input()
        op.add_input(input_op.get_next(), 0)
    # Still `num_active_tasks` should only include data tasks.
    assert op.num_active_tasks() == num_actors
    assert actor_pool.num_pending_actors() == num_actors

    # Let the data tasks complete.
    signal_actor.send.remote()
    while len(op._data_tasks) > 0:
        run_one_op_task(op)
    assert op.num_active_tasks() == 0
    assert actor_pool.num_pending_actors() == num_actors

    # Mark the inputs done and take all outputs.
    # The operator should be completed, even if there are pending actors.
    op.all_inputs_done()
    while op.has_next():
        op.get_next()
    assert actor_pool.num_pending_actors() == num_actors
    assert op.completed()


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


def test_limit_operator(ray_start_regular_shared):
    """Test basic functionalities of LimitOperator."""
    num_refs = 3
    num_rows_per_block = 3
    total_rows = num_refs * num_rows_per_block
    # Test limits with different values, from 0 to more than input size.
    limits = list(range(0, total_rows + 2))
    for limit in limits:
        refs = make_ref_bundles([[i] * num_rows_per_block for i in range(num_refs)])
        input_op = InputDataBuffer(DataContext.get_current(), refs)
        limit_op = LimitOperator(limit, input_op, DataContext.get_current())
        limit_op.mark_execution_finished = MagicMock(
            wraps=limit_op.mark_execution_finished
        )
        if limit == 0:
            # If the limit is 0, the operator should be completed immediately.
            assert limit_op.completed()
            assert limit_op._limit_reached()
        cur_rows = 0
        loop_count = 0
        while input_op.has_next() and not limit_op._limit_reached():
            loop_count += 1
            assert not limit_op.completed(), limit
            assert not limit_op._execution_finished, limit
            limit_op.add_input(input_op.get_next(), 0)
            while limit_op.has_next():
                # Drain the outputs. So the limit operator
                # will be completed when the limit is reached.
                limit_op.get_next()
            cur_rows += num_rows_per_block
            if cur_rows >= limit:
                assert limit_op.mark_execution_finished.call_count == 1, limit
                assert limit_op.completed(), limit
                assert limit_op._limit_reached(), limit
                assert limit_op._execution_finished, limit
            else:
                assert limit_op.mark_execution_finished.call_count == 0, limit
                assert not limit_op.completed(), limit
                assert not limit_op._limit_reached(), limit
                assert not limit_op._execution_finished, limit
        limit_op.mark_execution_finished()
        # After inputs done, the number of output bundles
        # should be the same as the number of `add_input`s.
        assert limit_op.num_outputs_total() == loop_count, limit
        assert limit_op.completed(), limit


def test_limit_operator_memory_leak_fix(ray_start_regular_shared, tmp_path):
    """Test that LimitOperator properly drains upstream output queues.

    This test verifies the memory leak fix by directly using StreamingExecutor
    to access the actual topology and check queued blocks after execution.
    """
    for i in range(100):
        data = [{"id": i * 5 + j, "value": f"row_{i * 5 + j}"} for j in range(5)]
        table = pa.Table.from_pydict(
            {"id": [row["id"] for row in data], "value": [row["value"] for row in data]}
        )
        parquet_file = tmp_path / f"test_data_{i}.parquet"
        pq.write_table(table, str(parquet_file))

    parquet_files = [str(tmp_path / f"test_data_{i}.parquet") for i in range(100)]

    ds = (
        ray.data.read_parquet(parquet_files, override_num_blocks=100)
        .limit(5)
        .map(lambda x: x)
    )

    execution_plan = ds._plan
    physical_plan = get_execution_plan(execution_plan._logical_plan)

    # Use StreamingExecutor directly to have access to the actual topology
    executor = StreamingExecutor(DataContext.get_current())
    output_iterator = executor.execute(physical_plan.dag)

    # Collect all results and count rows
    total_rows = 0
    for bundle in output_iterator:
        for block_ref in bundle.block_refs:
            block = ray.get(block_ref)
            total_rows += block.num_rows
    assert (
        total_rows == 5
    ), f"Expected exactly 5 rows after limit(5), but got {total_rows}"

    # Find the ReadParquet operator's OpState
    topology = executor._topology
    read_parquet_op_state = None
    for op, op_state in topology.items():
        if "ReadParquet" in op.name:
            read_parquet_op_state = op_state
            break

    # Check the output queue size
    output_queue_size = len(read_parquet_op_state.output_queue)
    assert output_queue_size == 0, f"Expected 0 items, but got {output_queue_size}."


def _get_bundles(bundle: RefBundle):
    output = []
    for block_ref in bundle.block_refs:
        output.append(list(ray.get(block_ref)["id"]))
    return output


def _make_ref_bundles(raw_bundles: List[List[List[Any]]]) -> List[RefBundle]:
    rbs = []
    for raw_bundle in raw_bundles:
        blocks = []
        schema = None
        for raw_block in raw_bundle:
            print(f">>> {raw_block=}")

            block = pd.DataFrame({"id": raw_block})
            blocks.append(
                (ray.put(block), BlockAccessor.for_block(block).get_metadata())
            )
            schema = BlockAccessor.for_block(block).schema()

        rb = RefBundle(blocks=blocks, owns_blocks=True, schema=schema)

        rbs.append(rb)

    return rbs


@pytest.mark.parametrize(
    "target,in_bundles,expected_bundles",
    [
        (
            # Unit target, should leave unchanged.
            1,
            [
                # Input bundles
                [[1]],
                [[2]],
                [[3, 4]],
                [[5]],
            ],
            [
                # Output bundles
                [[1]],
                [[2]],
                [[3, 4]],
                [[5]],
            ],
        ),
        (
            # No target, should leave unchanged.
            None,
            [
                # Input bundles
                [[1]],
                [[2]],
                [[3, 4]],
                [[5]],
            ],
            [
                # Output bundles
                [[1]],
                [[2]],
                [[3, 4]],
                [[5]],
            ],
        ),
        (
            # Proper handling of empty blocks
            2,
            [
                # Input bundles
                [[1]],
                [[]],
                [[]],
                [[2, 3]],
                [[]],
                [[]],
            ],
            [
                # Output bundles
                [[1], [], [], [2, 3]],
                [[], []],
            ],
        ),
        (
            # Test bundling, finalizing, passing, leftovers, etc.
            2,
            [
                # Input bundles
                [[1], [2]],
                [[3, 4, 5]],
                [[6], [7]],
                [[8]],
                [[9, 10], [11]],
            ],
            [[[1], [2]], [[3, 4, 5]], [[6], [7]], [[8], [9, 10], [11]]],
        ),
        (
            # Test bundling, finalizing, passing, leftovers, etc.
            3,
            [
                # Input bundles
                [[1]],
                [[2, 3]],
                [[4, 5, 6, 7]],
                [[8, 9], [10, 11]],
            ],
            [
                # Output bundles
                [[1], [2, 3]],
                [[4, 5, 6, 7]],
                [[8, 9], [10, 11]],
            ],
        ),
    ],
)
def test_block_ref_bundler_basic(target, in_bundles, expected_bundles):
    # Test that the bundler creates the expected output bundles.
    bundler = _BlockRefBundler(target)
    bundles = _make_ref_bundles(in_bundles)
    out_bundles = []
    for bundle in bundles:
        bundler.add_bundle(bundle)
        while bundler.has_bundle():
            out_bundle = _get_bundles(bundler.get_next_bundle()[1])
            out_bundles.append(out_bundle)

    bundler.done_adding_bundles()

    if bundler.has_bundle():
        out_bundle = _get_bundles(bundler.get_next_bundle()[1])
        out_bundles.append(out_bundle)

    # Assert expected output
    assert out_bundles == expected_bundles
    # Assert that all bundles have been ingested
    assert bundler.num_bundles() == 0

    for bundle, expected in zip(out_bundles, expected_bundles):
        assert bundle == expected


@pytest.mark.parametrize(
    "target,n,num_bundles,num_out_bundles,out_bundle_size",
    [
        (5, 20, 20, 4, 5),
        (5, 24, 10, 4, 6),
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
            _, out_bundle = bundler.get_next_bundle()
            out_bundles.append(out_bundle)
    bundler.done_adding_bundles()
    if bundler.has_bundle():
        _, out_bundle = bundler.get_next_bundle()
        out_bundles.append(out_bundle)
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
    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
    op.all_inputs_done()
    run_op_tasks_sync(op)

    _take_outputs(op)
    assert op.completed()


def test_limit_estimated_num_output_bundles():
    # Test limit operator estimation
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i, i] for i in range(100)])
    )
    op = LimitOperator(100, input_op, DataContext.get_current())

    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
        run_op_tasks_sync(op)
        assert op._estimated_num_output_bundles == 50

    op.all_inputs_done()

    # 2 rows per bundle, 100 / 2 = 50 blocks output
    assert op._estimated_num_output_bundles == 50

    # Test limit operator estimation where: limit > # of rows
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i, i] for i in range(100)])
    )
    op = LimitOperator(300, input_op, DataContext.get_current())

    while input_op.has_next():
        op.add_input(input_op.get_next(), 0)
        run_op_tasks_sync(op)
        assert op._estimated_num_output_bundles == 100

    op.all_inputs_done()

    # all blocks are outputted
    assert op._estimated_num_output_bundles == 100


def test_all_to_all_estimated_num_output_bundles():
    # Test all to all operator
    input_op = InputDataBuffer(
        DataContext.get_current(), make_ref_bundles([[i] for i in range(100)])
    )

    def all_transform(bundles: List[RefBundle], ctx):
        return bundles, {}

    estimated_output_blocks = 500
    op1 = AllToAllOperator(
        all_transform,
        input_op,
        DataContext.get_current(),
        DataContext.get_current().target_max_block_size,
        estimated_output_blocks,
    )
    op2 = AllToAllOperator(
        all_transform,
        op1,
        DataContext.get_current(),
        DataContext.get_current().target_max_block_size,
    )

    while input_op.has_next():
        op1.add_input(input_op.get_next(), 0)
    op1.all_inputs_done()
    run_op_tasks_sync(op1)

    while op1.has_next():
        op2.add_input(op1.get_next(), 0)
    op2.all_inputs_done()
    run_op_tasks_sync(op2)

    # estimated output blocks for op2 should fallback to op1
    assert op2._estimated_num_output_bundles is None
    assert op2.num_outputs_total() == estimated_output_blocks


def test_input_data_buffer_does_not_free_inputs():
    # Tests https://github.com/ray-project/ray/issues/46282
    block = pd.DataFrame({"id": [0]})
    block_ref = ray.put(block)
    metadata = BlockAccessor.for_block(block).get_metadata()
    schema = BlockAccessor.for_block(block).schema()
    op = InputDataBuffer(
        DataContext.get_current(),
        input_data=[
            RefBundle([(block_ref, metadata)], owns_blocks=False, schema=schema)
        ],
    )

    op.get_next()
    gc.collect()

    # `InputDataBuffer` should still hold a reference to the input block even after
    # `get_next` is called.
    assert len(gc.get_referrers(block_ref)) > 0


@pytest.mark.parametrize(
    "blocks_data,per_block_limit,expected_output",
    [
        # Test case 1: Single block, limit less than block size
        ([[1, 2, 3, 4, 5]], 3, [[1, 2, 3]]),
        # Test case 2: Single block, limit equal to block size
        ([[1, 2, 3]], 3, [[1, 2, 3]]),
        # Test case 3: Single block, limit greater than block size
        ([[1, 2]], 5, [[1, 2]]),
        # Test case 4: Multiple blocks, limit spans across blocks
        ([[1, 2], [3, 4], [5, 6]], 3, [[1, 2], [3]]),
        # Test case 5: Multiple blocks, limit exactly at block boundary
        ([[1, 2], [3, 4]], 2, [[1, 2]]),
        # Test case 6: Empty blocks
        ([], 5, []),
        # Test case 7: Zero limit
        ([[1, 2, 3]], 0, []),
    ],
)
def test_per_block_limit_fn(blocks_data, per_block_limit, expected_output):
    """Test the _per_block_limit_fn function with various inputs."""
    import pandas as pd

    # Convert test data to pandas blocks
    blocks = [pd.DataFrame({"value": data}) for data in blocks_data]

    # Create a mock TaskContext
    ctx = TaskContext(op_name="test", task_idx=0, target_max_block_size_override=None)

    # Call the function
    result_blocks = list(_per_block_limit_fn(blocks, ctx, per_block_limit))

    # Convert result back to lists for comparison
    result_data = []
    for block in result_blocks:
        block_data = block["value"].tolist()
        result_data.append(block_data)

    assert result_data == expected_output


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
