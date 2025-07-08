import os
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray._private.test_utils import run_string_as_driver_nonblocking
from ray.data._internal.datasource.parquet_datasink import ParquetDatasink
from ray.data._internal.datasource.parquet_datasource import ParquetDatasource
from ray.data._internal.execution.backpressure_policy import BackpressurePolicy
from ray.data._internal.execution.execution_callback import (
    EXECUTION_CALLBACKS_ENV_VAR,
    ExecutionCallback,
    add_execution_callback,
    get_execution_callbacks,
    remove_execution_callback,
)
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    PhysicalOperator,
)
from ray.data._internal.execution.interfaces.physical_operator import MetadataOpTask
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.map_transformer import (
    create_map_transformer_from_block_fn,
)
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor import (
    StreamingExecutor,
    _debug_dump_topology,
    _validate_dag,
)
from ray.data._internal.execution.streaming_executor_state import (
    OpBufferQueue,
    OpState,
    _rank_operators,
    build_streaming_topology,
    get_eligible_operators,
    process_completed_tasks,
    select_operator_to_run,
    update_operator_states,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data._internal.logical.operators.map_operator import MapRows
from ray.data._internal.logical.operators.read_operator import Read
from ray.data._internal.logical.operators.write_operator import Write
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def mock_resource_manager(
    global_limits=None,
    global_usage=None,
):
    empty_resource = ExecutionResources(0, 0, 0)
    global_limits = global_limits or empty_resource
    global_usage = global_usage or empty_resource
    return MagicMock(
        get_global_limits=MagicMock(return_value=global_limits),
        get_global_usage=MagicMock(return_value=global_usage),
        op_resource_allocator_enabled=MagicMock(return_value=True),
    )


def mock_autoscaler():
    return MagicMock()


@ray.remote
def sleep():
    time.sleep(999)


def make_map_transformer(block_fn):
    def map_fn(block_iter):
        for block in block_iter:
            yield block_fn(block)

    return create_map_transformer_from_block_fn(map_fn)


def make_ref_bundle(x):
    return make_ref_bundles([[x]])[0]


@pytest.mark.parametrize(
    "verbose_progress",
    [True, False],
)
def test_build_streaming_topology(verbose_progress):
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]),
        o1,
        DataContext.get_current(),
    )
    o3 = MapOperator.create(
        make_map_transformer(lambda block: [b * 2 for b in block]),
        o2,
        DataContext.get_current(),
    )
    topo, num_progress_bars = build_streaming_topology(
        o3, ExecutionOptions(verbose_progress=verbose_progress)
    )
    assert len(topo) == 3, topo
    if verbose_progress:
        assert num_progress_bars == 3, num_progress_bars
    else:
        assert num_progress_bars == 1, num_progress_bars
    assert o1 in topo, topo
    assert not topo[o1].input_queues, topo
    assert topo[o1].output_queue == topo[o2].input_queues[0], topo
    assert topo[o2].output_queue == topo[o3].input_queues[0], topo
    assert list(topo) == [o1, o2, o3]


def test_disallow_non_unique_operators():
    inputs = make_ref_bundles([[x] for x in range(20)])
    # An operator [o1] cannot used in the same DAG twice.
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]),
        o1,
        DataContext.get_current(),
    )
    o3 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]),
        o1,
        DataContext.get_current(),
    )
    o4 = PhysicalOperator(
        "test_combine",
        [o2, o3],
        DataContext.get_current(),
        target_max_block_size=None,
    )
    with pytest.raises(ValueError):
        build_streaming_topology(o4, ExecutionOptions(verbose_progress=True))


@pytest.fixture
def sleep_task_ref():
    sleep_task_ref = sleep.remote()
    yield sleep_task_ref
    ray.cancel(sleep_task_ref, force=True)


def test_process_completed_tasks(sleep_task_ref):
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]),
        o1,
        DataContext.get_current(),
    )
    topo, _ = build_streaming_topology(o2, ExecutionOptions(verbose_progress=True))

    # Test processing output bundles.
    assert len(topo[o1].output_queue) == 0, topo
    process_completed_tasks(topo, [], 0)
    update_operator_states(topo)
    assert len(topo[o1].output_queue) == 20, topo

    # Test processing completed work items.
    sleep_task_callback = MagicMock()
    sleep_task = MetadataOpTask(0, sleep_task_ref, sleep_task_callback)
    done_task_callback = MagicMock()
    done_task = MetadataOpTask(0, ray.put("done"), done_task_callback)
    o2.get_active_tasks = MagicMock(return_value=[sleep_task, done_task])
    o2.all_inputs_done = MagicMock()
    o1.mark_execution_finished = MagicMock()
    process_completed_tasks(topo, [], 0)
    update_operator_states(topo)
    sleep_task_callback.assert_not_called()
    done_task_callback.assert_called_once()
    o2.all_inputs_done.assert_not_called()
    o1.mark_execution_finished.assert_not_called()

    # Test input finalization.
    done_task_callback = MagicMock()
    done_task = MetadataOpTask(0, ray.put("done"), done_task_callback)
    o2.get_active_tasks = MagicMock(return_value=[done_task])
    o2.all_inputs_done = MagicMock()
    o1.mark_execution_finished = MagicMock()
    o1.completed = MagicMock(return_value=True)
    topo[o1].output_queue.clear()
    process_completed_tasks(topo, [], 0)
    update_operator_states(topo)
    done_task_callback.assert_called_once()
    o2.all_inputs_done.assert_called_once()
    o1.mark_execution_finished.assert_not_called()

    # Test dependents completed.
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]),
        o1,
        DataContext.get_current(),
    )
    o3 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]),
        o2,
        DataContext.get_current(),
    )
    topo, _ = build_streaming_topology(o3, ExecutionOptions(verbose_progress=True))

    o3.mark_execution_finished()
    o2.mark_execution_finished = MagicMock()
    process_completed_tasks(topo, [], 0)
    update_operator_states(topo)
    o2.mark_execution_finished.assert_called_once()


def test_get_eligible_operators_to_run():
    opts = ExecutionOptions()
    inputs = make_ref_bundles([[x] for x in range(1)])
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: block),
        o1,
        DataContext.get_current(),
        name="O2",
    )
    o3 = MapOperator.create(
        make_map_transformer(lambda block: block),
        o2,
        DataContext.get_current(),
        name="O3",
    )
    topo, _ = build_streaming_topology(o3, opts)

    resource_manager = mock_resource_manager(
        global_limits=ExecutionResources.for_limits(1, 1, 1),
    )
    memory_usage = {o1: 0, o2: 0, o3: 0}
    resource_manager.get_op_usage = MagicMock(
        side_effect=lambda op: ExecutionResources(0, 0, memory_usage[op])
    )
    resource_manager.op_resource_allocator.can_submit_new_task = MagicMock(
        return_value=True
    )

    def _get_eligible_ops_to_run(ensure_liveness: bool):
        return get_eligible_operators(topo, [], ensure_liveness=ensure_liveness)

    # Test empty.
    assert _get_eligible_ops_to_run(ensure_liveness=False) == []

    # `o2` is the only operator with at least one input.
    topo[o1].output_queue.append(make_ref_bundle("dummy1"))
    memory_usage[o1] += 1

    assert _get_eligible_ops_to_run(ensure_liveness=False) == [o2]

    # Both `o2` and `o3` have at least one input, but `o3` has less memory usage.
    topo[o2].output_queue.append(make_ref_bundle("dummy3"))
    memory_usage[o2] += 1
    assert _get_eligible_ops_to_run(ensure_liveness=False) == [o2, o3]

    # `o2`s queue is not empty, but it can't accept new inputs anymore
    with patch.object(o2, "should_add_input") as _mock:
        _mock.return_value = False
        assert _get_eligible_ops_to_run(ensure_liveness=False) == [o3]

    # Completed ops are not eligible
    with patch.object(o3, "completed") as _mock:
        _mock.return_value = True
        assert _get_eligible_ops_to_run(ensure_liveness=False) == [o2]

    # `o2` operator is now back-pressured
    class TestBackpressurePolicy(BackpressurePolicy):
        def __init__(self, op_to_block):
            self._op_to_block = op_to_block

        def can_add_input(self, op):
            if op is self._op_to_block:
                return False
            return True

        def max_task_output_bytes_to_read(self, op):
            return None

    test_policy = TestBackpressurePolicy(o2)

    def _get_eligible_ops_to_run_with_policy(ensure_liveness: bool):
        return get_eligible_operators(
            topo, [test_policy], ensure_liveness=ensure_liveness
        )

    assert _get_eligible_ops_to_run_with_policy(ensure_liveness=False) == [o3]

    # Complete `o3`
    with patch.object(o3, "completed") as _mock:
        _mock.return_value = True
        # Clear up input queue
        topo[o3].input_queues[0].clear()

        # To ensure liveness back-pressure limits will be ignored
        assert _get_eligible_ops_to_run_with_policy(ensure_liveness=True) == [o2]


def test_rank_operators():
    inputs = make_ref_bundles([[x] for x in range(1)])

    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: block), o1, DataContext.get_current()
    )
    o3 = MapOperator.create(
        make_map_transformer(lambda block: block), o2, DataContext.get_current()
    )
    o4 = LimitOperator(1, o3, DataContext.get_current())

    resource_manager = mock_resource_manager(
        global_usage=ExecutionResources(cpu=1),
        global_limits=ExecutionResources.for_limits(cpu=1),
    )

    def _get_op_usage_mocked(op):
        if op is o1:
            return ExecutionResources(object_store_memory=1024)
        elif op is o2:
            return ExecutionResources(object_store_memory=2048)
        elif op is o3:
            return ExecutionResources(object_store_memory=4096)

        return ExecutionResources(object_store_memory=8092)

    resource_manager.get_op_usage.side_effect = _get_op_usage_mocked

    ranks = _rank_operators([o1, o2, o3, o4], resource_manager)

    assert [(True, 1024), (True, 2048), (True, 4096), (False, 8092)] == ranks


def test_select_ops_to_run():
    opts = ExecutionOptions()

    inputs = make_ref_bundles([[x] for x in range(1)])

    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: block), o1, DataContext.get_current()
    )
    o3 = MapOperator.create(
        make_map_transformer(lambda block: block), o2, DataContext.get_current()
    )
    o4 = LimitOperator(1, o3, DataContext.get_current())

    resource_manager = mock_resource_manager(
        global_usage=ExecutionResources(cpu=1),
        global_limits=ExecutionResources.for_limits(cpu=1),
    )

    def _get_op_usage_mocked(op):
        if op is o1:
            return ExecutionResources(object_store_memory=1024)
        elif op is o2:
            return ExecutionResources(object_store_memory=2048)
        elif op is o3:
            return ExecutionResources(object_store_memory=4096)

        return ExecutionResources(object_store_memory=8092)

    resource_manager.get_op_usage.side_effect = _get_op_usage_mocked

    # NOTE: This value is irrelevant since we mock out get_eligible_operators
    ensure_liveness = False

    with patch(
        "ray.data._internal.execution.streaming_executor_state.get_eligible_operators"
    ) as _mock:
        # Case 1: Should pick the `o4` since it has throttling disabled
        _mock.return_value = [o1, o2, o3, o4]

        topo, _ = build_streaming_topology(o4, opts)

        selected = select_operator_to_run(
            topo, resource_manager, [], ensure_liveness=ensure_liveness
        )

        assert selected is o4

        # Case 2: Should pick the `o1` since it has lowest object store usage
        _mock.return_value = [o1, o2, o3]

        topo, _ = build_streaming_topology(o3, opts)

        selected = select_operator_to_run(
            topo, resource_manager, [], ensure_liveness=ensure_liveness
        )

        assert selected is o1


def test_dispatch_next_task():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o1_state = OpState(o1, [])
    o2 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]),
        o1,
        DataContext.get_current(),
    )
    op_state = OpState(o2, [o1_state.output_queue])

    # TODO: test multiple inqueues with the union operator.
    ref1 = make_ref_bundle("dummy1")
    ref2 = make_ref_bundle("dummy2")
    op_state.input_queues[0].append(ref1)
    op_state.input_queues[0].append(ref2)

    o2.add_input = MagicMock()
    op_state.dispatch_next_task()
    o2.add_input.assert_called_once_with(ref1, input_index=0)

    o2.add_input = MagicMock()
    op_state.dispatch_next_task()
    o2.add_input.assert_called_once_with(ref2, input_index=0)


def test_debug_dump_topology():
    opt = ExecutionOptions()
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]),
        o1,
        DataContext.get_current(),
    )
    o3 = MapOperator.create(
        make_map_transformer(lambda block: [b * 2 for b in block]),
        o2,
        DataContext.get_current(),
    )
    topo, _ = build_streaming_topology(o3, opt)
    resource_manager = ResourceManager(
        topo,
        ExecutionOptions(),
        MagicMock(return_value=ExecutionResources.zero()),
        DataContext.get_current(),
    )
    resource_manager.update_usages()
    # Just a sanity check to ensure it doesn't crash.
    _debug_dump_topology(topo, resource_manager)


def test_validate_dag():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]),
        o1,
        DataContext.get_current(),
        compute_strategy=ray.data.ActorPoolStrategy(size=8),
    )
    o3 = MapOperator.create(
        make_map_transformer(lambda block: [b * 2 for b in block]),
        o2,
        DataContext.get_current(),
        compute_strategy=ray.data.ActorPoolStrategy(size=4),
    )
    _validate_dag(o3, ExecutionResources.for_limits())
    _validate_dag(o3, ExecutionResources.for_limits(cpu=20))
    _validate_dag(o3, ExecutionResources.for_limits(gpu=0))
    with pytest.raises(ValueError):
        _validate_dag(o3, ExecutionResources.for_limits(cpu=10))


# Mock the `scale_up` method to avoid creating and leaking resources.
@patch(
    "ray.data._internal.execution.operators.actor_pool_map_operator._ActorPool.scale",
    return_value=1,
)
def test_configure_output_locality(mock_scale_up):
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]),
        o1,
        DataContext.get_current(),
    )
    o3 = MapOperator.create(
        make_map_transformer(lambda block: [b * 2 for b in block]),
        o2,
        DataContext.get_current(),
        compute_strategy=ray.data.ActorPoolStrategy(size=1),
    )
    # No locality.
    build_streaming_topology(o3, ExecutionOptions(locality_with_output=False))
    assert o2._ray_remote_args.get("scheduling_strategy") is None
    assert o3._ray_remote_args.get("scheduling_strategy") == "SPREAD"

    # Current node locality.
    build_streaming_topology(o3, ExecutionOptions(locality_with_output=True))
    s1 = o2._get_runtime_ray_remote_args()["scheduling_strategy"]
    assert isinstance(s1, NodeAffinitySchedulingStrategy)
    assert s1.node_id == ray.get_runtime_context().get_node_id()
    s2 = o3._get_runtime_ray_remote_args()["scheduling_strategy"]
    assert isinstance(s2, NodeAffinitySchedulingStrategy)
    assert s2.node_id == ray.get_runtime_context().get_node_id()

    # Multi node locality.
    build_streaming_topology(
        o3, ExecutionOptions(locality_with_output=["node1", "node2"])
    )
    s1a = o2._get_runtime_ray_remote_args()["scheduling_strategy"]
    s1b = o2._get_runtime_ray_remote_args()["scheduling_strategy"]
    s1c = o2._get_runtime_ray_remote_args()["scheduling_strategy"]
    assert s1a.node_id == "node1"
    assert s1b.node_id == "node2"
    assert s1c.node_id == "node1"
    s2a = o3._get_runtime_ray_remote_args()["scheduling_strategy"]
    s2b = o3._get_runtime_ray_remote_args()["scheduling_strategy"]
    s2c = o3._get_runtime_ray_remote_args()["scheduling_strategy"]
    assert s2a.node_id == "node1"
    assert s2b.node_id == "node2"
    assert s2c.node_id == "node1"


class OpBufferQueueTest(unittest.TestCase):
    def test_multi_threading(self):
        num_blocks = 5_000
        num_splits = 8
        num_per_split = num_blocks // num_splits
        ref_bundles = make_ref_bundles([[[i]] for i in range(num_blocks)])

        queue = OpBufferQueue()
        for i, ref_bundle in enumerate(ref_bundles):
            ref_bundle.output_split_idx = i % num_splits
            queue.append(ref_bundle)

        def consume(output_split_idx):
            nonlocal queue

            count = 0
            while queue.has_next(output_split_idx):
                ref_bundle = queue.pop(output_split_idx)
                count += 1
                assert ref_bundle is not None
                assert ref_bundle.output_split_idx == output_split_idx
            assert count == num_per_split
            return True

        with ThreadPoolExecutor(max_workers=num_splits) as executor:
            futures = [executor.submit(consume, i) for i in range(num_splits)]

        for f in futures:
            assert f.result() is True, f.result()


def test_exception_concise_stacktrace():
    driver_script = """
import ray

def map(_):
    raise ValueError("foo")

ray.data.range(1).map(map).take_all()
    """
    proc = run_string_as_driver_nonblocking(driver_script)
    out_str = proc.stdout.read().decode("utf-8") + proc.stderr.read().decode("utf-8")
    # Test that the stack trace only contains the UDF exception, but not any other
    # exceptions raised when the executor is handling the UDF exception.
    assert (
        "During handling of the above exception, another exception occurred"
        not in out_str
    ), out_str


def test_streaming_exec_schedule_s():
    ds = ray.data.range(1)
    for _ in ds.iter_batches():
        continue

    ds_stats = ds._plan.stats()
    assert 0 < ds_stats.streaming_exec_schedule_s.get() < 1


def test_execution_callbacks():
    """Test ExecutionCallback."""

    class CustomExecutionCallback(ExecutionCallback):
        def __init__(self):
            self._before_execution_starts_called = False
            self._after_execution_succeeds_called = False
            self._execution_error = None
            self._on_execution_step_called = False

        def before_execution_starts(self, executor: StreamingExecutor):
            self._before_execution_starts_called = True

        def on_execution_step(self, executor: "StreamingExecutor"):
            self._on_execution_step_called = True

        def after_execution_succeeds(self, executor: StreamingExecutor):
            self._after_execution_succeeds_called = True

        def after_execution_fails(self, executor: StreamingExecutor, error: Exception):
            self._execution_error = error

    # Test the success case.
    ds = ray.data.range(10)
    ctx = ds.context
    callback = CustomExecutionCallback()
    add_execution_callback(callback, ctx)
    assert callback in get_execution_callbacks(ctx)

    ds.take_all()

    assert callback._before_execution_starts_called
    assert callback._after_execution_succeeds_called
    assert callback._on_execution_step_called
    assert callback._execution_error is None

    remove_execution_callback(callback, ctx)
    assert callback not in get_execution_callbacks(ctx)

    # Test the case where the dataset fails due to an error in the UDF.
    ds = ray.data.range(10)
    ctx = ds.context
    ctx.raise_original_map_exception = True
    callback = CustomExecutionCallback()
    add_execution_callback(callback, ctx)

    def map_fn(_):
        raise ValueError("")

    with pytest.raises(ValueError):
        ds.map(map_fn).take_all()

    assert callback._before_execution_starts_called
    assert not callback._after_execution_succeeds_called
    assert callback._on_execution_step_called
    error = callback._execution_error
    assert isinstance(error, ValueError), error

    # Test the case the dataset is canceled by "ctrl-c".
    ds = ray.data.range(10)
    ctx = ds.context
    callback = CustomExecutionCallback()
    add_execution_callback(callback, ctx)

    def patched_get_outupt_blocking(*args, **kwargs):
        raise KeyboardInterrupt()

    with patch(
        "ray.data._internal.execution.streaming_executor.OpState.get_output_blocking",
        new=patched_get_outupt_blocking,
    ):
        with pytest.raises(KeyboardInterrupt):
            ds.take_all()

    assert callback._before_execution_starts_called
    assert not callback._after_execution_succeeds_called
    assert callback._on_execution_step_called
    error = callback._execution_error
    assert isinstance(error, KeyboardInterrupt), error


@patch("importlib.import_module")
@patch.dict(os.environ, {EXECUTION_CALLBACKS_ENV_VAR: "my.module.TestCallback"})
def test_env_callbacks_loaded(mock_import):
    """Test loading execution callbacks from environment variable."""
    # Setup mock for importing the module
    mock_module = MagicMock()
    mock_callback_cls = MagicMock()
    mock_callback = MagicMock()
    mock_callback_cls.return_value = mock_callback
    mock_module.TestCallback = mock_callback_cls
    mock_import.return_value = mock_module

    # Get callbacks should initialize from env
    ctx = DataContext.get_current()
    callbacks = get_execution_callbacks(ctx)

    # Verify the callback was imported and initialized
    mock_import.assert_called_once_with("my.module")
    mock_callback_cls.assert_called_once()

    assert len([c for c in callbacks if c is mock_callback]) == 1


@patch("importlib.import_module")
@patch.dict(
    os.environ, {EXECUTION_CALLBACKS_ENV_VAR: "module1.Callback1,module2.Callback2"}
)
def test_multiple_env_callbacks(mock_import):
    """Test loading multiple callbacks from environment variable."""
    # Setup mock for importing multiple modules
    mock_module1 = MagicMock()
    mock_module2 = MagicMock()
    mock_callback1 = MagicMock()
    mock_callback2 = MagicMock()
    mock_module1.Callback1.return_value = mock_callback1
    mock_module2.Callback2.return_value = mock_callback2

    # Return different mock modules depending on the import path
    def side_effect(name):
        if name == "module1":
            return mock_module1
        elif name == "module2":
            return mock_module2
        raise ImportError(f"No module named '{name}'")

    mock_import.side_effect = side_effect

    # Get callbacks should initialize from env
    ctx = DataContext.get_current()
    callbacks = get_execution_callbacks(ctx)

    # Verify both callbacks were imported and initialized
    assert len([c for c in callbacks if c is mock_callback1]) == 1
    assert len([c for c in callbacks if c is mock_callback2]) == 1


@patch.dict(os.environ, {EXECUTION_CALLBACKS_ENV_VAR: "invalid_module"})
def test_invalid_callback_path():
    """Test handling of invalid callback paths in environment variable."""
    # Should raise ValueError due to missing class name
    with pytest.raises(ValueError):
        get_execution_callbacks(DataContext.get_current())


@patch("importlib.import_module")
@patch.dict(
    os.environ, {EXECUTION_CALLBACKS_ENV_VAR: "nonexistent.module.TestCallback"}
)
def test_import_error_handling(mock_import):
    """Test handling of import errors when loading callbacks."""
    # Make import fail
    mock_import.side_effect = ImportError("No module named 'nonexistent'")

    # Should re-raise as ValueError with context
    with pytest.raises(ValueError):
        get_execution_callbacks(DataContext.get_current())


def test_callbacks_initialized_once():
    """Test that environment callbacks are only initialized once per context."""
    with patch(
        "ray.data._internal.execution.execution_callback._initialize_env_callbacks"
    ) as mock_init:
        # First call should initialize
        ctx = DataContext.get_current()
        get_execution_callbacks(ctx)
        mock_init.assert_called_once_with(ctx)

        # Reset the mock to check if called again
        mock_init.reset_mock()

        # Second call should not initialize again
        get_execution_callbacks(ctx)
        mock_init.assert_not_called()


def test_execution_callbacks_executor_arg(tmp_path, restore_data_context):
    """Test the executor arg in ExecutionCallback."""

    _executor = None

    class CustomExecutionCallback(ExecutionCallback):
        def after_execution_succeeds(self, executor: StreamingExecutor):
            nonlocal _executor
            _executor = executor

    input_path = tmp_path / "input"
    os.makedirs(input_path)
    output_path = tmp_path / "output"

    ctx = DataContext.get_current()
    callback = CustomExecutionCallback()
    add_execution_callback(callback, ctx)
    ds = ray.data.read_parquet(input_path)

    def udf(row):
        return row

    ds = ds.map(udf)

    ds = ds.write_parquet(output_path)

    # Test inspecting the metadata of each operator.
    # E.g., the original input and output paths and the UDF.
    assert _executor is not None
    assert len(_executor._topology) == 2
    physical_ops = list(_executor._topology.keys())
    assert isinstance(physical_ops[0], InputDataBuffer)
    assert isinstance(physical_ops[1], MapOperator)
    logical_ops = physical_ops[1]._logical_operators

    assert len(logical_ops) == 3
    assert isinstance(logical_ops[0], Read)
    datasource = logical_ops[0]._datasource
    assert isinstance(datasource, ParquetDatasource)
    assert datasource._unresolved_paths == input_path

    assert isinstance(logical_ops[1], MapRows)
    assert logical_ops[1]._fn == udf

    assert isinstance(logical_ops[2], Write)
    datasink = logical_ops[2]._datasink_or_legacy_datasource
    assert isinstance(datasink, ParquetDatasink)
    assert datasink.unresolved_path == output_path


def test_create_topology_metadata():
    """Test that create_topology_metadata correctly serializes the DAG structure."""
    from ray.data._internal.metadata_exporter import Topology as TopologyMetadata

    # Create a simple DAG with a few connected operators
    inputs = make_ref_bundles([[x] for x in range(10)])
    o1 = InputDataBuffer(DataContext.get_current(), inputs)
    o2 = MapOperator.create(
        make_map_transformer(lambda block: [b * -1 for b in block]),
        o1,
        DataContext.get_current(),
    )
    o3 = MapOperator.create(
        make_map_transformer(lambda block: [b * 2 for b in block]),
        o2,
        DataContext.get_current(),
    )

    # Create a StreamingExecutor instance
    executor = StreamingExecutor(DataContext.get_current())

    # Initialize the topology on the executor
    executor._topology, _ = build_streaming_topology(o3, ExecutionOptions())

    # Call the _dump_dag_structure method
    op_to_id = {
        op: executor._get_operator_id(op, i)
        for i, op in enumerate(executor._topology.keys())
    }
    topology_metadata = TopologyMetadata.create_topology_metadata(o3, op_to_id)

    # Verify the structure of the returned dictionary
    assert len(topology_metadata.operators) == 3  # We should have 3 operators

    # Find each operator by name - the operators are simplified in the representation
    operators_by_name = {op.name: op for op in topology_metadata.operators}

    # Check input data buffer (appears as "Input" in the structure)
    assert "Input" in operators_by_name
    input_buffer = operators_by_name["Input"]
    assert input_buffer.id is not None
    assert input_buffer.uuid is not None
    assert input_buffer.input_dependencies == []

    # Check map operators (appear as "Map" in the structure)
    assert "Map" in operators_by_name

    # Since there are two Map operators with the same name, we need to identify them by their ID
    map_ops = [op for op in topology_metadata.operators if op.name == "Map"]
    assert len(map_ops) == 2

    # Sort by ID to get them in order
    map_ops.sort(key=lambda op: op.id)
    map_op1, map_op2 = map_ops

    # First map operator should depend on the input buffer
    assert len(map_op1.input_dependencies) == 1
    assert map_op1.input_dependencies[0] == input_buffer.id

    # Second map operator should depend on the first map operator
    assert len(map_op2.input_dependencies) == 1
    assert map_op2.input_dependencies[0] == map_op1.id


def test_create_topology_metadata_with_sub_stages():
    """Test that _dump_dag_structure correctly handles sub-stages."""
    from ray.data._internal.metadata_exporter import Topology as TopologyMetadata

    inputs = make_ref_bundles([[x] for x in range(5)])

    # Create a base operator
    o1 = InputDataBuffer(DataContext.get_current(), inputs)

    # Create an operator with sub-stages
    o2 = MapOperator.create(
        make_map_transformer(lambda block: [b * 2 for b in block]),
        o1,
        DataContext.get_current(),
    )

    # Add fake sub-stages to test the sub-stages feature
    o2._sub_progress_bar_names = ["SubStage1", "SubStage2"]

    # Create the executor and set up topology
    executor = StreamingExecutor(DataContext.get_current())
    executor._topology, _ = build_streaming_topology(o2, ExecutionOptions())

    # Get the DAG structure
    op_to_id = {
        op: executor._get_operator_id(op, i)
        for i, op in enumerate(executor._topology.keys())
    }
    topology_metadata = TopologyMetadata.create_topology_metadata(o2, op_to_id)

    # Find the operator with sub-stages (appears as "Map" in the structure)
    map_op = None
    for op in topology_metadata.operators:
        if op.name == "Map":
            map_op = op
            break

    assert map_op is not None
    assert len(map_op.sub_stages) == 2

    # Check that sub-stages have the expected structure
    sub_stage1, sub_stage2 = map_op.sub_stages
    assert sub_stage1.name == "SubStage1"
    assert sub_stage1.id.endswith("_sub_0")
    assert sub_stage2.name == "SubStage2"
    assert sub_stage2.id.endswith("_sub_1")


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
