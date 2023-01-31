import pytest
import time
from unittest.mock import MagicMock

import ray
from ray.data._internal.execution.interfaces import (
    ExecutionOptions,
    ExecutionResources,
    PhysicalOperator,
)
from ray.data._internal.execution.streaming_executor import (
    _debug_dump_topology,
    _validate_topology,
)
from ray.data._internal.execution.streaming_executor_state import (
    OpState,
    build_streaming_topology,
    process_completed_tasks,
    select_operator_to_run,
    _execution_allowed,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import make_ref_bundles
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


@ray.remote
def sleep():
    time.sleep(999)


def make_transform(block_fn):
    def map_fn(block_iter):
        for block in block_iter:
            yield block_fn(block)

    return map_fn


def test_build_streaming_topology():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator.create(make_transform(lambda block: [b * 2 for b in block]), o2)
    topo, _ = build_streaming_topology(o3, ExecutionOptions())
    assert len(topo) == 3, topo
    assert o1 in topo, topo
    assert not topo[o1].inqueues, topo
    assert topo[o1].outqueue == topo[o2].inqueues[0], topo
    assert topo[o2].outqueue == topo[o3].inqueues[0], topo
    assert list(topo) == [o1, o2, o3]


def test_disallow_non_unique_operators():
    inputs = make_ref_bundles([[x] for x in range(20)])
    # An operator [o1] cannot used in the same DAG twice.
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator.create(make_transform(lambda block: [b * -1 for b in block]), o1)
    o4 = PhysicalOperator("test_combine", [o2, o3])
    with pytest.raises(ValueError):
        build_streaming_topology(o4, ExecutionOptions())


def test_process_completed_tasks():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(make_transform(lambda block: [b * -1 for b in block]), o1)
    topo, _ = build_streaming_topology(o2, ExecutionOptions())

    # Test processing output bundles.
    assert len(topo[o1].outqueue) == 0, topo
    process_completed_tasks(topo)
    assert len(topo[o1].outqueue) == 20, topo

    # Test processing completed work items.
    sleep_ref = sleep.remote()
    done_ref = ray.put("done")
    o2.get_work_refs = MagicMock(return_value=[sleep_ref, done_ref])
    o2.notify_work_completed = MagicMock()
    o2.inputs_done = MagicMock()
    process_completed_tasks(topo)
    o2.notify_work_completed.assert_called_once_with(done_ref)
    o2.inputs_done.assert_not_called()

    # Test input finalization.
    o2.get_work_refs = MagicMock(return_value=[done_ref])
    o2.notify_work_completed = MagicMock()
    o2.inputs_done = MagicMock()
    o1.completed = MagicMock(return_value=True)
    topo[o1].outqueue.clear()
    process_completed_tasks(topo)
    o2.notify_work_completed.assert_called_once_with(done_ref)
    o2.inputs_done.assert_called_once()


def test_select_operator_to_run():
    opt = ExecutionOptions()
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator.create(make_transform(lambda block: [b * 2 for b in block]), o2)
    topo, _ = build_streaming_topology(o3, opt)

    # Test empty.
    assert (
        select_operator_to_run(topo, ExecutionResources(), ExecutionResources(), True)
        is None
    )

    # Test backpressure based on queue length between operators.
    topo[o1].outqueue.append("dummy1")
    assert (
        select_operator_to_run(topo, ExecutionResources(), ExecutionResources(), True)
        == o2
    )
    topo[o1].outqueue.append("dummy2")
    assert (
        select_operator_to_run(topo, ExecutionResources(), ExecutionResources(), True)
        == o2
    )
    topo[o2].outqueue.append("dummy3")
    assert (
        select_operator_to_run(topo, ExecutionResources(), ExecutionResources(), True)
        == o3
    )

    # Test backpressure includes num active tasks as well.
    o3.num_active_work_refs = MagicMock(return_value=2)
    o3.internal_queue_size = MagicMock(return_value=0)
    assert (
        select_operator_to_run(topo, ExecutionResources(), ExecutionResources(), True)
        == o2
    )
    # nternal queue size is added to num active tasks.
    o3.num_active_work_refs = MagicMock(return_value=0)
    o3.internal_queue_size = MagicMock(return_value=2)
    assert (
        select_operator_to_run(topo, ExecutionResources(), ExecutionResources(), True)
        == o2
    )
    o2.num_active_work_refs = MagicMock(return_value=2)
    o2.internal_queue_size = MagicMock(return_value=0)
    assert (
        select_operator_to_run(topo, ExecutionResources(), ExecutionResources(), True)
        == o3
    )
    o2.num_active_work_refs = MagicMock(return_value=0)
    o2.internal_queue_size = MagicMock(return_value=2)
    assert (
        select_operator_to_run(topo, ExecutionResources(), ExecutionResources(), True)
        == o3
    )


def test_dispatch_next_task():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o1_state = OpState(o1, [])
    o2 = MapOperator.create(make_transform(lambda block: [b * -1 for b in block]), o1)
    op_state = OpState(o2, [o1_state.outqueue])

    # TODO: test multiple inqueues with the union operator.
    op_state.inqueues[0].append("dummy1")
    op_state.inqueues[0].append("dummy2")

    o2.add_input = MagicMock()
    op_state.dispatch_next_task()
    assert o2.add_input.called_once_with("dummy1")

    o2.add_input = MagicMock()
    op_state.dispatch_next_task()
    assert o2.add_input.called_once_with("dummy2")


def test_debug_dump_topology():
    opt = ExecutionOptions()
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator.create(make_transform(lambda block: [b * 2 for b in block]), o2)
    topo, _ = build_streaming_topology(o3, opt)
    # Just a sanity check to ensure it doesn't crash.
    _debug_dump_topology(topo)


def test_validate_topology():
    opt = ExecutionOptions()
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(
        make_transform(lambda block: [b * -1 for b in block]),
        o1,
        compute_strategy=ray.data.ActorPoolStrategy(8, 8),
    )
    o3 = MapOperator.create(
        make_transform(lambda block: [b * 2 for b in block]),
        o2,
        compute_strategy=ray.data.ActorPoolStrategy(4, 4),
    )
    topo, _ = build_streaming_topology(o3, opt)
    _validate_topology(topo, ExecutionResources())
    _validate_topology(topo, ExecutionResources(cpu=20))
    _validate_topology(topo, ExecutionResources(gpu=0))
    with pytest.raises(ValueError):
        _validate_topology(topo, ExecutionResources(cpu=10))


def test_execution_allowed():
    op = InputDataBuffer([])

    # CPU.
    op.incremental_resource_usage = MagicMock(return_value=ExecutionResources(cpu=1))
    assert _execution_allowed(op, ExecutionResources(cpu=1), ExecutionResources(cpu=2))
    assert not _execution_allowed(
        op, ExecutionResources(cpu=2), ExecutionResources(cpu=2)
    )
    assert _execution_allowed(op, ExecutionResources(cpu=2), ExecutionResources(gpu=2))

    # GPU.
    op.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=1, gpu=1)
    )
    assert _execution_allowed(op, ExecutionResources(gpu=1), ExecutionResources(gpu=2))
    assert not _execution_allowed(
        op, ExecutionResources(gpu=2), ExecutionResources(gpu=2)
    )

    # Test conversion to indicator (0/1).
    op.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=100, gpu=100)
    )
    assert _execution_allowed(op, ExecutionResources(gpu=1), ExecutionResources(gpu=2))
    assert _execution_allowed(
        op, ExecutionResources(gpu=1.5), ExecutionResources(gpu=2)
    )
    assert not _execution_allowed(
        op, ExecutionResources(gpu=2), ExecutionResources(gpu=2)
    )

    # Test conversion to indicator (0/1).
    op.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=0.1, gpu=0.1)
    )
    assert _execution_allowed(op, ExecutionResources(gpu=1), ExecutionResources(gpu=2))
    assert _execution_allowed(
        op, ExecutionResources(gpu=1.5), ExecutionResources(gpu=2)
    )
    assert not _execution_allowed(
        op, ExecutionResources(gpu=2), ExecutionResources(gpu=2)
    )


def test_select_ops_ensure_at_least_one_live_operator():
    opt = ExecutionOptions()
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(
        make_transform(lambda block: [b * -1 for b in block]),
        o1,
    )
    o3 = MapOperator.create(
        make_transform(lambda block: [b * 2 for b in block]),
        o2,
    )
    topo, _ = build_streaming_topology(o3, opt)
    topo[o2].outqueue.append("dummy1")
    o1.num_active_work_refs = MagicMock(return_value=2)
    assert (
        select_operator_to_run(
            topo, ExecutionResources(cpu=1), ExecutionResources(cpu=1), True
        )
        is None
    )
    o1.num_active_work_refs = MagicMock(return_value=0)
    assert (
        select_operator_to_run(
            topo, ExecutionResources(cpu=1), ExecutionResources(cpu=1), True
        )
        is o3
    )
    assert (
        select_operator_to_run(
            topo, ExecutionResources(cpu=1), ExecutionResources(cpu=1), False
        )
        is None
    )


def test_configure_output_locality():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator.create(
        make_transform(lambda block: [b * 2 for b in block]),
        o2,
        compute_strategy=ray.data.ActorPoolStrategy(1, 1),
    )
    topo, _ = build_streaming_topology(o3, ExecutionOptions(locality_with_output=False))
    assert o2._ray_remote_args.get("scheduling_strategy") is None
    assert o3._ray_remote_args.get("scheduling_strategy") == "SPREAD"
    topo, _ = build_streaming_topology(o3, ExecutionOptions(locality_with_output=True))
    assert isinstance(
        o2._ray_remote_args["scheduling_strategy"], NodeAffinitySchedulingStrategy
    )
    assert isinstance(
        o3._ray_remote_args["scheduling_strategy"],
        NodeAffinitySchedulingStrategy,
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
