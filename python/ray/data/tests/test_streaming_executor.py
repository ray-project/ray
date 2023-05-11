import collections
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
    _validate_dag,
)
from ray.data._internal.execution.streaming_executor_state import (
    AutoscalingState,
    OpState,
    TopologyResourceUsage,
    DownstreamMemoryInfo,
    build_streaming_topology,
    process_completed_tasks,
    select_operator_to_run,
    _execution_allowed,
)
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.util import make_ref_bundles
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
from ray.data.tests.conftest import *  # noqa


EMPTY_DOWNSTREAM_USAGE = collections.defaultdict(lambda: DownstreamMemoryInfo(0, 0))
NO_USAGE = TopologyResourceUsage(ExecutionResources(), EMPTY_DOWNSTREAM_USAGE)


@ray.remote
def sleep():
    time.sleep(999)


def make_transform(block_fn):
    def map_fn(block_iter):
        for block in block_iter:
            yield block_fn(block)

    return map_fn


@pytest.mark.parametrize(
    "verbose_progress",
    [True, False],
)
def test_build_streaming_topology(verbose_progress):
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator.create(make_transform(lambda block: [b * 2 for b in block]), o2)
    topo, num_progress_bars = build_streaming_topology(
        o3, ExecutionOptions(verbose_progress=verbose_progress)
    )
    assert len(topo) == 3, topo
    if verbose_progress:
        assert num_progress_bars == 3, num_progress_bars
    else:
        assert num_progress_bars == 1, num_progress_bars
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
        build_streaming_topology(o4, ExecutionOptions(verbose_progress=True))


def test_process_completed_tasks():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(make_transform(lambda block: [b * -1 for b in block]), o1)
    topo, _ = build_streaming_topology(o2, ExecutionOptions(verbose_progress=True))

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
    o1.all_dependents_complete = MagicMock()
    process_completed_tasks(topo)
    o2.notify_work_completed.assert_called_once_with(done_ref)
    o2.inputs_done.assert_not_called()
    o1.all_dependents_complete.assert_not_called()

    # Test input finalization.
    o2.get_work_refs = MagicMock(return_value=[done_ref])
    o2.notify_work_completed = MagicMock()
    o2.inputs_done = MagicMock()
    o1.all_dependents_complete = MagicMock()
    o1.completed = MagicMock(return_value=True)
    topo[o1].outqueue.clear()
    process_completed_tasks(topo)
    o2.notify_work_completed.assert_called_once_with(done_ref)
    o2.inputs_done.assert_called_once()
    o1.all_dependents_complete.assert_not_called()

    # Test dependents completed.
    o2.need_more_inputs = MagicMock(return_value=False)
    o1.all_dependents_complete = MagicMock()
    process_completed_tasks(topo)
    o1.all_dependents_complete.assert_called_once()


def test_select_operator_to_run():
    opt = ExecutionOptions()
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator.create(make_transform(lambda block: [b * 2 for b in block]), o2)
    topo, _ = build_streaming_topology(o3, opt)

    # Test empty.
    assert (
        select_operator_to_run(
            topo, NO_USAGE, ExecutionResources(), True, "dummy", AutoscalingState()
        )
        is None
    )

    # Test backpressure based on queue length between operators.
    topo[o1].outqueue.append("dummy1")
    assert (
        select_operator_to_run(
            topo, NO_USAGE, ExecutionResources(), True, "dummy", AutoscalingState()
        )
        == o2
    )
    topo[o1].outqueue.append("dummy2")
    assert (
        select_operator_to_run(
            topo, NO_USAGE, ExecutionResources(), True, "dummy", AutoscalingState()
        )
        == o2
    )
    topo[o2].outqueue.append("dummy3")
    assert (
        select_operator_to_run(
            topo, NO_USAGE, ExecutionResources(), True, "dummy", AutoscalingState()
        )
        == o3
    )

    # Test backpressure includes num active tasks as well.
    o3.num_active_work_refs = MagicMock(return_value=2)
    o3.internal_queue_size = MagicMock(return_value=0)
    assert (
        select_operator_to_run(
            topo, NO_USAGE, ExecutionResources(), True, "dummy", AutoscalingState()
        )
        == o2
    )
    # Internal queue size is added to num active tasks.
    o3.num_active_work_refs = MagicMock(return_value=0)
    o3.internal_queue_size = MagicMock(return_value=2)
    assert (
        select_operator_to_run(
            topo, NO_USAGE, ExecutionResources(), True, "dummy", AutoscalingState()
        )
        == o2
    )
    o2.num_active_work_refs = MagicMock(return_value=2)
    o2.internal_queue_size = MagicMock(return_value=0)
    assert (
        select_operator_to_run(
            topo, NO_USAGE, ExecutionResources(), True, "dummy", AutoscalingState()
        )
        == o3
    )
    o2.num_active_work_refs = MagicMock(return_value=0)
    o2.internal_queue_size = MagicMock(return_value=2)
    assert (
        select_operator_to_run(
            topo, NO_USAGE, ExecutionResources(), True, "dummy", AutoscalingState()
        )
        == o3
    )

    # Test prioritization of nothrottle ops.
    o2.throttling_disabled = MagicMock(return_value=True)
    assert (
        select_operator_to_run(
            topo, NO_USAGE, ExecutionResources(), True, "dummy", AutoscalingState()
        )
        == o2
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


def test_validate_dag():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(
        make_transform(lambda block: [b * -1 for b in block]),
        o1,
        compute_strategy=ray.data.ActorPoolStrategy(size=8),
    )
    o3 = MapOperator.create(
        make_transform(lambda block: [b * 2 for b in block]),
        o2,
        compute_strategy=ray.data.ActorPoolStrategy(size=4),
    )
    _validate_dag(o3, ExecutionResources())
    _validate_dag(o3, ExecutionResources(cpu=20))
    _validate_dag(o3, ExecutionResources(gpu=0))
    with pytest.raises(ValueError):
        _validate_dag(o3, ExecutionResources(cpu=10))


def test_execution_allowed():
    op = InputDataBuffer([])

    def stub(res: ExecutionResources) -> TopologyResourceUsage:
        return TopologyResourceUsage(res, EMPTY_DOWNSTREAM_USAGE)

    # CPU.
    op.incremental_resource_usage = MagicMock(return_value=ExecutionResources(cpu=1))
    assert _execution_allowed(
        op, stub(ExecutionResources(cpu=1)), ExecutionResources(cpu=2)
    )
    assert not _execution_allowed(
        op, stub(ExecutionResources(cpu=2)), ExecutionResources(cpu=2)
    )
    assert _execution_allowed(
        op, stub(ExecutionResources(cpu=2)), ExecutionResources(gpu=2)
    )

    # GPU.
    op.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=1)
    )
    assert _execution_allowed(
        op, stub(ExecutionResources(gpu=1)), ExecutionResources(gpu=2)
    )
    assert not _execution_allowed(
        op, stub(ExecutionResources(gpu=2)), ExecutionResources(gpu=2)
    )

    # Test conversion to indicator (0/1).
    op.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=100)
    )
    assert _execution_allowed(
        op, stub(ExecutionResources(gpu=1)), ExecutionResources(gpu=2)
    )
    assert _execution_allowed(
        op, stub(ExecutionResources(gpu=1.5)), ExecutionResources(gpu=2)
    )
    assert not _execution_allowed(
        op, stub(ExecutionResources(gpu=2)), ExecutionResources(gpu=2)
    )

    # Test conversion to indicator (0/1).
    op.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0.1)
    )
    assert _execution_allowed(
        op, stub(ExecutionResources(gpu=1)), ExecutionResources(gpu=2)
    )
    assert _execution_allowed(
        op, stub(ExecutionResources(gpu=1.5)), ExecutionResources(gpu=2)
    )
    assert not _execution_allowed(
        op, stub(ExecutionResources(gpu=2)), ExecutionResources(gpu=2)
    )


def test_resource_constrained_triggers_autoscaling():
    from ray.data._internal.execution.autoscaling_requester import (
        get_or_create_autoscaling_requester_actor,
    )

    ray.shutdown()
    ray.init(num_cpus=3, num_gpus=1)

    def run_execution(
        execution_id: str, incremental_cpu: int = 1, autoscaling_state=None
    ):
        if autoscaling_state is None:
            autoscaling_state = AutoscalingState()
        opt = ExecutionOptions()
        inputs = make_ref_bundles([[x] for x in range(20)])
        o1 = InputDataBuffer(inputs)
        o2 = MapOperator.create(
            make_transform(lambda block: [b * -1 for b in block]),
            o1,
        )
        o2.num_active_work_refs = MagicMock(return_value=1)
        o3 = MapOperator.create(
            make_transform(lambda block: [b * 2 for b in block]),
            o2,
        )
        o3.num_active_work_refs = MagicMock(return_value=1)
        o4 = MapOperator.create(
            make_transform(lambda block: [b * 3 for b in block]),
            o3,
            compute_strategy=ray.data.ActorPoolStrategy(min_size=1, max_size=2),
            ray_remote_args={"num_gpus": incremental_cpu},
        )
        o4.num_active_work_refs = MagicMock(return_value=1)
        o4.incremental_resource_usage = MagicMock(
            return_value=ExecutionResources(gpu=1)
        )
        topo = build_streaming_topology(o4, opt)[0]
        # Make sure only two operator's inqueues has data.
        topo[o2].inqueues[0].append("dummy")
        topo[o4].inqueues[0].append("dummy")
        selected_op = select_operator_to_run(
            topo,
            TopologyResourceUsage(
                ExecutionResources(cpu=2, gpu=1, object_store_memory=1000),
                EMPTY_DOWNSTREAM_USAGE,
            ),
            ExecutionResources(cpu=2, gpu=1, object_store_memory=1000),
            True,
            execution_id,
            autoscaling_state,
        )
        assert selected_op is None
        for op in topo:
            op.shutdown()

    test_timeout = 3
    ac = get_or_create_autoscaling_requester_actor()
    ray.get(ac._test_set_timeout.remote(test_timeout))

    run_execution("1")
    assert ray.get(ac._aggregate_requests.remote()) == [
        {"CPU": 1},
        {"CPU": 1},
        {"CPU": 1},
        {"GPU": 1},
        {"GPU": 1},
        {"CPU": 1},
    ]

    # For the same execution_id, the later request overrides the previous one.
    run_execution("1")
    assert ray.get(ac._aggregate_requests.remote()) == [
        {"CPU": 1},
        {"CPU": 1},
        {"CPU": 1},
        {"GPU": 1},
        {"GPU": 1},
        {"CPU": 1},
    ]

    # Having another execution, so the resource bundles expanded.
    run_execution("2")
    assert ray.get(ac._aggregate_requests.remote()) == [
        {"CPU": 1},
        {"CPU": 1},
        {"CPU": 1},
        {"GPU": 1},
        {"GPU": 1},
        {"CPU": 1},
        {"CPU": 1},
        {"CPU": 1},
        {"GPU": 1},
        {"GPU": 1},
    ]

    # Requesting for existing execution again, so no change in resource bundles.
    run_execution("1")
    assert ray.get(ac._aggregate_requests.remote()) == [
        {"CPU": 1},
        {"CPU": 1},
        {"CPU": 1},
        {"GPU": 1},
        {"GPU": 1},
        {"CPU": 1},
        {"CPU": 1},
        {"CPU": 1},
        {"GPU": 1},
        {"GPU": 1},
    ]

    # After the timeout, all requests should have been purged.
    time.sleep(test_timeout + 1)
    ray.get(ac._purge.remote())
    assert ray.get(ac._aggregate_requests.remote()) == []

    # Test throttling by sending 100 requests: only one request actually
    # got sent to the actor.
    autoscaling_state = AutoscalingState()
    for i in range(5):
        run_execution("1", 1, autoscaling_state)
    assert ray.get(ac._aggregate_requests.remote()) == [
        {"CPU": 1},
        {"CPU": 1},
        {"CPU": 1},
        {"GPU": 1},
        {"GPU": 1},
        {"CPU": 1},
    ]


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
            topo,
            TopologyResourceUsage(ExecutionResources(cpu=1), EMPTY_DOWNSTREAM_USAGE),
            ExecutionResources(cpu=1),
            True,
            "dummy",
            AutoscalingState(),
        )
        is None
    )
    o1.num_active_work_refs = MagicMock(return_value=0)
    assert (
        select_operator_to_run(
            topo,
            TopologyResourceUsage(ExecutionResources(cpu=1), EMPTY_DOWNSTREAM_USAGE),
            ExecutionResources(cpu=1),
            True,
            "dummy",
            AutoscalingState(),
        )
        is o3
    )
    assert (
        select_operator_to_run(
            topo,
            TopologyResourceUsage(ExecutionResources(cpu=1), EMPTY_DOWNSTREAM_USAGE),
            ExecutionResources(cpu=1),
            False,
            "dummy",
            AutoscalingState(),
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


def test_calculate_topology_usage():
    inputs = make_ref_bundles([[x] for x in range(20)])
    o1 = InputDataBuffer(inputs)
    o2 = MapOperator.create(make_transform(lambda block: [b * -1 for b in block]), o1)
    o3 = MapOperator.create(make_transform(lambda block: [b * 2 for b in block]), o2)
    o2.current_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=5, object_store_memory=500)
    )
    o3.current_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=10, object_store_memory=1000)
    )
    topo, _ = build_streaming_topology(o3, ExecutionOptions())
    inputs[0].size_bytes = MagicMock(return_value=200)
    topo[o2].outqueue = [inputs[0]]
    usage = TopologyResourceUsage.of(topo)
    assert len(usage.downstream_memory_usage) == 3, usage
    assert usage.overall == ExecutionResources(15, 0, 1700)
    assert usage.downstream_memory_usage[o1].object_store_memory == 1700, usage
    assert usage.downstream_memory_usage[o1].topology_fraction == 1, usage
    assert usage.downstream_memory_usage[o2].object_store_memory == 1700, usage
    assert usage.downstream_memory_usage[o2].topology_fraction == 1, usage
    assert usage.downstream_memory_usage[o3].object_store_memory == 1000, usage
    assert usage.downstream_memory_usage[o3].topology_fraction == 0.5, usage


def test_execution_allowed_downstream_aware_memory_throttling():
    op = InputDataBuffer([])
    op.incremental_resource_usage = MagicMock(return_value=ExecutionResources())
    # Below global.
    assert _execution_allowed(
        op,
        TopologyResourceUsage(
            ExecutionResources(object_store_memory=1000),
            {op: DownstreamMemoryInfo(1, 1000)},
        ),
        ExecutionResources(object_store_memory=1100),
    )
    # Above global.
    assert not _execution_allowed(
        op,
        TopologyResourceUsage(
            ExecutionResources(object_store_memory=1000),
            {op: DownstreamMemoryInfo(1, 1000)},
        ),
        ExecutionResources(object_store_memory=900),
    )
    # Above global, but below downstream quota of 50%.
    assert _execution_allowed(
        op,
        TopologyResourceUsage(
            ExecutionResources(object_store_memory=1000),
            {op: DownstreamMemoryInfo(0.5, 400)},
        ),
        ExecutionResources(object_store_memory=900),
    )
    # Above global, and above downstream quota of 50%.
    assert not _execution_allowed(
        op,
        TopologyResourceUsage(
            ExecutionResources(object_store_memory=1000),
            {op: DownstreamMemoryInfo(0.5, 600)},
        ),
        ExecutionResources(object_store_memory=900),
    )


def test_execution_allowed_nothrottle():
    op = InputDataBuffer([])
    op.incremental_resource_usage = MagicMock(return_value=ExecutionResources())
    # Above global.
    assert not _execution_allowed(
        op,
        TopologyResourceUsage(
            ExecutionResources(object_store_memory=1000),
            {op: DownstreamMemoryInfo(1, 1000)},
        ),
        ExecutionResources(object_store_memory=900),
    )

    # Throttling disabled.
    op.throttling_disabled = MagicMock(return_value=True)
    assert _execution_allowed(
        op,
        TopologyResourceUsage(
            ExecutionResources(object_store_memory=1000),
            {op: DownstreamMemoryInfo(1, 1000)},
        ),
        ExecutionResources(object_store_memory=900),
    )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
