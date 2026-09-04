import math
import time
from collections import defaultdict
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple
from unittest.mock import MagicMock, PropertyMock, patch

import pytest
from freezegun import freeze_time

from ray.data._internal.compute import ComputeStrategy
from ray.data._internal.execution.block_ref_counter import BlockRefCounter
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.join import JoinOperator
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.operators.union_operator import UnionOperator
from ray.data._internal.execution.resource_manager import (
    ReservationOpResourceAllocator,
    ResourceManager,
    create_resource_allocator,
)
from ray.data._internal.execution.streaming_executor_state import (
    IdleDetector,
    OutputBackpressureGuard,
    Topology,
    build_streaming_topology,
)
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa
from ray.data.tests.conftest import noop_counter


class StubBlockRefCounter(BlockRefCounter):
    """Test double that stubs BlockRefCounter."""

    def __init__(self):
        self._bytes_by_producer = defaultdict(int)

    def on_block_produced(self, block_ref, size_bytes, producer_id):
        self._bytes_by_producer[producer_id] += size_bytes

    def get_object_store_memory_usage(self, producer_id):
        return self._bytes_by_producer.get(producer_id, 0)

    def clear(self):
        self._bytes_by_producer.clear()


def mock_map_op(
    input_op: PhysicalOperator,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    compute_strategy: Optional[ComputeStrategy] = None,
    name="Map",
):
    op = MapOperator.create(
        MagicMock(),
        input_op,
        DataContext.get_current(),
        ray_remote_args=ray_remote_args or {},
        compute_strategy=compute_strategy,
        name=name,
    )
    return op


def mock_union_op(input_ops):
    op = UnionOperator(
        DataContext.get_current(),
        *input_ops,
    )
    return op


def mock_join_op(left_input_op, right_input_op):
    left_input_op._logical_operators = [MagicMock()]
    right_input_op._logical_operators = [MagicMock()]

    with patch(
        "ray.data._internal.execution.operators.hash_shuffle._get_total_cluster_resources"
    ) as mock:
        mock.return_value = ExecutionResources(cpu=1)

        op = JoinOperator(
            DataContext.get_current(),
            left_input_op,
            right_input_op,
            ("id",),
            ("id",),
            "inner",
            num_partitions=1,
            partition_size_hint=1,
        )

    op.start = MagicMock(side_effect=lambda *_: None)
    return op


def mock_all_to_all_op(input_op, name="MockShuffle"):
    """Create a mock AllToAllOperator (shuffle) for testing."""
    op = AllToAllOperator(
        bulk_fn=MagicMock(),
        input_op=input_op,
        data_context=DataContext.get_current(),
        name=name,
    )
    return op


def _resource_manager_for_limits_only_test(
    options: ExecutionOptions,
    get_total_resources,
):
    """``ResourceManager`` requires a valid single-sink topology; these tests only
    call ``get_global_limits()`` and never iterate real operators."""
    sink = MagicMock(spec=PhysicalOperator)
    sink.output_dependencies = []
    topology = {sink: MagicMock()}
    return ResourceManager(
        topology,
        options,
        get_total_resources,
        DataContext.get_current(),
        BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
    )


def _build_reservation_allocator(
    num_map_ops: int,
    ctx: Optional[DataContext] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
) -> Tuple[ResourceManager, Topology, List[PhysicalOperator]]:
    """Build a real ``ResourceManager`` over a linear chain of ``num_map_ops`` map
    operators fed by an ``InputDataBuffer``.

    Returns ``(resource_manager, topo, map_ops)``, where ``map_ops`` is ordered
    upstream -> downstream and excludes the input buffer.
    """
    ctx = ctx or DataContext.get_current()
    op = InputDataBuffer(ctx, [])
    map_ops = []
    for _ in range(num_map_ops):
        op = mock_map_op(op, ray_remote_args=ray_remote_args)
        map_ops.append(op)
    topo = build_streaming_topology(op, ExecutionOptions(), noop_counter())
    resource_manager = ResourceManager(
        topo,
        ExecutionOptions(),
        MagicMock(),
        ctx,
        BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
    )
    return resource_manager, topo, map_ops


def _build_blocked_downstream(
    num_map_ops: int = 2,
    ctx: Optional[DataContext] = None,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    *,
    num_cpus_budget: float,
    object_store_budget: float,
    pressure_fraction: Optional[float],
    execution_usage: float,
    overshoot_ratio: Optional[float] = None,
    op_object_store_usage: float = 0,
) -> tuple:
    """Build an executor whose most-downstream op is idle with queued input and an
    exhausted output budget.

    Returns ``(metrics_patch, guard, alloc, o2, o3)``, where ``o2`` feeds the
    blocked downstream op ``o3``. ``metrics_patch`` must be entered to make each
    task emit one byte of object-store output. The idle detector is disabled so
    that ``should_unblock`` reflects Case 1 alone rather than falling back to it.
    Use ``_set_queued_input_blocks(guard._topology, o3, n)`` to change the queue.

    ``overshoot_ratio`` together with ``op_object_store_usage`` drive the shared
    -allocation throttle: ``o3`` is given a 100-byte object-store reservation, so
    a usage above ``overshoot_ratio * 100`` marks it as overshooting.
    """
    ctx = ctx or DataContext.get_current()
    resource_manager, topo, map_ops = _build_reservation_allocator(
        num_map_ops, ctx=ctx, ray_remote_args=ray_remote_args
    )
    o2, o3 = map_ops[-2], map_ops[-1]
    guard = OutputBackpressureGuard(topo, resource_manager)
    alloc = resource_manager.op_resource_allocator

    o3.num_active_tasks = MagicMock(return_value=0)
    guard._idle_detector.detect_idle = MagicMock(return_value=False)
    topo[o3].total_enqueued_input_blocks = MagicMock(return_value=1)
    alloc._op_budgets[o3] = ExecutionResources(
        cpu=num_cpus_budget, gpu=0, object_store_memory=object_store_budget
    )

    alloc._object_store_memory_pressure_fraction = pressure_fraction
    alloc._object_store_reservation_overshoot_ratio = overshoot_ratio
    alloc._op_reserved[o3] = ExecutionResources(cpu=1, gpu=0, object_store_memory=80)
    alloc._reserved_for_op_outputs[o3] = 20.0
    resource_manager._op_usages[o3] = ExecutionResources(
        object_store_memory=op_object_store_usage
    )
    resource_manager._global_limits = ExecutionResources(object_store_memory=1000)
    resource_manager._global_limits_last_update_time = time.time()
    resource_manager._global_usage = ExecutionResources(
        object_store_memory=execution_usage
    )
    return (
        patch.object(
            type(o3.metrics),
            "obj_store_mem_max_pending_output_per_task",
            new_callable=PropertyMock,
            return_value=1,
        ),
        guard,
        alloc,
        o2,
        o3,
    )


def _set_queued_input_blocks(
    topo: "Topology", op: PhysicalOperator, count: int
) -> None:
    """Set how many input blocks are queued for ``op``."""
    topo[op].total_enqueued_input_blocks = MagicMock(return_value=count)


class TestResourceManager:
    """Unit tests for ResourceManager."""

    def test_global_limits(self):
        cluster_resources = {"CPU": 10, "GPU": 5, "object_store_memory": 1000}
        default_object_store_memory_limit = math.ceil(
            cluster_resources["object_store_memory"]
            * ResourceManager.DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION
        )

        def get_total_resources():
            return ExecutionResources.from_resource_dict(cluster_resources)

        # Test default resource limits.
        # When no resource limits are set, the resource limits should default to
        # the cluster resources for CPU/GPU, and
        # DEFAULT_OBJECT_STORE_MEMORY_LIMIT_FRACTION of cluster object store memory.
        options = ExecutionOptions()
        resource_manager = _resource_manager_for_limits_only_test(
            options, get_total_resources
        )
        expected = ExecutionResources(
            cpu=cluster_resources["CPU"],
            gpu=cluster_resources["GPU"],
            object_store_memory=default_object_store_memory_limit,
        )
        assert resource_manager.get_global_limits() == expected

        # Test setting resource_limits
        options = ExecutionOptions()
        options.resource_limits = ExecutionResources(
            cpu=1, gpu=2, object_store_memory=100
        )
        resource_manager = _resource_manager_for_limits_only_test(
            options, get_total_resources
        )
        expected = ExecutionResources(
            cpu=1,
            gpu=2,
            object_store_memory=100,
        )
        assert resource_manager.get_global_limits() == expected

        # Test setting exclude_resources
        # The actual limit should be the default limit minus the excluded resources.
        options = ExecutionOptions()
        options.exclude_resources = ExecutionResources(
            cpu=1, gpu=2, object_store_memory=100
        )
        resource_manager = _resource_manager_for_limits_only_test(
            options, get_total_resources
        )
        expected = ExecutionResources(
            cpu=cluster_resources["CPU"] - 1,
            gpu=cluster_resources["GPU"] - 2,
            object_store_memory=default_object_store_memory_limit - 100,
        )
        assert resource_manager.get_global_limits() == expected

        # Test that we don't support setting both resource_limits
        # and exclude_resources.
        with pytest.raises(ValueError):
            options = ExecutionOptions()
            options.resource_limits = ExecutionResources(cpu=2)
            options.exclude_resources = ExecutionResources(cpu=1)
            options.validate()

    def test_global_limits_cache(self):
        get_total_resources = MagicMock(return_value=ExecutionResources(4, 1, 0))

        cache_interval_s = 0.1
        with patch.object(
            ResourceManager,
            "GLOBAL_LIMITS_UPDATE_INTERVAL_S",
            cache_interval_s,
        ):
            resource_manager = _resource_manager_for_limits_only_test(
                ExecutionOptions(),
                get_total_resources,
            )
            expected_resource = ExecutionResources(4, 1, 0)
            # The first call should call ray.cluster_resources().
            assert resource_manager.get_global_limits() == expected_resource
            assert get_total_resources.call_count == 1
            # The second call should return the cached value.
            assert resource_manager.get_global_limits() == expected_resource
            assert get_total_resources.call_count == 1
            time.sleep(cache_interval_s)
            # After the cache interval, the third call should call
            # ray.cluster_resources() again.
            assert resource_manager.get_global_limits() == expected_resource
            assert get_total_resources.call_count == 2

    def test_update_usage(self):
        """Test calculating op_usage."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)
        counter = StubBlockRefCounter()
        topo = build_streaming_topology(o3, ExecutionOptions(), counter)

        mock_cpu = {
            o1: 0,
            o2: 5,
            o3: 8,
        }
        mock_pending_task_outputs = {
            o1: 0,
            o2: 100,
            o3: 200,
        }
        mock_counter_bytes = {
            o1: 0,
            o2: 300,
            o3: 400,
        }

        for op in [o1, o2, o3]:
            op.current_logical_usage = MagicMock(
                return_value=ExecutionResources(cpu=mock_cpu[op], gpu=0, memory=0)
            )
            op.running_logical_usage = MagicMock(
                return_value=ExecutionResources(cpu=mock_cpu[op], gpu=0, memory=0)
            )
            op.pending_logical_usage = MagicMock(return_value=ExecutionResources.zero())
            op.extra_resource_usage = MagicMock(return_value=ExecutionResources.zero())
            op._metrics = MagicMock(
                obj_store_mem_pending_task_outputs=mock_pending_task_outputs[op],
            )

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
            counter,
        )
        resource_manager._op_resource_allocator = None

        for op in [o2, o3]:
            if mock_counter_bytes[op]:
                counter.on_block_produced(None, mock_counter_bytes[op], op.id)

        resource_manager.update_usages()

        global_cpu = 0
        global_mem = 0
        for op in [o1, o2, o3]:
            if op == o1:
                # InputDataBuffer memory is not counted.
                expected_mem = 0
            else:
                expected_mem = mock_pending_task_outputs[op] + mock_counter_bytes[op]
            op_usage = resource_manager.get_op_usage(op)
            assert op_usage.cpu == mock_cpu[op]
            assert op_usage.gpu == 0
            assert op_usage.object_store_memory == expected_mem
            if op != o1:
                assert (
                    resource_manager._mem_op_internal[op]
                    == mock_pending_task_outputs[op]
                )
                assert resource_manager._mem_op_outputs[op] == mock_counter_bytes[op]
            global_cpu += mock_cpu[op]
            global_mem += expected_mem

        assert resource_manager.get_global_usage() == ExecutionResources(
            global_cpu, 0, global_mem
        )

    def test_object_store_usage(self, restore_data_context):
        """ResourceManager reads per-operator memory from BlockRefCounter."""

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        counter = StubBlockRefCounter()
        topo = build_streaming_topology(o3, ExecutionOptions(), counter)
        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(return_value=ExecutionResources.zero()),
            DataContext.get_current(),
            counter,
        )

        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 0
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # Simulate o2 producing a 100-byte block.
        counter.on_block_produced(None, 100, o2.id)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 100
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # Simulate o3 producing a 200-byte block.
        counter.on_block_produced(None, 200, o3.id)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o2).object_store_memory == 100
        assert resource_manager.get_op_usage(o3).object_store_memory == 200

        # After clear(), all usage resets to 0.
        counter.clear()
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o2).object_store_memory == 0
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

    def test_external_consumer_bytes_not_double_counted(self, restore_data_context):
        """external_consumer_bytes (iterator prefetch) does not inflate
        get_op_usage. BlockRefCounter already tracks prefetch buffer blocks
        via live ObjectRefs. external_consumer_bytes is only used by
        DownstreamCapacityBackpressurePolicy for the terminal edge ratio."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        counter = StubBlockRefCounter()
        topo = build_streaming_topology(o3, ExecutionOptions(), counter)
        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(return_value=ExecutionResources.zero()),
            DataContext.get_current(),
            counter,
        )

        counter.on_block_produced(None, 100, o3.id)
        resource_manager.set_external_consumer_bytes(50)
        resource_manager.update_usages()

        assert resource_manager.get_op_usage(o3).object_store_memory == 100
        assert resource_manager.get_external_consumer_bytes() == 50

    def test_union_no_double_counting(self, restore_data_context):
        """UnionOperator passthrough does not inflate global memory usage."""

        o1 = InputDataBuffer(DataContext.get_current(), [])
        map_a = mock_map_op(o1, name="MapA")
        o2 = InputDataBuffer(DataContext.get_current(), [])
        map_b = mock_map_op(o2, name="MapB")
        union_op = mock_union_op([map_a, map_b])
        downstream = mock_map_op(union_op, name="Downstream")

        counter = StubBlockRefCounter()
        topo = build_streaming_topology(downstream, ExecutionOptions(), counter)
        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(return_value=ExecutionResources(object_store_memory=10_000)),
            DataContext.get_current(),
            counter,
        )

        counter.on_block_produced(None, 100, map_a.id)
        counter.on_block_produced(None, 200, map_b.id)

        resource_manager.update_usages()

        assert resource_manager.get_op_usage(map_a).object_store_memory == 100
        assert resource_manager.get_op_usage(map_b).object_store_memory == 200
        assert resource_manager.get_op_usage(union_op).object_store_memory == 0

        total_obj_store = resource_manager.get_global_usage().object_store_memory
        assert total_obj_store == 300

    def test_get_completed_ops_usage(self, restore_data_context):
        """Test that _get_completed_ops_usage returns total usage of completed ops."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = LimitOperator(1, o2, DataContext.get_current())
        o4 = mock_map_op(o3)
        o5 = mock_map_op(o4)

        topo = build_streaming_topology(o5, ExecutionOptions(), noop_counter())

        o1.mark_execution_finished()
        o2.mark_execution_finished()

        op_usages = {
            o1: ExecutionResources.zero(),
            o2: ExecutionResources(cpu=2, object_store_memory=50),
            o3: ExecutionResources(cpu=1, object_store_memory=25),
            o4: ExecutionResources.zero(),
            o5: ExecutionResources.zero(),
        }

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
            BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])

        # o2 is completed and o3 is downstream ineligible (LimitOperator)
        # Total usage should be o2 + o3
        completed_ops_usage = resource_manager._get_completed_ops_usage()
        assert completed_ops_usage == ExecutionResources(cpu=3, object_store_memory=75)

    def test_get_completed_ops_usage_complex_graph(self, restore_data_context):
        """
        o1 (InputDataBuffer)
                |
                v
                o2 (MapOperator, completed)
                |
                v
                o3 (LimitOperator)
                |
                v                    o4 (InputDataBuffer)
                |                    |
                |                    v
                |                    o5 (MapOperator, completed)
                |                    |
                v                    v
                o6 (UnionOperator) <--
                |
                v
                o8 (JoinOperator) <-- o7 (InputDataBuffer, completed)
        """
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = LimitOperator(1, o2, DataContext.get_current())
        o4 = InputDataBuffer(DataContext.get_current(), [])
        o5 = mock_map_op(o4)
        o6 = mock_union_op([o3, o5])
        o7 = InputDataBuffer(DataContext.get_current(), [])
        o8 = mock_join_op(o7, o6)

        topo = build_streaming_topology(o8, ExecutionOptions(), noop_counter())

        o1.mark_execution_finished()
        o2.mark_execution_finished()
        o4.mark_execution_finished()
        o5.mark_execution_finished()
        o7.mark_execution_finished()

        op_usages = {
            o1: ExecutionResources.zero(),
            o2: ExecutionResources(cpu=2, object_store_memory=150),
            o3: ExecutionResources(cpu=2, object_store_memory=50),
            o4: ExecutionResources.zero(),
            o5: ExecutionResources(cpu=3, object_store_memory=100),
            o6: ExecutionResources.zero(),
            o7: ExecutionResources(cpu=1, object_store_memory=100),
            o8: ExecutionResources.zero(),
        }

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
            BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])

        # Completed ops: o2, o5, o7
        # Downstream ineligible: o3 (LimitOperator after o2)
        # Total usage should be o2 + o3 + o5 + o7
        completed_ops_usage = resource_manager._get_completed_ops_usage()

        assert completed_ops_usage == ExecutionResources(cpu=8, object_store_memory=400)

    def test_set_external_consumer_bytes_rejects_negative(self, restore_data_context):
        resource_manager = _resource_manager_for_limits_only_test(
            ExecutionOptions(),
            MagicMock(return_value=ExecutionResources.zero()),
        )
        with pytest.raises(AssertionError):
            resource_manager.set_external_consumer_bytes(-1)

    def test_external_consumer_bytes_surfaced_in_op_usage_str(
        self, restore_data_context
    ):
        """The terminal operator's verbose usage string should include
        external_consumer=... when an external consumer is registered, so users
        can see how much of the operator's object-store memory is held by a
        downstream iterator vs. the operator's own queues."""
        cluster_resources = ExecutionResources(cpu=10, gpu=0, object_store_memory=1000)

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        topo = build_streaming_topology(o3, ExecutionOptions(), noop_counter())
        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            lambda: cluster_resources,
            DataContext.get_current(),
            BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
        )

        for op in [o1, o2, o3]:
            op.current_logical_usage = MagicMock(return_value=ExecutionResources.zero())
            op.running_logical_usage = MagicMock(return_value=ExecutionResources.zero())
            op.pending_logical_usage = MagicMock(return_value=ExecutionResources.zero())

        resource_manager.update_usages()

        # No external consumer yet: nothing extra in the usage string.
        terminal_str = resource_manager.get_op_usage_str(o3, verbose=True)
        upstream_str = resource_manager.get_op_usage_str(o2, verbose=True)
        assert "external_consumer=" not in terminal_str
        assert "external_consumer=" not in upstream_str

        # Register an external consumer. Only the terminal operator's string
        # should pick up `external_consumer=...`.
        resource_manager.set_external_consumer_bytes(200)
        resource_manager.update_usages()
        terminal_str = resource_manager.get_op_usage_str(o3, verbose=True)
        upstream_str = resource_manager.get_op_usage_str(o2, verbose=True)
        assert "external_consumer=200.0B" in terminal_str
        assert "external_consumer=" not in upstream_str

        # The field is inside the existing `(in=...,out=...)` parenthetical.
        assert ",external_consumer=" in terminal_str

        # Non-verbose output omits the field (existing format unchanged).
        terminal_str_brief = resource_manager.get_op_usage_str(o3, verbose=False)
        assert "external_consumer=" not in terminal_str_brief

    def test_topology_rejects_multiple_terminal_operators(self, restore_data_context):
        ctx = DataContext.get_current()
        a = PhysicalOperator("a", [], ctx)
        b = PhysicalOperator("b", [], ctx)
        topology = {a: MagicMock(), b: MagicMock()}
        with pytest.raises(ValueError, match="Expected exactly one terminal operator"):
            ResourceManager(
                topology,
                ExecutionOptions(),
                MagicMock(return_value=ExecutionResources.zero()),
                DataContext.get_current(),
                BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
            )

    def test_topology_rejects_empty_topology(self, restore_data_context):
        with pytest.raises(ValueError, match="topology must be non-empty"):
            ResourceManager(
                {},
                ExecutionOptions(),
                MagicMock(return_value=ExecutionResources.zero()),
                DataContext.get_current(),
                BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
            )

    def test_topology_rejects_no_terminal_operator(self, restore_data_context):
        # Every op has a downstream in this dict, so there should be no operator with empty
        # output_dependencies (e.g. a 2-node cycle). Real streaming DAGs from
        # build_streaming_topology always have a unique sink.
        a = MagicMock(spec=PhysicalOperator)
        b = MagicMock(spec=PhysicalOperator)
        a.output_dependencies = [b]
        b.output_dependencies = [a]
        topology = {a: MagicMock(), b: MagicMock()}
        with pytest.raises(ValueError, match="No terminal operator found"):
            ResourceManager(
                topology,
                ExecutionOptions(),
                MagicMock(return_value=ExecutionResources.zero()),
                DataContext.get_current(),
                BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
            )

    def test_is_blocking_materializing_op(self, restore_data_context):
        """Test _is_blocking_materializing_op correctly identifies blocking materializing ops.

        Cases tested:
        1. Operator itself is a blocking materializing op (AllToAllOperator) -> True
        2. Operator has downstream ineligible blocking materializing op -> True
        3. Operator with no downstream blocking materializing ops -> False

        Note: AllToAllOperator.throttling_disabled() returns True, making it
        ineligible for resource allocation. This means shuffle operators are
        always in the "downstream ineligible" chain from eligible operators.
        """
        # Build pipeline: o1 -> o2 -> o3 (limit) -> o4 (shuffle) -> o5
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1, name="Map1")
        o3 = LimitOperator(1, o2, DataContext.get_current())
        o4 = mock_all_to_all_op(o3, name="Sort")
        o5 = mock_map_op(o4, name="Map2")

        topo = build_streaming_topology(o5, ExecutionOptions(), noop_counter())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
            BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
        )

        # Case 1: Shuffle operator itself is blocking materializing
        assert resource_manager._is_blocking_materializing_op(o4) is True

        # Case 2: Map operator before shuffle (o2) should return True because
        # its downstream ineligible chain includes:
        # - o3 (LimitOperator - ineligible, not in eligible types)
        # - o4 (AllToAllOperator - ineligible because throttling_disabled=True)
        # Since o4 is a blocking materializing op, the check returns True
        assert resource_manager._is_blocking_materializing_op(o2) is True

        # o3 (LimitOperator) also returns True because its downstream ineligible
        # chain includes o4 (shuffle)
        assert resource_manager._is_blocking_materializing_op(o3) is True

        # Case 3: o5 (Map after shuffle) has no downstream ops -> False
        assert resource_manager._is_blocking_materializing_op(o5) is False

        # Case 4: Extend pipeline with ops that have no blocking materializing downstream
        # o5 -> o6 (limit) -> o7
        o6 = LimitOperator(1, o5, DataContext.get_current())
        o7 = mock_map_op(o6, name="Map3")

        topo2 = build_streaming_topology(o7, ExecutionOptions(), noop_counter())
        resource_manager2 = ResourceManager(
            topo2,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
            BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
        )

        # o5's downstream (o6, o7) has no blocking materializing ops
        assert resource_manager2._is_blocking_materializing_op(o5) is False
        assert resource_manager2._is_blocking_materializing_op(o7) is False

    def test_memory_limit_blocks_task_submission(self, restore_data_context):
        """Test that tasks are blocked when memory limit is exceeded."""
        # Cluster has 1000 bytes of memory
        cluster_resources = ExecutionResources(cpu=1, gpu=0, memory=1000)

        # Request 2000 bytes memory
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(
            o1,
            ray_remote_args={"num_cpus": 1, "memory": 2000},
            name="HighMemoryTask",
        )

        topo = build_streaming_topology(o2, ExecutionOptions(), noop_counter())
        options = ExecutionOptions()

        resource_manager = ResourceManager(
            topology=topo,
            options=options,
            get_total_resources=lambda: cluster_resources,
            data_context=DataContext.get_current(),
            block_ref_counter=BlockRefCounter(
                add_object_out_of_scope_callback=lambda *_: True
            ),
        )
        resource_manager.update_usages()

        # Task cannot be submitted because it exceeds memory limit
        allocator = create_resource_allocator(
            resource_manager, DataContext.get_current()
        )
        assert allocator is not None
        allocator.update_budgets(limits=resource_manager.get_global_limits())
        can_submit = allocator.can_submit_new_task(o2)
        assert (
            not can_submit
        ), "Task should be blocked: requires 2000 bytes but only 1000 bytes memory available"


class TestOutputBackpressureGuard:
    """Tests for OutputBackpressureGuard.should_unblock."""

    def test_unblock_backpressure_terminal_operator(self, restore_data_context):
        """Terminal operator (no downstream eligible ops) with no external
        consumer should always unblock (e.g., write pipeline)."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = LimitOperator(1, o2, DataContext.get_current())

        topo = build_streaming_topology(o3, ExecutionOptions(), noop_counter())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
            BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
        )
        guard = OutputBackpressureGuard(topo, resource_manager)

        # o2 is terminal (no downstream eligible ops beyond it) and no external
        # consumer — should unblock (e.g., write pipeline).
        assert guard.should_unblock(o2) is True

        # Add o4 operator - o2 is no longer terminal
        o4 = mock_map_op(o3)

        topo = build_streaming_topology(o4, ExecutionOptions(), noop_counter())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
            BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
        )
        guard = OutputBackpressureGuard(topo, resource_manager)

        # Mock downstream (o4) having active tasks and input blocks (ie unblocking
        # conditions not met)
        o4.num_active_tasks = MagicMock(return_value=1)
        guard._idle_detector.detect_idle = MagicMock(return_value=False)

        # o2 is not terminal anymore, falls back to idle detector which returns False
        assert guard.should_unblock(o2) is False

    def test_no_unblock_backpressure_terminal_with_external_consumer(
        self, restore_data_context
    ):
        """Terminal operator with an external consumer should only unblock
        when consumers are starving (blocked waiting for output)."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = LimitOperator(1, o2, DataContext.get_current())

        topo = build_streaming_topology(o3, ExecutionOptions(), noop_counter())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
            BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
        )
        guard = OutputBackpressureGuard(topo, resource_manager)

        # Register an external consumer (e.g., iter_batches or streaming_split).
        resource_manager.set_external_consumer_bytes(0)

        dag_output_state = topo[o3]

        # No consumers waiting — should NOT unblock (prevents pileup).
        dag_output_state._num_waiting_consumers = 0
        assert guard.should_unblock(o2) is False

        # Simulate a consumer blocked in get_output_blocking (starving).
        # The output node is o3 (LimitOperator), which tracks waiting consumers.
        dag_output_state._num_waiting_consumers = 1
        assert guard.should_unblock(o2) is True

        # Consumer gets data and stops waiting — should NOT unblock again.
        dag_output_state._num_waiting_consumers = 0
        assert guard.should_unblock(o2) is False

    def test_unblock_backpressure_downstream_idle(self, restore_data_context):
        """Unblock when downstream is idle (no active tasks) to maintain liveness."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        topo = build_streaming_topology(o3, ExecutionOptions(), noop_counter())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
            BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
        )
        guard = OutputBackpressureGuard(topo, resource_manager)
        o3.num_active_tasks = MagicMock(return_value=0)

        # Case 1: Downstream cannot submit (resource constrained) - unblock to free resources
        resource_manager.op_resource_allocator.can_submit_new_task = MagicMock(
            return_value=False
        )
        assert guard.should_unblock(o2) is True

        # Case 2: Downstream can submit but has no input blocks - unblock to produce data
        resource_manager.op_resource_allocator.can_submit_new_task = MagicMock(
            return_value=True
        )
        topo[o3].total_enqueued_input_blocks = MagicMock(return_value=0)
        assert guard.should_unblock(o2) is True

    @pytest.mark.parametrize(
        "overshoot_ratio, op_usage",
        [
            # Throttle disabled.
            (None, 200),
            # Throttle enabled and this op is overshooting (200 > 1.5 * 100).
            (1.5, 200),
            # Throttle enabled and this op is within its reservation.
            (1.5, 120),
        ],
    )
    def test_cpu_shortfall_always_relaxes_upstream(
        self, overshoot_ratio, op_usage, restore_data_context
    ):
        """A CPU/GPU shortfall is always authoritative, throttled or not.

        The throttle withholds only object-store budget, so a CPU shortfall is
        never self-inflicted and always means real contention. The operator must
        be held back so Case 1 relaxes upstream, letting upstream tasks finish and
        release the CPU this operator is waiting on. Admitting it instead would
        suppress that relaxation and leave ``IdleDetector``'s coarse interval as
        the only way out.
        """
        metrics_patch, guard, alloc, o2, o3 = _build_blocked_downstream(
            ray_remote_args={"num_cpus": 4},
            # Only 1 CPU of budget against a 4-CPU task.
            num_cpus_budget=1,
            object_store_budget=0,
            pressure_fraction=0.8,
            execution_usage=900,
            overshoot_ratio=overshoot_ratio,
            op_object_store_usage=op_usage,
        )
        with metrics_patch:
            assert alloc.can_submit_new_task(o3) is False
            assert guard.should_unblock(o2) is True

    def test_liveness_allowance_bounded_to_one_idle_task(self, restore_data_context):
        """The allowance is one task for an idle op with something to consume.

        Each condition is load-bearing: a busy task pool means progress is already
        happening, and an empty queue means upstream output -- not another task --
        is what this operator needs.
        """
        metrics_patch, guard, alloc, o2, o3 = _build_blocked_downstream(
            ray_remote_args={"num_cpus": 4},
            num_cpus_budget=4,
            object_store_budget=0,
            pressure_fraction=0.8,
            execution_usage=900,
        )
        topo = guard._topology
        with metrics_patch:
            # Tasks already running and nothing queued: no liveness case to make.
            o3.num_active_tasks.return_value = 7
            _set_queued_input_blocks(topo, o3, 0)
            assert alloc.can_submit_new_task(o3) is False

            # Busy pool alone is disqualifying, even with input waiting.
            _set_queued_input_blocks(topo, o3, 1)
            assert alloc.can_submit_new_task(o3) is False

            # Idle but nothing to consume: upstream output is what it needs.
            o3.num_active_tasks.return_value = 0
            _set_queued_input_blocks(topo, o3, 0)
            assert alloc.can_submit_new_task(o3) is False
            assert guard.should_unblock(o2) is True

            # Idle with queued input: the one allowed recovery task.
            _set_queued_input_blocks(topo, o3, 1)
            assert alloc.can_submit_new_task(o3) is True
            assert guard.should_unblock(o2) is False

    def test_liveness_allowance_requires_the_pressure_threshold(
        self, restore_data_context
    ):
        """The allowance is opt-in, so leaving the threshold unset preserves the
        pre-existing behavior exactly."""
        metrics_patch, guard, alloc, o2, o3 = _build_blocked_downstream(
            ray_remote_args={"num_cpus": 4},
            num_cpus_budget=4,
            object_store_budget=0,
            pressure_fraction=0.8,
            execution_usage=900,
        )
        with metrics_patch:
            # Output budget exhausted but CPU is sufficient: the allowance admits
            # the task, so upstream output stays blocked.
            assert alloc.can_submit_new_task(o3) is True
            assert guard.should_unblock(o2) is False

            # Without the opt-in threshold the allowance is off entirely, so the
            # exhausted output budget blocks submission as it did before.
            alloc._object_store_memory_pressure_fraction = None
            assert alloc.can_submit_new_task(o3) is False
            assert guard.should_unblock(o2) is True

    def test_unblock_backpressure_fallback_to_idle_detector(self, restore_data_context):
        """When unblock conditions not met, falls back to idle detector result."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        topo = build_streaming_topology(o3, ExecutionOptions(), noop_counter())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
            BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
        )
        guard = OutputBackpressureGuard(topo, resource_manager)

        # Case: Downstream has active tasks - falls back to idle detector
        o3.num_active_tasks = MagicMock(return_value=2)
        guard._idle_detector.detect_idle = MagicMock(return_value=False)
        assert guard.should_unblock(o2) is False

        # Case: Idle detector returns True - should unblock
        guard._idle_detector.detect_idle = MagicMock(return_value=True)
        assert guard.should_unblock(o2) is True

        # Case: Downstream has no active tasks but has input blocks - falls back to idle detector
        resource_manager.op_resource_allocator.can_submit_new_task = MagicMock(
            return_value=True
        )
        o3.num_active_tasks = MagicMock(return_value=0)
        topo[o3].total_enqueued_input_blocks = MagicMock(return_value=5)
        guard._idle_detector.detect_idle = MagicMock(return_value=False)
        assert guard.should_unblock(o2) is False

    def test_unblock_when_resource_allocator_disabled(self, restore_data_context):
        """When the op resource allocator is disabled, the guard treats
        downstream as schedulable (no budget to consult), so
        "downstream resource constrained" case never fires, but the other
        liveness conditions still do.
        """
        # Disable resource allocator
        DataContext.get_current().op_resource_reservation_enabled = False

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        topo = build_streaming_topology(o3, ExecutionOptions(), noop_counter())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
            BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
        )
        assert not resource_manager.op_resource_allocator_enabled()

        guard = OutputBackpressureGuard(topo, resource_manager)
        o3.num_active_tasks = MagicMock(return_value=0)

        # "Downstream idle with empty input queue" case should fire and unblock.
        topo[o3].total_enqueued_input_blocks = MagicMock(return_value=0)
        assert guard.should_unblock(o2) is True


class TestIdleDetector:
    """Tests for IdleDetector."""

    def test_idle_detector(self, restore_data_context):
        """Test IdleDetector behavior through its public interface."""
        idle_detector = IdleDetector()
        op = MagicMock()
        op.metrics.num_task_outputs_generated = 0

        with freeze_time() as frozen:
            # First call initializes state, returns False
            assert idle_detector.detect_idle(op) is False

            # Call within interval returns False (rate limited)
            frozen.tick(timedelta(seconds=idle_detector.DETECTION_INTERVAL_S - 1))
            assert idle_detector.detect_idle(op) is False

            # Call after interval with no output returns True (idle)
            frozen.tick(timedelta(seconds=2))
            assert idle_detector.detect_idle(op) is True

            # Operator produces output - next detection returns False (active)
            op.metrics.num_task_outputs_generated = 5
            assert idle_detector.detect_idle(op) is False

            # After output, wait for interval with no new output - returns True (idle again)
            frozen.tick(timedelta(seconds=idle_detector.DETECTION_INTERVAL_S + 1))
            assert idle_detector.detect_idle(op) is True


class TestReservationOpResourceAllocator:
    """Tests for ReservationOpResourceAllocator's object-store budget throttle."""

    @pytest.mark.parametrize(
        "overshoot_ratio, pressure_fraction, expect_ok",
        [
            # Both None disables the feature; accepted.
            (None, None, True),
            # Valid boundary values.
            (1.0, 1.0, True),
            (1.5, 0.8, True),
            # overshoot_ratio must be a finite float >= 1.0.
            (0.5, None, False),
            (math.inf, None, False),
            # pressure_fraction must be in (0.0, 1.0].
            (None, 0.0, False),
            (None, 1.5, False),
        ],
    )
    def test_init_validates_thresholds(
        self, overshoot_ratio, pressure_fraction, expect_ok
    ):
        """The constructor rejects out-of-range or non-finite thresholds."""

        def build():
            return ReservationOpResourceAllocator(
                MagicMock(),
                reservation_ratio=0.5,
                object_store_reservation_overshoot_ratio=overshoot_ratio,
                object_store_memory_pressure_fraction=pressure_fraction,
            )

        if expect_ok:
            build()
        else:
            with pytest.raises(ValueError):
                build()

    @pytest.mark.parametrize(
        "ratio, fraction, reserved_os, op_usage, global_usage, global_limit, expected",
        [
            # Feature off: neither threshold set.
            (None, None, 100, 300, 900, 1000, False),
            # Only the ratio set -- both are required.
            (1.5, None, 100, 200, 900, 1000, False),
            # Both set and under pressure, but usage is within 1.5x reservation.
            (1.5, 0.8, 100, 120, 900, 1000, False),
            # Over the ratio, but the execution is not under pressure.
            (1.5, 0.8, 100, 200, 50, 1000, False),
            # Over the ratio and under pressure.
            (1.5, 0.8, 100, 200, 900, 1000, True),
            # Limit unknown, so pressure can't be measured.
            (1.5, 0.8, 100, 200, 900, 0, False),
            # A zero reservation would make any usage "over" it, so it is skipped
            # rather than permanently throttling an under-provisioned operator.
            (1.5, 0.8, 0, 9999, 900, 1000, False),
        ],
    )
    def test_is_op_overshooting_object_store_reservation(
        self,
        ratio,
        fraction,
        reserved_os,
        op_usage,
        global_usage,
        global_limit,
        expected,
        restore_data_context,
    ):
        """The throttle needs both thresholds, plus pressure, plus a real overshoot."""
        resource_manager, _, map_ops = _build_reservation_allocator(2)
        op = map_ops[0]
        alloc = resource_manager.op_resource_allocator

        alloc._object_store_reservation_overshoot_ratio = ratio
        alloc._object_store_memory_pressure_fraction = fraction
        # Reservation is split between the op and its outputs; only the total
        # matters to the predicate.
        alloc._op_reserved[op] = ExecutionResources(
            cpu=1, gpu=0, object_store_memory=reserved_os * 0.8
        )
        alloc._reserved_for_op_outputs[op] = reserved_os * 0.2
        resource_manager._op_usages[op] = ExecutionResources(
            object_store_memory=op_usage
        )
        resource_manager._global_limits = ExecutionResources(
            object_store_memory=global_limit
        )
        resource_manager._global_limits_last_update_time = time.time()
        resource_manager._global_usage = ExecutionResources(
            object_store_memory=global_usage
        )

        assert alloc._is_op_overshooting_object_store_reservation(op) is expected

    def test_feeder_of_eligible_materializer_is_exempt(self, restore_data_context):
        """A feeder of a blocking materializer is exempt even when that
        materializer is *eligible*.

        ``_is_blocking_materializing_op`` walks only ineligible deps, so it misses
        the hash-shuffle family. Throttling their feeder is counterproductive:
        they emit nothing until fully fed.

        Only the immediate feeder is exempt -- in ``Map1 -> Map2 -> Join`` it's
        Map2 that holds the line, so exempting it keeps Map1 from backing up.
        """
        ctx = DataContext.get_current()
        map1 = mock_map_op(InputDataBuffer(ctx, []), name="Map1")
        upstream_map = mock_map_op(map1, name="MapIntoJoin")
        join = mock_join_op(upstream_map, InputDataBuffer(ctx, []))

        topo = build_streaming_topology(join, ExecutionOptions(), noop_counter())
        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            ctx,
            BlockRefCounter(add_object_out_of_scope_callback=lambda *_: True),
        )
        alloc = resource_manager.op_resource_allocator

        # The join is eligible, so the accounting walk can't see it. This is the
        # gap the scheduling check closes. Map1 is two hops away and stays out.
        assert resource_manager.is_op_eligible(join) is True
        assert resource_manager._is_blocking_materializing_op(upstream_map) is False
        assert resource_manager._feeds_blocking_materializing_op(upstream_map) is True
        assert resource_manager._feeds_blocking_materializing_op(map1) is False

        # Both thresholds set, the map overshoots its 100-byte reservation
        # (200 > 1.5 * 100), and the execution is under pressure (900/1000 > 0.8).
        alloc._object_store_reservation_overshoot_ratio = 1.5
        alloc._object_store_memory_pressure_fraction = 0.8
        alloc._op_reserved[upstream_map] = ExecutionResources(
            cpu=1, gpu=0, object_store_memory=80
        )
        alloc._reserved_for_op_outputs[upstream_map] = 20.0
        resource_manager._op_usages[upstream_map] = ExecutionResources(
            object_store_memory=200
        )
        resource_manager._global_limits = ExecutionResources(object_store_memory=1000)
        resource_manager._global_limits_last_update_time = time.time()
        resource_manager._global_usage = ExecutionResources(object_store_memory=900)

        # Every numbered condition holds, so only the exemption spares the map.
        assert alloc._is_execution_object_store_under_pressure() is True
        assert not alloc._is_op_overshooting_object_store_reservation(upstream_map)

        # Confirm the exemption is load-bearing: without it the same state throttles.
        resource_manager._feeds_blocking_materializing_op = MagicMock(
            return_value=False
        )
        assert alloc._is_op_overshooting_object_store_reservation(upstream_map)

    def test_update_budgets_splits_shared_evenly_when_op_overshooting(
        self, restore_data_context
    ):
        """The throttle withholds object-store only, and splits it evenly.

        Two properties are asserted together because they trade off against each
        other. The overshooting op must keep its CPU share -- starving it of CPU
        would stop it draining what it already holds, the opposite of the intent.
        And object-store must still divide *evenly* among the remaining ops: the
        object-store dimension needs its own participant count, since reusing the
        full set's divisor leaves the pool un-shrunk on the throttled op's turn and
        skews the split toward upstream.
        """
        resource_manager, _, (o2, o3, o4) = _build_reservation_allocator(3)
        alloc = resource_manager.op_resource_allocator
        eligible = [o2, o3, o4]

        # Drive the shared-allocation loop directly: bypass reservation
        # recomputation and give every op an equal, generous reservation so no
        # borrow/cap fires. Zero usage keeps ``remaining_shared == _total_shared``.
        alloc._update_reservation = MagicMock(return_value=ExecutionResources.zero())
        alloc._get_eligible_ops = MagicMock(return_value=eligible)
        alloc._total_shared = ExecutionResources(cpu=900, object_store_memory=900)
        resource_manager.get_mem_op_internal = MagicMock(return_value=0)
        resource_manager.get_mem_op_outputs = MagicMock(return_value=0)
        resource_manager.get_op_usage = MagicMock(
            return_value=ExecutionResources.zero()
        )
        for op in eligible:
            alloc._op_reserved[op] = ExecutionResources(object_store_memory=100)
            alloc._reserved_for_op_outputs[op] = 0.0
            op.min_scheduling_resources = MagicMock(
                return_value=ExecutionResources.zero()
            )

        # The middle op is over its reservation → loses its object-store share.
        alloc._is_op_overshooting_object_store_reservation = MagicMock(
            side_effect=lambda op: op is o3
        )

        alloc.update_budgets(limits=ExecutionResources.zero())

        # The two non-throttled ops split the 900 object-store pool evenly (450
        # each on top of their 100 reservation); a divisor that counted the
        # throttled op would give upstream o2 more than downstream o4.
        assert alloc._op_budgets[o2].object_store_memory == 550
        assert alloc._op_budgets[o4].object_store_memory == 550
        # The overshooting op keeps only its object-store reservation...
        assert alloc._op_budgets[o3].object_store_memory == 100
        # ...but still gets its full, equal share of CPU: the throttle is an
        # object-store policy and must not starve the op of the CPU it needs to
        # drain what it holds.
        assert alloc._op_budgets[o3].cpu == 300
        assert alloc._op_budgets[o2].cpu == 300
        assert alloc._op_budgets[o4].cpu == 300

    def test_borrowing_cannot_hand_object_store_back_to_throttled_op(
        self, restore_data_context
    ):
        """Borrowing may top up a throttled op's CPU, but never its object store.

        When an op's share leaves it below ``min_scheduling_resources`` the loop
        borrows the shortfall from the pool. For a throttled op that shortfall
        includes the object-store budget just withheld, so the throttle has to be
        re-applied after borrowing or it is silently undone.
        """
        resource_manager, _, (o2, o3) = _build_reservation_allocator(2)
        alloc = resource_manager.op_resource_allocator
        eligible = [o2, o3]

        alloc._update_reservation = MagicMock(return_value=ExecutionResources.zero())
        alloc._get_eligible_ops = MagicMock(return_value=eligible)
        alloc._total_shared = ExecutionResources(cpu=900, object_store_memory=900)
        resource_manager.get_mem_op_internal = MagicMock(return_value=0)
        resource_manager.get_mem_op_outputs = MagicMock(return_value=0)
        resource_manager.get_op_usage = MagicMock(
            return_value=ExecutionResources.zero()
        )
        for op in eligible:
            # A small reservation, so the op lands below the minimum below.
            alloc._op_reserved[op] = ExecutionResources(object_store_memory=10)
            alloc._reserved_for_op_outputs[op] = 0.0
            op.min_scheduling_resources = MagicMock(
                return_value=ExecutionResources.zero()
            )
        # o3 needs 50 bytes to schedule but its share was zeroed, so borrowing
        # would try to make up the 40-byte difference.
        o3.min_scheduling_resources = MagicMock(
            return_value=ExecutionResources(object_store_memory=50)
        )
        alloc._is_op_overshooting_object_store_reservation = MagicMock(
            side_effect=lambda op: op is o3
        )

        alloc.update_budgets(limits=ExecutionResources.zero())

        # Still just its reservation: the borrow was not allowed to restore the
        # withheld object-store budget.
        assert alloc._op_budgets[o3].object_store_memory == 10
        # CPU was still shared with it.
        assert alloc._op_budgets[o3].cpu == 450

    def test_update_budgets_withholds_leftover_from_overshooting_op(
        self, restore_data_context
    ):
        """Leftover shared resources must skip an overshooting op.

        After the main split, resources left over by a capped op are handed to the
        most downstream *uncapped* op. That scan has to skip throttled ops: the
        most downstream uncapped op here is overshooting, so an unfiltered scan
        would hand it the leftover and undo the throttle the split just applied.

        Unlike the sibling test above, this drives the *real*
        ``_is_op_overshooting_object_store_reservation`` rather than mocking it,
        so the predicate and the allocation loop are covered together.
        """
        resource_manager, _, (o2, o3, o4, o5) = _build_reservation_allocator(4)
        alloc = resource_manager.op_resource_allocator
        eligible = [o2, o3, o4, o5]

        alloc._update_reservation = MagicMock(return_value=ExecutionResources.zero())
        alloc._get_eligible_ops = MagicMock(return_value=eligible)
        alloc._total_shared = ExecutionResources(object_store_memory=900)
        resource_manager.get_mem_op_internal = MagicMock(return_value=0)
        resource_manager.get_mem_op_outputs = MagicMock(return_value=0)
        for op in eligible:
            alloc._op_reserved[op] = ExecutionResources(object_store_memory=100)
            alloc._reserved_for_op_outputs[op] = 0.0
            op.min_scheduling_resources = MagicMock(
                return_value=ExecutionResources.zero()
            )
            # Uncapped, so each is a candidate to receive the leftover.
            op.min_max_resource_requirements = MagicMock(
                return_value=(ExecutionResources.zero(), ExecutionResources.inf())
            )
        # Cap the most *upstream* op at its reservation. It is iterated last with
        # divisor 1, so without a cap it would absorb the whole remainder and
        # there would be no leftover to distribute.
        o2.min_max_resource_requirements = MagicMock(
            return_value=(
                ExecutionResources.zero(),
                ExecutionResources(object_store_memory=100),
            )
        )

        # Only o5 overshoots: 200 > 1.5 * 100. The loop reads usage through
        # `get_mem_op_*` (mocked to 0 above), so this usage only drives the
        # predicate, leaving every op's reserved budget intact at 100.
        resource_manager.get_op_usage = MagicMock(
            side_effect=lambda op, include_ineligible_downstream=False: (
                ExecutionResources(object_store_memory=200)
                if op is o5
                else ExecutionResources.zero()
            )
        )
        alloc._object_store_reservation_overshoot_ratio = 1.5
        alloc._object_store_memory_pressure_fraction = 0.8
        resource_manager._global_limits = ExecutionResources(object_store_memory=1000)
        resource_manager._global_limits_last_update_time = time.time()
        resource_manager._global_usage = ExecutionResources(object_store_memory=900)

        # o5 is the most downstream uncapped op, so it would win an unfiltered scan.
        assert alloc._is_op_overshooting_object_store_reservation(o5) is True
        assert alloc._is_op_overshooting_object_store_reservation(o4) is False

        alloc.update_budgets(limits=ExecutionResources.zero())

        # Sliding split over the three survivors [o4, o3, o2]: o4 takes 900/3=300,
        # o3 takes 600/2=300, o2 is capped to 0 -- leaving 300 over.
        assert alloc._op_budgets[o3].object_store_memory == 400
        assert alloc._op_budgets[o2].object_store_memory == 100
        # The 300 leftover goes to o4, the most downstream *non-overshooting* op.
        assert alloc._op_budgets[o4].object_store_memory == 700
        # o5 stays at its bare reservation: no share, and no leftover either.
        assert alloc._op_budgets[o5].object_store_memory == 100

    def test_leftover_skips_capped_ops(self, restore_data_context):
        """The leftover recipient must respect resource caps.

        Handing the remainder to a capped op would push its allocation past the
        very cap that produced the remainder.
        """
        resource_manager, _, (o2, o3, o4) = _build_reservation_allocator(3)
        alloc = resource_manager.op_resource_allocator
        eligible = [o2, o3, o4]

        alloc._update_reservation = MagicMock(return_value=ExecutionResources.zero())
        alloc._get_eligible_ops = MagicMock(return_value=eligible)
        alloc._total_shared = ExecutionResources(cpu=900)
        resource_manager.get_mem_op_internal = MagicMock(return_value=0)
        resource_manager.get_mem_op_outputs = MagicMock(return_value=0)
        resource_manager.get_op_usage = MagicMock(
            return_value=ExecutionResources.zero()
        )
        for op in eligible:
            alloc._op_reserved[op] = ExecutionResources.zero()
            alloc._reserved_for_op_outputs[op] = 0.0
            op.min_scheduling_resources = MagicMock(
                return_value=ExecutionResources.zero()
            )
        # o4 is most downstream and o2 most upstream; capping both leaves a
        # remainder that only the uncapped middle op may receive.
        for op in (o2, o4):
            op.min_max_resource_requirements = MagicMock(
                return_value=(ExecutionResources.zero(), ExecutionResources.zero())
            )
        o3.min_max_resource_requirements = MagicMock(
            return_value=(ExecutionResources.zero(), ExecutionResources.inf())
        )

        alloc.update_budgets(limits=ExecutionResources.zero())

        # o3 takes its 450 share plus the 450 the capped ops left behind.
        assert alloc._op_budgets[o3].cpu == 900
        # The capped ops stay at their cap rather than absorbing the leftover.
        assert alloc._op_budgets[o2].cpu == 0
        assert alloc._op_budgets[o4].cpu == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
