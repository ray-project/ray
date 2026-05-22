import math
import time
from datetime import timedelta
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

from ray.data._internal.compute import ComputeStrategy
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
    ResourceManager,
    create_resource_allocator,
)
from ray.data._internal.execution.streaming_executor_state import (
    IdleDetector,
    OutputBackpressureGuard,
    build_streaming_topology,
)
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa


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
    op.start(ExecutionOptions())
    return op


def mock_union_op(input_ops):
    op = UnionOperator(
        DataContext.get_current(),
        *input_ops,
    )
    op.start = MagicMock(side_effect=lambda _: None)
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

    op.start = MagicMock(side_effect=lambda _: None)
    return op


def mock_all_to_all_op(input_op, name="MockShuffle"):
    """Create a mock AllToAllOperator (shuffle) for testing."""
    op = AllToAllOperator(
        bulk_fn=MagicMock(),
        input_op=input_op,
        data_context=DataContext.get_current(),
        name=name,
    )
    op.start(ExecutionOptions())
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
    )


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
        """Test calculating op_usage.

        Object-store memory is now tracked via BlockRefCounter (one counter shared
        across the whole execution), NOT via queue-size metrics.  Each operator's
        object-store usage = pending_task_outputs (in-flight generator buffer) +
        block_ref_counter bytes attributed to that operator.
        """
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)
        topo = build_streaming_topology(o3, ExecutionOptions())

        mock_cpu = {
            o1: 0,
            o2: 5,
            o3: 8,
        }
        # Bytes in the streaming-generator buffer (not yet yielded as ObjectRefs).
        mock_pending_task_outputs = {
            o1: 0,
            o2: 100,
            o3: 200,
        }
        # Bytes attributed to each operator in the BlockRefCounter (produced ObjectRefs
        # that are still live somewhere in the pipeline).
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
        )
        resource_manager._op_resource_allocator = None

        # Seed the counter so each eligible op reports its expected counter bytes.
        for op in [o2, o3]:
            if mock_counter_bytes[op]:
                resource_manager.block_ref_counter._bytes_by_producer[
                    op.id
                ] = mock_counter_bytes[op]

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
        """Object-store usage is tracked via BlockRefCounter, not queue metrics.

        Memory is attributed to the operator that called on_block_produced and
        released automatically via the Ray Core out-of-scope callback.  This test
        drives the counter directly (bypassing Ray Core) to verify the integration
        between ResourceManager._estimate_object_store_memory_usage and the counter.
        """

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        topo = build_streaming_topology(o3, ExecutionOptions())
        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(return_value=ExecutionResources.zero()),
            DataContext.get_current(),
        )

        counter = resource_manager.block_ref_counter

        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 0
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # InputDataBuffer blocks are not registered — its usage stays 0.
        assert counter.get_object_store_memory_usage(o1.id) == 0

        # Simulate o2 producing a 100-byte block.
        fake_id1 = (1).to_bytes(28, "big")
        with counter._lock:
            counter._registered_ids.add(fake_id1)
            counter._bytes_by_producer[o2.id] += 100

        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 100
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # Simulate o3 producing a 200-byte block.
        fake_id2 = (2).to_bytes(28, "big")
        with counter._lock:
            counter._registered_ids.add(fake_id2)
            counter._bytes_by_producer[o3.id] += 200

        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o2).object_store_memory == 100
        assert resource_manager.get_op_usage(o3).object_store_memory == 200

        # Simulate Ray Core callback firing for o2's block (all refs dropped).
        with counter._lock:
            counter._registered_ids.discard(fake_id1)
            counter._bytes_by_producer[o2.id] -= 100

        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o2).object_store_memory == 0
        assert resource_manager.get_op_usage(o3).object_store_memory == 200

        # After clear(), all usage resets to 0.
        counter.clear()
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o2).object_store_memory == 0
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

    def test_union_no_double_counting(self, restore_data_context):
        """Blocks passing through UnionOperator are attributed to their original
        producer, not double-counted.

        UnionOperator is a passthrough — it never calls on_block_produced, so its
        block_ref_counter usage is always 0.  Blocks in Union's output queue are
        already captured by the upstream MapOperator's counter entry.  Global usage
        = sum of upstream producers only, without any inflation from Union.
        """

        o1 = InputDataBuffer(DataContext.get_current(), [])
        map_a = mock_map_op(o1, name="MapA")
        o2 = InputDataBuffer(DataContext.get_current(), [])
        map_b = mock_map_op(o2, name="MapB")
        union_op = mock_union_op([map_a, map_b])
        downstream = mock_map_op(union_op, name="Downstream")

        topo = build_streaming_topology(downstream, ExecutionOptions())
        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(return_value=ExecutionResources(object_store_memory=10_000)),
            DataContext.get_current(),
        )
        counter = resource_manager.block_ref_counter

        # Simulate map_a producing a 100-byte block and map_b producing 200 bytes.
        fake_a = (10).to_bytes(28, "big")
        fake_b = (20).to_bytes(28, "big")
        with counter._lock:
            counter._registered_ids.add(fake_a)
            counter._bytes_by_producer[map_a.id] += 100
            counter._registered_ids.add(fake_b)
            counter._bytes_by_producer[map_b.id] += 200

        resource_manager.update_usages()

        # map_a and map_b see their own bytes.
        assert resource_manager.get_op_usage(map_a).object_store_memory == 100
        assert resource_manager.get_op_usage(map_b).object_store_memory == 200

        # union_op itself has 0 bytes (it never calls on_block_produced).
        assert resource_manager.get_op_usage(union_op).object_store_memory == 0

        # Global usage = map_a (100) + map_b (200) only, not inflated by Union.
        # InputDataBuffer and downstream (no blocks yet) contribute 0.
        total_obj_store = resource_manager.get_global_usage().object_store_memory
        assert total_obj_store == 300

    def test_get_completed_ops_usage(self, restore_data_context):
        """Test that _get_completed_ops_usage returns total usage of completed ops."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = LimitOperator(1, o2, DataContext.get_current())
        o4 = mock_map_op(o3)
        o5 = mock_map_op(o4)

        o1.mark_execution_finished()
        o2.mark_execution_finished()

        topo = build_streaming_topology(o5, ExecutionOptions())

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

        o1.mark_execution_finished()
        o2.mark_execution_finished()
        o4.mark_execution_finished()
        o5.mark_execution_finished()
        o7.mark_execution_finished()

        topo = build_streaming_topology(o8, ExecutionOptions())

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
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])

        # Completed ops: o2, o5, o7
        # Downstream ineligible: o3 (LimitOperator after o2)
        # Total usage should be o2 + o3 + o5 + o7
        completed_ops_usage = resource_manager._get_completed_ops_usage()

        assert completed_ops_usage == ExecutionResources(cpu=8, object_store_memory=400)

    def test_external_consumer_bytes_attributed_to_terminal_operator(
        self, restore_data_context
    ):
        """External consumer bytes (e.g., iterator prefetch buffers) are charged
        to the terminal operator's object store usage, not as a global deduction."""
        cluster_resources = ExecutionResources(cpu=10, gpu=0, object_store_memory=1000)

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        o1.mark_execution_finished()
        o2.mark_execution_finished()

        topo = build_streaming_topology(o3, ExecutionOptions())
        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            lambda: cluster_resources,
            DataContext.get_current(),
        )

        for op in [o1, o2, o3]:
            op.current_logical_usage = MagicMock(return_value=ExecutionResources.zero())
            op.running_logical_usage = MagicMock(return_value=ExecutionResources.zero())
            op.pending_logical_usage = MagicMock(return_value=ExecutionResources.zero())

        assert resource_manager._op_resource_allocator is not None

        resource_manager.update_usages()
        baseline_terminal = resource_manager.get_op_usage(o3).object_store_memory
        baseline_upstream = resource_manager.get_op_usage(o2).object_store_memory

        def _available_pool_object_store():
            return (
                resource_manager.get_global_limits()
                .subtract(resource_manager._get_completed_ops_usage())
                .max(ExecutionResources.zero())
                .object_store_memory
            )

        pool_before = _available_pool_object_store()

        resource_manager.set_external_consumer_bytes(200)
        resource_manager.update_usages()

        assert (
            resource_manager.get_op_usage(o3).object_store_memory
            == baseline_terminal + 200
        )
        assert (
            resource_manager.get_op_usage(o2).object_store_memory == baseline_upstream
        )
        assert _available_pool_object_store() == pool_before

        resource_manager.set_external_consumer_bytes(0)
        resource_manager.update_usages()
        assert (
            resource_manager.get_op_usage(o3).object_store_memory == baseline_terminal
        )

        # Very large external bytes: terminal usage reflects them; update still succeeds.
        resource_manager.set_external_consumer_bytes(999999)
        resource_manager.update_usages()
        assert (
            resource_manager.get_op_usage(o3).object_store_memory
            == baseline_terminal + 999999
        )

    def test_set_external_consumer_bytes_rejects_negative(self, restore_data_context):
        resource_manager = _resource_manager_for_limits_only_test(
            ExecutionOptions(),
            MagicMock(return_value=ExecutionResources.zero()),
        )
        with pytest.raises(AssertionError):
            resource_manager.set_external_consumer_bytes(-1)

    def test_external_consumer_bytes_input_data_buffer_sink(self, restore_data_context):
        """When the execute DAG is only an InputDataBuffer, prefetch bytes still
        attach to that terminal sink instead of being dropped by the
        InputDataBuffer early return."""
        buf = InputDataBuffer(DataContext.get_current(), [])
        topo = build_streaming_topology(buf, ExecutionOptions())
        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            lambda: ExecutionResources(cpu=10, gpu=0, object_store_memory=1000),
            DataContext.get_current(),
        )
        buf.current_logical_usage = MagicMock(return_value=ExecutionResources.zero())
        buf.running_logical_usage = MagicMock(return_value=ExecutionResources.zero())
        buf.pending_logical_usage = MagicMock(return_value=ExecutionResources.zero())

        resource_manager.update_usages()
        assert resource_manager.get_op_usage(buf).object_store_memory == 0

        resource_manager.set_external_consumer_bytes(150)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(buf).object_store_memory == 150

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
            )

    def test_topology_rejects_empty_topology(self, restore_data_context):
        with pytest.raises(ValueError, match="topology must be non-empty"):
            ResourceManager(
                {},
                ExecutionOptions(),
                MagicMock(return_value=ExecutionResources.zero()),
                DataContext.get_current(),
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

        topo = build_streaming_topology(o5, ExecutionOptions())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
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

        topo2 = build_streaming_topology(o7, ExecutionOptions())
        resource_manager2 = ResourceManager(
            topo2,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
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

        topo = build_streaming_topology(o2, ExecutionOptions())
        options = ExecutionOptions()

        resource_manager = ResourceManager(
            topology=topo,
            options=options,
            get_total_resources=lambda: cluster_resources,
            data_context=DataContext.get_current(),
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

        topo = build_streaming_topology(o3, ExecutionOptions())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        guard = OutputBackpressureGuard(topo, resource_manager)

        # o2 is terminal (no downstream eligible ops beyond it) and no external
        # consumer — should unblock (e.g., write pipeline).
        assert guard.should_unblock(o2) is True

        # Add o4 operator - o2 is no longer terminal
        o4 = mock_map_op(o3)

        topo = build_streaming_topology(o4, ExecutionOptions())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
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

        topo = build_streaming_topology(o3, ExecutionOptions())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
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

        topo = build_streaming_topology(o3, ExecutionOptions())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
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

    def test_unblock_backpressure_fallback_to_idle_detector(self, restore_data_context):
        """When unblock conditions not met, falls back to idle detector result."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        topo = build_streaming_topology(o3, ExecutionOptions())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
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

        topo = build_streaming_topology(o3, ExecutionOptions())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
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


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
