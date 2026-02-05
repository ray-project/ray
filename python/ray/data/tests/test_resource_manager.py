import math
import time
from datetime import timedelta
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest
from freezegun import freeze_time

import ray
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
    OpResourceAllocator,
    ResourceManager,
)
from ray.data._internal.execution.streaming_executor_state import (
    build_streaming_topology,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.context import MAX_SAFE_BLOCK_SIZE_FACTOR, DataContext
from ray.data.tests.conftest import *  # noqa


def mock_map_op(
    input_op: PhysicalOperator,
    ray_remote_args: Optional[Dict[str, Any]] = None,
    compute_strategy: Optional[ComputeStrategy] = None,
    incremental_resource_usage: Optional[ExecutionResources] = None,
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
    if incremental_resource_usage is not None:
        op.incremental_resource_usage = MagicMock(
            return_value=incremental_resource_usage
        )
    return op


def mock_union_op(
    input_ops,
    incremental_resource_usage=None,
):
    op = UnionOperator(
        DataContext.get_current(),
        *input_ops,
    )
    op.start = MagicMock(side_effect=lambda _: None)
    if incremental_resource_usage is not None:
        op.incremental_resource_usage = MagicMock(
            return_value=incremental_resource_usage
        )
    return op


def mock_join_op(
    left_input_op,
    right_input_op,
    incremental_resource_usage=None,
):
    left_input_op._logical_operators = [(MagicMock())]
    right_input_op._logical_operators = [(MagicMock())]

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
    if incremental_resource_usage is not None:
        op.incremental_resource_usage = MagicMock(
            return_value=incremental_resource_usage
        )
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
        resource_manager = ResourceManager(
            MagicMock(), options, get_total_resources, DataContext.get_current()
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
        resource_manager = ResourceManager(
            MagicMock(), options, get_total_resources, DataContext.get_current()
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
        resource_manager = ResourceManager(
            MagicMock(), options, get_total_resources, DataContext.get_current()
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
            resource_manager = ResourceManager(
                MagicMock(),
                ExecutionOptions(),
                get_total_resources,
                DataContext.get_current(),
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
        topo = build_streaming_topology(o3, ExecutionOptions())

        # Mock different metrics that contribute to the resource usage.
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
        mock_internal_outqueue = {
            o1: 0,
            o2: 300,
            o3: 400,
        }
        mock_external_outqueue_sizes = {
            o1: 100,
            o2: 500,
            o3: 600,
        }
        mock_internal_inqueue = {
            o1: 0,
            o2: 700,
            o3: 800,
        }
        mock_pending_task_inputs = {
            o1: 0,
            o2: 900,
            o3: 1000,
        }

        for op in [o1, o2, o3]:
            op.update_resource_usage = MagicMock()
            op.current_processor_usage = MagicMock(
                return_value=ExecutionResources(cpu=mock_cpu[op], gpu=0)
            )
            op.running_processor_usage = MagicMock(
                return_value=ExecutionResources(cpu=mock_cpu[op], gpu=0)
            )
            op.pending_processor_usage = MagicMock(
                return_value=ExecutionResources.zero()
            )
            op.extra_resource_usage = MagicMock(return_value=ExecutionResources.zero())
            op._metrics = MagicMock(
                obj_store_mem_pending_task_outputs=mock_pending_task_outputs[op],
                obj_store_mem_internal_outqueue=mock_internal_outqueue[op],
                obj_store_mem_internal_inqueue=mock_internal_inqueue[op],
                obj_store_mem_pending_task_inputs=mock_pending_task_inputs[op],
            )
            ref_bundle = MagicMock(
                size_bytes=MagicMock(return_value=mock_external_outqueue_sizes[op])
            )
            topo[op].add_output(ref_bundle)

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        resource_manager._op_resource_allocator = None
        resource_manager.update_usages()

        global_cpu = 0
        global_mem = 0
        for op in [o1, o2, o3]:
            if op == o1:
                # Resource usage of InputDataBuffer doesn't count.
                expected_mem = 0
            else:
                expected_mem = (
                    mock_pending_task_outputs[op]
                    + mock_internal_outqueue[op]
                    + mock_external_outqueue_sizes[op]
                )
                for next_op in op.output_dependencies:
                    expected_mem += (
                        +mock_internal_inqueue[next_op]
                        + mock_pending_task_inputs[next_op]
                    )
            op_usage = resource_manager.get_op_usage(op)
            assert op_usage.cpu == mock_cpu[op]
            assert op_usage.gpu == 0
            assert op_usage.object_store_memory == expected_mem
            if op != o1:
                # _mem_op_internal only includes pending_task_outputs
                assert (
                    resource_manager._mem_op_internal[op]
                    == mock_pending_task_outputs[op]
                )
                assert (
                    resource_manager._mem_op_outputs[op]
                    == expected_mem - resource_manager._mem_op_internal[op]
                )
            global_cpu += mock_cpu[op]
            global_mem += expected_mem

        assert resource_manager.get_global_usage() == ExecutionResources(
            global_cpu, 0, global_mem
        )

    def test_object_store_usage(self, restore_data_context):
        input = make_ref_bundles([[x] for x in range(1)])[0]
        input.size_bytes = MagicMock(return_value=1)

        o1 = InputDataBuffer(DataContext.get_current(), [input])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        topo = build_streaming_topology(o3, ExecutionOptions())
        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(return_value=ExecutionResources.zero()),
            DataContext.get_current(),
        )
        ray.data.DataContext.get_current()._max_num_blocks_in_streaming_gen_buffer = 1
        ray.data.DataContext.get_current().target_max_block_size = 2

        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 0
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # Objects in an operator's internal inqueue typically count toward the previous
        # operator's object store memory usage. However, data from an
        # `InputDataBuffer` aren't counted because they were created outside of this
        # execution.
        o2.metrics.on_input_queued(input)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 0
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # During no-sample phase, obj_store_mem_pending_task_outputs uses fallback
        # estimate based on target_max_block_size.
        o2.metrics.on_input_dequeued(input)
        o2.metrics.on_task_submitted(0, input)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        # No sample available yet, uses fallback: target_max_block_size * factor * buffer
        ctx = ray.data.DataContext.get_current()
        expected_pending_output = (
            ctx.target_max_block_size
            * MAX_SAFE_BLOCK_SIZE_FACTOR
            * ctx._max_num_blocks_in_streaming_gen_buffer
        )
        assert o2.metrics.obj_store_mem_pending_task_outputs == expected_pending_output
        op2_usage = resource_manager.get_op_usage(o2).object_store_memory
        assert op2_usage == expected_pending_output
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # When the task finishes, we move the data from the streaming generator to the
        # operator's internal outqueue.
        o2.metrics.on_output_queued(input)
        o2.metrics.on_task_finished(0, None)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 1
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        o2.metrics.on_output_dequeued(input)
        topo[o2].output_queue.append(input)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 1
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # Objects in the current operator's internal inqueue count towards the previous
        # operator's object store memory usage.
        o3.metrics.on_input_queued(topo[o2].output_queue.pop())
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 1
        assert resource_manager.get_op_usage(o3).object_store_memory == 0

        # Task inputs count toward the previous operator's object store memory
        # usage. During no-sample phase, pending task outputs uses fallback estimate.
        o3.metrics.on_input_dequeued(input)
        o3.metrics.on_task_submitted(0, input)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 1
        # No sample available yet, uses fallback estimate
        assert o3.metrics.obj_store_mem_pending_task_outputs == expected_pending_output
        op3_usage = resource_manager.get_op_usage(o3).object_store_memory
        assert op3_usage == expected_pending_output

        # Task inputs no longer count once the task is finished.
        o3.metrics.on_output_queued(input)
        o3.metrics.on_task_finished(0, None)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 0
        assert resource_manager.get_op_usage(o3).object_store_memory == 1

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
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
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
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])

        # Completed ops: o2, o5, o7
        # Downstream ineligible: o3 (LimitOperator after o2)
        # Total usage should be o2 + o3 + o5 + o7
        completed_ops_usage = resource_manager._get_completed_ops_usage()

        assert completed_ops_usage == ExecutionResources(cpu=8, object_store_memory=400)

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
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
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
            topo2, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )

        # o5's downstream (o6, o7) has no blocking materializing ops
        assert resource_manager2._is_blocking_materializing_op(o5) is False
        assert resource_manager2._is_blocking_materializing_op(o7) is False


class TestResourceAllocatorUnblockingStreamingOutputBackpressure:
    """Tests for OpResourceAllocator._should_unblock_streaming_output_backpressure."""

    def test_unblock_backpressure_terminal_operator(self, restore_data_context):
        """Terminal operator (no downstream) should always unblock."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)

        topo = build_streaming_topology(o2, ExecutionOptions())

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        allocator = resource_manager._op_resource_allocator

        # o2 is terminal (no output_dependencies), should always unblock
        assert allocator._should_unblock_streaming_output_backpressure(o2) is True

        # Add o3 operator - o2 is no longer terminal
        o3 = mock_map_op(o2)

        topo = build_streaming_topology(o3, ExecutionOptions())

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        allocator = resource_manager._op_resource_allocator

        # Mock downstream (o3) has active tasks and input blocks (ie unblocking
        # conditions not met)
        o3.num_active_tasks = MagicMock(return_value=1)
        allocator._idle_detector.detect_idle = MagicMock(return_value=False)

        # o2 is not terminal anymore, falls back to idle detector which returns False
        assert allocator._should_unblock_streaming_output_backpressure(o2) is False

    def test_unblock_backpressure_downstream_idle(self, restore_data_context):
        """Unblock when downstream is idle (no active tasks) to maintain liveness."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        topo = build_streaming_topology(o3, ExecutionOptions())

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        allocator = resource_manager._op_resource_allocator
        o3.num_active_tasks = MagicMock(return_value=0)

        # Case 1: Downstream cannot submit (resource constrained) - unblock to free resources
        allocator.can_submit_new_task = MagicMock(return_value=False)
        assert allocator._should_unblock_streaming_output_backpressure(o2) is True

        # Case 2: Downstream can submit but has no input blocks - unblock to produce data
        allocator.can_submit_new_task = MagicMock(return_value=True)
        topo[o3].total_enqueued_input_blocks = MagicMock(return_value=0)
        assert allocator._should_unblock_streaming_output_backpressure(o2) is True

    def test_unblock_backpressure_fallback_to_idle_detector(self, restore_data_context):
        """When unblock conditions not met, falls back to idle detector result."""
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1)
        o3 = mock_map_op(o2)

        topo = build_streaming_topology(o3, ExecutionOptions())

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        allocator = resource_manager._op_resource_allocator

        # Case: Downstream has active tasks - falls back to idle detector
        o3.num_active_tasks = MagicMock(return_value=2)
        allocator._idle_detector.detect_idle = MagicMock(return_value=False)
        assert allocator._should_unblock_streaming_output_backpressure(o2) is False

        # Case: Idle detector returns True - should unblock
        allocator._idle_detector.detect_idle = MagicMock(return_value=True)
        assert allocator._should_unblock_streaming_output_backpressure(o2) is True

        # Case: Downstream has no active tasks but has input blocks - falls back to idle detector
        allocator.can_submit_new_task = MagicMock(return_value=True)
        o3.num_active_tasks = MagicMock(return_value=0)
        topo[o3].total_enqueued_input_blocks = MagicMock(return_value=5)
        allocator._idle_detector.detect_idle = MagicMock(return_value=False)
        assert allocator._should_unblock_streaming_output_backpressure(o2) is False


class TestIdleDetector:
    """Tests for OpResourceAllocator.IdleDetector."""

    def test_idle_detector(self, restore_data_context):
        """Test IdleDetector behavior through its public interface."""
        idle_detector = OpResourceAllocator.IdleDetector()
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
