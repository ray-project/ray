import math
import time
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

import ray
from ray.data._internal.execution.interfaces.execution_options import (
    ExecutionOptions,
    ExecutionResources,
)
from ray.data._internal.execution.operators.input_data_buffer import InputDataBuffer
from ray.data._internal.execution.operators.limit_operator import LimitOperator
from ray.data._internal.execution.operators.map_operator import MapOperator
from ray.data._internal.execution.resource_manager import (
    ReservationOpResourceAllocator,
    ResourceManager,
)
from ray.data._internal.execution.streaming_executor_state import (
    build_streaming_topology,
)
from ray.data._internal.execution.util import make_ref_bundles
from ray.data.context import DataContext
from ray.data.tests.conftest import *  # noqa


def mock_map_op(
    input_op,
    ray_remote_args=None,
    compute_strategy=None,
    incremental_resource_usage=None,
):
    op = MapOperator.create(
        MagicMock(),
        input_op,
        DataContext.get_current(),
        ray_remote_args=ray_remote_args or {},
        compute_strategy=compute_strategy,
    )
    op.start = MagicMock(side_effect=lambda _: None)
    if incremental_resource_usage is not None:
        op.incremental_resource_usage = MagicMock(
            return_value=incremental_resource_usage
        )
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
        topo, _ = build_streaming_topology(o3, ExecutionOptions())

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
            op.current_processor_usage = MagicMock(
                return_value=ExecutionResources(cpu=mock_cpu[op], gpu=0)
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
                assert (
                    resource_manager._mem_op_internal[op]
                    == mock_pending_task_outputs[op] + mock_internal_outqueue[op]
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

        topo, _ = build_streaming_topology(o3, ExecutionOptions())
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

        # Operators estimate pending task outputs using the target max block size.
        # In this case, the target max block size is 2 and there is at most 1 block
        # in the streaming generator buffer, so the estimated usage is 2.
        o2.metrics.on_input_dequeued(input)
        o2.metrics.on_task_submitted(0, input)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 2
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

        # Task inputs count toward the previous operator's object store memory usage,
        # and task outputs count toward the current operator's object store memory
        # usage.
        o3.metrics.on_input_dequeued(input)
        o3.metrics.on_task_submitted(0, input)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 1
        assert resource_manager.get_op_usage(o3).object_store_memory == 2

        # Task inputs no longer count once the task is finished.
        o3.metrics.on_output_queued(input)
        o3.metrics.on_task_finished(0, None)
        resource_manager.update_usages()
        assert resource_manager.get_op_usage(o1).object_store_memory == 0
        assert resource_manager.get_op_usage(o2).object_store_memory == 0
        assert resource_manager.get_op_usage(o3).object_store_memory == 1


class TestReservationOpResourceAllocator:
    """Tests for ReservationOpResourceAllocator."""

    def test_basic(self, restore_data_context):
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1, incremental_resource_usage=ExecutionResources(1, 0, 15))
        o3 = mock_map_op(o2, incremental_resource_usage=ExecutionResources(1, 0, 10))
        o4 = LimitOperator(1, o3, DataContext.get_current())

        op_usages = {op: ExecutionResources.zero() for op in [o1, o2, o3, o4]}
        op_internal_usage = dict.fromkeys([o1, o2, o3, o4], 0)
        op_outputs_usages = dict.fromkeys([o1, o2, o3, o4], 0)

        topo, _ = build_streaming_topology(o4, ExecutionOptions())

        global_limits = ExecutionResources.zero()

        def mock_get_global_limits():
            nonlocal global_limits
            return global_limits

        def can_submit_new_task(allocator, op):
            """Helper to check if operator can submit new tasks based on budget."""
            budget = allocator.get_budget(op)
            if budget is None:
                return True
            return op.incremental_resource_usage().satisfies_limit(budget)

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager._mem_op_internal = op_internal_usage
        resource_manager._mem_op_outputs = op_outputs_usages

        resource_manager.get_global_limits = MagicMock(
            side_effect=mock_get_global_limits
        )

        assert resource_manager.op_resource_allocator_enabled()
        allocator = resource_manager._op_resource_allocator
        assert isinstance(allocator, ReservationOpResourceAllocator)

        # Test initial state when no resources are used.
        global_limits = ExecutionResources(cpu=16, gpu=0, object_store_memory=1000)
        allocator.update_usages()
        # +-----+------------------+------------------+--------------+
        # |     | _op_reserved     | _reserved_for    | used shared  |
        # |     | (used/remaining) | _op_outputs      | resources    |
        # |     |                  | (used/remaining) |              |
        # +-----+------------------+------------------+--------------+
        # | op2 | 0/125            | 0/125            | 0            |
        # +-----+------------------+------------------+--------------+
        # | op3 | 0/125            | 0/125            | 0            |
        # +-----+------------------+------------------+--------------+
        # o1 and o4 are not handled.
        assert o1 not in allocator._op_reserved
        assert o4 not in allocator._op_reserved
        assert o1 not in allocator._op_budgets
        assert o4 not in allocator._op_budgets
        # Test reserved resources for o2 and o3.
        assert allocator._op_reserved[o2] == ExecutionResources(4, 0, 125)
        assert allocator._op_reserved[o3] == ExecutionResources(4, 0, 125)
        assert allocator._reserved_for_op_outputs[o2] == 125
        assert allocator._reserved_for_op_outputs[o3] == 125
        # 50% of the global limits are shared.
        assert allocator._total_shared == ExecutionResources(8, 0, 500)
        # Test budgets.
        assert allocator._op_budgets[o2] == ExecutionResources(8, float("inf"), 375)
        assert allocator._op_budgets[o3] == ExecutionResources(8, float("inf"), 375)
        # Test can_submit_new_task and max_task_output_bytes_to_read.
        assert can_submit_new_task(allocator, o2)
        assert can_submit_new_task(allocator, o3)
        assert allocator.max_task_output_bytes_to_read(o2) == 500
        assert allocator.max_task_output_bytes_to_read(o3) == 500

        # Test when each operator uses some resources.
        op_usages[o2] = ExecutionResources(6, 0, 500)
        op_internal_usage[o2] = 400
        op_outputs_usages[o2] = 100
        op_usages[o3] = ExecutionResources(2, 0, 125)
        op_internal_usage[o3] = 30
        op_outputs_usages[o3] = 25
        op_usages[o4] = ExecutionResources(0, 0, 50)

        allocator.update_usages()
        # +-----+------------------+------------------+--------------+
        # |     | _op_reserved     | _reserved_for    | used shared  |
        # |     | (used/remaining) | _op_outputs      | resources    |
        # |     |                  | (used/remaining) |              |
        # +-----+------------------+------------------+--------------+
        # | op2 | 125/0            | 100/25           | 400-125=275  |
        # +-----+------------------+------------------+--------------+
        # | op3 | 30/95            | (25+50)/50       | 0            |
        # +-----+------------------+------------------+--------------+
        # remaining shared = 1000/2 - 275 = 225
        # Test budgets.
        # memory_budget[o2] = 0 + 225/2 = 112.5
        assert allocator._op_budgets[o2] == ExecutionResources(3, float("inf"), 112.5)
        # memory_budget[o3] = 95 + 225/2 = 207.5
        assert allocator._op_budgets[o3] == ExecutionResources(5, float("inf"), 207.5)
        # Test can_submit_new_task and max_task_output_bytes_to_read.
        assert can_submit_new_task(allocator, o2)
        assert can_submit_new_task(allocator, o3)
        # max_task_output_bytes_to_read(o2) = 112.5 + 25 = 137.5
        # (will be rounded down).
        assert allocator.max_task_output_bytes_to_read(o2) == 137
        # max_task_output_bytes_to_read(o3) = 207.5 + 50 = 257.5
        # (will be rounded down).
        assert allocator.max_task_output_bytes_to_read(o3) == 257

        # Test global_limits updated.
        global_limits = ExecutionResources(cpu=12, gpu=0, object_store_memory=800)
        allocator.update_usages()
        # +-----+------------------+------------------+--------------+
        # |     | _op_reserved     | _reserved_for    | used shared  |
        # |     | (used/remaining) | _op_outputs      | resources    |
        # |     |                  | (used/remaining) |              |
        # +-----+------------------+------------------+--------------+
        # | op2 | 100/0            | 100/0            | 400-100=300  |
        # +-----+------------------+------------------+--------------+
        # | op3 | 30/70            | (25+50)/25       | 0            |
        # +-----+------------------+------------------+--------------+
        # remaining shared = 800/2 - 300 = 100
        # Test reserved resources for o2 and o3.
        assert allocator._op_reserved[o2] == ExecutionResources(3, 0, 100)
        assert allocator._op_reserved[o3] == ExecutionResources(3, 0, 100)
        assert allocator._reserved_for_op_outputs[o2] == 100
        assert allocator._reserved_for_op_outputs[o3] == 100
        # 50% of the global limits are shared.
        assert allocator._total_shared == ExecutionResources(6, 0, 400)

        # Test budgets.
        # memory_budget[o2] = 0 + 100/2 = 50
        assert allocator._op_budgets[o2] == ExecutionResources(1.5, float("inf"), 50)
        # memory_budget[o3] = 70 + 100/2 = 120
        assert allocator._op_budgets[o3] == ExecutionResources(2.5, float("inf"), 120)
        # Test can_submit_new_task and max_task_output_bytes_to_read.
        assert can_submit_new_task(allocator, o2)
        assert can_submit_new_task(allocator, o3)
        # max_task_output_bytes_to_read(o2) = 50 + 0 = 50
        assert allocator.max_task_output_bytes_to_read(o2) == 50
        # max_task_output_bytes_to_read(o3) = 120 + 25 = 145
        assert allocator.max_task_output_bytes_to_read(o3) == 145

    def test_reserve_incremental_resource_usage(self, restore_data_context):
        """Test that we'll reserve at least incremental_resource_usage()
        for each operator."""
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        global_limits = ExecutionResources(cpu=7, gpu=0, object_store_memory=800)
        incremental_usage = ExecutionResources(cpu=3, gpu=0, object_store_memory=500)

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1, incremental_resource_usage=incremental_usage)
        o3 = mock_map_op(o2, incremental_resource_usage=incremental_usage)
        o4 = mock_map_op(o3, incremental_resource_usage=incremental_usage)
        o5 = mock_map_op(o4, incremental_resource_usage=incremental_usage)
        topo, _ = build_streaming_topology(o5, ExecutionOptions())

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        resource_manager.get_op_usage = MagicMock(
            return_value=ExecutionResources.zero()
        )
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        assert isinstance(allocator, ReservationOpResourceAllocator)

        allocator.update_usages()
        # incremental_usage should be reserved for o2.
        assert allocator._op_reserved[o2] == incremental_usage
        # Remaining resources are CPU = 7 - 3 = 4, object_store_memory = 800 - 500 = 300.
        # We have enough CPUs for o3's incremental_usage, but not enough
        # object_store_memory. We'll still reserve the incremental_usage by
        # oversubscribing object_store_memory.
        assert allocator._op_reserved[o3] == incremental_usage
        # Now the remaining resources are CPU = 4 - 3 = 1,
        # object_store_memory = 300 - 500 = -200.
        # We don't oversubscribing CPUs, we'll only reserve
        # incremental_usage.object_store_memory.
        assert allocator._op_reserved[o4] == ExecutionResources(
            0, 0, incremental_usage.object_store_memory
        )
        # Same for o5
        assert allocator._op_reserved[o5] == ExecutionResources(
            0, 0, incremental_usage.object_store_memory
        )
        assert allocator._total_shared == ExecutionResources(1, 0, 0)
        for op in [o2, o3, o4]:
            assert allocator._reserved_for_op_outputs[op] == 50

    @patch(
        "ray.data._internal.execution.interfaces.op_runtime_metrics.OpRuntimeMetrics."
        "obj_store_mem_max_pending_output_per_task",
        new_callable=PropertyMock(return_value=100),
    )
    def test_reserve_min_resources_for_gpu_ops(
        self, mock_property, restore_data_context
    ):
        """Test that we'll reserve enough resources for ActorPoolMapOperator
        that uses GPU."""
        global_limits = ExecutionResources(cpu=6, gpu=0, object_store_memory=1600)

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(
            o1,
            ray_remote_args={"num_cpus": 0, "num_gpus": 1},
            compute_strategy=ray.data.ActorPoolStrategy(size=8),
        )
        topo, _ = build_streaming_topology(o2, ExecutionOptions())

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        resource_manager.get_op_usage = MagicMock(
            return_value=ExecutionResources.zero()
        )
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        assert isinstance(allocator, ReservationOpResourceAllocator)

        allocator.update_usages()

        assert allocator._op_reserved[o2].object_store_memory == 800

    def test_does_not_reserve_more_than_max_resource_usage(self):
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = MapOperator.create(
            MagicMock(),
            o1,
            DataContext.get_current(),
        )
        o2.min_max_resource_requirements = MagicMock(
            return_value=(
                ExecutionResources(cpu=0, object_store_memory=0),
                ExecutionResources(cpu=1, object_store_memory=1),
            )
        )
        topo, _ = build_streaming_topology(o2, ExecutionOptions())
        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        resource_manager.get_op_usage = MagicMock(
            return_value=ExecutionResources.zero()
        )
        # Mock an extremely large cluster.
        resource_manager.get_global_limits = MagicMock(
            return_value=ExecutionResources(cpu=1024, object_store_memory=1024**4)
        )
        allocator = resource_manager._op_resource_allocator

        allocator.update_usages()

        # The operator's max resource usage is 1 CPU and 1 byte object store memory, so
        # we'll reserve that despite the large global limits.
        assert allocator._op_reserved[o2] == ExecutionResources(
            cpu=1, object_store_memory=1
        )

    def test_only_handle_eligible_ops(self, restore_data_context):
        """Test that we only handle non-completed map ops."""
        DataContext.get_current().op_resource_reservation_enabled = True

        input = make_ref_bundles([[x] for x in range(1)])
        o1 = InputDataBuffer(DataContext.get_current(), input)
        o2 = mock_map_op(o1)
        o3 = LimitOperator(1, o2, DataContext.get_current())
        topo, _ = build_streaming_topology(o3, ExecutionOptions())

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        resource_manager.get_op_usage = MagicMock(
            return_value=ExecutionResources.zero()
        )
        resource_manager.get_global_limits = MagicMock(
            return_value=ExecutionResources.zero()
        )

        assert resource_manager.op_resource_allocator_enabled()
        allocator = resource_manager._op_resource_allocator
        assert isinstance(allocator, ReservationOpResourceAllocator)

        allocator.update_usages()
        assert o1 not in allocator._op_budgets
        assert o2 in allocator._op_budgets
        assert o3 not in allocator._op_budgets

        o2.mark_execution_finished()
        allocator.update_usages()
        assert o2 not in allocator._op_budgets


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
