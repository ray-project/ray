from unittest.mock import MagicMock, patch, PropertyMock

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
from ray.data.tests.test_resource_manager import (
    mock_join_op,
    mock_map_op,
    mock_union_op,
)


class TestReservationOpResourceAllocator:
    """Tests for ReservationOpResourceAllocator."""

    @pytest.fixture(scope="class", autouse=True)
    def enable_reservation_based_resource_allocator(self):
        # Switch to V1
        with patch(
            "ray.data._internal.execution.DEFAULT_USE_OP_RESOURCE_ALLOCATOR_VERSION",
            new="V1",
        ):
            yield

    def test_basic(self, restore_data_context):
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        o1 = InputDataBuffer(DataContext.get_current(), [])
        # Use ray_remote_args to set CPU requirements instead of mocking
        o2 = mock_map_op(o1, ray_remote_args={"num_cpus": 1})
        o3 = mock_map_op(o2, ray_remote_args={"num_cpus": 1})
        o4 = LimitOperator(1, o3, DataContext.get_current())

        # Mock min_max_resource_requirements to return default unbounded behavior
        for op in [o2, o3]:
            op.min_max_resource_requirements = MagicMock(
                return_value=(ExecutionResources.zero(), ExecutionResources.inf())
            )

        op_usages = {op: ExecutionResources.zero() for op in [o1, o2, o3, o4]}
        op_internal_usage = dict.fromkeys([o1, o2, o3, o4], 0)
        op_outputs_usages = dict.fromkeys([o1, o2, o3, o4], 0)

        topo = build_streaming_topology(o4, ExecutionOptions())

        global_limits = ExecutionResources.zero()

        def mock_get_global_limits():
            nonlocal global_limits
            return global_limits

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
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
        allocator.update_budgets(
            limits=global_limits,
        )
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
        assert allocator._op_budgets[o2] == ExecutionResources(8, 0, 375)
        assert allocator._op_budgets[o3] == ExecutionResources(8, 0, 375)
        # Test max_task_output_bytes_to_read.
        assert allocator.max_task_output_bytes_to_read(o2) == 500
        assert allocator.max_task_output_bytes_to_read(o3) == 500
        # Test get_allocation.
        # Ineligible operators should return None.
        assert allocator.get_allocation(o1) is None
        assert allocator.get_allocation(o4) is None
        # allocation = op_reserved + op_shared = (4, 0, 125) + (4, 0, 250) = (8, 0, 375)
        # When usage is zero, allocation equals budget.
        assert allocator.get_allocation(o2) == ExecutionResources(8, 0, 375)
        assert allocator.get_allocation(o3) == ExecutionResources(8, 0, 375)

        # Test when each operator uses some resources.
        op_usages[o2] = ExecutionResources(6, 0, 500)
        op_internal_usage[o2] = 400
        op_outputs_usages[o2] = 100
        op_usages[o3] = ExecutionResources(2, 0, 125)
        op_internal_usage[o3] = 30
        op_outputs_usages[o3] = 25
        op_usages[o4] = ExecutionResources(0, 0, 50)

        allocator.update_budgets(
            limits=global_limits,
        )
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
        # memory_budget[o2] = 0 + 225/2 = 113 (rounded up)
        assert allocator._op_budgets[o2] == ExecutionResources(3, 0, 113)
        # memory_budget[o3] = 95 + 225/2 = 207 (rounded down)
        assert allocator._op_budgets[o3] == ExecutionResources(5, 0, 207)
        # Test max_task_output_bytes_to_read.
        # max_task_output_bytes_to_read(o2) = 112.5 + 25 = 138 (rounded up)
        assert allocator.max_task_output_bytes_to_read(o2) == 138
        # max_task_output_bytes_to_read(o3) = 207.5 + 50 = 257 (rounded down)
        assert allocator.max_task_output_bytes_to_read(o3) == 257
        # Test get_allocation.
        # allocation = budget + usage
        # budget[o2] = (3, 0, 113), budget[o3] = (5, 0, 207)
        # usage[o2] = (6, 0, 500), usage[o3] = (2, 0, 125)
        assert allocator.get_allocation(o2) == ExecutionResources(9, 0, 613)
        assert allocator.get_allocation(o3) == ExecutionResources(7, 0, 332)

        # Test global_limits updated.
        global_limits = ExecutionResources(cpu=12, gpu=0, object_store_memory=800)
        allocator.update_budgets(
            limits=global_limits,
        )
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
        assert allocator._op_budgets[o2] == ExecutionResources(1.5, 0, 50)
        # memory_budget[o3] = 70 + 100/2 = 120
        assert allocator._op_budgets[o3] == ExecutionResources(2.5, 0, 120)
        # Test max_task_output_bytes_to_read.
        # max_task_output_bytes_to_read(o2) = 50 + 0 = 50
        assert allocator.max_task_output_bytes_to_read(o2) == 50
        # max_task_output_bytes_to_read(o3) = 120 + 25 = 145
        assert allocator.max_task_output_bytes_to_read(o3) == 145
        # Test get_allocation.
        # allocation = budget + usage
        # budget[o2] = (1.5, 0, 50), budget[o3] = (2.5, 0, 120)
        # usage[o2] = (6, 0, 500), usage[o3] = (2, 0, 125)
        assert allocator.get_allocation(o2) == ExecutionResources(7.5, 0, 550)
        assert allocator.get_allocation(o3) == ExecutionResources(4.5, 0, 245)

    def test_reserve_min_resource_requirements(self, restore_data_context):
        """Test that we'll reserve at least min_resource_requirements
        for each operator."""
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        global_limits = ExecutionResources(cpu=7, gpu=0, object_store_memory=800)
        min_resources = ExecutionResources(cpu=3, gpu=0, object_store_memory=500)

        o1 = InputDataBuffer(DataContext.get_current(), [])
        # Use ray_remote_args to set CPU requirements
        o2 = mock_map_op(o1, ray_remote_args={"num_cpus": 3})
        o3 = mock_map_op(o2, ray_remote_args={"num_cpus": 3})
        o4 = mock_map_op(o3, ray_remote_args={"num_cpus": 3})
        o5 = mock_map_op(o4, ray_remote_args={"num_cpus": 3})

        # Set min_max_resource_requirements to specify minimum resources
        for op in [o2, o3, o4, o5]:
            op.min_max_resource_requirements = MagicMock(
                return_value=(
                    min_resources,
                    ExecutionResources(cpu=100, gpu=0, object_store_memory=10000),
                )
            )

        topo = build_streaming_topology(o5, ExecutionOptions())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(
            return_value=ExecutionResources.zero()
        )
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        assert isinstance(allocator, ReservationOpResourceAllocator)

        allocator.update_budgets(
            limits=global_limits,
        )
        # min_resources should be reserved for o2.
        assert allocator._op_reserved[o2] == min_resources
        # Remaining resources are CPU = 7 - 3 = 4, object_store_memory = 800 - 500 = 300.
        # We have enough CPUs for o3's min_resources, but not enough
        # object_store_memory. We'll still reserve the min_resources by
        # oversubscribing object_store_memory.
        assert allocator._op_reserved[o3] == min_resources
        # Now the remaining resources are CPU = 4 - 3 = 1,
        # object_store_memory = 300 - 500 = -200.
        # We don't oversubscribe CPUs, we'll only reserve
        # min_resources.object_store_memory.
        assert allocator._op_reserved[o4] == ExecutionResources(
            0, 0, min_resources.object_store_memory
        )
        # Same for o5
        assert allocator._op_reserved[o5] == ExecutionResources(
            0, 0, min_resources.object_store_memory
        )
        assert allocator._total_shared == ExecutionResources(1, 0, 0)
        for op in [o2, o3, o4]:
            assert allocator._reserved_for_op_outputs[op] == 50

    def test_reserve_min_resources_for_gpu_ops(self, restore_data_context):
        """Test that we'll reserve enough resources for ActorPoolMapOperator
        that uses GPU."""
        global_limits = ExecutionResources(cpu=6, gpu=8, object_store_memory=1600)

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(
            o1,
            ray_remote_args={"num_cpus": 0, "num_gpus": 1},
            compute_strategy=ray.data.ActorPoolStrategy(size=8),
        )
        # Mock min_max_resource_requirements to return a minimum of 800 bytes
        # (simulating 8 actors * 100 bytes per pending output)
        o2.min_max_resource_requirements = MagicMock(
            return_value=(
                ExecutionResources(cpu=0, gpu=8, object_store_memory=800),
                ExecutionResources(cpu=0, gpu=8, object_store_memory=float("inf")),
            )
        )

        topo = build_streaming_topology(o2, ExecutionOptions())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(
            return_value=ExecutionResources.zero()
        )
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        assert isinstance(allocator, ReservationOpResourceAllocator)

        allocator.update_budgets(
            limits=global_limits,
        )

        # With min_resource_requirements of 800 bytes, reservation should be at least 800
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
        topo = build_streaming_topology(o2, ExecutionOptions())
        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(
            return_value=ExecutionResources.zero()
        )
        # Mock an extremely large cluster.
        resource_manager.get_global_limits = MagicMock(
            return_value=ExecutionResources(cpu=1024, object_store_memory=1024**4)
        )
        allocator = resource_manager._op_resource_allocator

        global_limits = resource_manager.get_global_limits()
        allocator.update_budgets(
            limits=global_limits,
        )

        # The operator's max resource usage is 1 CPU and 1 byte object store memory, so
        # we'll reserve that despite the large global limits.
        assert allocator._op_reserved[o2] == ExecutionResources(
            cpu=1, object_store_memory=1
        )

    def test_budget_capped_by_max_resource_usage(self, restore_data_context):
        """Test that the total allocation is capped by max_resource_usage.

        Total allocation = max(total_reserved, op_usage) + op_shared
        We cap op_shared so that total allocation <= max_resource_usage.
        Excess shared resources should remain available for other operators.
        """
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1, ray_remote_args={"num_cpus": 1})
        o3 = mock_map_op(o2, ray_remote_args={"num_cpus": 1})

        # o2 has a small max CPU, so its CPU shared allocation will be capped.
        # o3 has unlimited max_resource_usage.
        o2.min_max_resource_requirements = MagicMock(
            return_value=(
                ExecutionResources.zero(),
                ExecutionResources(cpu=4, object_store_memory=float("inf")),
            )
        )
        o3.min_max_resource_requirements = MagicMock(
            return_value=(
                ExecutionResources.zero(),
                ExecutionResources.inf(),
            )
        )

        topo = build_streaming_topology(o3, ExecutionOptions())

        global_limits = ExecutionResources(cpu=20, object_store_memory=400)

        op_usages = {
            o1: ExecutionResources.zero(),
            o2: ExecutionResources(cpu=2, object_store_memory=40),
            o3: ExecutionResources(cpu=2, object_store_memory=40),
        }

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager._mem_op_internal = {o1: 0, o2: 40, o3: 40}
        resource_manager._mem_op_outputs = {o1: 0, o2: 0, o3: 0}
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        assert isinstance(allocator, ReservationOpResourceAllocator)
        allocator.update_budgets(limits=global_limits)

        # All tuples below are (cpu, object_store_memory).
        #
        # Reservation phase:
        # - default_reserved per op = global_limits * 0.5 / 2 = (5, 100)
        # - reserved_for_outputs per op = 100 / 2 = 50
        # - o2's reserved_for_tasks is capped by max (4, inf) -> (4, 50)
        # - o3's reserved_for_tasks = (5, 50)
        # - total_shared = global_limits - o2_total_reserved - o3_total_reserved
        #                = (20, 400) - (4, 100) - (5, 100) = (11, 200)
        #
        # Budget phase (first loop calculates reserved_remaining):
        # - o2: reserved_remaining = reserved_for_tasks - usage = (4, 50) - (2, 40) = (2, 10)
        # - o3: reserved_remaining = (5, 50) - (2, 40) = (3, 10)
        #
        # Shared allocation (second loop, reversed order):
        # - o3: op_shared = remaining_shared / 2 = (5.5, 100), no cap
        #       budget = reserved_remaining + op_shared = (3, 10) + (5.5, 100) = (8.5, 110)
        # - o2: op_shared = (5.5, 100), CPU capped to (0, 100)
        #       budget = (2, 10) + (0, 100) = (2, 110)
        #       remaining_shared = (5.5, 0)
        # - After loop, remaining (5.5, 0) given to most downstream op (o3):
        #       o3 budget = (8.5, 110) + (5.5, 0) = (14, 110)
        assert allocator._op_budgets[o2] == ExecutionResources(
            cpu=2, object_store_memory=110
        )
        assert allocator._op_budgets[o3] == ExecutionResources(
            cpu=14, object_store_memory=110
        )

    def test_budget_capped_by_max_resource_usage_all_capped(self, restore_data_context):
        """Test when all operators are capped, remaining shared resources are not given."""
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1, ray_remote_args={"num_cpus": 1})
        o3 = mock_map_op(o2, ray_remote_args={"num_cpus": 1})

        # Both operators are capped.
        o2.min_max_resource_requirements = MagicMock(
            return_value=(
                ExecutionResources.zero(),
                ExecutionResources(cpu=4, object_store_memory=float("inf")),
            )
        )
        o3.min_max_resource_requirements = MagicMock(
            return_value=(
                ExecutionResources.zero(),
                ExecutionResources(cpu=4, object_store_memory=float("inf")),
            )
        )

        topo = build_streaming_topology(o3, ExecutionOptions())

        global_limits = ExecutionResources(cpu=20, object_store_memory=400)

        op_usages = {
            o1: ExecutionResources.zero(),
            o2: ExecutionResources(cpu=2, object_store_memory=40),
            o3: ExecutionResources(cpu=2, object_store_memory=40),
        }

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager._mem_op_internal = {o1: 0, o2: 40, o3: 40}
        resource_manager._mem_op_outputs = {o1: 0, o2: 0, o3: 0}
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        allocator.update_budgets(limits=global_limits)

        # Both ops are capped (max cpu=4), so remaining CPU is not given to any op.
        # o2: reserved_remaining (2, 10) + capped op_shared (0, 100) = (2, 110)
        # o3: reserved_remaining (2, 10) + capped op_shared (0, 100) = (2, 110)
        assert allocator._op_budgets[o2] == ExecutionResources(
            cpu=2, object_store_memory=110
        )
        assert allocator._op_budgets[o3] == ExecutionResources(
            cpu=2, object_store_memory=110
        )

    def test_only_handle_eligible_ops(self, restore_data_context):
        """Test that we only handle non-completed map ops."""
        DataContext.get_current().op_resource_reservation_enabled = True

        input = make_ref_bundles([[x] for x in range(1)])
        o1 = InputDataBuffer(DataContext.get_current(), input)
        o2 = mock_map_op(o1)
        o3 = LimitOperator(1, o2, DataContext.get_current())
        topo = build_streaming_topology(o3, ExecutionOptions())

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
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

        global_limits = resource_manager.get_global_limits()
        allocator.update_budgets(
            limits=global_limits,
        )
        assert o1 not in allocator._op_budgets
        assert o2 in allocator._op_budgets
        assert o3 not in allocator._op_budgets

        o2.mark_execution_finished()
        allocator.update_budgets(
            limits=global_limits,
        )
        assert o2 not in allocator._op_budgets

    def test_gpu_allocation(self, restore_data_context):
        """Test GPU allocation for GPU vs non-GPU operators.

        With unified allocation (no GPU special-casing), GPU flows through
        the normal shared allocation path just like CPU and memory.
        """
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        o1 = InputDataBuffer(DataContext.get_current(), [])

        # Non-GPU operator (unbounded)
        o2 = mock_map_op(o1)
        o2.min_max_resource_requirements = MagicMock(
            return_value=(ExecutionResources(0, 0, 0), ExecutionResources.inf())
        )

        # GPU operator (unbounded)
        o3 = mock_map_op(o2, ray_remote_args={"num_gpus": 1})
        o3.min_max_resource_requirements = MagicMock(
            return_value=(ExecutionResources(0, 1, 0), ExecutionResources.inf())
        )

        topo = build_streaming_topology(o3, ExecutionOptions())

        global_limits = ExecutionResources(gpu=4)
        op_usages = {
            o1: ExecutionResources.zero(),
            o2: ExecutionResources.zero(),
            o3: ExecutionResources(gpu=1),  # GPU op using 1 GPU
        }

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager._mem_op_internal = dict.fromkeys([o1, o2, o3], 0)
        resource_manager._mem_op_outputs = dict.fromkeys([o1, o2, o3], 0)
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        allocator.update_budgets(
            limits=global_limits,
        )

        # Both unbounded operators get shared GPU allocation
        # GPU flows through normal allocation, both get GPU budget > 0
        assert allocator._op_budgets[o2].gpu > 0
        assert allocator._op_budgets[o3].gpu > 0

    def test_multiple_gpu_operators(self, restore_data_context):
        """Test GPU allocation for multiple GPU operators."""
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        o1 = InputDataBuffer(DataContext.get_current(), [])

        # Two GPU operators
        o2 = mock_map_op(o1, ray_remote_args={"num_gpus": 1})
        o2.min_max_resource_requirements = MagicMock(
            return_value=(ExecutionResources(0, 1, 0), ExecutionResources(0, 1, 0))
        )

        o3 = mock_map_op(o2, ray_remote_args={"num_gpus": 1})
        o3.min_max_resource_requirements = MagicMock(
            return_value=(ExecutionResources(0, 1, 0), ExecutionResources(0, 1, 0))
        )

        topo = build_streaming_topology(o3, ExecutionOptions())

        global_limits = ExecutionResources(gpu=4)
        op_usages = {
            o1: ExecutionResources.zero(),
            o2: ExecutionResources(gpu=1),  # Using 1 GPU
            o3: ExecutionResources(gpu=0),  # Not using GPU yet
        }

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager._mem_op_internal = dict.fromkeys([o1, o2, o3], 0)
        resource_manager._mem_op_outputs = dict.fromkeys([o1, o2, o3], 0)
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        allocator.update_budgets(
            limits=global_limits,
        )

        # Both operators are capped at their max of 1 GPU
        # o2: using 1 GPU, reserved 1, so reserved_remaining = 0, gets 0 shared (capped)
        # o3: using 0 GPU, reserved 1, so reserved_remaining = 1, gets 0 shared (capped)
        assert allocator._op_budgets[o2].gpu == 0
        assert allocator._op_budgets[o3].gpu == 1

    def test_gpu_usage_exceeds_global_limits(self, restore_data_context):
        """Test that GPU budget is 0 when usage exceeds limits."""
        o1 = InputDataBuffer(DataContext.get_current(), [])

        # One GPU operator
        o2 = mock_map_op(o1, ray_remote_args={"num_gpus": 1})
        o2.min_max_resource_requirements = MagicMock(
            return_value=(ExecutionResources(0, 1, 0), ExecutionResources(0, 2, 0))
        )

        topo = build_streaming_topology(o2, ExecutionOptions())

        global_limits = ExecutionResources(gpu=1)
        op_usages = {
            o1: ExecutionResources.zero(),
            # o2 uses 2 GPUs but only 1 is available. This can happen if you set
            # `concurrency` to 2 but there's only 1 GPU in the cluster. In this case,
            # one actor will be running and the other will be stuck pending.
            o2: ExecutionResources(gpu=2),
        }

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager._mem_op_internal = dict.fromkeys([o1, o2], 0)
        resource_manager._mem_op_outputs = dict.fromkeys([o1, o2], 0)
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        allocator.update_budgets(
            limits=global_limits,
        )

        # When usage (2) exceeds limits (1), the budget should be 0
        # because reserved_remaining = reserved - usage = negative, clamped to 0
        assert allocator._op_budgets[o2].gpu == 0

    def test_gpu_unbounded_operator_can_autoscale(self, restore_data_context):
        """Test that unbounded GPU operators (max_size=None) get GPU budget for autoscaling.

        This is a regression test for the bug where ActorPoolStrategy(min_size=1, max_size=None)
        with GPU actors would not get any GPU budget, preventing autoscaling.
        """
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        o1 = InputDataBuffer(DataContext.get_current(), [])

        # Unbounded GPU operator (simulating ActorPoolStrategy with max_size=None)
        # min = 1 GPU (for 1 actor), max = inf (unbounded)
        o2 = mock_map_op(o1, ray_remote_args={"num_gpus": 1})
        o2.min_max_resource_requirements = MagicMock(
            return_value=(ExecutionResources(0, 1, 0), ExecutionResources.inf())
        )

        topo = build_streaming_topology(o2, ExecutionOptions())

        global_limits = ExecutionResources(gpu=8)
        op_usages = {
            o1: ExecutionResources.zero(),
            o2: ExecutionResources(gpu=1),  # Currently using 1 GPU
        }

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager._mem_op_internal = dict.fromkeys([o1, o2], 0)
        resource_manager._mem_op_outputs = dict.fromkeys([o1, o2], 0)
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        allocator.update_budgets(
            limits=global_limits,
        )

        # The unbounded GPU operator should get GPU budget > 0 so it can autoscale
        # With 8 GPUs available and 1 used, there should be budget for more
        assert allocator._op_budgets[o2].gpu > 0, (
            f"Unbounded GPU operator should get GPU budget for autoscaling, "
            f"but got {allocator._op_budgets[o2].gpu}"
        )

    def test_actor_pool_gpu_operator_gets_gpu_budget_in_cpu_pipeline(
        self, restore_data_context
    ):
        """Test GPU ActorPool gets budget in a pipeline with multiple CPU operators.

        Regression test for a following pipeline:
            Input -> ListFiles -> ReadFiles -> Preprocess -> Infer(GPU) -> Write

        The GPU inference operator (ActorPool with GPUs) was stuck at 1 actor
        because it had gpu_budget=0, preventing autoscaling.

        Root cause: The borrowing logic used incremental_resource_usage() which
        returns gpu=0 for ActorPoolMapOperator (since submitting tasks to existing
        actors doesn't need new GPUs). The fix uses min_scheduling_resources()
        which returns the per-actor GPU requirement.
        """
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        # Build pipeline: Input -> Read -> Preprocess -> Infer(GPU) -> Write
        # This mirrors the production pipeline structure
        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1, ray_remote_args={"num_cpus": 1}, name="ReadFiles")
        o3 = mock_map_op(o2, ray_remote_args={"num_cpus": 1}, name="Preprocess")
        o4 = mock_map_op(
            o3,
            ray_remote_args={"num_cpus": 0, "num_gpus": 1},
            compute_strategy=ray.data.ActorPoolStrategy(min_size=1, max_size=4),
            name="Infer",
        )
        o5 = mock_map_op(o4, ray_remote_args={"num_cpus": 1}, name="Write")

        topo = build_streaming_topology(o5, ExecutionOptions())

        # Cluster with 2 GPUs available
        global_limits = ExecutionResources(
            cpu=16, gpu=2, object_store_memory=10_000_000
        )

        # Simulate state where GPU operator has 1 actor running
        op_usages = {
            o1: ExecutionResources.zero(),
            o2: ExecutionResources.zero(),
            o3: ExecutionResources.zero(),
            o4: ExecutionResources(gpu=1),  # 1 GPU actor running
            o5: ExecutionResources.zero(),
        }

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager._mem_op_internal = dict.fromkeys([o1, o2, o3, o4, o5], 0)
        resource_manager._mem_op_outputs = dict.fromkeys([o1, o2, o3, o4, o5], 0)
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        allocator.update_budgets(limits=global_limits)

        # Verify the GPU operator gets GPU budget to scale up.
        # With 2 GPUs total, 1 used, the operator should have budget for 1 more.
        # Before the fix: budget.gpu=0 (couldn't scale)
        # After the fix: budget.gpu=1 (can scale to 1 more actor)
        assert allocator.get_budget(o4) == ExecutionResources(
            cpu=0, gpu=1, object_store_memory=1875000
        )
        assert allocator.get_allocation(o4) == ExecutionResources(
            cpu=0, gpu=2, object_store_memory=1875000
        )

    def test_gpu_bounded_vs_unbounded_operators(self, restore_data_context):
        """Test GPU allocation when one operator is bounded and one is unbounded.

        With unified allocation, bounded operator is capped, unbounded gets remaining.
        """
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        o1 = InputDataBuffer(DataContext.get_current(), [])

        # Bounded GPU operator (max 2 GPUs)
        o2 = mock_map_op(o1, ray_remote_args={"num_gpus": 1})
        o2.min_max_resource_requirements = MagicMock(
            return_value=(ExecutionResources(0, 1, 0), ExecutionResources(0, 2, 0))
        )

        # Unbounded GPU operator
        o3 = mock_map_op(o2, ray_remote_args={"num_gpus": 1})
        o3.min_max_resource_requirements = MagicMock(
            return_value=(ExecutionResources(0, 1, 0), ExecutionResources.inf())
        )

        topo = build_streaming_topology(o3, ExecutionOptions())

        global_limits = ExecutionResources(gpu=8)
        op_usages = {
            o1: ExecutionResources.zero(),
            o2: ExecutionResources.zero(),
            o3: ExecutionResources.zero(),
        }

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager._mem_op_internal = dict.fromkeys([o1, o2, o3], 0)
        resource_manager._mem_op_outputs = dict.fromkeys([o1, o2, o3], 0)
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        allocator.update_budgets(
            limits=global_limits,
        )

        # o2 is capped at 2 GPUs (its max)
        assert allocator._op_budgets[o2].gpu == 2

        # o3 (unbounded) gets remaining GPUs after o2's excess is returned
        # With 8 total GPUs and o2 capped at 2, o3 gets 6
        assert allocator._op_budgets[o3].gpu == 6

    @pytest.mark.parametrize("max_actors", [4, float("inf")])
    def test_gpu_not_reserved_for_non_gpu_operators(
        self, restore_data_context, max_actors
    ):
        """Test that GPU budget is not reserved for operators that don't use GPUs.

        This tests a realistic inference pipeline DAG:
            Read (CPU) -> Infer1 (GPU) -> Infer2 (GPU) -> Write (CPU)

        Non-GPU operators (Read, Write) should have 0 GPUs reserved, ensuring
        all GPUs are available for GPU operators (Infer1, Infer2).
        """
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        o1 = InputDataBuffer(DataContext.get_current(), [])

        # Read: CPU-only operator (unbounded, gpu=0 in max since it doesn't use GPUs)
        read_op = mock_map_op(o1, name="Read")
        read_op.min_max_resource_requirements = MagicMock(
            return_value=(
                ExecutionResources(cpu=1, gpu=0, object_store_memory=0),
                ExecutionResources.for_limits(gpu=0),
            )
        )

        # Infer1: GPU operator
        infer1_op = mock_map_op(read_op, ray_remote_args={"num_gpus": 1}, name="Infer1")
        infer1_op.min_max_resource_requirements = MagicMock(
            return_value=(
                ExecutionResources(cpu=0, gpu=1, object_store_memory=0),
                ExecutionResources(
                    cpu=0, gpu=max_actors, object_store_memory=float("inf")
                ),
            )
        )

        # Infer2: GPU operator
        infer2_op = mock_map_op(
            infer1_op, ray_remote_args={"num_gpus": 1}, name="Infer2"
        )
        infer2_op.min_max_resource_requirements = MagicMock(
            return_value=(
                ExecutionResources(cpu=0, gpu=1, object_store_memory=0),
                ExecutionResources(
                    cpu=0, gpu=max_actors, object_store_memory=float("inf")
                ),
            )
        )

        # Write: CPU-only operator (unbounded, gpu=0 in max since it doesn't use GPUs)
        write_op = mock_map_op(infer2_op, name="Write")
        write_op.min_max_resource_requirements = MagicMock(
            return_value=(
                ExecutionResources(cpu=1, gpu=0, object_store_memory=0),
                ExecutionResources.for_limits(gpu=0),
            )
        )

        topo = build_streaming_topology(write_op, ExecutionOptions())

        global_limits = ExecutionResources(cpu=8, gpu=8, object_store_memory=10_000_000)
        ops = [o1, read_op, infer1_op, infer2_op, write_op]
        op_usages = {op: ExecutionResources.zero() for op in ops}

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager._mem_op_internal = dict.fromkeys(ops, 0)
        resource_manager._mem_op_outputs = dict.fromkeys(ops, 0)
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        allocator.update_budgets(limits=global_limits)

        # Non-GPU operators should have 0 GPUs reserved
        assert allocator._op_reserved[read_op].gpu == 0
        assert allocator._op_reserved[write_op].gpu == 0

        # GPU operators should have GPUs reserved
        assert allocator._op_reserved[infer1_op].gpu > 0
        assert allocator._op_reserved[infer2_op].gpu > 0

        # All 8 GPUs should be available (reserved for GPU ops + shared pool)
        total_gpu_reserved = sum(
            allocator._op_reserved[op].gpu
            for op in [read_op, infer1_op, infer2_op, write_op]
        )
        assert total_gpu_reserved + allocator._total_shared.gpu == 8

    def test_reservation_accounts_for_completed_ops(self, restore_data_context):
        """Test that resource reservation properly accounts for completed ops."""
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1, ray_remote_args={"num_cpus": 1})
        o3 = mock_map_op(o2, ray_remote_args={"num_cpus": 1})
        o4 = mock_map_op(o3, ray_remote_args={"num_cpus": 1})

        # Mock min_max_resource_requirements to return default unbounded behavior
        for op in [o2, o3, o4]:
            op.min_max_resource_requirements = MagicMock(
                return_value=(ExecutionResources.zero(), ExecutionResources.inf())
            )

        o1.mark_execution_finished()
        o2.mark_execution_finished()

        op_usages = {
            o1: ExecutionResources.zero(),
            o2: ExecutionResources(cpu=2, object_store_memory=50),
            o3: ExecutionResources.zero(),
            o4: ExecutionResources.zero(),
        }
        op_internal_usage = dict.fromkeys([o1, o2, o3, o4], 0)
        op_outputs_usages = dict.fromkeys([o1, o2, o3, o4], 0)

        topo = build_streaming_topology(o4, ExecutionOptions())

        global_limits = ExecutionResources(cpu=10, object_store_memory=250)

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager._mem_op_internal = op_internal_usage
        resource_manager._mem_op_outputs = op_outputs_usages
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        # Update allocated budgets
        resource_manager._update_allocated_budgets()

        # Check that o2's usage was subtracted from remaining resources
        # global_limits (10 CPU, 250 mem) - o1 usage (0) - o2 usage (2 CPU, 50 mem) = remaining (8 CPU, 200 mem)
        # With 2 eligible ops (o3, o4) and 50% reservation ratio:
        # Each op gets reserved: (8 CPU, 200 mem) * 0.5 / 2 = (2 CPU, 50 mem)

        allocator = resource_manager._op_resource_allocator

        # Verify that reservations are calculated correctly
        assert allocator._op_reserved[o3].cpu == 2.0
        assert allocator._op_reserved[o4].cpu == 2.0

        # The total reserved memory should account for o2's usage being subtracted
        total_reserved_memory = (
            allocator._op_reserved[o3].object_store_memory
            + allocator._reserved_for_op_outputs[o3]
            + allocator._op_reserved[o4].object_store_memory
            + allocator._reserved_for_op_outputs[o4]
        )

        assert abs(total_reserved_memory - 100) < 1.0

    def test_reservation_accounts_for_completed_ops_complex_graph(
        self, restore_data_context
    ):
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
                o8 (ZipOperator) <-- o7 (InputDataBuffer, completed)
        """
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1, ray_remote_args={"num_cpus": 1})
        o3 = LimitOperator(1, o2, DataContext.get_current())
        o4 = InputDataBuffer(DataContext.get_current(), [])
        o5 = mock_map_op(o4, ray_remote_args={"num_cpus": 1})
        o6 = mock_union_op([o3, o5])
        o7 = InputDataBuffer(DataContext.get_current(), [])
        o8 = mock_join_op(o7, o6)

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
        op_internal_usage = dict.fromkeys([o1, o2, o3, o4, o5, o6, o7, o8], 0)
        op_outputs_usages = dict.fromkeys([o1, o2, o3, o4, o5, o6, o7, o8], 0)

        topo = build_streaming_topology(o8, ExecutionOptions())

        global_limits = ExecutionResources.zero()

        def mock_get_global_limits():
            nonlocal global_limits
            return global_limits

        resource_manager = ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(),
            DataContext.get_current(),
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager.get_global_limits = MagicMock(
            side_effect=mock_get_global_limits
        )
        resource_manager._mem_op_internal = op_internal_usage
        resource_manager._mem_op_outputs = op_outputs_usages

        global_limits = ExecutionResources(cpu=20, object_store_memory=2000)

        resource_manager._update_allocated_budgets()

        """
        global_limits (20 CPU, 2000 mem) - o2 usage (2 CPU, 150 mem) - o3 usage (2 CPU, 50 mem) - o5 usage (3 CPU, 100 mem) - o7 usage (1 CPU, 100 mem) = remaining (12 CPU, 1600 mem)
        +-----+------------------+------------------+--------------+
        |     | _op_reserved     | _reserved_for    | used shared  |
        |     | (used/remaining) | _op_outputs      | resources    |
        |     |                  | (used/remaining) |              |
        +-----+------------------+------------------+--------------+
        | op6 | 0/200            | 0/200            | 0            |
        +-----+------------------+------------------+--------------+
        | op8 | 0/200            | 0/200            | 0            |
        +-----+------------------+------------------+--------------+
        """
        allocator = resource_manager._op_resource_allocator

        assert set(allocator._op_budgets.keys()) == {o6, o8}
        assert set(allocator._op_reserved.keys()) == {o6, o8}
        assert allocator._op_reserved[o6] == ExecutionResources(
            cpu=3, object_store_memory=200
        )
        assert allocator._op_reserved[o8] == ExecutionResources(
            cpu=3, object_store_memory=200
        )
        assert allocator._reserved_for_op_outputs[o6] == 200
        assert allocator._reserved_for_op_outputs[o8] == 200
        assert allocator._total_shared == ExecutionResources(
            cpu=6, object_store_memory=800
        )
        assert allocator._op_budgets[o6] == ExecutionResources(
            cpu=6, object_store_memory=600
        )
        # object_store_memory budget is unlimited, since join is a materializing
        # operator
        assert allocator._op_budgets[o8] == ExecutionResources(
            cpu=6, object_store_memory=float("inf")
        )

        # Test when resources are used.
        op_usages[o6] = ExecutionResources(2, 0, 500)
        op_internal_usage[o6] = 300
        op_outputs_usages[o6] = 200
        op_usages[o8] = ExecutionResources(2, 0, 100)
        op_internal_usage[o8] = 50
        op_outputs_usages[o8] = 50
        """
        +-----+------------------+------------------+--------------+
        |     | _op_reserved     | _reserved_for    | used shared  |
        |     | (used/remaining) | _op_outputs      | resources    |
        |     |                  | (used/remaining) |              |
        +-----+------------------+------------------+--------------+
        | op6 | 200/0            | 200/0            | 100          |
        +-----+------------------+------------------+--------------+
        | op8 | 50/150           | 50/150           | 0            |
        +-----+------------------+------------------+--------------+
        """

        resource_manager._update_allocated_budgets()

        assert allocator._op_budgets[o6] == ExecutionResources(
            cpu=4, object_store_memory=350
        )
        # object_store_memory budget is unlimited, since join is a materializing
        # operator
        assert allocator._op_budgets[o8] == ExecutionResources(
            cpu=4, object_store_memory=float("inf")
        )

        # Test when completed ops update the usage.
        op_usages[o5] = ExecutionResources.zero()

        resource_manager._update_allocated_budgets()

        """
        global_limits (20 CPU, 2000 mem) - o2 usage (2 CPU, 150 mem) - o3 usage (2 CPU, 50 mem) - o5 usage (0 CPU, 0 mem) - o7 usage (1 CPU, 100 mem) = remaining (15 CPU, 1700 mem)
        +-----+------------------+------------------+--------------+
        |     | _op_reserved     | _reserved_for    | used shared  |
        |     | (used/remaining) | _op_outputs      | resources    |
        |     |                  | (used/remaining) |              |
        +-----+------------------+------------------+--------------+
        | op6 | 213/0            | 200/13           | 300-213=87   |
        +-----+------------------+------------------+--------------+
        | op8 | 50/163           | 50/163           | 0            |
        +-----+------------------+------------------+--------------+
        """
        assert set(allocator._op_budgets.keys()) == {o6, o8}
        assert set(allocator._op_reserved.keys()) == {o6, o8}
        assert allocator._op_reserved[o6] == ExecutionResources(
            cpu=3.75, object_store_memory=213
        )
        assert allocator._op_reserved[o8] == ExecutionResources(
            cpu=3.75, object_store_memory=213
        )
        assert allocator._reserved_for_op_outputs[o6] == 212
        assert allocator._reserved_for_op_outputs[o8] == 212
        assert allocator._total_shared == ExecutionResources(
            cpu=7.5, object_store_memory=850
        )
        # object_store_memory budget = 0 + (850 - 87) / 2 = 381 (rounded down)
        assert allocator._op_budgets[o6] == ExecutionResources(
            cpu=5.5, object_store_memory=381
        )
        # object_store_memory budget is unlimited, since join is a materializing
        # operator
        assert allocator._op_budgets[o8] == ExecutionResources(
            cpu=5.5, object_store_memory=float("inf")
        )


def test_on_task_dispatched_decrements_budget_without_mutation(restore_data_context):
    """`ReservationOpResourceAllocator.on_task_dispatched(op)` decrements
    that op's budget by one task's ``incremental_resource_usage()`` and
    clamps at zero, producing a *new* ExecutionResources — never mutating
    the previous instance in place. The "no in-place mutation" guarantee
    matters because ExecutionResources is treated as a value type
    elsewhere; mutating the cached budget would leak to any caller
    holding a prior reference.
    """
    # Minimal 2-op pipeline: InputDataBuffer -> Map.
    input_op = InputDataBuffer(DataContext.get_current(), MagicMock())
    op2 = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 2})
    # Override op2's incremental_resource_usage to exercise a multi-cpu path.
    op2.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=2, gpu=0)
    )

    topo = build_streaming_topology(op2, ExecutionOptions())
    rm = ResourceManager(
        topo,
        ExecutionOptions(),
        MagicMock(return_value=ExecutionResources(cpu=8, gpu=0)),
        DataContext.get_current(),
    )
    rm.update_usages()
    alloc = rm._op_resource_allocator
    assert isinstance(alloc, ReservationOpResourceAllocator)

    before_budget = alloc.get_budget(op2)
    assert before_budget is not None
    before_snapshot = (
        before_budget.cpu,
        before_budget.gpu,
        before_budget.object_store_memory,
        before_budget.memory,
    )

    rm.on_task_dispatched(op2)

    after_budget = alloc.get_budget(op2)
    assert after_budget is not before_budget  # new object, not in-place
    assert after_budget.cpu == max(before_snapshot[0] - 2.0, 0.0)
    # Prior budget object unchanged.
    assert (
        before_budget.cpu,
        before_budget.gpu,
        before_budget.object_store_memory,
        before_budget.memory,
    ) == before_snapshot


def test_on_task_dispatched_clamps_at_zero_never_negative(restore_data_context):
    """`on_task_dispatched` clamps each resource field at zero — a stale
    budget + an oversized ``incremental_resource_usage()`` never produces
    a negative budget that would confuse downstream schedulability
    checks.
    """
    input_op = InputDataBuffer(DataContext.get_current(), MagicMock())
    op2 = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 1})
    op2.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=1000, gpu=0)
    )

    topo = build_streaming_topology(op2, ExecutionOptions())
    rm = ResourceManager(
        topo,
        ExecutionOptions(),
        MagicMock(return_value=ExecutionResources(cpu=8, gpu=0)),
        DataContext.get_current(),
    )
    rm.update_usages()
    rm.on_task_dispatched(op2)
    alloc = rm._op_resource_allocator
    budget = alloc.get_budget(op2)
    assert budget is not None
    assert budget.cpu == 0.0
    assert budget.gpu == 0.0
    assert budget.object_store_memory == 0.0
    assert budget.memory == 0.0


def test_on_task_dispatched_decrements_object_store_memory_by_predicted_output(
    restore_data_context,
):
    """``on_task_dispatched`` decrements the op's budget's ``object_store_memory``
    by ``op.metrics.obj_store_mem_max_pending_output_per_task``, independently of
    what ``incremental_resource_usage()`` reports for that dimension.

    Why this is the right shape:
    - ``incremental_resource_usage()`` reports what Ray core reserves at
      dispatch (CPU/GPU/memory). Plasma is *not* a Ray-core reservation —
      it is committed reactively as the task writes its output, not reserved
      at dispatch. So operators correctly return ``object_store_memory=0``
      from ``incremental_resource_usage()``.
    - But ``can_submit_new_task()`` gates on
      ``budget.object_store_memory >= op.metrics.obj_store_mem_max_pending_output_per_task``,
      so the budget's object-store dimension *must* decrement per dispatch
      or the op silently over-commits plasma within a scheduling step.
    - The fix uses the same per-task-max metric the gate already reads, so
      the "decrement here" and "check there" are consistent.

    The extreme case this matters: actor-pool ops. Their
    ``incremental_resource_usage()`` is ``(0, 0, 0)`` entirely (submitting
    to an existing actor reserves nothing new), so without this fix nothing
    at all decrements their budget within a scheduling step.
    """
    input_op = InputDataBuffer(DataContext.get_current(), MagicMock())
    op2 = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 1})
    # Simulate an actor-style op: incremental_resource_usage reports
    # nothing (actor dispatch to an existing actor reserves no additional
    # Ray-core resources).
    op2.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )
    # Simulate a predicted per-task output of 100 MiB. This is the
    # estimate `can_submit_new_task` gates on. The property is computed,
    # so patch it at class level for the duration of the test.
    per_task_output_bytes = 100 * 1024 * 1024

    topo = build_streaming_topology(op2, ExecutionOptions())
    rm = ResourceManager(
        topo,
        ExecutionOptions(),
        MagicMock(
            return_value=ExecutionResources(
                cpu=8, gpu=0, object_store_memory=10 * per_task_output_bytes
            )
        ),
        DataContext.get_current(),
    )
    rm.update_usages()
    alloc = rm._op_resource_allocator
    assert isinstance(alloc, ReservationOpResourceAllocator)

    before_budget = alloc.get_budget(op2)
    assert before_budget is not None
    assert before_budget.object_store_memory > 0, (
        "Test precondition: op should have nonzero object-store budget "
        "so the decrement is observable."
    )
    before_obj_store = before_budget.object_store_memory

    with patch.object(
        type(op2.metrics),
        "obj_store_mem_max_pending_output_per_task",
        new_callable=PropertyMock,
        return_value=per_task_output_bytes,
    ):
        rm.on_task_dispatched(op2)

    after_budget = alloc.get_budget(op2)
    # object-store dimension must decrement by the predicted per-task
    # output, clamped at zero. This is the fix — previously the whole
    # decrement was a no-op for actor ops since incremental_resource_usage
    # was (0, 0, 0) and object_store_memory is not part of it.
    expected_after_obj_store = max(
        before_obj_store - per_task_output_bytes, 0.0
    )
    assert after_budget.object_store_memory == expected_after_obj_store, (
        f"Expected object_store_memory to decrement by {per_task_output_bytes}; "
        f"before={before_obj_store}, after={after_budget.object_store_memory}"
    )
    # CPU dimension should stay the same since
    # incremental_resource_usage() returns cpu=0 (actor-pool semantics).
    assert after_budget.cpu == before_budget.cpu


def test_on_task_dispatched_no_decrement_when_per_task_output_metric_missing(
    restore_data_context,
):
    """If ``obj_store_mem_max_pending_output_per_task`` is not yet known
    (before the first task completes, the running-max metric is 0/None),
    ``on_task_dispatched`` should not decrement the budget's object-store
    dimension. This matches reality: we can't predict the output size
    until at least one task has produced output.
    """
    input_op = InputDataBuffer(DataContext.get_current(), MagicMock())
    op2 = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 1})
    op2.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )

    topo = build_streaming_topology(op2, ExecutionOptions())
    rm = ResourceManager(
        topo,
        ExecutionOptions(),
        MagicMock(
            return_value=ExecutionResources(
                cpu=8, gpu=0, object_store_memory=1 * 1024 * 1024 * 1024
            )
        ),
        DataContext.get_current(),
    )
    rm.update_usages()
    alloc = rm._op_resource_allocator
    before_obj_store = alloc.get_budget(op2).object_store_memory
    # Metric not populated yet (freshly-started op, no tasks finished) —
    # property returns None.
    with patch.object(
        type(op2.metrics),
        "obj_store_mem_max_pending_output_per_task",
        new_callable=PropertyMock,
        return_value=None,
    ):
        rm.on_task_dispatched(op2)
    after_obj_store = alloc.get_budget(op2).object_store_memory
    assert after_obj_store == before_obj_store, (
        "When per-task-output metric is unknown (0/None), budget's "
        "object_store_memory should not decrement."
    )


def test_on_task_dispatched_updates_op_usages_read_by_gating_consumers(
    restore_data_context,
):
    """``on_task_dispatched`` must keep ``_op_usages[op]`` and
    ``_op_running_usages[op]`` fresh within a scheduling step, not just
    the allocator's ``_op_budgets[op]``. These dicts are read by gating
    consumers inside the inner dispatch loop — ``DefaultRanker`` reads
    them through ``get_op_usage()``, and
    ``DownstreamCapacityBackpressurePolicy`` reads them through
    ``get_op_usage()`` in its ``_get_queue_size_bytes()`` path. If they
    stay at the last ``update_usages()`` snapshot, those consumers make
    decisions as if the task just dispatched hadn't committed any
    resources, which lets the scheduler over-dispatch before
    ``update_usages()`` runs again.

    Invariant under test: after a dispatch, ``_op_usages[op]`` and
    ``_op_running_usages[op]`` each advance by the same delta the
    allocator used to decrement the budget (the sum of
    ``incremental_resource_usage()`` and
    ``obj_store_mem_max_pending_output_per_task``).
    """
    input_op = InputDataBuffer(DataContext.get_current(), MagicMock())
    op2 = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 2})
    op2.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=2, gpu=0)
    )
    per_task_output_bytes = 50 * 1024 * 1024  # 50 MiB

    topo = build_streaming_topology(op2, ExecutionOptions())
    rm = ResourceManager(
        topo,
        ExecutionOptions(),
        MagicMock(
            return_value=ExecutionResources(
                cpu=8, gpu=0, object_store_memory=1024 * 1024 * 1024
            )
        ),
        DataContext.get_current(),
    )
    rm.update_usages()

    before_usage = rm._op_usages[op2]
    before_running = rm._op_running_usages[op2]

    with patch.object(
        type(op2.metrics),
        "obj_store_mem_max_pending_output_per_task",
        new_callable=PropertyMock,
        return_value=per_task_output_bytes,
    ):
        rm.on_task_dispatched(op2)

    after_usage = rm._op_usages[op2]
    after_running = rm._op_running_usages[op2]

    # Core-resource dimension advances by incremental_resource_usage().
    assert after_usage.cpu == before_usage.cpu + 2.0
    assert after_running.cpu == before_running.cpu + 2.0
    # Object-store dimension advances by the predicted per-task output
    # (same value the gate consumes in can_submit_new_task).
    assert (
        after_usage.object_store_memory
        == before_usage.object_store_memory + per_task_output_bytes
    )
    assert (
        after_running.object_store_memory
        == before_running.object_store_memory + per_task_output_bytes
    )
    # Value-type discipline: each update replaces the old instance
    # rather than mutating it. Prior references remain pinned to their
    # snapshot values.
    assert after_usage is not before_usage
    assert after_running is not before_running


def test_on_task_dispatched_updates_mem_op_internal_and_dashboard_metric(
    restore_data_context,
):
    """``on_task_dispatched`` must also keep ``_mem_op_internal[op]``
    fresh — ``ConcurrencyCapBackpressurePolicy.can_add_input()`` reads
    it through ``get_mem_op_internal()`` inside the inner dispatch loop,
    and a stale value would under-report pending-output plasma pressure
    and let the scheduler dispatch past the concurrency cap.

    The dashboard-facing ``op._metrics.obj_store_mem_used`` field must
    also mirror the new ``_op_usages[op].object_store_memory`` so
    DatasetStats and the Ray Data dashboard observe the same value as
    the gating consumers within the step (rather than the last
    ``update_usages()`` snapshot).
    """
    input_op = InputDataBuffer(DataContext.get_current(), MagicMock())
    op2 = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 1})
    op2.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=0, gpu=0)
    )
    per_task_output_bytes = 25 * 1024 * 1024  # 25 MiB

    topo = build_streaming_topology(op2, ExecutionOptions())
    rm = ResourceManager(
        topo,
        ExecutionOptions(),
        MagicMock(
            return_value=ExecutionResources(
                cpu=8, gpu=0, object_store_memory=1024 * 1024 * 1024
            )
        ),
        DataContext.get_current(),
    )
    rm.update_usages()

    before_mem_internal = rm._mem_op_internal[op2]

    with patch.object(
        type(op2.metrics),
        "obj_store_mem_max_pending_output_per_task",
        new_callable=PropertyMock,
        return_value=per_task_output_bytes,
    ):
        rm.on_task_dispatched(op2)

    # _mem_op_internal advances by the per-task plasma commitment.
    assert (
        rm._mem_op_internal[op2]
        == before_mem_internal + per_task_output_bytes
    )
    # Dashboard metric mirrors the updated _op_usages[op].object_store_memory.
    assert (
        op2._metrics.obj_store_mem_used
        == rm._op_usages[op2].object_store_memory
    )


def test_execution_resources_subtract_clamp_zero():
    """ExecutionResources.subtract_clamp_zero equals
    subtract(...).max(zero()) but produces one intermediate object
    instead of two — hot-path shorthand for incremental-budget decrement.
    """
    a = ExecutionResources(cpu=4.0, gpu=1.0, object_store_memory=100, memory=200)
    b = ExecutionResources(cpu=10.0, gpu=0.5, object_store_memory=30, memory=400)

    fused = a.subtract_clamp_zero(b)
    naive = a.subtract(b).max(ExecutionResources.zero())
    assert fused.cpu == naive.cpu == 0.0  # 4 - 10 clamped
    assert fused.gpu == naive.gpu == 0.5
    assert fused.object_store_memory == naive.object_store_memory == 70
    assert fused.memory == naive.memory == 0.0  # 200 - 400 clamped


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))

# --- M5 design-tradeoff pin ---------------------------------------
#
# M5 trades exact per-dispatch budget freshness for amortized work:
# the inner loop only updates the dispatched op's state, while pristine
# walked the full topology after every dispatch. Within a single
# scheduling step this means peer-budget redistribution (pristine moves
# slack from idle ops to busy ones via update_budgets) is deferred to
# the step boundary. The drift IS bounded — the next update_usages()
# at step top resyncs — but within a step a borrow-needy op dispatches
# fewer tasks than pristine would.
#
# This pin documents the drift shape with a 2-op topology where op A
# overruns its reservation while op B is idle. If a future change
# closes the drift (e.g., partial peer-budget refresh per dispatch)
# this test fails and forces a conscious update.

def test_m5_borrow_drift_in_multi_op_topology_is_real_and_quantified(
    restore_data_context,
):
    """**Adversarial-pass finding (M5)**: v3's incremental budget
    decrement does NOT redistribute slack from idle ops. In a 2-op
    topology where op A wants more dispatch headroom than its
    initial allocation while op B is idle, pristine's per-dispatch
    `update_budgets` redistributes B's slack to A; v3 doesn't. So
    v3's A.budget drains faster than pristine's.

    This is the "bounded design-intended drift" the audit
    documented in §4.5 but I had failed to quantify or test until
    an adversarial probe forced it. The drift IS bounded by one
    scheduling step (next update_usages corrects it) but within a
    step, v3 dispatches FEWER tasks than pristine on a
    borrow-needy op.

    Setup:
    - 16 CPU global, 2 eligible ops (A and B), 50% reservation
      ratio. Each op reserves 4 CPU; shared pool = 8.
    - Initial budget per op = 4 reserved + 8/2 share = 8.
    - Op A dispatches 8 tasks (each 1 CPU).
    - Op B is idle (zero usage).

    Expected:
    - v3: A.budget shrinks linearly: 8 -> 7 -> ... -> 0.
    - Pristine: A.budget shrinks slower because it redistributes
      B's idle share to A.
    - Drift at end of sequence: pristine > v3 (drift is positive).
    """
    from unittest.mock import MagicMock, PropertyMock, patch

    from ray.data._internal.execution.interfaces.execution_options import (
        ExecutionOptions,
    )
    from ray.data._internal.execution.streaming_executor_state import (
        build_streaming_topology,
    )

    # Build 2-op topology.
    ctx_input = DataContext.get_current()
    input_op = InputDataBuffer(ctx_input, MagicMock())
    op_a = mock_map_op(input_op=input_op, ray_remote_args={"num_cpus": 1})
    op_b = mock_map_op(input_op=op_a, ray_remote_args={"num_cpus": 1})

    # Track A's dispatched count; have A's logical-usage methods
    # reflect it so pristine's update_usages picks up the dispatches.
    state = {"a": 0}
    op_a.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=1, gpu=0)
    )
    op_a.current_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(cpu=state["a"], gpu=0)
    )
    op_a.running_logical_usage = MagicMock(
        side_effect=lambda: ExecutionResources(cpu=state["a"], gpu=0)
    )
    op_a.pending_logical_usage = MagicMock(
        return_value=ExecutionResources.zero()
    )
    op_b.incremental_resource_usage = MagicMock(
        return_value=ExecutionResources(cpu=1, gpu=0)
    )
    op_b.current_logical_usage = MagicMock(
        return_value=ExecutionResources.zero()
    )
    op_b.running_logical_usage = MagicMock(
        return_value=ExecutionResources.zero()
    )
    op_b.pending_logical_usage = MagicMock(
        return_value=ExecutionResources.zero()
    )

    def build_rm():
        topo = build_streaming_topology(op_b, ExecutionOptions())
        return ResourceManager(
            topo,
            ExecutionOptions(),
            MagicMock(
                return_value=ExecutionResources(
                    cpu=16, gpu=0, object_store_memory=10 ** 11
                )
            ),
            DataContext.get_current(),
        )

    rm_v3 = build_rm()
    rm_pristine = build_rm()
    rm_v3.update_usages()
    rm_pristine.update_usages()

    # Initial budgets must match.
    assert (
        rm_v3._op_resource_allocator.get_budget(op_a).cpu
        == rm_pristine._op_resource_allocator.get_budget(op_a).cpu
    )

    # Drive 8 dispatches on A.
    with patch.object(
        type(op_a.metrics),
        "obj_store_mem_max_pending_output_per_task",
        new_callable=PropertyMock,
        return_value=1024,
    ):
        for _ in range(8):
            state["a"] += 1
            rm_v3.on_task_dispatched(op_a)
            rm_pristine.update_usages()

    v3_final = rm_v3._op_resource_allocator.get_budget(op_a).cpu
    pristine_final = rm_pristine._op_resource_allocator.get_budget(op_a).cpu
    drift = pristine_final - v3_final

    # v3 hits zero. Pristine still has budget thanks to redistribution.
    assert v3_final == 0.0, (
        f"v3 A.budget should hit zero after 8 dispatches; got {v3_final}"
    )
    assert pristine_final > 0.0, (
        f"Pristine A.budget should be positive (B's slack redistributed); "
        f"got {pristine_final}"
    )
    assert drift > 0.0, (
        f"Expected positive drift (pristine > v3); got {drift}. The drift "
        f"is bounded "
        f"design-intended behavior — pristine recomputes redistribution "
        f"per dispatch, v3 only at step boundaries."
    )
