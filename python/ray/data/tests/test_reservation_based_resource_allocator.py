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
    get_ineligible_op_usage,
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
        o2 = mock_map_op(o1, incremental_resource_usage=ExecutionResources(1, 0, 15))
        o3 = mock_map_op(o2, incremental_resource_usage=ExecutionResources(1, 0, 10))
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

        # Set min_max_resource_requirements to use incremental_resource_usage as minimum
        for op in [o2, o3, o4, o5]:
            op.min_max_resource_requirements = MagicMock(
                return_value=(
                    incremental_usage,
                    ExecutionResources(cpu=100, gpu=0, object_store_memory=10000),
                )
            )

        topo = build_streaming_topology(o5, ExecutionOptions())

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
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
        topo = build_streaming_topology(o2, ExecutionOptions())

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
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
        o2 = mock_map_op(o1, incremental_resource_usage=ExecutionResources(1, 0, 10))
        o3 = mock_map_op(o2, incremental_resource_usage=ExecutionResources(1, 0, 10))

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
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
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
        o2 = mock_map_op(o1, incremental_resource_usage=ExecutionResources(1, 0, 10))
        o3 = mock_map_op(o2, incremental_resource_usage=ExecutionResources(1, 0, 10))

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
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
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
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
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
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
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
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
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
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
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
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
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

    def test_get_ineligible_op_usage(self, restore_data_context):
        DataContext.get_current().op_resource_reservation_enabled = True

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
            o2: ExecutionResources(cpu=1, object_store_memory=1),
            o3: ExecutionResources(cpu=2, object_store_memory=2),
            o4: ExecutionResources(cpu=3, object_store_memory=3),
            o5: ExecutionResources(cpu=4, object_store_memory=4),
        }

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])

        # Ineligible: o1 (finished), o2 (finished), o3 (throttling disabled)
        ineligible_usage = get_ineligible_op_usage(topo, resource_manager)

        # Expected: o1 + o2 + o3 = (0+1+2, 0+10+20)
        assert ineligible_usage == ExecutionResources(cpu=3, object_store_memory=3)

    def test_get_ineligible_op_usage_complex_graph(self, restore_data_context):
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
            o2: ExecutionResources(cpu=2, object_store_memory=2),
            o3: ExecutionResources(cpu=3, object_store_memory=3),
            o4: ExecutionResources.zero(),
            o5: ExecutionResources(cpu=5, object_store_memory=5),
            o6: ExecutionResources(cpu=6, object_store_memory=6),
            o7: ExecutionResources.zero(),
            o8: ExecutionResources(cpu=8, object_store_memory=8),
        }

        resource_manager = ResourceManager(
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])

        ineligible_usage = get_ineligible_op_usage(topo, resource_manager)

        # Ineligible: o1, o2, o3, o4, o5, o7 -> sum = 2+3+5 = 10
        assert ineligible_usage == ExecutionResources(cpu=10, object_store_memory=10)

    def test_reservation_accounts_for_completed_ops(self, restore_data_context):
        """Test that resource reservation properly accounts for completed ops."""
        DataContext.get_current().op_resource_reservation_enabled = True
        DataContext.get_current().op_resource_reservation_ratio = 0.5

        o1 = InputDataBuffer(DataContext.get_current(), [])
        o2 = mock_map_op(o1, incremental_resource_usage=ExecutionResources(1, 0, 10))
        o3 = mock_map_op(o2, incremental_resource_usage=ExecutionResources(1, 0, 10))
        o4 = mock_map_op(o3, incremental_resource_usage=ExecutionResources(1, 0, 10))

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
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager._mem_op_internal = op_internal_usage
        resource_manager._mem_op_outputs = op_outputs_usages
        resource_manager.get_global_limits = MagicMock(return_value=global_limits)

        allocator = resource_manager._op_resource_allocator
        allocator.update_budgets(
            limits=global_limits,
        )

        # Check that o2's usage was subtracted from remaining resources
        # global_limits (10 CPU, 250 mem) - o1 usage (0) - o2 usage (2 CPU, 50 mem) = remaining (8 CPU, 200 mem)
        # With 2 eligible ops (o3, o4) and 50% reservation ratio:
        # Each op gets reserved: (8 CPU, 200 mem) * 0.5 / 2 = (2 CPU, 50 mem)

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
        o2 = mock_map_op(o1, incremental_resource_usage=ExecutionResources(1, 0, 15))
        o3 = LimitOperator(1, o2, DataContext.get_current())
        o4 = InputDataBuffer(DataContext.get_current(), [])
        o5 = mock_map_op(o4, incremental_resource_usage=ExecutionResources(1, 0, 10))
        o6 = mock_union_op(
            [o3, o5], incremental_resource_usage=ExecutionResources(1, 0, 20)
        )
        o7 = InputDataBuffer(DataContext.get_current(), [])
        o8 = mock_join_op(
            o7, o6, incremental_resource_usage=ExecutionResources(1, 0, 30)
        )

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
            topo, ExecutionOptions(), MagicMock(), DataContext.get_current()
        )
        resource_manager.get_op_usage = MagicMock(side_effect=lambda op: op_usages[op])
        resource_manager.get_global_limits = MagicMock(
            side_effect=mock_get_global_limits
        )
        resource_manager._mem_op_internal = op_internal_usage
        resource_manager._mem_op_outputs = op_outputs_usages

        allocator = resource_manager._op_resource_allocator
        global_limits = ExecutionResources(cpu=20, object_store_memory=2000)
        allocator.update_budgets(
            limits=global_limits,
        )
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
        assert allocator._op_budgets[o8] == ExecutionResources(
            cpu=6, object_store_memory=600
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
        allocator.update_budgets(
            limits=global_limits,
        )
        assert allocator._op_budgets[o6] == ExecutionResources(
            cpu=4, object_store_memory=350
        )
        assert allocator._op_budgets[o8] == ExecutionResources(
            cpu=4, object_store_memory=500
        )

        # Test when completed ops update the usage.
        op_usages[o5] = ExecutionResources.zero()
        allocator.update_budgets(
            limits=global_limits,
        )
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
        # object_store_memory budget = 163 + (850 - 87) / 2 = 545 (rounded up)
        assert allocator._op_budgets[o8] == ExecutionResources(
            cpu=5.5, object_store_memory=545
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
