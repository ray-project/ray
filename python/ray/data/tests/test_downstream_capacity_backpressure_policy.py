import types
from unittest.mock import MagicMock, patch

import pytest

from ray.data._internal.execution.backpressure_policy.downstream_capacity_backpressure_policy import (
    DownstreamCapacityBackpressurePolicy,
)
from ray.data._internal.execution.interfaces.physical_operator import (
    OpRuntimeMetrics,
    PhysicalOperator,
)
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.execution.operators.base_physical_operator import (
    AllToAllOperator,
)
from ray.data._internal.execution.operators.task_pool_map_operator import (
    TaskPoolMapOperator,
)
from ray.data._internal.execution.resource_manager import ResourceManager
from ray.data._internal.execution.streaming_executor_state import OpState
from ray.data.context import DataContext


class TestDownstreamCapacityBackpressurePolicy:
    @pytest.fixture(autouse=True)
    def setup_budget_fraction_mock(self):
        """Fixture to patch get_utilized_object_store_budget_fraction for all tests."""
        with patch(
            "ray.data._internal.execution.backpressure_policy."
            "downstream_capacity_backpressure_policy."
            "get_utilized_object_store_budget_fraction"
        ) as mock_func:
            self._mock_get_utilized_budget_fraction = mock_func
            yield

    def _mock_operator(
        self,
        op_class: type = PhysicalOperator,
        num_tasks_running: int = 5,
        obj_store_mem_internal_inqueue: int = 1000,
        obj_store_mem_pending_task_inputs: int = 1000,
        throttling_disabled: bool = False,
        has_execution_finished: bool = False,
    ):
        """Helper method to create mock operator.

        Args:
            op_class: The operator class to mock.
            num_tasks_running: Number of tasks running.
            obj_store_mem_internal_inqueue: Object store memory in internal queue.
            obj_store_mem_pending_task_inputs: Object store memory for pending inputs.
            throttling_disabled: If True, operator is ineligible for backpressure.
            has_execution_finished: If True, operator is ineligible for backpressure.

        Returns:
            A mock operator with the specified configuration.
        """
        mock_operator = MagicMock(spec=op_class)
        mock_operator.metrics = MagicMock(spec=OpRuntimeMetrics)
        mock_operator.metrics.num_tasks_running = num_tasks_running
        mock_operator.metrics.obj_store_mem_internal_inqueue = (
            obj_store_mem_internal_inqueue
        )
        mock_operator.metrics.obj_store_mem_pending_task_inputs = (
            obj_store_mem_pending_task_inputs
        )
        mock_operator.metrics.obj_store_mem_pending_task_outputs = 0
        mock_operator.output_dependencies = []

        # Set up eligibility methods (used by ResourceManager.is_op_eligible)
        mock_operator.throttling_disabled.return_value = throttling_disabled
        mock_operator.has_execution_finished.return_value = has_execution_finished

        op_state = MagicMock(spec=OpState)
        op_state.output_queue_bytes.return_value = 0
        return mock_operator, op_state

    def _mock_materializing_operator(self):
        """Helper method to create mock materializing operator (e.g., AllToAllOperator).

        This creates a mock that passes isinstance(op, AllToAllOperator).
        We use __class__ assignment to make isinstance work with MagicMock.
        """
        mock_operator = MagicMock(spec=AllToAllOperator)
        mock_operator.__class__ = AllToAllOperator  # Make isinstance work
        mock_operator.metrics = MagicMock(spec=OpRuntimeMetrics)
        mock_operator.metrics.num_tasks_running = 0
        mock_operator.metrics.obj_store_mem_internal_inqueue = 0
        mock_operator.metrics.obj_store_mem_pending_task_inputs = 0
        mock_operator.metrics.obj_store_mem_pending_task_outputs = 0
        mock_operator.output_dependencies = []
        mock_operator.has_execution_finished.return_value = False

        mock_operator.throttling_disabled = types.MethodType(
            AllToAllOperator.throttling_disabled, mock_operator
        )

        op_state = MagicMock(spec=OpState)
        op_state.output_queue_bytes.return_value = 0
        return mock_operator, op_state

    def _mock_task_pool_map_operator(
        self,
        num_tasks_running: int = 5,
        max_concurrency_limit: int = 10,
        obj_store_mem_internal_inqueue: int = 1000,
        obj_store_mem_pending_task_inputs: int = 1000,
    ):
        """Helper method to create mock TaskPoolMapOperator."""
        op, op_state = self._mock_operator(
            TaskPoolMapOperator,
            num_tasks_running,
            obj_store_mem_internal_inqueue,
            obj_store_mem_pending_task_inputs,
        )
        op.get_max_concurrency_limit.return_value = max_concurrency_limit
        return op, op_state

    def _mock_actor_pool_map_operator(
        self,
        num_tasks_running: int = 5,
        max_size: int = 5,
        max_tasks_in_flight_per_actor: int = 2,
        obj_store_mem_internal_inqueue: int = 1000,
        obj_store_mem_pending_task_inputs: int = 1000,
    ):
        """Helper method to create mock ActorPoolMapOperator."""
        op, op_state = self._mock_operator(
            ActorPoolMapOperator,
            num_tasks_running,
            obj_store_mem_internal_inqueue,
            obj_store_mem_pending_task_inputs,
        )
        actor_pool = MagicMock()
        actor_pool.max_size.return_value = max_size
        actor_pool.max_tasks_in_flight_per_actor.return_value = (
            max_tasks_in_flight_per_actor
        )
        op.get_autoscaling_actor_pools.return_value = [actor_pool]
        return op, op_state

    def _create_policy(
        self,
        topology,
        data_context=None,
        resource_manager=None,
    ):
        """Helper method to create policy instance."""
        context = data_context or DataContext()
        rm = resource_manager or MagicMock()
        return DownstreamCapacityBackpressurePolicy(
            data_context=context,
            topology=topology,
            resource_manager=rm,
        )

    def _create_context(self, backpressure_ratio=2.0):
        """Helper to create DataContext with backpressure ratio."""
        context = DataContext()
        context.downstream_capacity_backpressure_ratio = backpressure_ratio
        return context

    def _mock_resource_manager(
        self,
        internal_usage=100,
        outputs_usage=100,
        external_bytes=100,
    ):
        """Helper to create a resource manager mock with common settings."""
        rm = MagicMock()
        # Bind real methods from ResourceManager
        rm.is_op_eligible = types.MethodType(ResourceManager.is_op_eligible, rm)
        rm._get_downstream_ineligible_ops = types.MethodType(
            ResourceManager._get_downstream_ineligible_ops, rm
        )
        rm._is_blocking_materializing_op = types.MethodType(
            ResourceManager._is_blocking_materializing_op, rm
        )
        rm.get_op_internal_object_store_usage.return_value = internal_usage
        rm.get_op_outputs_object_store_usage_with_downstream.return_value = (
            outputs_usage
        )
        rm.get_external_consumer_bytes.return_value = external_bytes
        return rm

    def _set_utilized_budget_fraction(self, rm, fraction):
        """Helper to set utilized budget fraction.

        The policy checks: utilized_fraction <= OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        With threshold=0.9, skip backpressure when utilized_fraction <= 0.9.
        To trigger backpressure, set utilized_fraction > 0.9.
        """
        self._mock_get_utilized_budget_fraction.return_value = fraction
        return fraction

    def _set_queue_ratio(self, op, op_state, rm, queue_size, downstream_capacity):
        """Helper to set queue ratio via mocks.

        Matches _get_queue_ratio logic:
        - queue_size_bytes = output_queue_bytes() + sum(get_op_usage(ineligible).object_store_memory)
        - downstream_capacity_size_bytes = sum(eligible_downstream.metrics.obj_store_mem_pending_task_inputs)
        - If downstream_capacity == 0, returns 0 (no backpressure)
        - Else returns queue_size / downstream_capacity

        Returns the calculated queue_ratio for assertions.
        """
        # Set queue size via output_queue_bytes
        op_state.output_queue_bytes.return_value = queue_size

        # Set downstream capacity on the first output dependency
        if op.output_dependencies:
            downstream_op = op.output_dependencies[0]
            downstream_op.metrics.obj_store_mem_pending_task_inputs = (
                downstream_capacity
            )

        if downstream_capacity == 0:
            return 0
        return queue_size / downstream_capacity

    def test_backpressure_disabled_when_ratio_is_none(self):
        """Test that backpressure is disabled when ratio is None."""
        op, op_state = self._mock_operator()
        topology = {op: op_state}
        context = self._create_context(backpressure_ratio=None)

        policy = self._create_policy(topology, data_context=context)
        assert policy.can_add_input(op) is True

    def test_backpressure_skipped_for_ineligible_op(self):
        """Test that backpressure is skipped for ineligible operators.

        An operator is ineligible when throttling_disabled=True or
        has_execution_finished=True.
        """
        # Create operator with throttling_disabled=True (ineligible)
        op, op_state = self._mock_operator(throttling_disabled=True)
        topology = {op: op_state}
        context = self._create_context()
        rm = self._mock_resource_manager()

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        assert policy.can_add_input(op) is True

    def test_backpressure_skipped_for_materializing_downstream(self):
        """Test that backpressure is skipped when downstream is materializing.

        Creates topology: cur_op -> materializing_op (AllToAllOperator).
        """
        # Create the current operator
        op, op_state = self._mock_operator()
        # Create a materializing downstream operator
        materializing_op, materializing_op_state = self._mock_materializing_operator()
        # Set up topology: op -> materializing_op
        op.output_dependencies = [materializing_op]
        topology = {op: op_state, materializing_op: materializing_op_state}
        context = self._create_context()
        rm = self._mock_resource_manager()

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        assert policy.can_add_input(op) is True

    def test_backpressure_skipped_for_low_utilization(self):
        """Test backpressure skipped when utilized budget fraction is low."""
        op, op_state = self._mock_task_pool_map_operator()
        topology = {op: op_state}
        context = self._create_context()
        rm = self._mock_resource_manager()

        # Utilized budget fraction below threshold = skip backpressure
        # With threshold=0.9, skip backpressure when utilized <= 0.9
        threshold = (
            DownstreamCapacityBackpressurePolicy.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        )
        self._set_utilized_budget_fraction(rm, threshold - 0.05)  # 0.85

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        assert policy.can_add_input(op) is True

    def test_backpressure_skipped_at_threshold(self):
        """Test backpressure skipped when utilized fraction equals threshold."""
        op, op_state = self._mock_task_pool_map_operator()
        topology = {op: op_state}
        context = self._create_context()
        rm = self._mock_resource_manager()

        # Utilized budget fraction at threshold = skip backpressure
        # With threshold=0.9, utilized <= 0.9 skips backpressure
        threshold = (
            DownstreamCapacityBackpressurePolicy.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        )
        self._set_utilized_budget_fraction(rm, threshold)  # 0.9

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        assert policy.can_add_input(op) is True

    def test_backpressure_triggered_high_utilization(self):
        """Test backpressure applied when utilized budget fraction is high."""
        op, op_state = self._mock_task_pool_map_operator()
        downstream_op, downstream_op_state = self._mock_operator()
        op.output_dependencies = [downstream_op]
        topology = {op: op_state, downstream_op: downstream_op_state}
        context = self._create_context(backpressure_ratio=2.0)
        rm = self._mock_resource_manager()

        # Utilized budget fraction above threshold = apply backpressure
        # With threshold=0.9, apply backpressure when utilized > 0.9
        threshold = (
            DownstreamCapacityBackpressurePolicy.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        )
        self._set_utilized_budget_fraction(rm, threshold + 0.05)  # 0.95

        # Queue ratio > 2.0: 1000 / 200 = 5
        queue_ratio = self._set_queue_ratio(
            op, op_state, rm, queue_size=1000, downstream_capacity=200
        )
        assert queue_ratio > 2.0

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        assert policy.can_add_input(op) is False

    def test_backpressure_triggered_high_queue_ratio(self):
        """Test backpressure triggered when queue/capacity ratio is high."""
        op, op_state = self._mock_operator()
        downstream_op, downstream_op_state = self._mock_operator()
        op.output_dependencies = [downstream_op]
        topology = {op: op_state, downstream_op: downstream_op_state}
        context = self._create_context(backpressure_ratio=2.0)
        rm = self._mock_resource_manager()

        # Utilized budget fraction above threshold = check queue ratio
        threshold = (
            DownstreamCapacityBackpressurePolicy.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        )
        self._set_utilized_budget_fraction(rm, threshold + 0.05)  # 0.95

        # Queue ratio > 2.0: 1000 / 200 = 5
        queue_ratio = self._set_queue_ratio(
            op, op_state, rm, queue_size=1000, downstream_capacity=200
        )
        assert queue_ratio > 2.0

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        assert policy.can_add_input(op) is False

    def test_no_backpressure_low_queue_ratio(self):
        """Test no backpressure when queue/capacity ratio is acceptable."""
        op, op_state = self._mock_operator()
        downstream_op, downstream_op_state = self._mock_operator()
        op.output_dependencies = [downstream_op]
        topology = {op: op_state, downstream_op: downstream_op_state}
        context = self._create_context(backpressure_ratio=2.0)
        rm = self._mock_resource_manager()

        # Utilized budget fraction below threshold = skip backpressure
        threshold = (
            DownstreamCapacityBackpressurePolicy.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        )
        self._set_utilized_budget_fraction(rm, threshold - 0.1)  # 0.8

        # Queue ratio < 2.0: 500 / 1000 = 0.5
        queue_ratio = self._set_queue_ratio(
            op, op_state, rm, queue_size=500, downstream_capacity=1000
        )
        assert queue_ratio < 2.0

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        assert policy.can_add_input(op) is True

    def test_no_backpressure_zero_downstream_capacity(self):
        """Test backpressure skipped when downstream capacity is zero."""
        op, op_state = self._mock_task_pool_map_operator()
        downstream_op, downstream_op_state = self._mock_operator()
        op.output_dependencies = [downstream_op]
        topology = {op: op_state, downstream_op: downstream_op_state}
        context = self._create_context()
        rm = self._mock_resource_manager(internal_usage=0, outputs_usage=500)

        # Low utilized budget fraction = skip backpressure
        threshold = (
            DownstreamCapacityBackpressurePolicy.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        )
        self._set_utilized_budget_fraction(rm, threshold - 0.05)  # 0.85

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        assert policy.can_add_input(op) is True

    def test_max_bytes_returns_none_when_backpressure_disabled(self):
        """Test max_task_output_bytes_to_read returns None when disabled."""
        op, op_state = self._mock_operator()
        topology = {op: op_state}
        context = self._create_context(backpressure_ratio=None)

        policy = self._create_policy(topology, data_context=context)
        assert policy.max_task_output_bytes_to_read(op) is None

    def test_max_bytes_returns_none_for_ineligible_op(self):
        """Test max_task_output_bytes_to_read returns None for ineligible op."""
        # Create operator with throttling_disabled=True (ineligible)
        op, op_state = self._mock_operator(throttling_disabled=True)
        topology = {op: op_state}
        context = self._create_context()
        rm = self._mock_resource_manager()

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        assert policy.max_task_output_bytes_to_read(op) is None

    def test_max_bytes_returns_none_for_low_utilization(self):
        """Test max_task_output_bytes_to_read returns None for low utilization."""
        op, op_state = self._mock_task_pool_map_operator()
        topology = {op: op_state}
        context = self._create_context()
        rm = self._mock_resource_manager()

        # Low utilized budget fraction = skip backpressure
        threshold = (
            DownstreamCapacityBackpressurePolicy.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        )
        self._set_utilized_budget_fraction(rm, threshold - 0.05)  # 0.85

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        assert policy.max_task_output_bytes_to_read(op) is None

    def test_max_bytes_returns_zero_for_high_utilization(self):
        """Test max_task_output_bytes_to_read returns 0 for high utilization."""
        op, op_state = self._mock_task_pool_map_operator()
        downstream_op, downstream_op_state = self._mock_operator()
        op.output_dependencies = [downstream_op]
        topology = {op: op_state, downstream_op: downstream_op_state}
        context = self._create_context(backpressure_ratio=2.0)
        rm = self._mock_resource_manager()

        # High utilized budget fraction = apply backpressure
        threshold = (
            DownstreamCapacityBackpressurePolicy.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        )
        self._set_utilized_budget_fraction(rm, threshold + 0.05)  # 0.95

        # Queue ratio > 2.0: 1000 / 200 = 5
        self._set_queue_ratio(
            op, op_state, rm, queue_size=1000, downstream_capacity=200
        )

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        assert policy.max_task_output_bytes_to_read(op) == 0

    def test_max_bytes_returns_zero_for_high_queue_ratio(self):
        """Test max_task_output_bytes_to_read returns 0 for high queue ratio."""
        op, op_state = self._mock_task_pool_map_operator()
        downstream_op, downstream_op_state = self._mock_operator()
        op.output_dependencies = [downstream_op]
        topology = {op: op_state, downstream_op: downstream_op_state}
        context = self._create_context(backpressure_ratio=2.0)
        rm = self._mock_resource_manager()

        # High utilized budget fraction = check queue ratio
        threshold = (
            DownstreamCapacityBackpressurePolicy.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        )
        self._set_utilized_budget_fraction(rm, threshold + 0.05)  # 0.95

        # Queue ratio > 2.0: 1000 / 200 = 5
        self._set_queue_ratio(
            op, op_state, rm, queue_size=1000, downstream_capacity=200
        )

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        assert policy.max_task_output_bytes_to_read(op) == 0

    def test_max_bytes_returns_none_when_no_backpressure(self):
        """Test max_task_output_bytes_to_read returns None when no backpressure."""
        op, op_state = self._mock_task_pool_map_operator()
        downstream_op, downstream_op_state = self._mock_operator()
        op.output_dependencies = [downstream_op]
        topology = {op: op_state, downstream_op: downstream_op_state}
        context = self._create_context(backpressure_ratio=2.0)
        rm = self._mock_resource_manager()

        # High utilized budget fraction = check queue ratio
        threshold = (
            DownstreamCapacityBackpressurePolicy.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        )
        self._set_utilized_budget_fraction(rm, threshold + 0.05)  # 0.95

        # Queue ratio < 2.0: 500 / 1000 = 0.5
        self._set_queue_ratio(
            op, op_state, rm, queue_size=500, downstream_capacity=1000
        )

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )
        result = policy.max_task_output_bytes_to_read(op)
        # Queue ratio is below threshold, so no backpressure limit.
        assert result is None

    def test_backpressure_applied_fast_producer_slow_consumer(self):
        """Test backpressure IS applied when producer is faster than consumer.

        In a fast producer → slow consumer scenario:
        - Queue builds up (producer outputs faster than consumer can process)
        - Downstream capacity is low (slow consumer has fewer pending inputs)
        - Queue/capacity ratio exceeds threshold → backpressure applied
        """
        # Fast producer -> slow consumer topology
        producer_op, producer_state = self._mock_task_pool_map_operator(
            num_tasks_running=5,  # Fast producer, many concurrent tasks
            max_concurrency_limit=10,
        )
        consumer_op, consumer_state = self._mock_task_pool_map_operator(
            num_tasks_running=1,  # Slow consumer, few concurrent tasks
            max_concurrency_limit=2,
        )
        producer_op.output_dependencies = [consumer_op]

        topology = {
            producer_op: producer_state,
            consumer_op: consumer_state,
        }

        context = self._create_context(backpressure_ratio=2.0)
        rm = self._mock_resource_manager()

        # High utilization to trigger backpressure evaluation
        threshold = (
            DownstreamCapacityBackpressurePolicy.OBJECT_STORE_BUDGET_UTIL_THRESHOLD
        )
        self._set_utilized_budget_fraction(rm, threshold + 0.05)

        # Fast producer scenario: large queue, low downstream capacity
        # Queue ratio = 2000 / 200 = 10 (well above 2.0 threshold)
        queue_ratio = self._set_queue_ratio(
            producer_op,
            producer_state,
            rm,
            queue_size=2000,  # Large queue (producer outputting fast)
            downstream_capacity=200,  # Low capacity (slow consumer)
        )
        assert queue_ratio > 2.0  # Verify ratio exceeds backpressure threshold

        policy = self._create_policy(
            topology, data_context=context, resource_manager=rm
        )

        # Producer should be backpressured (cannot add more inputs)
        assert policy.can_add_input(producer_op) is False
        # Output bytes should be limited to 0 (full backpressure)
        assert policy.max_task_output_bytes_to_read(producer_op) == 0


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
