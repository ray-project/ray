import unittest
from unittest.mock import MagicMock

import pytest

from ray.data._internal.execution.backpressure_policy.downstream_capacity_backpressure_policy import (
    DownstreamCapacityBackpressurePolicy,
)
from ray.data._internal.execution.interfaces.physical_operator import PhysicalOperator
from ray.data._internal.execution.operators.actor_pool_map_operator import (
    ActorPoolMapOperator,
)
from ray.data._internal.execution.streaming_executor_state import OpState, Topology
from ray.data.context import DataContext


class TestDownstreamCapacityBackpressurePolicy(unittest.TestCase):
    def setUp(self):
        self.context = DataContext()
        self.context.set_config("backpressure_task_slot_ratio", "2")
        self.context.set_config("backpressure_max_queued_bundles", "4000")
        self.mock_resource_manager = MagicMock()

    def _mock_operator(
        self,
        op_class: PhysicalOperator,
        num_enqueued_input_bundles=0,
        num_active_tasks=0,
    ):
        """Helper method to create mock operator."""
        mock_operator = MagicMock(spec=op_class)
        mock_operator.num_active_tasks.return_value = num_active_tasks

        op_state = MagicMock(spec=OpState)
        op_state.total_enqueued_input_bundles.return_value = num_enqueued_input_bundles
        return mock_operator, op_state

    def _mock_actor_pool_operator(self, num_enqueued_input_bundles=0, num_task_slots=0):
        """Helper method to create mock actor pool operator."""
        mock_operator, mock_op_state = self._mock_operator(
            ActorPoolMapOperator, num_enqueued_input_bundles, num_task_slots
        )
        mock_operator._actor_pool = MagicMock()
        mock_operator._actor_pool.num_task_slots.return_value = num_task_slots
        return mock_operator, mock_op_state

    def _create_policy(
        self, data_context: DataContext = None, topology: Topology = None
    ):
        """Helper method to create policy instance."""
        context = data_context or self.context
        return DownstreamCapacityBackpressurePolicy(
            data_context=context,
            topology=topology,
            resource_manager=self.mock_resource_manager,
        )

    def test_can_add_input_no_backpressure_low_queue_size(self):
        """Test can_add_input returns True when queue size is low."""
        op, op_state = self._mock_operator(PhysicalOperator)
        op_output_dependency, op_output_state = self._mock_operator(
            PhysicalOperator, num_enqueued_input_bundles=100, num_active_tasks=10
        )
        op.output_dependencies = [op_output_dependency]
        policy = self._create_policy(
            topology={
                op: op_state,
                op_output_dependency: op_output_state,
            }
        )
        result = policy.can_add_input(op)
        self.assertTrue(result)

    def test_can_add_input_backpressure_triggered_both_thresholds(self):
        """Test backpressure triggered when both thresholds are exceeded."""
        op, op_state = self._mock_operator(PhysicalOperator)
        op_output_dependency, op_output_state = self._mock_operator(
            PhysicalOperator, num_enqueued_input_bundles=10000, num_active_tasks=10
        )
        op.output_dependencies = [op_output_dependency]

        policy = self._create_policy(
            topology={
                op: op_state,
                op_output_dependency: op_output_state,
            }
        )
        result = policy.can_add_input(op)
        self.assertFalse(result)

    def test_can_add_input_mixed_operators_actor_pool_triggers_backpressure(self):
        """Test backpressure with mixed operators where ActorPoolMapOperator triggers backpressure."""
        op, op_state = self._mock_operator(PhysicalOperator)

        (
            actor_pool_output_dep,
            actor_pool_output_dep_state,
        ) = self._mock_actor_pool_operator(
            num_enqueued_input_bundles=10000, num_task_slots=10
        )
        op.output_dependencies = [actor_pool_output_dep]

        policy = self._create_policy(
            topology={
                op: op_state,
                actor_pool_output_dep: actor_pool_output_dep_state,
            }
        )
        result = policy.can_add_input(op)
        self.assertFalse(result)

    def test_backpressure_disabled_by_default(self):
        """Test that backpressure is disabled by default when no configuration is set."""
        default_context = DataContext()
        op, op_state = self._mock_operator(PhysicalOperator)
        op_output_dependency, op_output_dependency_state = self._mock_operator(
            PhysicalOperator, num_enqueued_input_bundles=100000, num_active_tasks=1
        )
        op.output_dependencies = [op_output_dependency]
        policy = self._create_policy(
            data_context=default_context,
            topology={
                op: op_state,
                op_output_dependency: op_output_dependency_state,
            },
        )

        result = policy.can_add_input(op)
        self.assertTrue(result, "Backpressure should be disabled by default")
        self.assertTrue(
            policy._backpressure_disabled, "Backpressure should be marked as disabled"
        )


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
