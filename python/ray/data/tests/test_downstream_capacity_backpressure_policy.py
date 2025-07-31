from unittest.mock import MagicMock

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
from ray.data._internal.execution.streaming_executor_state import OpState, Topology
from ray.data.context import DataContext


class TestDownstreamCapacityBackpressurePolicy:
    def _mock_operator(
        self,
        op_class: PhysicalOperator = PhysicalOperator,
        num_enqueued_input_bundles: int = 0,
        num_task_inputs_processed: int = 0,
        num_tasks_finished: int = 0,
        max_concurrent_tasks: int = 100,
    ):
        """Helper method to create mock operator."""
        mock_operator = MagicMock(spec=op_class)
        mock_operator.metrics = MagicMock(spec=OpRuntimeMetrics)
        mock_operator.metrics.num_task_inputs_processed = num_task_inputs_processed
        mock_operator.metrics.num_tasks_finished = num_tasks_finished
        mock_operator.num_active_tasks.return_value = max_concurrent_tasks

        op_state = MagicMock(spec=OpState)
        op_state.total_enqueued_input_bundles.return_value = num_enqueued_input_bundles
        return mock_operator, op_state

    def _mock_actor_pool_map_operator(
        self,
        num_enqueued_input_bundles: int,
        num_task_inputs_processed: int,
        num_tasks_finished: int,
        max_concurrent_tasks: int = 100,
    ):
        """Helper method to create mock actor pool map operator."""
        op, op_state = self._mock_operator(
            ActorPoolMapOperator,
            num_enqueued_input_bundles,
            num_task_inputs_processed,
            num_tasks_finished,
            max_concurrent_tasks,
        )
        actor_pool = MagicMock(
            spec="ray.data._internal.execution.operators.actor_pool_map_operator._ActorPool"
        )
        actor_pool.max_concurrent_tasks = MagicMock(return_value=max_concurrent_tasks)
        op.get_autoscaling_actor_pools.return_value = [actor_pool]
        return op, op_state

    def _create_policy(
        self, data_context: DataContext = None, topology: Topology = None
    ):
        """Helper method to create policy instance."""
        context = data_context or self.context
        return DownstreamCapacityBackpressurePolicy(
            data_context=context,
            topology=topology,
            resource_manager=MagicMock(),
        )

    @pytest.mark.parametrize(
        "mock_method",
        [
            (_mock_operator),
            (_mock_actor_pool_map_operator),
        ],
    )
    @pytest.mark.parametrize(
        "num_enqueued, num_task_inputs_processed, num_tasks_finished, backpressure_ratio, max_queued_bundles, expected_result, test_name",
        [
            (100, 100, 10, 2, 4000, True, "no_backpressure_low_queue"),
            (5000, 100, 10, 2, 4000, False, "high_queue_pressure"),
            (100, 0, 0, 2, 400, True, "zero_inputs_protection"),
            (1000000, 1, 1, None, None, True, "default disabled"),
        ],
    )
    def test_backpressure_conditions(
        self,
        mock_method,
        num_enqueued,
        num_task_inputs_processed,
        num_tasks_finished,
        backpressure_ratio,
        max_queued_bundles,
        expected_result,
        test_name,
    ):
        """Parameterized test covering various backpressure conditions."""
        context = DataContext()
        context.downstream_capacity_backpressure_ratio = backpressure_ratio
        context.downstream_capacity_backpressure_max_queued_bundles = max_queued_bundles

        op, op_state = self._mock_operator(PhysicalOperator)
        op_output_dep, op_output_state = mock_method(
            self,
            num_enqueued_input_bundles=num_enqueued,
            num_task_inputs_processed=num_task_inputs_processed,
            num_tasks_finished=num_tasks_finished,
        )
        op.output_dependencies = [op_output_dep]

        policy = self._create_policy(
            context, topology={op: op_state, op_output_dep: op_output_state}
        )
        result = policy.can_add_input(op)

        assert result == expected_result, test_name
        assert (
            backpressure_ratio is None or max_queued_bundles is None
        ) == policy._backpressure_disabled, test_name


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
