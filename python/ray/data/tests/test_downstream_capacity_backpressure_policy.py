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
        num_enqueued_blocks: int = 0,
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
        op_state.total_enqueued_input_blocks.return_value = num_enqueued_blocks
        return mock_operator, op_state

    def _mock_actor_pool_map_operator(
        self,
        num_enqueued_blocks: int,
        num_task_inputs_processed: int,
        num_tasks_finished: int,
        max_concurrent_tasks: int = 100,
    ):
        """Helper method to create mock actor pool map operator."""
        op, op_state = self._mock_operator(
            ActorPoolMapOperator,
            num_enqueued_blocks,
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
            num_enqueued_blocks=num_enqueued,
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

    @pytest.mark.parametrize(
        "mock_method",
        [
            (_mock_operator),
            (_mock_actor_pool_map_operator),
        ],
    )
    @pytest.mark.parametrize(
        "num_enqueued, avg_num_inputs_per_task, avg_bytes_per_output, max_concurrent_tasks, outputs_ratio, expected_result, test_name",
        [
            # outputs_ratio disabled returns None
            (0, 10, 1000, 10, None, None, "outputs_ratio_none"),
            (0, 10, 1000, 10, 0, None, "outputs_ratio_zero"),
            (0, 10, 1000, 10, -1, None, "outputs_ratio_negative"),
            # No metrics available returns None
            (0, None, 1000, 10, 1.0, None, "no_avg_inputs_metric"),
            (0, 0, 1000, 10, 1.0, None, "zero_avg_inputs_metric"),
            (0, 10, None, 10, 1.0, None, "no_avg_bytes_metric"),
            (0, 10, 0, 10, 1.0, None, "zero_avg_bytes_metric"),
            # No concurrent tasks returns None
            (0, 10, 1000, 0, 1.0, None, "no_concurrent_tasks"),
            # At capacity returns 0
            (100, 10, 1000, 10, 1.0, 0, "at_capacity"),
            (150, 10, 1000, 10, 1.0, 0, "over_capacity"),
            # Normal case: remaining_capacity = (max_concurrent * avg_inputs * ratio) - enqueued
            # = (10 * 10 * 1.0) - 50 = 50 blocks, * 1000 bytes = 50000 bytes
            (50, 10, 1000, 10, 1.0, 50000, "half_capacity"),
            # With ratio=2.0: (10 * 10 * 2.0) - 50 = 150 blocks, * 1000 = 150000 bytes
            (50, 10, 1000, 10, 2.0, 150000, "half_capacity_double_ratio"),
        ],
    )
    def test_max_task_output_bytes_to_read(
        self,
        mock_method,
        num_enqueued,
        avg_num_inputs_per_task,
        avg_bytes_per_output,
        max_concurrent_tasks,
        outputs_ratio,
        expected_result,
        test_name,
    ):
        """Parameterized test for max_task_output_bytes_to_read method."""
        context = DataContext()
        context.downstream_capacity_outputs_ratio = outputs_ratio

        op, op_state = self._mock_operator(PhysicalOperator)
        op.metrics.average_bytes_per_output = avg_bytes_per_output

        op_output_dep, op_output_state = mock_method(
            self,
            num_enqueued_blocks=num_enqueued,
            num_task_inputs_processed=0,
            num_tasks_finished=0,
            max_concurrent_tasks=max_concurrent_tasks,
        )
        op_output_dep.metrics.average_num_inputs_per_task = avg_num_inputs_per_task
        op.output_dependencies = [op_output_dep]

        policy = self._create_policy(
            context, topology={op: op_state, op_output_dep: op_output_state}
        )
        result = policy.max_task_output_bytes_to_read(op)

        assert result == expected_result, test_name


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
