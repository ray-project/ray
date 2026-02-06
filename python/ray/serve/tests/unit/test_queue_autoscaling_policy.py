import sys
import time

import pytest

from ray.serve.autoscaling_policy import async_inference_autoscaling_policy
from ray.serve.config import AutoscalingConfig, AutoscalingContext


def create_autoscaling_context(
    current_num_replicas: int = 1,
    target_ongoing_requests: float = 10.0,
    total_pending_async_requests: int = 0,
    total_num_requests: int = 0,
    min_replicas: int = 0,
    max_replicas: int = 20,
) -> AutoscalingContext:
    """Helper to create AutoscalingContext for tests."""
    config = AutoscalingConfig(
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        target_ongoing_requests=target_ongoing_requests,
    )

    return AutoscalingContext(
        deployment_id=None,
        deployment_name="test_deployment",
        app_name="test_app",
        current_num_replicas=current_num_replicas,
        target_num_replicas=current_num_replicas,
        running_replicas=[],
        total_num_requests=total_num_requests,
        total_queued_requests=None,
        aggregated_metrics={},
        raw_metrics={},
        capacity_adjusted_min_replicas=min_replicas,
        capacity_adjusted_max_replicas=max_replicas,
        policy_state={},
        last_scale_up_time=None,
        last_scale_down_time=None,
        current_time=time.time(),
        config=config,
        total_pending_async_requests=total_pending_async_requests,
    )


class TestAsyncInferenceAutoscalingPolicy:
    """Tests for async_inference_autoscaling_policy function.

    Note: This tests the raw policy function which returns float values.
    Bounds, ceiling, and delay logic are applied by _apply_autoscaling_config
    decorator, not by the policy itself.
    """

    @pytest.mark.parametrize(
        "queue_length,target_per_replica,expected_replicas",
        [
            (10, 10, 1.0),  # 10/10 = 1.0
            (15, 10, 1.5),  # 15/10 = 1.5
            (100, 10, 10.0),  # 100/10 = 10.0
            (5, 10, 0.5),  # 5/10 = 0.5
        ],
    )
    def test_basic_scaling_formula(
        self,
        queue_length,
        target_per_replica,
        expected_replicas,
    ):
        """Test basic scaling formula: total_workload / target_per_replica."""
        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_ongoing_requests=target_per_replica,
            total_pending_async_requests=queue_length,
        )

        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        assert new_replicas == expected_replicas

    def test_scaling_with_combined_workload(self):
        """Test that scaling uses sum of queue_length and total_num_requests."""
        # Total workload = 30 + 20 = 50
        # With target_ongoing_requests=10, expected = 50/10 = 5.0
        ctx = create_autoscaling_context(
            current_num_replicas=2,
            total_pending_async_requests=30,
            total_num_requests=20,
        )

        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        assert new_replicas == 5.0

    def test_zero_workload_returns_zero(self):
        """Test that zero workload returns zero desired replicas."""
        ctx = create_autoscaling_context(current_num_replicas=5)

        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        # Raw policy returns 0.0; bounds are applied separately
        assert new_replicas == 0.0

    def test_cold_start_with_zero_replicas(self):
        """Test cold start path when current_num_replicas is 0."""
        ctx = create_autoscaling_context(
            current_num_replicas=0,
            total_pending_async_requests=50,
            max_replicas=10,
        )

        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        assert new_replicas == 1.0

    def test_returns_empty_policy_state(self):
        """Test that policy returns empty dict for policy state."""
        ctx = create_autoscaling_context(
            current_num_replicas=5,
            total_pending_async_requests=50,
        )

        _, policy_state = async_inference_autoscaling_policy(ctx)
        assert policy_state == {}


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
