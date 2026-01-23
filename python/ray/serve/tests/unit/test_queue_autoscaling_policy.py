import sys
import time
from typing import Any, Dict, Optional

import pytest

from ray.serve._private.constants import (
    CONTROL_LOOP_INTERVAL_S,
    SERVE_AUTOSCALING_DECISION_COUNTERS_KEY,
)
from ray.serve.autoscaling_policy import (
    _apply_scaling_decision_smoothing,
    async_inference_autoscaling_policy,
)
from ray.serve.config import AutoscalingConfig, AutoscalingContext


def create_autoscaling_context(
    current_num_replicas: int = 1,
    target_num_replicas: int = 1,
    min_replicas: int = 0,
    max_replicas: int = 10,
    target_ongoing_requests: float = 10.0,
    total_num_requests: int = 0,
    async_inference_task_queue_length: int = 0,
    upscale_delay_s: float = 0.0,
    downscale_delay_s: float = 0.0,
    downscale_to_zero_delay_s: Optional[float] = None,
    policy_state: Optional[Dict[str, Any]] = None,
    current_time: Optional[float] = None,
    deployment_name: str = "test_deployment",
    app_name: str = "test_app",
) -> AutoscalingContext:
    """Helper to create AutoscalingContext for tests."""
    config = AutoscalingConfig(
        min_replicas=min_replicas,
        max_replicas=max_replicas,
        target_ongoing_requests=target_ongoing_requests,
        upscale_delay_s=upscale_delay_s,
        downscale_delay_s=downscale_delay_s,
        downscale_to_zero_delay_s=downscale_to_zero_delay_s,
    )

    return AutoscalingContext(
        deployment_id=None,
        deployment_name=deployment_name,
        app_name=app_name,
        current_num_replicas=current_num_replicas,
        target_num_replicas=target_num_replicas,
        running_replicas=[],
        total_num_requests=total_num_requests,
        total_queued_requests=None,
        aggregated_metrics={},
        raw_metrics={},
        capacity_adjusted_min_replicas=min_replicas,
        capacity_adjusted_max_replicas=max_replicas,
        policy_state=policy_state or {},
        last_scale_up_time=None,
        last_scale_down_time=None,
        current_time=current_time or time.time(),
        config=config,
        async_inference_task_queue_length=async_inference_task_queue_length,
    )


class TestAsyncInferenceAutoscalingPolicy:
    """Tests for async_inference_autoscaling_policy function."""

    @pytest.mark.parametrize(
        "queue_length,target_per_replica,expected_replicas",
        [
            (10, 10, 1),  # 10/10 = 1
            (15, 10, 2),  # 15/10 = 1.5 -> ceil = 2
            (100, 10, 10),  # 100/10 = 10
            (5, 10, 1),  # 5/10 = 0.5 -> ceil = 1 (clamped to min)
        ],
    )
    def test_basic_scaling_formula(
        self,
        queue_length,
        target_per_replica,
        expected_replicas,
    ):
        """Test basic scaling formula: ceil(queue_length / target_per_replica)."""
        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=target_per_replica,
            async_inference_task_queue_length=queue_length,
            min_replicas=0,
            max_replicas=20,
        )

        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        assert new_replicas == expected_replicas

    def test_scaling_with_combined_workload(self):
        """Test that scaling uses sum of queue_length and total_num_requests."""
        queue_length = 30
        total_num_requests = 20
        # Total workload = 30 + 20 = 50
        # With target_ongoing_requests=10, expected = ceil(50/10) = 5

        ctx = create_autoscaling_context(
            current_num_replicas=2,
            target_num_replicas=2,
            target_ongoing_requests=10,
            total_num_requests=total_num_requests,
            async_inference_task_queue_length=queue_length,
            min_replicas=0,
            max_replicas=20,
        )

        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        assert new_replicas == 5

    def test_scale_down_to_one_before_zero(self):
        """Test that scaling to zero goes through 1 first (policy enforces 1->0)."""
        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=10,
            async_inference_task_queue_length=0,
            min_replicas=0,
            max_replicas=20,
        )

        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        # Policy enforces min of 1 for non-zero to zero transition
        assert new_replicas == 1

    def test_respects_max_replicas(self):
        """Test that scaling respects max_replicas bound."""
        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=10,
            async_inference_task_queue_length=1000,
            max_replicas=10,
        )

        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        assert new_replicas == 10

    def test_respects_min_replicas(self):
        """Test that scaling respects min_replicas bound."""
        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=10,
            async_inference_task_queue_length=5,
            min_replicas=3,
        )

        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        assert new_replicas == 3

    def test_scale_from_zero(self):
        """Test scaling up from zero replicas."""
        ctx = create_autoscaling_context(
            current_num_replicas=0,
            target_num_replicas=0,
            target_ongoing_requests=10,
            async_inference_task_queue_length=50,
            min_replicas=0,
            max_replicas=10,
        )

        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        assert new_replicas == 1

    def test_scale_from_zero_with_one_task(self):
        """Test scaling from zero with a single task."""
        ctx = create_autoscaling_context(
            current_num_replicas=0,
            target_num_replicas=0,
            target_ongoing_requests=10,
            async_inference_task_queue_length=1,
            min_replicas=0,
            max_replicas=10,
        )

        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        assert new_replicas == 1

    def test_stay_at_zero_with_empty_queue(self):
        """Test staying at zero replicas when queue is empty."""
        ctx = create_autoscaling_context(
            current_num_replicas=0,
            target_num_replicas=0,
            target_ongoing_requests=10,
            async_inference_task_queue_length=0,
            min_replicas=0,
        )

        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        assert new_replicas == 0


class TestAsyncInferenceAutoscalingPolicyDelays:
    """Tests for upscale and downscale delays in async_inference_autoscaling_policy."""

    def test_upscale_delay(self):
        """Test that upscale decisions require delay."""
        upscale_delay_s = 30.0
        wait_periods = int(upscale_delay_s / CONTROL_LOOP_INTERVAL_S)

        policy_state = {}
        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=10,
            async_inference_task_queue_length=200,
            max_replicas=20,
            upscale_delay_s=upscale_delay_s,
            policy_state=policy_state,
        )

        # First wait_periods calls should not scale
        for i in range(wait_periods):
            new_replicas, policy_state = async_inference_autoscaling_policy(ctx)
            ctx.policy_state = policy_state
            assert new_replicas == 5, f"Should not scale up at iteration {i}"

        # Next call should scale
        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        assert new_replicas == 20

    def test_downscale_delay(self):
        """Test that downscale decisions require delay."""
        downscale_delay_s = 60.0
        wait_periods = int(downscale_delay_s / CONTROL_LOOP_INTERVAL_S)

        policy_state = {}
        ctx = create_autoscaling_context(
            current_num_replicas=10,
            target_num_replicas=10,
            target_ongoing_requests=10,
            async_inference_task_queue_length=20,
            min_replicas=1,
            downscale_delay_s=downscale_delay_s,
            policy_state=policy_state,
        )

        # First wait_periods calls should not scale down
        for i in range(wait_periods):
            new_replicas, policy_state = async_inference_autoscaling_policy(ctx)
            ctx.policy_state = policy_state
            assert new_replicas == 10, f"Should not scale down at iteration {i}"

        # Next call should scale
        new_replicas, _ = async_inference_autoscaling_policy(ctx)
        assert new_replicas == 2


class TestAsyncInferenceAutoscalingPolicyState:
    """Tests for policy state management."""

    def test_preserves_decision_counter(self):
        """Test that decision counter is preserved across calls."""
        policy_state = {}
        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=10,
            async_inference_task_queue_length=200,
            upscale_delay_s=30.0,
            policy_state=policy_state,
        )

        _, policy_state = async_inference_autoscaling_policy(ctx)
        assert policy_state.get(SERVE_AUTOSCALING_DECISION_COUNTERS_KEY, 0) == 1

        # Call again
        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=10,
            async_inference_task_queue_length=200,
            upscale_delay_s=30.0,
            policy_state=policy_state,
        )
        _, policy_state = async_inference_autoscaling_policy(ctx)
        assert policy_state.get(SERVE_AUTOSCALING_DECISION_COUNTERS_KEY, 0) == 2


class TestApplyScalingDecisionSmoothing:
    """Tests for _apply_scaling_decision_smoothing helper function."""

    def _create_config(
        self,
        upscale_delay_s: float = 0.0,
        downscale_delay_s: float = 0.0,
        downscale_to_zero_delay_s: Optional[float] = None,
    ) -> AutoscalingConfig:
        """Helper to create AutoscalingConfig for smoothing tests."""
        return AutoscalingConfig(
            min_replicas=0,
            max_replicas=100,
            upscale_delay_s=upscale_delay_s,
            downscale_delay_s=downscale_delay_s,
            downscale_to_zero_delay_s=downscale_to_zero_delay_s,
        )

    def test_no_change_resets_counter(self):
        """Test that no change in desired replicas resets the decision counter."""
        config = self._create_config()
        decision_replicas, counter = _apply_scaling_decision_smoothing(
            desired_num_replicas=5,
            curr_target_num_replicas=5,
            decision_counter=3,  # Previous positive counter
            config=config,
        )

        assert decision_replicas == 5
        assert counter == 0

    def test_scale_up_after_delay(self):
        """Test that scale up happens after delay is met."""
        upscale_delay_s = 30.0
        wait_periods = int(upscale_delay_s / CONTROL_LOOP_INTERVAL_S)
        config = self._create_config(upscale_delay_s=upscale_delay_s)

        # Simulate reaching the delay threshold
        decision_replicas, counter = _apply_scaling_decision_smoothing(
            desired_num_replicas=10,
            curr_target_num_replicas=5,
            decision_counter=wait_periods,  # At threshold
            config=config,
        )

        # Should scale now
        assert decision_replicas == 10
        assert counter == 0

    def test_scale_down_after_delay(self):
        """Test that scale down happens after delay is met."""
        downscale_delay_s = 60.0
        wait_periods = int(downscale_delay_s / CONTROL_LOOP_INTERVAL_S)
        config = self._create_config(downscale_delay_s=downscale_delay_s)

        # Simulate reaching the delay threshold (negative)
        decision_replicas, counter = _apply_scaling_decision_smoothing(
            desired_num_replicas=3,
            curr_target_num_replicas=5,
            decision_counter=-(wait_periods),  # At threshold
            config=config,
        )

        # Should scale now
        assert decision_replicas == 3
        assert counter == 0

    def test_scale_down_enforces_min_one_for_non_zero_transition(self):
        """Test that scaling down from >1 enforces minimum of 1 replica."""
        downscale_delay_s = 0.0  # No delay for this test
        config = self._create_config(downscale_delay_s=downscale_delay_s)

        # Try to scale from 5 to 0 directly
        decision_replicas, _ = _apply_scaling_decision_smoothing(
            desired_num_replicas=0,
            curr_target_num_replicas=5,
            decision_counter=-1,
            config=config,
        )

        # Should be clamped to 1, not 0 (must go through 1->0 transition)
        assert decision_replicas == 1

    def test_immediate_scale_up_with_zero_delay(self):
        """Test immediate scale up when upscale_delay_s is 0."""
        config = self._create_config(upscale_delay_s=0.0)
        decision_replicas, counter = _apply_scaling_decision_smoothing(
            desired_num_replicas=10,
            curr_target_num_replicas=5,
            decision_counter=0,
            config=config,
        )

        # Should scale immediately
        assert decision_replicas == 10
        assert counter == 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
