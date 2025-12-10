import sys
import time
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest

from ray.serve._private.constants import CONTROL_LOOP_INTERVAL_S
from ray.serve._private.queue_monitor import QUEUE_MONITOR_ACTOR_PREFIX
from ray.serve.autoscaling_policy import queue_based_autoscaling_policy
from ray.serve.config import AutoscalingConfig, AutoscalingContext


def create_autoscaling_context(
    current_num_replicas: int = 1,
    target_num_replicas: int = 1,
    min_replicas: int = 0,
    max_replicas: int = 10,
    target_ongoing_requests: float = 10.0,
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
        config=config,
        current_num_replicas=current_num_replicas,
        target_num_replicas=target_num_replicas,
        total_num_requests=0,
        capacity_adjusted_min_replicas=min_replicas,
        capacity_adjusted_max_replicas=max_replicas,
        policy_state=policy_state or {},
        deployment_id=None,
        deployment_name=deployment_name,
        app_name=app_name,
        running_replicas=None,
        current_time=current_time or time.time(),
        total_queued_requests=None,
        total_running_requests=None,
        aggregated_metrics={},
        raw_metrics={},
        last_scale_up_time=None,
        last_scale_down_time=None,
    )


@pytest.fixture
def mock_ray_actor_methods():
    """Fixture to mock ray.get_actor and ray.get for QueueMonitor actor access."""
    with patch("ray.serve.autoscaling_policy.ray.get_actor") as mock_get_actor, \
         patch("ray.serve.autoscaling_policy.ray.get") as mock_ray_get:
        yield mock_get_actor, mock_ray_get


def setup_queue_monitor_mocks(
    mock_get_actor, mock_ray_get, queue_length, deployment_name="test_deployment",
    config_dict=None
):
    """Helper to set up all mocks for a successful queue monitor query."""
    queue_monitor_actor_name = f"{QUEUE_MONITOR_ACTOR_PREFIX}{deployment_name}"

    # Mock the actor handle
    mock_actor = MagicMock()
    mock_queue_length_ref = MagicMock()
    mock_config_ref = MagicMock()
    mock_actor.get_queue_length.remote.return_value = mock_queue_length_ref
    mock_actor.get_config.remote.return_value = mock_config_ref
    mock_get_actor.return_value = mock_actor

    # Mock ray.get to return the queue length or config based on the ref
    if config_dict is None:
        config_dict = {"broker_url": "redis://localhost", "queue_name": "test_queue"}

    def mock_ray_get_side_effect(ref, **kwargs):
        if ref == mock_queue_length_ref:
            return queue_length
        elif ref == mock_config_ref:
            return config_dict
        return queue_length  # fallback

    mock_ray_get.side_effect = mock_ray_get_side_effect

    return queue_monitor_actor_name


class TestQueueBasedAutoscalingPolicy:
    """Tests for queue_based_autoscaling_policy function."""

    def test_queue_monitor_unavailable_maintains_replicas(
        self, mock_ray_actor_methods
    ):
        """Test that unavailable QueueMonitor maintains current replica count."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        # Mock actor not found
        mock_get_actor.side_effect = ValueError("Actor not found")

        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
        )

        new_replicas, _ = queue_based_autoscaling_policy(ctx)
        assert new_replicas == 5

    def test_queue_monitor_query_fails_maintains_replicas(
        self, mock_ray_actor_methods
    ):
        """Test that failed query maintains current replica count."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        # Mock successful actor but failed ray.get
        mock_actor = MagicMock()
        mock_get_actor.return_value = mock_actor
        mock_ray_get.side_effect = Exception("Query failed")

        ctx = create_autoscaling_context(
            current_num_replicas=3,
            target_num_replicas=3,
        )

        new_replicas, _ = queue_based_autoscaling_policy(ctx)
        assert new_replicas == 3

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
        mock_ray_actor_methods,
        queue_length,
        target_per_replica,
        expected_replicas,
    ):
        """Test basic scaling formula: ceil(queue_length / target_per_replica)."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        setup_queue_monitor_mocks(mock_get_actor, mock_ray_get, queue_length)

        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=target_per_replica,
            min_replicas=0,
            max_replicas=20,
            upscale_delay_s=0,
            downscale_delay_s=0,
        )

        new_replicas, _ = queue_based_autoscaling_policy(ctx)
        assert new_replicas == expected_replicas

    def test_scale_down_to_one_before_zero(
        self, mock_ray_actor_methods
    ):
        """Test that scaling to zero goes through 1 first (policy enforces 1->0)."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        setup_queue_monitor_mocks(mock_get_actor, mock_ray_get, 0)

        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=10,
            min_replicas=0,
            max_replicas=20,
            upscale_delay_s=0,
            downscale_delay_s=0,
        )

        new_replicas, _ = queue_based_autoscaling_policy(ctx)
        # Policy enforces min of 1 for non-zero to zero transition
        assert new_replicas == 1

    def test_respects_max_replicas(
        self, mock_ray_actor_methods
    ):
        """Test that scaling respects max_replicas bound."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        setup_queue_monitor_mocks(mock_get_actor, mock_ray_get, 1000)

        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=10,
            max_replicas=10,
            upscale_delay_s=0,
        )

        new_replicas, _ = queue_based_autoscaling_policy(ctx)
        assert new_replicas == 10

    def test_respects_min_replicas(
        self, mock_ray_actor_methods
    ):
        """Test that scaling respects min_replicas bound."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        setup_queue_monitor_mocks(mock_get_actor, mock_ray_get, 5)

        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=10,
            min_replicas=3,
            upscale_delay_s=0,
            downscale_delay_s=0,
        )

        new_replicas, _ = queue_based_autoscaling_policy(ctx)
        assert new_replicas == 3

    def test_scale_from_zero(
        self, mock_ray_actor_methods
    ):
        """Test scaling up from zero replicas."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        setup_queue_monitor_mocks(mock_get_actor, mock_ray_get, 50)

        ctx = create_autoscaling_context(
            current_num_replicas=0,
            target_num_replicas=0,
            target_ongoing_requests=10,
            min_replicas=0,
            max_replicas=10,
        )

        new_replicas, _ = queue_based_autoscaling_policy(ctx)
        assert new_replicas == 5  # ceil(50/10)

    def test_scale_from_zero_with_one_task(
        self, mock_ray_actor_methods
    ):
        """Test scaling from zero with a single task."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        setup_queue_monitor_mocks(mock_get_actor, mock_ray_get, 1)

        ctx = create_autoscaling_context(
            current_num_replicas=0,
            target_num_replicas=0,
            target_ongoing_requests=10,
            min_replicas=0,
            max_replicas=10,
        )

        new_replicas, _ = queue_based_autoscaling_policy(ctx)
        assert new_replicas == 1

    def test_stay_at_zero_with_empty_queue(
        self, mock_ray_actor_methods
    ):
        """Test staying at zero replicas when queue is empty."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        setup_queue_monitor_mocks(mock_get_actor, mock_ray_get, 0)

        ctx = create_autoscaling_context(
            current_num_replicas=0,
            target_num_replicas=0,
            target_ongoing_requests=10,
            min_replicas=0,
        )

        new_replicas, _ = queue_based_autoscaling_policy(ctx)
        assert new_replicas == 0

    def test_correct_queue_monitor_actor_name(
        self, mock_ray_actor_methods
    ):
        """Test that correct QueueMonitor actor name is used."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        queue_monitor_actor_name = f"{QUEUE_MONITOR_ACTOR_PREFIX}my_task_consumer"
        setup_queue_monitor_mocks(
            mock_get_actor, mock_ray_get, 50,
            deployment_name="my_task_consumer"
        )

        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            deployment_name="my_task_consumer",
            app_name="my_app",
        )

        queue_based_autoscaling_policy(ctx)

        # Verify ray.get_actor was called with the correct actor name
        mock_get_actor.assert_called_once_with(
            queue_monitor_actor_name,
            namespace="serve",
        )


class TestQueueBasedAutoscalingPolicyDelays:
    """Tests for upscale and downscale delays in queue_based_autoscaling_policy."""

    def test_upscale_delay(
        self, mock_ray_actor_methods
    ):
        """Test that upscale decisions require delay."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        setup_queue_monitor_mocks(mock_get_actor, mock_ray_get, 200)

        upscale_delay_s = 30.0
        wait_periods = int(upscale_delay_s / CONTROL_LOOP_INTERVAL_S)

        policy_state = {}
        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=10,
            max_replicas=20,
            upscale_delay_s=upscale_delay_s,
            policy_state=policy_state,
        )

        # First wait_periods calls should not scale
        for i in range(wait_periods):
            new_replicas, policy_state = queue_based_autoscaling_policy(ctx)
            ctx = create_autoscaling_context(
                current_num_replicas=5,
                target_num_replicas=5,
                target_ongoing_requests=10,
                max_replicas=20,
                upscale_delay_s=upscale_delay_s,
                policy_state=policy_state,
            )
            assert new_replicas == 5, f"Should not scale up at iteration {i}"

        # Next call should scale
        new_replicas, _ = queue_based_autoscaling_policy(ctx)
        assert new_replicas == 20

    def test_downscale_delay(
        self, mock_ray_actor_methods
    ):
        """Test that downscale decisions require delay."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        setup_queue_monitor_mocks(mock_get_actor, mock_ray_get, 20)

        downscale_delay_s = 60.0
        wait_periods = int(downscale_delay_s / CONTROL_LOOP_INTERVAL_S)

        policy_state = {}
        ctx = create_autoscaling_context(
            current_num_replicas=10,
            target_num_replicas=10,
            target_ongoing_requests=10,
            min_replicas=1,
            downscale_delay_s=downscale_delay_s,
            policy_state=policy_state,
        )

        # First wait_periods calls should not scale down
        for i in range(wait_periods):
            new_replicas, policy_state = queue_based_autoscaling_policy(ctx)
            ctx = create_autoscaling_context(
                current_num_replicas=10,
                target_num_replicas=10,
                target_ongoing_requests=10,
                min_replicas=1,
                downscale_delay_s=downscale_delay_s,
                policy_state=policy_state,
            )
            assert new_replicas == 10, f"Should not scale down at iteration {i}"

        # Next call should scale
        new_replicas, _ = queue_based_autoscaling_policy(ctx)
        assert new_replicas == 2


class TestQueueBasedAutoscalingPolicyState:
    """Tests for policy state management."""

    def test_stores_queue_length_in_state(
        self, mock_ray_actor_methods
    ):
        """Test that queue_length is stored in policy state."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        setup_queue_monitor_mocks(mock_get_actor, mock_ray_get, 42)

        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            upscale_delay_s=0,
            downscale_delay_s=0,
        )

        _, policy_state = queue_based_autoscaling_policy(ctx)
        assert policy_state.get("last_queue_length") == 42

    def test_preserves_decision_counter(
        self, mock_ray_actor_methods
    ):
        """Test that decision counter is preserved across calls."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        setup_queue_monitor_mocks(mock_get_actor, mock_ray_get, 200)

        policy_state = {}
        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=10,
            upscale_delay_s=30.0,
            policy_state=policy_state,
        )

        _, policy_state = queue_based_autoscaling_policy(ctx)
        assert policy_state.get("decision_counter", 0) == 1

        # Call again
        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            target_ongoing_requests=10,
            upscale_delay_s=30.0,
            policy_state=policy_state,
        )
        _, policy_state = queue_based_autoscaling_policy(ctx)
        assert policy_state.get("decision_counter", 0) == 2


class TestQueueBasedAutoscalingPolicyActorRecovery:
    """Tests for QueueMonitor actor recovery via policy_state."""

    def test_stores_config_in_policy_state(
        self, mock_ray_actor_methods
    ):
        """Test that QueueMonitor config is stored in policy_state on first call."""
        mock_get_actor, mock_ray_get = mock_ray_actor_methods
        config_dict = {"broker_url": "redis://myredis:6379", "queue_name": "myqueue"}
        setup_queue_monitor_mocks(
            mock_get_actor, mock_ray_get, 50, config_dict=config_dict
        )

        ctx = create_autoscaling_context(
            current_num_replicas=5,
            target_num_replicas=5,
            upscale_delay_s=0,
            downscale_delay_s=0,
        )

        _, policy_state = queue_based_autoscaling_policy(ctx)

        assert "queue_monitor_config" in policy_state
        assert policy_state["queue_monitor_config"]["broker_url"] == "redis://myredis:6379"
        assert policy_state["queue_monitor_config"]["queue_name"] == "myqueue"

    def test_recovers_actor_from_policy_state(self):
        """Test that actor is recreated from policy_state when not found."""
        with patch("ray.serve.autoscaling_policy.ray.get_actor") as mock_get_actor, \
             patch("ray.serve.autoscaling_policy.ray.get") as mock_ray_get, \
             patch("ray.serve.autoscaling_policy.create_queue_monitor_actor") as mock_create_actor:

            # First call: actor not found
            mock_get_actor.side_effect = ValueError("Actor not found")

            # Mock the newly created actor
            mock_new_actor = MagicMock()
            mock_queue_length_ref = MagicMock()
            mock_new_actor.get_queue_length.remote.return_value = mock_queue_length_ref
            mock_create_actor.return_value = mock_new_actor
            mock_ray_get.return_value = 100  # Queue length

            # Pass stored config in policy_state
            stored_config = {"broker_url": "redis://localhost:6379", "queue_name": "tasks"}
            policy_state = {"queue_monitor_config": stored_config}

            ctx = create_autoscaling_context(
                current_num_replicas=1,
                target_num_replicas=1,
                target_ongoing_requests=10,
                max_replicas=20,
                upscale_delay_s=0,
                policy_state=policy_state,
            )

            new_replicas, _ = queue_based_autoscaling_policy(ctx)

            # Verify actor was recreated
            mock_create_actor.assert_called_once()
            call_kwargs = mock_create_actor.call_args.kwargs
            assert call_kwargs["deployment_name"] == "test_deployment"
            assert call_kwargs["config"].broker_url == "redis://localhost:6379"
            assert call_kwargs["config"].queue_name == "tasks"

            # Verify scaling happened based on queue length
            assert new_replicas == 10  # ceil(100/10)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
