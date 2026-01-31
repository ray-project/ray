import sys

import pytest

from ray.serve._private.common import DeploymentID, ReplicaID, TimeStampedValue
from ray.serve._private.constants import CONTROL_LOOP_INTERVAL_S
from ray.serve.autoscaling_policy import (
    _apply_app_level_autoscaling_config,
    _apply_autoscaling_config,
    _apply_bounds,
    _apply_delay_logic,
    _apply_scaling_factors,
    replica_queue_length_autoscaling_policy,
)
from ray.serve.config import AutoscalingConfig, AutoscalingContext

wrapped_replica_queue_length_autoscaling_policy = _apply_autoscaling_config(
    replica_queue_length_autoscaling_policy
)


def create_context_with_overrides(
    base_ctx: AutoscalingContext, **kwargs
) -> AutoscalingContext:
    """Helper to create a new AutoscalingContext with specified attributes overridden.

    Args:
        base_ctx: The base AutoscalingContext to copy values from.
        **kwargs: Attributes to override in the new context.

    Returns:
        A new AutoscalingContext with overridden values.
    """
    # Get all constructor parameters with defaults from base context
    params = {
        "config": base_ctx.config,
        "deployment_id": base_ctx.deployment_id,
        "deployment_name": base_ctx.deployment_name,
        "app_name": base_ctx.app_name,
        "current_num_replicas": base_ctx.current_num_replicas,
        "target_num_replicas": base_ctx.target_num_replicas,
        "running_replicas": base_ctx.running_replicas,
        "total_num_requests": base_ctx.total_num_requests,
        "total_queued_requests": base_ctx.total_queued_requests,
        "aggregated_metrics": base_ctx.aggregated_metrics,
        "raw_metrics": base_ctx.raw_metrics,
        "capacity_adjusted_min_replicas": base_ctx.capacity_adjusted_min_replicas,
        "capacity_adjusted_max_replicas": base_ctx.capacity_adjusted_max_replicas,
        "policy_state": base_ctx.policy_state,
        "last_scale_up_time": base_ctx.last_scale_up_time,
        "last_scale_down_time": base_ctx.last_scale_down_time,
        "current_time": base_ctx.current_time,
    }

    # Override with provided kwargs
    params.update(kwargs)

    return AutoscalingContext(**params)


def _run_upscale_downscale_flow(
    policy,
    config: AutoscalingConfig,
    ctx: AutoscalingContext,
    overload_requests: int,
    start_replicas: int,
    upscale_target: int,
):
    """
    This runs the upscale and downscale flow to test the delays during upscale and
    downscale.
    This can be used by both the default autoscaling policy and custom autoscaling policy
    with default parameters to verify scaling is properly enabled.
    The downscale flow is from upscale_target upto zero
    """

    upscale_wait_periods = int(config.upscale_delay_s / CONTROL_LOOP_INTERVAL_S)
    downscale_wait_periods = int(config.downscale_delay_s / CONTROL_LOOP_INTERVAL_S)
    # Check if downscale_to_zero_delay_s is set
    if config.downscale_to_zero_delay_s:
        downscale_to_zero_wait_periods = int(
            config.downscale_to_zero_delay_s / CONTROL_LOOP_INTERVAL_S
        )
    else:
        downscale_to_zero_wait_periods = int(
            config.downscale_delay_s / CONTROL_LOOP_INTERVAL_S
        )

    # Initialize local policy_state from the base context, so both default and
    # decorated policies can persist their internal state
    policy_state = ctx.policy_state or {}

    # We should scale up only after enough consecutive scale-up decisions.
    for i in range(upscale_wait_periods):
        ctx = create_context_with_overrides(
            ctx,
            total_num_requests=overload_requests,
            current_num_replicas=start_replicas,
            target_num_replicas=start_replicas,
            policy_state=policy_state,
        )
        new_num_replicas, policy_state = policy(ctx=ctx)
        assert new_num_replicas == start_replicas, i

    ctx = create_context_with_overrides(
        ctx,
        total_num_requests=overload_requests,
        current_num_replicas=start_replicas,
        target_num_replicas=start_replicas,
        policy_state=policy_state,
    )
    new_num_replicas, policy_state = policy(ctx=ctx)
    assert new_num_replicas == upscale_target

    no_requests = 0

    # We should scale down only after enough consecutive scale-down decisions.
    for i in range(downscale_wait_periods):
        ctx = create_context_with_overrides(
            ctx,
            total_num_requests=no_requests,
            current_num_replicas=upscale_target,
            target_num_replicas=upscale_target,
            policy_state=policy_state,
        )
        new_num_replicas, policy_state = policy(ctx=ctx)
        assert new_num_replicas == upscale_target, i

    ctx = create_context_with_overrides(
        ctx,
        total_num_requests=no_requests,
        current_num_replicas=upscale_target,
        target_num_replicas=upscale_target,
        policy_state=policy_state,
    )
    new_num_replicas, policy_state = policy(ctx=ctx)
    assert new_num_replicas == 1

    # We should scale down to zero only after enough consecutive downscale-to-zero decisions.
    for i in range(downscale_to_zero_wait_periods):
        ctx = create_context_with_overrides(
            ctx,
            total_num_requests=no_requests,
            current_num_replicas=1,
            target_num_replicas=1,
            policy_state=policy_state,
        )
        new_num_replicas, policy_state = policy(ctx=ctx)
        assert new_num_replicas == 1, i

    ctx = create_context_with_overrides(
        ctx,
        total_num_requests=no_requests,
        current_num_replicas=1,
        target_num_replicas=1,
        policy_state=policy_state,
    )
    new_num_replicas, policy_state = policy(ctx=ctx)
    assert new_num_replicas == 0

    # Get some scale-up decisions, but not enough to trigger a scale up.
    for i in range(int(upscale_wait_periods / 2)):
        ctx = create_context_with_overrides(
            ctx,
            total_num_requests=overload_requests,
            current_num_replicas=1,
            target_num_replicas=1,
            policy_state=policy_state,
        )
        new_num_replicas, policy_state = policy(ctx=ctx)
        assert new_num_replicas == 1, i

        # Interrupt with a scale-down decision.
    ctx = create_context_with_overrides(
        ctx,
        total_num_requests=0,
        current_num_replicas=1,
        target_num_replicas=1,
        policy_state=policy_state,
    )
    _, policy_state = policy(ctx=ctx)

    # The counter should be reset, so it should require `upscale_wait_periods`
    # more periods before we actually scale up.
    for i in range(upscale_wait_periods):
        ctx = create_context_with_overrides(
            ctx,
            total_num_requests=overload_requests,
            current_num_replicas=1,
            target_num_replicas=1,
            policy_state=policy_state,
        )
        new_num_replicas, policy_state = policy(ctx=ctx)
        assert new_num_replicas == 1, i

    ctx = create_context_with_overrides(
        ctx,
        total_num_requests=overload_requests,
        current_num_replicas=1,
        target_num_replicas=1,
        policy_state=policy_state,
    )
    new_num_replicas, policy_state = policy(ctx=ctx)
    assert new_num_replicas == upscale_target

    # Get some scale-down decisions, but not enough to trigger a scale down.
    for i in range(int(downscale_wait_periods / 2)):
        ctx = create_context_with_overrides(
            ctx,
            total_num_requests=no_requests,
            current_num_replicas=upscale_target,
            target_num_replicas=upscale_target,
            policy_state=policy_state,
        )
        new_num_replicas, policy_state = policy(ctx=ctx)
        assert new_num_replicas == upscale_target, i

    # Interrupt with a scale-up decision.
    ctx = create_context_with_overrides(
        ctx,
        total_num_requests=overload_requests,
        current_num_replicas=upscale_target,
        target_num_replicas=upscale_target,
        policy_state=policy_state,
    )
    _, policy_state = policy(ctx=ctx)

    # The counter should be reset so it should require `downscale_wait_periods`
    # more periods before we actually scale down.
    # We should scale down only after enough consecutive scale-down decisions.
    for i in range(downscale_wait_periods):
        ctx = create_context_with_overrides(
            ctx,
            total_num_requests=no_requests,
            current_num_replicas=upscale_target,
            target_num_replicas=upscale_target,
            policy_state=policy_state,
        )
        new_num_replicas, policy_state = policy(ctx=ctx)
        assert new_num_replicas == upscale_target, i

    # First scale down to 1 replica
    ctx = create_context_with_overrides(
        ctx,
        total_num_requests=no_requests,
        current_num_replicas=upscale_target,
        target_num_replicas=upscale_target,
        policy_state=policy_state,
    )
    new_num_replicas, policy_state = policy(ctx=ctx)
    assert new_num_replicas == 1

    # Scale down to 0, but not enough to trigger a complete scale down to zero.
    for i in range(int(downscale_to_zero_wait_periods / 2)):
        ctx = create_context_with_overrides(
            ctx,
            total_num_requests=no_requests,
            current_num_replicas=1,
            target_num_replicas=1,
            policy_state=policy_state,
        )
        new_num_replicas, policy_state = policy(ctx=ctx)
        assert new_num_replicas == 1, i

    # Interrupt with a scale-up decision.
    ctx = create_context_with_overrides(
        ctx,
        total_num_requests=overload_requests,
        current_num_replicas=1,
        target_num_replicas=1,
        policy_state=policy_state,
    )
    _, policy_state = policy(ctx=ctx)

    # The counter should be reset so it should require `downscale_to_zero_wait_periods`
    # more periods before we actually scale down.
    for i in range(downscale_to_zero_wait_periods):
        ctx = create_context_with_overrides(
            ctx,
            total_num_requests=no_requests,
            current_num_replicas=1,
            target_num_replicas=1,
            policy_state=policy_state,
        )
        new_num_replicas, policy_state = policy(ctx=ctx)
        assert new_num_replicas == 1, i

    ctx = create_context_with_overrides(
        ctx,
        total_num_requests=no_requests,
        current_num_replicas=1,
        target_num_replicas=1,
        policy_state=policy_state,
    )
    new_num_replicas, policy_state = policy(ctx=ctx)
    assert new_num_replicas == 0


class TestReplicaQueueLengthPolicy:
    @pytest.mark.parametrize(
        "use_upscale_smoothing_factor,use_upscaling_factor",
        [(True, True), (True, False), (False, True)],
    )
    def test_scaling_factor_scale_up_from_0_replicas(
        self, use_upscale_smoothing_factor, use_upscaling_factor
    ):
        """Test that the scaling factor is respected when scaling up
        from 0 replicas.
        """

        min_replicas = 0
        max_replicas = 2
        config = AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            upscale_smoothing_factor=10 if use_upscale_smoothing_factor else None,
            upscaling_factor=10 if use_upscaling_factor else None,
        )
        ctx = AutoscalingContext(
            target_num_replicas=0,
            total_num_requests=1,
            current_num_replicas=0,
            config=config,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state={},
            deployment_id=None,
            deployment_name=None,
            app_name=None,
            running_replicas=None,
            current_time=None,
            total_queued_requests=None,
            aggregated_metrics=None,
            raw_metrics=None,
            last_scale_up_time=None,
            last_scale_down_time=None,
        )
        new_num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(ctx=ctx)

        # 1 * 10
        assert new_num_replicas == 10

        if use_upscale_smoothing_factor:
            config.upscale_smoothing_factor = 0.5
        if use_upscaling_factor:
            config.upscaling_factor = 0.5

        new_num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(ctx=ctx)

        # math.ceil(1 * 0.5)
        assert new_num_replicas == 1

    @pytest.mark.parametrize(
        "use_downscale_smoothing_factor,use_downscaling_factor",
        [(True, True), (True, False), (False, True)],
    )
    def test_scaling_factor_scale_down_to_0_replicas(
        self, use_downscale_smoothing_factor, use_downscaling_factor
    ):
        """Test that a deployment scales down to 0 for non-default smoothing factors."""

        # With smoothing factor > 1, the desired number of replicas should
        # immediately drop to 0 (while respecting upscale and downscale delay)
        min_replicas = 0
        max_replicas = 5
        policy_state = {}
        config = AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            downscale_smoothing_factor=10 if use_downscale_smoothing_factor else None,
            downscaling_factor=10 if use_downscaling_factor else None,
            upscale_delay_s=0,
            downscale_delay_s=0,
        )
        ctx = AutoscalingContext(
            config=config,
            total_num_requests=0,
            current_num_replicas=5,
            target_num_replicas=5,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
            deployment_id=None,
            deployment_name=None,
            app_name=None,
            running_replicas=None,
            current_time=None,
            total_queued_requests=None,
            aggregated_metrics=None,
            raw_metrics=None,
            last_scale_up_time=None,
            last_scale_down_time=None,
        )
        new_num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(ctx=ctx)
        # Downscaling to 0 first stops at 1
        assert new_num_replicas == 1
        # Need to trigger this the second time to go to zero
        ctx.target_num_replicas = 1
        ctx.current_num_replicas = 1
        new_num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(ctx=ctx)
        assert new_num_replicas == 0

        # With smoothing factor < 1, the desired number of replicas shouldn't
        # get stuck at a positive number, and instead should eventually drop
        # to zero
        if use_downscale_smoothing_factor:
            config.downscale_smoothing_factor = 0.2
        if use_downscaling_factor:
            config.downscaling_factor = 0.2

        # policy_manager = AutoscalingPolicyManager(config)
        num_replicas = 5
        for _ in range(5):
            ctx = create_context_with_overrides(
                ctx,
                total_num_requests=0,
                current_num_replicas=num_replicas,
                target_num_replicas=num_replicas,
            )
            num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(ctx=ctx)

        assert num_replicas == 0

    @pytest.mark.parametrize("downscale_to_zero_delay_s", [None, 300])
    def test_upscale_downscale_delay(self, downscale_to_zero_delay_s):
        """Unit test for upscale_delay_s, downscale_delay_s and downscale_to_zero_delay_s"""
        min_replicas = 0
        max_replicas = 2
        policy_state = {}
        config = AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            target_ongoing_requests=1,
            upscale_delay_s=30.0,
            downscale_delay_s=600.0,
            downscale_to_zero_delay_s=downscale_to_zero_delay_s,
        )

        overload_requests = 100

        ctx = AutoscalingContext(
            config=config,
            total_num_requests=1,
            current_num_replicas=0,
            target_num_replicas=0,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
            deployment_id=None,
            deployment_name=None,
            app_name=None,
            running_replicas=None,
            current_time=None,
            total_queued_requests=None,
            aggregated_metrics=None,
            raw_metrics=None,
            last_scale_up_time=None,
            last_scale_down_time=None,
        )

        # Scale up when there are 0 replicas and current_handle_queued_queries > 0
        new_num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(ctx=ctx)
        assert new_num_replicas == 1
        # Run the basic upscale/downscale flow
        _run_upscale_downscale_flow(
            wrapped_replica_queue_length_autoscaling_policy,
            config,
            ctx,
            overload_requests=overload_requests,
            start_replicas=1,
            upscale_target=2,
        )

    def test_replicas_delayed_startup(self):
        """Unit test simulating replicas taking time to start up."""
        min_replicas = 1
        max_replicas = 200
        policy_state = {}
        config = {
            "min_replicas": min_replicas,
            "max_replicas": max_replicas,
            "upscale_delay_s": 0,
            "downscale_delay_s": 100000,
            "target_ongoing_requests": 1,
        }
        config = AutoscalingConfig(**config)

        ctx = AutoscalingContext(
            config=config,
            target_num_replicas=1,
            total_num_requests=100,
            current_num_replicas=1,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
            deployment_id=None,
            deployment_name=None,
            app_name=None,
            running_replicas=None,
            current_time=None,
            total_queued_requests=None,
            aggregated_metrics=None,
            raw_metrics=None,
            last_scale_up_time=None,
            last_scale_down_time=None,
        )

        # new_num_replicas = policy_manager.get_decision_num_replicas(1, 100, 1)
        new_num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(ctx=ctx)
        assert new_num_replicas == 100

        # New target is 100, but no new replicas finished spinning up during this
        # timestep.
        ctx = create_context_with_overrides(
            ctx,
            total_num_requests=100,
            current_num_replicas=1,
            target_num_replicas=100,
        )
        new_num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(ctx=ctx)
        assert new_num_replicas == 100

        # Two new replicas spun up during this timestep.
        ctx = create_context_with_overrides(
            ctx,
            total_num_requests=123,
            current_num_replicas=3,
            target_num_replicas=100,
        )
        new_num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(ctx=ctx)
        assert new_num_replicas == 123

        # A lot of queries got drained and a lot of replicas started up, but
        # new_num_replicas should not decrease, because of the downscale delay.
        ctx = create_context_with_overrides(
            ctx,
            total_num_requests=10,
            current_num_replicas=4,
            target_num_replicas=123,
        )
        new_num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(ctx=ctx)
        assert new_num_replicas == 123

    @pytest.mark.parametrize("delay_s", [30.0, 0.0])
    def test_fluctuating_ongoing_requests(self, delay_s):
        """
        Simulates a workload that switches between too many and too few
        ongoing requests.
        """

        min_replicas = 1
        max_replicas = 10
        policy_state = {}
        config = {
            "min_replicas": min_replicas,
            "max_replicas": max_replicas,
            "upscale_delay_s": delay_s,
            "downscale_delay_s": delay_s,
            "target_ongoing_requests": 50,
        }
        config = AutoscalingConfig(**config)

        if delay_s > 0:
            wait_periods = int(delay_s / CONTROL_LOOP_INTERVAL_S)
            assert wait_periods > 1

        underload_requests, overload_requests = 2 * 20, 100
        trials = 1000

        ctx = AutoscalingContext(
            config=config,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
            target_num_replicas=None,
            total_num_requests=None,
            current_num_replicas=None,
            deployment_id=None,
            deployment_name=None,
            app_name=None,
            running_replicas=None,
            current_time=None,
            total_queued_requests=None,
            aggregated_metrics=None,
            raw_metrics=None,
            last_scale_up_time=None,
            last_scale_down_time=None,
        )

        new_num_replicas = None
        for trial in range(trials):
            if trial % 2 == 0:
                ctx = create_context_with_overrides(
                    ctx,
                    total_num_requests=overload_requests,
                    current_num_replicas=1,
                    target_num_replicas=1,
                )
                new_num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(
                    ctx=ctx
                )
                if delay_s > 0:
                    assert new_num_replicas == 1, trial
                else:
                    assert new_num_replicas == 2, trial
            else:
                ctx = create_context_with_overrides(
                    ctx,
                    total_num_requests=underload_requests,
                    current_num_replicas=2,
                    target_num_replicas=2,
                )
                new_num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(
                    ctx=ctx
                )
                if delay_s > 0:
                    assert new_num_replicas == 2, trial
                else:
                    assert new_num_replicas == 1, trial

    @pytest.mark.parametrize("ongoing_requests", [20, 100, 10])
    def test_single_replica_receives_all_requests(self, ongoing_requests):
        target_requests = 5

        min_replicas = 1
        max_replicas = 50
        policy_state = {}
        config = AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            target_ongoing_requests=target_requests,
            upscale_delay_s=0.0,
            downscale_delay_s=0.0,
        )

        ctx = AutoscalingContext(
            config=config,
            total_num_requests=ongoing_requests,
            current_num_replicas=4,
            target_num_replicas=4,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
            deployment_id=None,
            deployment_name=None,
            app_name=None,
            running_replicas=None,
            current_time=None,
            total_queued_requests=None,
            aggregated_metrics=None,
            raw_metrics=None,
            last_scale_up_time=None,
            last_scale_down_time=None,
        )

        new_num_replicas, _ = wrapped_replica_queue_length_autoscaling_policy(ctx=ctx)
        assert new_num_replicas == ongoing_requests / target_requests

    def test_callable_and_direct_values(self):
        config = AutoscalingConfig(min_replicas=1, max_replicas=10)
        deployment_id = DeploymentID(name="test", app_name="test_app")
        replica_id = ReplicaID(unique_id="r1", deployment_id=deployment_id)

        # Test callables with lazy evaluation and caching
        call_counts = {"requests": 0, "queued": 0, "agg": 0, "raw": 0}

        ctx = AutoscalingContext(
            config=config,
            deployment_id=None,
            deployment_name="test",
            app_name=None,
            current_num_replicas=5,
            target_num_replicas=5,
            running_replicas=[],
            total_num_requests=lambda: (
                call_counts.update({"requests": call_counts["requests"] + 1}),
                42.0,
            )[1],
            total_queued_requests=lambda: (
                call_counts.update({"queued": call_counts["queued"] + 1}),
                10.0,
            )[1],
            aggregated_metrics=lambda: (
                call_counts.update({"agg": call_counts["agg"] + 1}),
                {"m": {replica_id: 5.0}},
            )[1],
            raw_metrics=lambda: (
                call_counts.update({"raw": call_counts["raw"] + 1}),
                {"m": {replica_id: [TimeStampedValue(1.0, 5.0)]}},
            )[1],
            capacity_adjusted_min_replicas=1,
            capacity_adjusted_max_replicas=10,
            policy_state={},
            last_scale_up_time=None,
            last_scale_down_time=None,
            current_time=None,
        )

        # Callables not executed until accessed
        assert all(c == 0 for c in call_counts.values())

        # First access executes callables
        assert ctx.total_num_requests == 42.0
        assert ctx.total_queued_requests == 10.0
        assert ctx.aggregated_metrics == {"m": {replica_id: 5.0}}
        assert ctx.raw_metrics["m"][replica_id][0].value == 5.0
        assert all(c == 1 for c in call_counts.values())

        # Second access uses cached values
        _ = ctx.total_num_requests
        _ = ctx.total_queued_requests
        _ = ctx.aggregated_metrics
        _ = ctx.raw_metrics
        assert all(c == 1 for c in call_counts.values())

        # Test direct values (non-callable)
        ctx2 = AutoscalingContext(
            config=config,
            deployment_id=None,
            deployment_name="test",
            app_name=None,
            current_num_replicas=5,
            target_num_replicas=5,
            running_replicas=[],
            total_num_requests=100.0,
            total_queued_requests=20.0,
            aggregated_metrics={"m2": {replica_id: 15.0}},
            raw_metrics={"m2": {replica_id: [TimeStampedValue(2.0, 25.0)]}},
            capacity_adjusted_min_replicas=1,
            capacity_adjusted_max_replicas=10,
            policy_state={},
            last_scale_up_time=None,
            last_scale_down_time=None,
            current_time=None,
        )

        assert ctx2.total_num_requests == 100.0
        assert ctx2.total_queued_requests == 20.0
        assert ctx2.aggregated_metrics == {"m2": {replica_id: 15.0}}
        assert ctx2.raw_metrics["m2"][replica_id][0].value == 25.0


class TestAutoscalingConfigParameters:
    def test_apply_scaling_factors_upscale(self):
        config = AutoscalingConfig(
            min_replicas=1, max_replicas=30, upscaling_factor=0.5
        )
        desired_num_replicas = 20
        current_num_replicas = 10
        result = _apply_scaling_factors(
            desired_num_replicas, current_num_replicas, config
        )
        # Expected: 10 + 0.5 * (20 - 10) = 15
        assert result == 15

    def test_apply_scaling_factors_downscale(self):
        config = AutoscalingConfig(
            min_replicas=1, max_replicas=30, downscaling_factor=0.5
        )
        desired_num_replicas = 5
        current_num_replicas = 20
        result = _apply_scaling_factors(
            desired_num_replicas, current_num_replicas, config
        )
        # Expected: 20 - 0.5 * (20 - 5) = ceil(12.5) = 13
        assert result == 13

    def test_apply_scaling_factors_stuck_downscale(self):
        config = AutoscalingConfig(
            min_replicas=1, max_replicas=30, downscaling_factor=0.5
        )
        desired_num_replicas = 9
        current_num_replicas = 10
        result = _apply_scaling_factors(
            desired_num_replicas, current_num_replicas, config
        )
        # Expected: 10 - 0.5 * (10 - 9) = 9.5 = ceil(9.5) = 10. The logic then adjusts it to 9.
        assert result == 9

    def test_apply_bounds(self):
        num_replicas = 5
        capacity_adjusted_min_replicas = 1
        capacity_adjusted_max_replicas = 10
        result = _apply_bounds(
            num_replicas, capacity_adjusted_min_replicas, capacity_adjusted_max_replicas
        )
        # Expected: max(1, min(10, 5)) = 5
        assert result == 5

    def test_apply_delay_logic_upscale(self):
        """Test upscale delay requires consecutive periods."""
        config = AutoscalingConfig(
            min_replicas=1,
            max_replicas=10,
            upscale_delay_s=0.3,
        )

        ctx = AutoscalingContext(
            target_num_replicas=1,
            current_num_replicas=1,
            config=config,
            capacity_adjusted_min_replicas=1,
            capacity_adjusted_max_replicas=10,
            policy_state={},
            deployment_id=None,
            deployment_name=None,
            app_name=None,
            running_replicas=None,
            current_time=None,
            total_num_requests=None,
            total_queued_requests=None,
            aggregated_metrics=None,
            raw_metrics=None,
            last_scale_up_time=None,
            last_scale_down_time=None,
        )
        upscale_wait_period = int(ctx.config.upscale_delay_s / CONTROL_LOOP_INTERVAL_S)
        for i in range(upscale_wait_period):
            decision, ctx.policy_state = _apply_delay_logic(
                desired_num_replicas=5,
                curr_target_num_replicas=ctx.target_num_replicas,
                config=ctx.config,
                policy_state=ctx.policy_state,
            )
            assert decision == 1, f"Should not scale up on iteration {i}"

        decision, _ = _apply_delay_logic(
            desired_num_replicas=5,
            curr_target_num_replicas=ctx.target_num_replicas,
            config=ctx.config,
            policy_state=ctx.policy_state,
        )
        assert decision == 5

    def test_apply_delay_logic_downscale(self):
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=10,
            downscale_to_zero_delay_s=0.4,
            downscale_delay_s=0.3,
        )
        ctx = AutoscalingContext(
            target_num_replicas=4,
            current_num_replicas=4,
            config=config,
            capacity_adjusted_min_replicas=0,
            capacity_adjusted_max_replicas=10,
            policy_state={},
            deployment_id=None,
            deployment_name=None,
            app_name=None,
            running_replicas=None,
            current_time=None,
            total_num_requests=None,
            total_queued_requests=None,
            aggregated_metrics=None,
            raw_metrics=None,
            last_scale_up_time=None,
            last_scale_down_time=None,
        )
        downscale_to_zero_wait_period = int(
            ctx.config.downscale_to_zero_delay_s / CONTROL_LOOP_INTERVAL_S
        )
        downscale_wait_period = int(
            ctx.config.downscale_delay_s / CONTROL_LOOP_INTERVAL_S
        )
        # Downscale form 4->1
        for i in range(downscale_wait_period):
            decision_num_replicas, ctx.policy_state = _apply_delay_logic(
                desired_num_replicas=0,
                curr_target_num_replicas=ctx.target_num_replicas,
                config=ctx.config,
                policy_state=ctx.policy_state,
            )
            assert (
                decision_num_replicas == 4
            ), f"Should not scale down to 0 on iteration {i}"

        decision_num_replicas, _ = _apply_delay_logic(
            desired_num_replicas=0,
            curr_target_num_replicas=ctx.target_num_replicas,
            config=ctx.config,
            policy_state=ctx.policy_state,
        )
        assert decision_num_replicas == 1
        ctx.target_num_replicas = decision_num_replicas
        # Downscale from 1->0
        for i in range(downscale_to_zero_wait_period):
            decision_num_replicas, ctx.policy_state = _apply_delay_logic(
                desired_num_replicas=0,
                curr_target_num_replicas=ctx.target_num_replicas,
                config=ctx.config,
                policy_state=ctx.policy_state,
            )
            assert (
                decision_num_replicas == 1
            ), f"Should not scale down from 1 to 0 on tick {i}"
        decision_num_replicas, _ = _apply_delay_logic(
            desired_num_replicas=0,
            curr_target_num_replicas=ctx.target_num_replicas,
            config=ctx.config,
            policy_state=ctx.policy_state,
        )
        assert decision_num_replicas == 0


@_apply_autoscaling_config
def simple_custom_policy(ctx: AutoscalingContext):
    """
    Custom policy to check default parameters are applied
    """
    if ctx.total_num_requests > 0:
        desired_num_replicas = 3
    else:
        desired_num_replicas = 0
    return desired_num_replicas, {}


@_apply_app_level_autoscaling_config
def simple_app_level_policy(ctxs):
    """App-level policy that always requests scaling up to 5 replicas."""
    return {deployment_id: 5 for deployment_id in ctxs.keys()}, {}


class TestCustomPolicyWithDefaultParameters:
    @pytest.mark.parametrize("downscale_to_zero_delay_s", [None, 300])
    def test_upscale_downscale_delay(self, downscale_to_zero_delay_s):
        """Unit test for upscale_delay_s, downscale_delay_s and downscale_to_zero_delay_s"""

        min_replicas = 0
        max_replicas = 4
        policy_state = {}
        config = AutoscalingConfig(
            min_replicas=min_replicas,
            max_replicas=max_replicas,
            target_ongoing_requests=1,
            upscale_delay_s=20.0,
            downscale_delay_s=400.0,
            downscale_to_zero_delay_s=downscale_to_zero_delay_s,
        )

        ctx = AutoscalingContext(
            config=config,
            total_num_requests=1,
            current_num_replicas=0,
            target_num_replicas=0,
            capacity_adjusted_min_replicas=min_replicas,
            capacity_adjusted_max_replicas=max_replicas,
            policy_state=policy_state,
            deployment_id=None,
            deployment_name=None,
            app_name=None,
            running_replicas=None,
            current_time=None,
            total_queued_requests=None,
            aggregated_metrics=None,
            raw_metrics=None,
            last_scale_up_time=None,
            last_scale_down_time=None,
        )

        # Scale up when there are 0 replicas and current_handle_queued_queries > 0
        new_num_replicas, _ = simple_custom_policy(ctx=ctx)
        assert new_num_replicas == 1
        _run_upscale_downscale_flow(
            simple_custom_policy,
            config,
            ctx,
            overload_requests=70,
            start_replicas=1,
            upscale_target=3,
        )

    @pytest.mark.parametrize(
        "current_replicas, total_requests, upscale_factor, downscale_factor, expected_replicas",
        [
            # Upscale cases
            # current=1, desired_raw=3
            # factor=0.5 → ceil(1 + 0.5*(3-1)) = 2
            (1, 100, 0.5, None, 2),
            # factor=1.0 → ceil(1 + 1.0*(3-1)) = 3
            (1, 100, 1.0, None, 3),
            # Downscale cases
            # current=3, desired_raw=0
            # factor=0.5 → ceil(3 + 0.5*(0-3)) = ceil(1.5) = 2
            (3, 0, None, 0.5, 2),
            # factor=1.0 → max(ceil(3 + 1.0*(0-3)),1) = 1
            (3, 0, None, 1.0, 1),
        ],
    )
    def test_apply_scaling_factors(
        self,
        current_replicas,
        total_requests,
        upscale_factor,
        downscale_factor,
        expected_replicas,
    ):
        """
        The test checks if the scaling factors are applied
        """
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=10,
            upscaling_factor=upscale_factor,
            downscaling_factor=downscale_factor,
            upscale_delay_s=0.0,
            downscale_delay_s=0.0,
            downscale_to_zero_delay_s=0.0,
        )
        ctx = AutoscalingContext(
            config=config,
            deployment_id=None,
            deployment_name="test",
            app_name=None,
            current_num_replicas=current_replicas,
            target_num_replicas=current_replicas,
            running_replicas=None,
            total_num_requests=0,
            total_queued_requests=None,
            aggregated_metrics=None,
            raw_metrics=None,
            capacity_adjusted_min_replicas=config.min_replicas,
            capacity_adjusted_max_replicas=config.max_replicas,
            policy_state={},
            last_scale_up_time=None,
            last_scale_down_time=None,
            current_time=None,
        )
        ctx = create_context_with_overrides(ctx, total_num_requests=total_requests)
        num_replicas, _ = simple_custom_policy(ctx)
        assert num_replicas == expected_replicas


class TestAppLevelPolicyWithDefaultParameters:
    def test_cold_start_fast_path(self):
        """App-level decorator should cold-start immediately (0 -> 1) even with delays."""
        config = AutoscalingConfig(
            min_replicas=0,
            max_replicas=10,
            target_ongoing_requests=10,
            upscale_delay_s=20.0,
            downscale_delay_s=200.0,
        )

        d1 = DeploymentID(name="d1", app_name="app")
        d2 = DeploymentID(name="d2", app_name="app")

        contexts = {
            d1: AutoscalingContext(
                config=config,
                deployment_id=d1,
                deployment_name="d1",
                app_name="app",
                current_num_replicas=0,
                target_num_replicas=0,
                running_replicas=[],
                total_num_requests=1,
                total_queued_requests=None,
                aggregated_metrics=None,
                raw_metrics=None,
                capacity_adjusted_min_replicas=0,
                capacity_adjusted_max_replicas=10,
                policy_state={},
                last_scale_up_time=None,
                last_scale_down_time=None,
                current_time=None,
            ),
            d2: AutoscalingContext(
                config=config,
                deployment_id=d2,
                deployment_name="d2",
                app_name="app",
                current_num_replicas=0,
                target_num_replicas=0,
                running_replicas=[],
                total_num_requests=1,
                total_queued_requests=None,
                aggregated_metrics=None,
                raw_metrics=None,
                capacity_adjusted_min_replicas=0,
                capacity_adjusted_max_replicas=10,
                policy_state={},
                last_scale_up_time=None,
                last_scale_down_time=None,
                current_time=None,
            ),
        }

        decisions, _ = simple_app_level_policy(contexts)
        assert decisions[d1] == 1
        assert decisions[d2] == 1


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
