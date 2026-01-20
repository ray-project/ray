import logging
import math
from typing import Any, Dict, Optional, Tuple

import ray
from ray.serve._private.constants import (
    CONTROL_LOOP_INTERVAL_S,
    SERVE_AUTOSCALING_DECISION_COUNTERS_KEY,
    SERVE_LOGGER_NAME,
)
from ray.serve._private.queue_monitor import get_queue_monitor_actor
from ray.serve.config import AutoscalingConfig, AutoscalingContext
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _calculate_desired_num_replicas(
    autoscaling_config: AutoscalingConfig,
    total_num_requests: int,
    num_running_replicas: int,
    override_min_replicas: Optional[float] = None,
    override_max_replicas: Optional[float] = None,
) -> int:
    """Returns the number of replicas to scale to based on the given metrics.

    Args:
        autoscaling_config: The autoscaling parameters to use for this
            calculation.
        current_num_ongoing_requests (List[float]): A list of the number of
            ongoing requests for each replica.  Assumes each entry has already
            been time-averaged over the desired lookback window.
        override_min_replicas: Overrides min_replicas from the config
            when calculating the final number of replicas.
        override_max_replicas: Overrides max_replicas from the config
            when calculating the final number of replicas.

    Returns:
        desired_num_replicas: The desired number of replicas to scale to, based
            on the input metrics and the current number of replicas.

    """
    if num_running_replicas == 0:
        raise ValueError("Number of replicas cannot be zero")

    # Example: if error_ratio == 2.0, we have two times too many ongoing
    # requests per replica, so we desire twice as many replicas.
    target_num_requests = (
        autoscaling_config.get_target_ongoing_requests() * num_running_replicas
    )
    error_ratio: float = total_num_requests / target_num_requests

    # If error ratio >= 1, then the number of ongoing requests per
    # replica exceeds the target and we will make an upscale decision,
    # so we apply the upscale smoothing factor. Otherwise, the number of
    # ongoing requests per replica is lower than the target and we will
    # make a downscale decision, so we apply the downscale smoothing
    # factor.
    if error_ratio >= 1:
        scaling_factor = autoscaling_config.get_upscaling_factor()
    else:
        scaling_factor = autoscaling_config.get_downscaling_factor()

    # Multiply the distance to 1 by the smoothing ("gain") factor (default=1).
    smoothed_error_ratio = 1 + ((error_ratio - 1) * scaling_factor)
    desired_num_replicas = math.ceil(num_running_replicas * smoothed_error_ratio)

    # If desired num replicas is "stuck" because of the smoothing factor
    # (meaning the traffic is low enough for the replicas to downscale
    # without the smoothing factor), decrease desired_num_replicas by 1.
    if (
        math.ceil(num_running_replicas * error_ratio) < num_running_replicas
        and desired_num_replicas == num_running_replicas
    ):
        desired_num_replicas -= 1

    min_replicas = autoscaling_config.min_replicas
    max_replicas = autoscaling_config.max_replicas
    if override_min_replicas is not None:
        min_replicas = override_min_replicas
    if override_max_replicas is not None:
        max_replicas = override_max_replicas

    # Ensure scaled_min_replicas <= desired_num_replicas <= scaled_max_replicas.
    desired_num_replicas = max(min_replicas, min(max_replicas, desired_num_replicas))

    return desired_num_replicas


@PublicAPI(stability="alpha")
def replica_queue_length_autoscaling_policy(
    ctx: AutoscalingContext,
) -> Tuple[int, Dict[str, Any]]:
    """The default autoscaling policy based on basic thresholds for scaling.
    There is a minimum threshold for the average queue length in the cluster
    to scale up and a maximum threshold to scale down. Each period, a 'scale
    up' or 'scale down' decision is made. This decision must be made for a
    specified number of periods in a row before the number of replicas is
    actually scaled. See config options for more details.  Assumes
    `get_decision_num_replicas` is called once every CONTROL_LOOP_PERIOD_S
    seconds.
    """

    curr_target_num_replicas: int = ctx.target_num_replicas
    total_num_requests: int = ctx.total_num_requests
    num_running_replicas: int = ctx.current_num_replicas
    config: Optional[AutoscalingConfig] = ctx.config
    capacity_adjusted_min_replicas: int = ctx.capacity_adjusted_min_replicas
    capacity_adjusted_max_replicas: int = ctx.capacity_adjusted_max_replicas
    policy_state: Dict[str, Any] = ctx.policy_state
    decision_counter = policy_state.get(SERVE_AUTOSCALING_DECISION_COUNTERS_KEY, 0)
    if num_running_replicas == 0:
        # When 0 replicas and queries are queued, scale up the replicas
        if total_num_requests > 0:
            return (
                max(
                    math.ceil(1 * config.get_upscaling_factor()),
                    curr_target_num_replicas,
                ),
                policy_state,
            )
        return curr_target_num_replicas, policy_state

    desired_num_replicas = _calculate_desired_num_replicas(
        config,
        total_num_requests,
        num_running_replicas=num_running_replicas,
        override_min_replicas=capacity_adjusted_min_replicas,
        override_max_replicas=capacity_adjusted_max_replicas,
    )

    decision_num_replicas, decision_counter = _apply_scaling_decision_smoothing(
        desired_num_replicas=desired_num_replicas,
        curr_target_num_replicas=curr_target_num_replicas,
        decision_counter=decision_counter,
        config=config,
    )

    policy_state["decision_counter"] = decision_counter
    policy_state[SERVE_AUTOSCALING_DECISION_COUNTERS_KEY] = decision_counter
    return decision_num_replicas, policy_state


@PublicAPI(stability="alpha")
def async_inference_autoscaling_policy(
    ctx: AutoscalingContext,
) -> Tuple[int, Dict[str, Any]]:
    """
    Async inference autoscaling policy for TaskConsumer deployments.

    This policy scales replicas based on the total workload from:
    - Pending tasks in the message queue (via QueueMonitor)
    - Ongoing HTTP requests (from context)

    Formula:
        total_workload = queue_length + total_num_requests
        desired_replicas = ceil(total_workload / target_ongoing_requests)

    Args:
        ctx: AutoscalingContext containing metrics, config, and state

    Returns:
        Tuple of (desired_num_replicas, updated_policy_state)
    """

    # Extract state
    policy_state: Dict[str, Any] = ctx.policy_state
    current_num_replicas: int = ctx.current_num_replicas
    curr_target_num_replicas: int = ctx.target_num_replicas
    total_num_requests: int = ctx.total_num_requests
    config: Optional[AutoscalingConfig] = ctx.config
    capacity_adjusted_min_replicas: int = ctx.capacity_adjusted_min_replicas
    capacity_adjusted_max_replicas: int = ctx.capacity_adjusted_max_replicas

    # Get decision counter from state (for smoothing)
    decision_counter = policy_state.get(SERVE_AUTOSCALING_DECISION_COUNTERS_KEY, 0)

    # === STEP 1: Get queue length from QueueMonitor actor ===
    try:
        queue_monitor_actor = get_queue_monitor_actor(ctx.deployment_name)
        queue_length = ray.get(
            queue_monitor_actor.get_queue_length.remote(), timeout=5.0
        )
    except Exception as e:
        logger.warning(
            f"[{ctx.deployment_name}] Could not query QueueMonitor: {e}, maintaining {curr_target_num_replicas} replicas"
        )
        return curr_target_num_replicas, policy_state

    # Calculate total workload = queue tasks + HTTP requests
    total_workload = queue_length + total_num_requests

    if current_num_replicas == 0:
        # When 0 replicas and there's workload, scale up the replicas
        if total_workload > 0:
            return (
                max(
                    math.ceil(1 * config.get_upscaling_factor()),
                    curr_target_num_replicas,
                ),
                policy_state,
            )
        return curr_target_num_replicas, policy_state

    # === STEP 2: Calculate desired replicas ===
    desired_num_replicas = _calculate_desired_num_replicas(
        config,
        total_num_requests=total_workload,
        num_running_replicas=current_num_replicas,
        override_min_replicas=capacity_adjusted_min_replicas,
        override_max_replicas=capacity_adjusted_max_replicas,
    )

    # === STEP 3: Apply smoothing (same logic as default policy) ===
    decision_num_replicas, decision_counter = _apply_scaling_decision_smoothing(
        desired_num_replicas=desired_num_replicas,
        curr_target_num_replicas=curr_target_num_replicas,
        decision_counter=decision_counter,
        config=config,
    )

    # Update policy state
    policy_state[SERVE_AUTOSCALING_DECISION_COUNTERS_KEY] = decision_counter
    return decision_num_replicas, policy_state


def _apply_scaling_decision_smoothing(
    desired_num_replicas: int,
    curr_target_num_replicas: int,
    decision_counter: int,
    config: AutoscalingConfig,
) -> Tuple[int, int]:
    """
    Apply smoothing logic to prevent oscillation in scaling decisions.

    This function implements delay-based smoothing: a scaling decision must be
    made for a consecutive number of periods before actually scaling.

    Args:
        desired_num_replicas: The calculated desired number of replicas.
        curr_target_num_replicas: Current target number of replicas.
        decision_counter: Counter tracking consecutive scaling decisions.
            Positive = consecutive scale-up decisions, negative = scale-down.
        config: Autoscaling configuration containing delay settings.

    Returns:
        Tuple of (decision_num_replicas, updated_decision_counter).
    """
    decision_num_replicas = curr_target_num_replicas

    # Scale up
    if desired_num_replicas > curr_target_num_replicas:
        if decision_counter < 0:
            decision_counter = 0
        decision_counter += 1

        # Only scale after upscale_delay_s
        if decision_counter > int(config.upscale_delay_s / CONTROL_LOOP_INTERVAL_S):
            decision_counter = 0
            decision_num_replicas = desired_num_replicas

    # Scale down
    elif desired_num_replicas < curr_target_num_replicas:
        if decision_counter > 0:
            decision_counter = 0
        decision_counter -= 1

        # Downscaling to zero is only allowed from 1 -> 0
        is_scaling_to_zero = curr_target_num_replicas == 1 and desired_num_replicas == 0
        if is_scaling_to_zero:
            # Use downscale_to_zero_delay_s if set, otherwise fall back to downscale_delay_s
            if config.downscale_to_zero_delay_s is not None:
                delay_s = config.downscale_to_zero_delay_s
            else:
                delay_s = config.downscale_delay_s
        else:
            delay_s = config.downscale_delay_s
            # Ensure desired_num_replicas >= 1 for non-zero scaling cases
            desired_num_replicas = max(1, desired_num_replicas)

        # Only scale after delay
        if decision_counter < -int(delay_s / CONTROL_LOOP_INTERVAL_S):
            decision_counter = 0
            decision_num_replicas = desired_num_replicas

    # No change
    else:
        decision_counter = 0

    return decision_num_replicas, decision_counter


default_autoscaling_policy = replica_queue_length_autoscaling_policy

default_async_inference_autoscaling_policy = async_inference_autoscaling_policy
