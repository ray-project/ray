import logging
import math
from typing import Any, Dict, Optional, Tuple

import ray
from ray.serve._private.constants import (
    CONTROL_LOOP_INTERVAL_S,
    SERVE_LOGGER_NAME,
    SERVE_NAMESPACE,
)
from ray.serve._private.queue_monitor import (
    QUEUE_MONITOR_ACTOR_PREFIX,
    QueueMonitorConfig,
    create_queue_monitor_actor,
)
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
    decision_counter = policy_state.get("decision_counter", 0)
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
    return decision_num_replicas, policy_state


@PublicAPI(stability="alpha")
def queue_based_autoscaling_policy(
    ctx: AutoscalingContext,
) -> Tuple[int, Dict[str, Any]]:
    """
    Autoscaling policy for TaskConsumer deployments based on queue depth.

    This policy scales replicas based on the number of pending tasks in the
    message queue, rather than HTTP request load.

    Formula:
        desired_replicas = ceil(queue_length / target_ongoing_requests)

    Behavior:
        - Queries QueueMonitor Ray actor directly via ray.get_actor()
        - If QueueMonitor unavailable, maintains current replica count
        - Uses same smoothing/delay logic as default policy to prevent oscillation

    Args:
        ctx: AutoscalingContext containing metrics, config, and state

    Returns:
        Tuple of (desired_num_replicas, updated_policy_state)
    """

    # Extract state
    policy_state: Dict[str, Any] = ctx.policy_state
    current_num_replicas: int = ctx.current_num_replicas
    curr_target_num_replicas: int = ctx.target_num_replicas
    config: Optional[AutoscalingConfig] = ctx.config
    capacity_adjusted_min_replicas: int = ctx.capacity_adjusted_min_replicas
    capacity_adjusted_max_replicas: int = ctx.capacity_adjusted_max_replicas

    # Get decision counter from state (for smoothing)
    decision_counter = policy_state.get("decision_counter", 0)

    # === STEP 1: Get queue length from QueueMonitor actor ===
    # Actor name format: "QUEUE_MONITOR::<deployment_name>"
    queue_monitor_actor_name = f"{QUEUE_MONITOR_ACTOR_PREFIX}{ctx.deployment_name}"
    queue_monitor_actor, actor_found = _get_or_recover_queue_monitor_actor(
        queue_monitor_actor_name=queue_monitor_actor_name,
        deployment_name=ctx.deployment_name,
        policy_state=policy_state,
    )

    if not actor_found:
        logger.warning(
            f"[{ctx.deployment_name}] QueueMonitor actor unavailable, maintaining {curr_target_num_replicas} replicas"
        )
        return curr_target_num_replicas, policy_state

    try:
        queue_length = ray.get(
            queue_monitor_actor.get_queue_length.remote(),
            timeout=5.0
        )

        # Store config in policy_state if not already stored (for future recovery)
        if "queue_monitor_config" not in policy_state:
            try:
                config_dict = ray.get(
                    queue_monitor_actor.get_config.remote(),
                    timeout=5.0
                )
                policy_state["queue_monitor_config"] = config_dict
                logger.info(f"[{ctx.deployment_name}] Stored QueueMonitor config in policy_state for recovery")
            except Exception as e:
                logger.warning(f"[{ctx.deployment_name}] Failed to store config in policy_state: {e}")

    except Exception as e:
        # Error querying actor - maintain current replicas
        logger.warning(
            f"[{ctx.deployment_name}] Could not query QueueMonitor ({e}), maintaining {curr_target_num_replicas} replicas"
        )
        return curr_target_num_replicas, policy_state

    # === STEP 2: Calculate desired replicas ===
    target_ongoing_requests = config.get_target_ongoing_requests()

    policy_state["last_queue_length"] = queue_length

    # Handle scale from zero
    if current_num_replicas == 0:
        if queue_length > 0:
            desired = math.ceil(queue_length / target_ongoing_requests)
            desired = max(1, min(desired, capacity_adjusted_max_replicas))
            return desired, policy_state
        return 0, policy_state

    # Calculate desired replicas based on queue depth
    desired_num_replicas = math.ceil(queue_length / target_ongoing_requests)

    # Clamp to min/max bounds
    desired_num_replicas = max(
        capacity_adjusted_min_replicas,
        min(capacity_adjusted_max_replicas, desired_num_replicas),
    )

    # === STEP 3: Apply smoothing (same logic as default policy) ===
    decision_num_replicas, decision_counter = _apply_scaling_decision_smoothing(
        desired_num_replicas=desired_num_replicas,
        curr_target_num_replicas=curr_target_num_replicas,
        decision_counter=decision_counter,
        config=config,
    )

    # Update policy state
    policy_state["decision_counter"] = decision_counter

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
        deployment_name: Optional deployment name for logging.
        log_context: Optional string with extra context for logging (e.g., "queue_length=10").

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
        if is_scaling_to_zero and config.downscale_to_zero_delay_s is not None:
            delay_s = config.downscale_to_zero_delay_s
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


def _get_or_recover_queue_monitor_actor(
    queue_monitor_actor_name: str,
    deployment_name: str,
    policy_state: Dict[str, Any],
) -> Tuple[Optional[ray.actor.ActorHandle], bool]:
    """
    Try to get an existing QueueMonitor actor, or recover it from policy_state.

    Args:
        queue_monitor_actor_name: The name of the QueueMonitor actor to look up.
        deployment_name: The deployment name (for logging).
        policy_state: The policy state dict that may contain stored config for recovery.

    Returns:
        Tuple of (queue_monitor_actor, actor_found). If actor_found is False,
        queue_monitor_actor will be None.
    """
    queue_monitor_actor = None
    actor_found = False

    # Try to get existing actor
    try:
        queue_monitor_actor = ray.get_actor(queue_monitor_actor_name, namespace=SERVE_NAMESPACE)
        actor_found = True
    except ValueError:
        # Actor not found - try to recover from policy_state
        logger.warning(f"[{deployment_name}] QueueMonitor actor not found, checking policy_state for recovery")

        stored_config = policy_state.get("queue_monitor_config")
        if stored_config is not None:
            # Attempt to recreate actor from stored config
            try:
                logger.info(f"[{deployment_name}] Attempting to recreate QueueMonitor actor from stored config")
                queue_config = QueueMonitorConfig(
                    broker_url=stored_config["broker_url"],
                    queue_name=stored_config["queue_name"],
                )
                queue_monitor_actor = create_queue_monitor_actor(
                    deployment_name=deployment_name,
                    config=queue_config,
                )
                actor_found = True
                logger.info(f"[{deployment_name}] Successfully recreated QueueMonitor actor")
            except Exception as e:
                logger.error(f"[{deployment_name}] Failed to recreate QueueMonitor actor: {e}")
        else:
            logger.warning(
                f"[{deployment_name}] No stored config in policy_state, "
                f"cannot recover QueueMonitor actor"
            )

    return queue_monitor_actor, actor_found


default_autoscaling_policy = replica_queue_length_autoscaling_policy

default_queue_based_autoscaling_policy = queue_based_autoscaling_policy
