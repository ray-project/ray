import functools
import logging
import math
from typing import Any, Callable, Dict, Optional, Tuple, Union

from ray.serve._private.common import DeploymentID
from ray.serve._private.constants import (
    CONTROL_LOOP_INTERVAL_S,
    SERVE_AUTOSCALING_DECISION_COUNTERS_KEY,
    SERVE_LOGGER_NAME,
)
from ray.serve.config import AutoscalingConfig, AutoscalingContext
from ray.util.annotations import PublicAPI

logger = logging.getLogger(SERVE_LOGGER_NAME)


def _apply_scaling_factors(
    desired_num_replicas: Union[int, float],
    current_num_replicas: int,
    autoscaling_config: AutoscalingConfig,
) -> int:
    """Apply scaling factors to the desired number of replicas.
    Returns the scaled number of replicas depending on the scaling factor.
    The computation uses the difference between desired and current to scale.

    """
    replicas_delta = desired_num_replicas - current_num_replicas
    scaling_factor = (
        autoscaling_config.get_upscaling_factor()
        if replicas_delta > 0
        else autoscaling_config.get_downscaling_factor()
    )
    scaled_num_replicas = math.ceil(
        current_num_replicas + scaling_factor * replicas_delta
    )
    # If the scaled_replicas are stuck during downscaling because of scaling factor, decrement by 1.
    if (
        math.ceil(float(desired_num_replicas)) < current_num_replicas
        and scaled_num_replicas == current_num_replicas
    ):
        scaled_num_replicas -= 1
    return scaled_num_replicas


def _apply_delay_logic(
    desired_num_replicas: int,
    curr_target_num_replicas: int,
    config: AutoscalingConfig,
    policy_state: Dict[str, Any],
) -> Tuple[int, Dict[str, Any]]:

    """Apply delay logic to the desired number of replicas."""
    decision_num_replicas = curr_target_num_replicas
    decision_counter = policy_state.get(SERVE_AUTOSCALING_DECISION_COUNTERS_KEY, 0)
    # Scale up.
    if desired_num_replicas > curr_target_num_replicas:
        # If the previous decision was to scale down (the counter was
        # negative), we reset it and then increment it (set to 1).
        # Otherwise, just increment.
        if decision_counter < 0:
            decision_counter = 0
        decision_counter += 1

        # Only actually scale the replicas if we've made this decision for
        # 'scale_up_consecutive_periods' in a row.
        if decision_counter > int(config.upscale_delay_s / CONTROL_LOOP_INTERVAL_S):
            decision_counter = 0
            decision_num_replicas = desired_num_replicas

    # Scale down.
    elif desired_num_replicas < curr_target_num_replicas:
        # If the previous decision was to scale up (the counter was
        # positive), reset it to zero before decrementing.

        if decision_counter > 0:
            decision_counter = 0
        decision_counter -= 1
        # Downscaling to zero is only allowed from 1 -> 0
        is_scaling_to_zero = curr_target_num_replicas == 1
        # Determine the delay to use
        if is_scaling_to_zero:
            # Check if the downscale_to_zero_delay_s is set
            if config.downscale_to_zero_delay_s is not None:
                delay_s = config.downscale_to_zero_delay_s
            else:
                delay_s = config.downscale_delay_s
        else:
            delay_s = config.downscale_delay_s
            # The desired_num_replicas>0 for downscaling cases other than 1->0
            desired_num_replicas = max(1, desired_num_replicas)
        # Only actually scale the replicas if we've made this decision for
        # 'scale_down_consecutive_periods' in a row.
        if decision_counter < -int(delay_s / CONTROL_LOOP_INTERVAL_S):
            decision_counter = 0
            decision_num_replicas = desired_num_replicas
    # Do nothing.
    else:
        decision_counter = 0

    policy_state[SERVE_AUTOSCALING_DECISION_COUNTERS_KEY] = decision_counter
    return decision_num_replicas, policy_state


def _apply_bounds(
    num_replicas: int,
    capacity_adjusted_min_replicas: int,
    capacity_adjusted_max_replicas: int,
) -> int:
    """Clip replica count to be within capacity-adjusted min/max bounds."""
    return max(
        capacity_adjusted_min_replicas,
        min(capacity_adjusted_max_replicas, num_replicas),
    )


def _apply_default_params(
    desired_num_replicas: Union[int, float],
    ctx: AutoscalingContext,
    policy_state: Dict[str, Any],
) -> Tuple[int, Dict[str, Any]]:
    """Apply the default parameters to the desired number of replicas."""

    desired_num_replicas = _apply_scaling_factors(
        desired_num_replicas, ctx.current_num_replicas, ctx.config
    )
    # Apply bounds
    bounded_num_replicas = _apply_bounds(
        desired_num_replicas,
        ctx.capacity_adjusted_min_replicas,
        ctx.capacity_adjusted_max_replicas,
    )
    # Apply delay logic
    # Only send the internal state here to avoid overwriting the custom policy state.
    final_num_replicas, updated_state = _apply_delay_logic(
        bounded_num_replicas, ctx.target_num_replicas, ctx.config, policy_state
    )

    return final_num_replicas, updated_state


def _apply_default_params_and_merge_state(
    policy_state: Dict[str, Any],
    user_policy_state: Dict[str, Any],
    desired_num_replicas: Union[int, float],
    ctx: AutoscalingContext,
) -> Tuple[int, Dict[str, Any]]:

    # Extract internal polciy state from policy_state
    internal_policy_state = {
        SERVE_AUTOSCALING_DECISION_COUNTERS_KEY: policy_state.get(
            SERVE_AUTOSCALING_DECISION_COUNTERS_KEY, 0
        )
    }
    # Only pass the internal state used for delay counters so we don't
    # overwrite any custom user state.
    final_num_replicas, updated_state = _apply_default_params(
        desired_num_replicas, ctx, internal_policy_state
    )
    # Merge internal updated_state with the user's custom policy state.
    if updated_state:
        user_policy_state.update(updated_state)
    return final_num_replicas, user_policy_state


def _merge_user_state_with_internal_state(
    policy_state: Dict[str, Any],
    user_policy_state: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge user state with previous policy state, preserving internal keys.

    This mutates and returns `user_policy_state`.
    """
    # Extract internal polciy state from policy_state
    internal_policy_state = {
        SERVE_AUTOSCALING_DECISION_COUNTERS_KEY: policy_state.get(
            SERVE_AUTOSCALING_DECISION_COUNTERS_KEY, 0
        )
    }
    user_policy_state.update(internal_policy_state)
    return user_policy_state


def _get_cold_start_scale_up_replicas(ctx: AutoscalingContext) -> Optional[int]:
    """
    Returns the desired number of replicas if the cold start fast path applies, otherwise returns None.
    """
    if ctx.current_num_replicas == 0:
        if ctx.total_num_requests > 0:
            return max(
                math.ceil(1 * ctx.config.get_upscaling_factor()),
                ctx.target_num_replicas,
            )
        return ctx.target_num_replicas
    return None


def _apply_autoscaling_config(
    policy_func: Callable[
        [AutoscalingContext], Tuple[Union[int, float], Dict[str, Any]]
    ]
) -> Callable[[AutoscalingContext], Tuple[int, Dict[str, Any]]]:
    """
    Wraps a custom policy function to automatically apply:
    - upscaling_factor / downscaling_factor
    - min_replicas / max_replicas bounds
    - upscale_delay_s / downscale_delay_s / downscale_to_zero_delay_s
    """

    @functools.wraps(policy_func)
    def wrapped_policy(ctx: AutoscalingContext) -> Tuple[int, Dict[str, Any]]:

        # Cold start fast path: 0 replicas bypasses delay logic for immediate scale-up
        cold_start_replicas = _get_cold_start_scale_up_replicas(ctx)
        if cold_start_replicas is not None:
            return cold_start_replicas, ctx.policy_state
        policy_state = ctx.policy_state.copy()
        desired_num_replicas, updated_custom_policy_state = policy_func(ctx)
        final_num_replicas, final_state = _apply_default_params_and_merge_state(
            policy_state, updated_custom_policy_state, desired_num_replicas, ctx
        )

        return final_num_replicas, final_state

    return wrapped_policy


def _apply_app_level_autoscaling_config(
    policy_func: Callable[
        [Dict[DeploymentID, AutoscalingContext]],
        Tuple[
            Dict[DeploymentID, Union[int, float]],
            Optional[Dict[DeploymentID, Dict]],
        ],
    ]
) -> Callable[
    [Dict[DeploymentID, AutoscalingContext]],
    Tuple[Dict[DeploymentID, int], Dict[DeploymentID, Dict]],
]:
    """
    Wraps an application-level custom policy function to automatically apply per-deployment:
    - upscaling_factor / downscaling_factor
    - min_replicas / max_replicas bounds
    - upscale_delay_s / downscale_delay_s / downscale_to_zero_delay_s
    """

    @functools.wraps(policy_func)
    def wrapped_policy(
        contexts: Dict[DeploymentID, AutoscalingContext]
    ) -> Tuple[Dict[DeploymentID, int], Dict[DeploymentID, Dict]]:

        # Store the policy state per deployment
        state_per_deployment = {}
        for dep_id, ctx in contexts.items():
            state_per_deployment[dep_id] = ctx.policy_state.copy()

        # Send to the actual policy
        desired_num_replicas_dict, updated_custom_policy_state = policy_func(contexts)
        updated_custom_policy_state = updated_custom_policy_state or {}

        # Build per-deployment replicas count and state dictionary.
        final_decisions: Dict[DeploymentID, int] = {}
        final_state: Dict[DeploymentID, Dict] = {}
        for dep_id, ctx in contexts.items():
            if dep_id not in desired_num_replicas_dict:
                final_state[dep_id] = state_per_deployment[dep_id]
                continue

            custom_policy_state_per_deployment = updated_custom_policy_state.get(
                dep_id, {}
            )
            # Cold start fast path: 0 replicas bypasses delay logic for immediate scale-up
            cold_start_replicas = _get_cold_start_scale_up_replicas(ctx)
            if cold_start_replicas is not None:
                final_decisions[dep_id] = cold_start_replicas
                # Merge user policy state with internal policy state
                final_state[dep_id] = _merge_user_state_with_internal_state(
                    state_per_deployment[dep_id],
                    custom_policy_state_per_deployment,
                )
                continue
            final_num_replicas, final_dep_state = _apply_default_params_and_merge_state(
                state_per_deployment[dep_id],
                custom_policy_state_per_deployment,
                desired_num_replicas_dict[dep_id],
                ctx,
            )
            final_decisions[dep_id] = final_num_replicas
            final_state[dep_id] = final_dep_state
        return final_decisions, final_state

    return wrapped_policy


def _core_replica_queue_length_policy(
    ctx: AutoscalingContext,
) -> Tuple[float, Dict[str, Any]]:
    num_running_replicas = ctx.current_num_replicas
    config = ctx.config
    if num_running_replicas == 0:
        raise ValueError("Number of replicas cannot be zero")
    target_num_requests = config.get_target_ongoing_requests() * num_running_replicas
    error_ratio = ctx.total_num_requests / target_num_requests
    desired_num_replicas = num_running_replicas * error_ratio
    return desired_num_replicas, {}


@PublicAPI(stability="alpha")
def replica_queue_length_autoscaling_policy(
    ctx: AutoscalingContext,
) -> Tuple[Union[int, float], Dict[str, Any]]:
    """The default autoscaling policy based on basic thresholds for scaling.
    There is a minimum threshold for the average queue length in the cluster
    to scale up and a maximum threshold to scale down. Each period, a 'scale
    up' or 'scale down' decision is made. This decision must be made for a
    specified number of periods in a row before the number of replicas is
    actually scaled. See config options for more details.  Assumes
    `get_decision_num_replicas` is called once every CONTROL_LOOP_PERIOD_S
    seconds.
    """
    # Adding this guard makes the public policy safe to call directly.
    cold_start_replicas = _get_cold_start_scale_up_replicas(ctx)
    if cold_start_replicas is not None:
        return cold_start_replicas, ctx.policy_state
    return _core_replica_queue_length_policy(ctx)


default_autoscaling_policy = replica_queue_length_autoscaling_policy
