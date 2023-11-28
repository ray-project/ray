import logging
import math
import os
from abc import ABCMeta, abstractmethod
from decimal import ROUND_HALF_UP, Decimal
from enum import Enum
from typing import Any, List, Optional

import requests

import ray
from ray._private.utils import import_attr
from ray.serve._private.constants import CONTROL_LOOP_PERIOD_S, SERVE_LOGGER_NAME
from ray.serve.config import DEFAULT_AUTOSCALING_POLICY, AutoscalingConfig

logger = logging.getLogger(SERVE_LOGGER_NAME)
PROMETHEUS_HOST = os.environ.get("RAY_PROMETHEUS_HOST", "http://localhost:9090")


class TargetCapacityScaleDirection(str, Enum):
    """Determines what direction the target capacity is scaling."""

    UP = "UP"
    DOWN = "DOWN"


def calculate_desired_num_replicas(
    autoscaling_config: AutoscalingConfig, current_num_ongoing_requests: List[float]
) -> int:  # (desired replicas):
    """Returns the number of replicas to scale to based on the given metrics.

    Args:
        autoscaling_config: The autoscaling parameters to use for this
            calculation.
        current_num_ongoing_requests (List[float]): A list of the number of
            ongoing requests for each replica.  Assumes each entry has already
            been time-averaged over the desired lookback window.

    Returns:
        desired_num_replicas: The desired number of replicas to scale to, based
            on the input metrics and the current number of replicas.

    """
    current_num_replicas = len(current_num_ongoing_requests)
    if current_num_replicas == 0:
        raise ValueError("Number of replicas cannot be zero")

    # The number of ongoing requests per replica, averaged over all replicas.
    num_ongoing_requests_per_replica: float = sum(current_num_ongoing_requests) / len(
        current_num_ongoing_requests
    )

    # Example: if error_ratio == 2.0, we have two times too many ongoing
    # requests per replica, so we desire twice as many replicas.
    error_ratio: float = (
        num_ongoing_requests_per_replica
        / autoscaling_config.target_num_ongoing_requests_per_replica
    )

    # If error ratio >= 1, then the number of ongoing requests per
    # replica exceeds the target and we will make an upscale decision,
    # so we apply the upscale smoothing factor. Otherwise, the number of
    # ongoing requests per replica is lower than the target and we will
    # make a downscale decision, so we apply the downscale smoothing
    # factor.
    if error_ratio >= 1:
        smoothing_factor = autoscaling_config.get_upscale_smoothing_factor()
    else:
        smoothing_factor = autoscaling_config.get_downscale_smoothing_factor()

    # Multiply the distance to 1 by the smoothing ("gain") factor (default=1).
    smoothed_error_ratio = 1 + ((error_ratio - 1) * smoothing_factor)
    desired_num_replicas = math.ceil(current_num_replicas * smoothed_error_ratio)

    # If error_ratio = 0, meaning there is no more traffic, and desired
    # num replicas is stuck at a positive number due to the math.ceil
    # above, decrease desired_num_replicas by one so that the deployment
    # can eventually scale to 0.
    if (
        error_ratio == 0
        and desired_num_replicas == current_num_replicas
        and desired_num_replicas >= 1
    ):
        desired_num_replicas -= 1

    # Ensure min_replicas <= desired_num_replicas <= max_replicas.
    desired_num_replicas = min(autoscaling_config.max_replicas, desired_num_replicas)
    desired_num_replicas = max(autoscaling_config.min_replicas, desired_num_replicas)

    return desired_num_replicas


def get_capacity_adjusted_num_replicas(
    num_replicas: int, target_capacity: Optional[float]
) -> int:
    """Return the target state `num_replicas` adjusted by the `target_capacity`.

    The output will only ever be 0 if the passed `num_replicas` is 0. This is to
    support autoscaling deployments using scale-to-zero (we assume that any other
    deployment should always have at least 1 replica).

    Rather than using the default `round` behavior in Python, which rounds half to
    even, uses the `decimal` module to round half up (standard rounding behavior).
    """
    if target_capacity is None or target_capacity == 100:
        return num_replicas

    if num_replicas == 0:
        return 0

    adjusted_num_replicas = Decimal(num_replicas * target_capacity) / Decimal(100.0)
    rounded_adjusted_num_replicas = adjusted_num_replicas.to_integral_value(
        rounding=ROUND_HALF_UP
    )
    return max(1, int(rounded_adjusted_num_replicas))


class AutoscalingContext:
    """Contains the context for an autoscaling policy.

    This context includes the current number of replicas, the current number
    of ongoing requests, and the current number of queued queries.
    """

    def __init__(
        self,
        curr_target_num_replicas: int,
        current_num_ongoing_requests: List[float],
        current_handle_queued_queries: float,
        target_capacity: Optional[float] = None,
        target_capacity_scale_direction: Optional[TargetCapacityScaleDirection] = None,
        adjust_capacity: bool = False,
    ):
        """
        Arguments:
            curr_target_num_replicas: The number of replicas that the
                deployment is currently trying to scale to.
            current_num_ongoing_requests: List of number of
                ongoing requests for each replica.
            current_handle_queued_queries: The number of handle queued queries,
                if there are multiple handles, the max number of queries at
                a single handle should be passed in
            target_capacity: The target capacity of the deployment.
            target_capacity_scale_direction: The direction of the target capacity scale.
            adjust_capacity: whether the number of replicas should be adjusted by
                the current target_capacity.
        """
        self.curr_target_num_replicas = curr_target_num_replicas
        self.current_num_ongoing_requests = current_num_ongoing_requests
        self.current_handle_queued_queries = current_handle_queued_queries
        self.target_capacity = target_capacity
        self.target_capacity_scale_direction = target_capacity_scale_direction
        self.adjust_capacity = adjust_capacity

    def prometheus_metrics(self, metrics_name: str) -> List[Any]:
        """Return the current metrics from Prometheus given the metrics name."""
        try:
            resp = requests.get(
                f"{PROMETHEUS_HOST}/api/v1/query",
                params={"query": metrics_name},
            )
            return resp.json()["data"]["result"]
        except requests.exceptions.ConnectionError:
            return []
        except requests.exceptions.JSONDecodeError:
            return []
        except KeyError:
            return []

    @property
    def cpu_utilization(self) -> List[Any]:
        return self.prometheus_metrics("ray_node_cpu_utilization")

    @property
    def gpu_utilization(self) -> List[Any]:
        return self.prometheus_metrics("ray_node_gpus_utilization")


class AutoscalingPolicy:
    """Defines the interface for an autoscaling policy.

    To add a new autoscaling policy, a class should be defined that provides
    this interface. The class may be stateful, in which case it may also want
    to provide a non-default constructor. However, this state will be lost when
    the controller recovers from a failure.
    """

    __metaclass__ = ABCMeta

    def __init__(self, config: AutoscalingConfig):
        """Initialize the policy using the specified config dictionary."""
        self.config = config

    @abstractmethod
    def get_decision_num_replicas(
        self, autoscaling_context: AutoscalingContext
    ) -> Optional[int]:
        """Make a decision to scale replicas.

        Returns:
            int: The new number of replicas to scale to.
        """
        return autoscaling_context.curr_target_num_replicas


class BasicAutoscalingPolicy(AutoscalingPolicy):
    """The default autoscaling policy based on basic thresholds for scaling.
    There is a minimum threshold for the average queue length in the cluster
    to scale up and a maximum threshold to scale down. Each period, a 'scale
    up' or 'scale down' decision is made. This decision must be made for a
    specified number of periods in a row before the number of replicas is
    actually scaled. See config options for more details.  Assumes
    `get_decision_num_replicas` is called once every CONTROL_LOOP_PERIOD_S
    seconds.
    """

    def __init__(self, config: AutoscalingConfig):
        self.config = config
        # TODO(architkulkarni): Make configurable via AutoscalingConfig
        self.loop_period_s = CONTROL_LOOP_PERIOD_S
        self.scale_up_consecutive_periods = int(
            config.upscale_delay_s / self.loop_period_s
        )
        self.scale_down_consecutive_periods = int(
            config.downscale_delay_s / self.loop_period_s
        )

        # Keeps track of previous decisions. Each time the load is above
        # 'scale_up_threshold', the counter is incremented and each time it is
        # below 'scale_down_threshold', the counter is decremented. When the
        # load is between the thresholds or a scaling decision is made, the
        # counter is reset to 0.
        # TODO(architkulkarni): It may be too noisy to reset the counter each
        # time the direction changes, especially if we calculate frequently
        # (like every 0.1s).  A potentially less noisy option is to not reset
        # the counter, and instead only increment/decrement until we reach
        # scale_up_periods or scale_down_periods.
        self.decision_counter = 0

    def _calculate_base_desired_replica_numbers(
        self, context: AutoscalingContext
    ) -> int:
        if len(context.current_num_ongoing_requests) == 0:
            # When 0 replicas and queries are queued, scale up the replicas
            if context.current_handle_queued_queries > 0:
                return max(
                    math.ceil(1 * self.config.get_upscale_smoothing_factor()),
                    context.curr_target_num_replicas,
                )
            return context.curr_target_num_replicas

        decision_num_replicas = context.curr_target_num_replicas

        desired_num_replicas = calculate_desired_num_replicas(
            self.config, context.current_num_ongoing_requests
        )
        # Scale up.
        if desired_num_replicas > context.curr_target_num_replicas:
            # If the previous decision was to scale down (the counter was
            # negative), we reset it and then increment it (set to 1).
            # Otherwise, just increment.
            if self.decision_counter < 0:
                self.decision_counter = 0
            self.decision_counter += 1

            # Only actually scale the replicas if we've made this decision for
            # 'scale_up_consecutive_periods' in a row.
            if self.decision_counter > self.scale_up_consecutive_periods:
                self.decision_counter = 0
                decision_num_replicas = desired_num_replicas

        # Scale down.
        elif desired_num_replicas < context.curr_target_num_replicas:
            # If the previous decision was to scale up (the counter was
            # positive), reset it to zero before decrementing.
            if self.decision_counter > 0:
                self.decision_counter = 0
            self.decision_counter -= 1

            # Only actually scale the replicas if we've made this decision for
            # 'scale_down_consecutive_periods' in a row.
            if self.decision_counter < -self.scale_down_consecutive_periods:
                self.decision_counter = 0
                decision_num_replicas = desired_num_replicas

        # Do nothing.
        else:
            self.decision_counter = 0

        return decision_num_replicas

    def _clip_desired_replica_numbers(
        self,
        context: AutoscalingContext,
        decision_num_replicas: int,
    ) -> Optional[int]:
        if (
            getattr(context, "target_capacity", None) is None
            or getattr(context, "target_capacity_scale_direction", None) is None
        ):
            return decision_num_replicas

        # Clip the replica count by capacity-adjusted bounds.
        # TODO (shrekris-anyscale): this should logic should be pushed into the
        # autoscaling_policy. Need to discuss what the right API would look like.
        upper_bound = get_capacity_adjusted_num_replicas(
            self.config.max_replicas, context.target_capacity
        )
        if (
            context.target_capacity_scale_direction == TargetCapacityScaleDirection.UP
            and self.config.initial_replicas is not None
        ):
            lower_bound = get_capacity_adjusted_num_replicas(
                self.config.initial_replicas, context.target_capacity
            )
        else:
            lower_bound = get_capacity_adjusted_num_replicas(
                self.config.min_replicas, context.target_capacity
            )

        clipped_decision_num_replicas = max(
            lower_bound, min(decision_num_replicas, upper_bound)
        )

        if (
            clipped_decision_num_replicas == context.curr_target_num_replicas
            and context.adjust_capacity is False
        ):
            return

        return clipped_decision_num_replicas

    def get_decision_num_replicas(
        self, autoscaling_context: AutoscalingContext
    ) -> Optional[int]:
        base_desired_replica_numbers = self._calculate_base_desired_replica_numbers(
            context=autoscaling_context,
        )
        decision_num_replicas = self._clip_desired_replica_numbers(
            context=autoscaling_context,
            decision_num_replicas=base_desired_replica_numbers,
        )

        return decision_num_replicas


class CustomScalingPolicy(AutoscalingPolicy):
    """A custom autoscaling policy that can be specified by the user."""

    def __init__(self, config: AutoscalingConfig):
        self.config = config
        # self.custom_scaling_callable = import_attr(config.autoscaling_policy)
        self.custom_config_ref = None

    def get_custom_config_ref(self, autoscaling_context: AutoscalingContext):
        """Get the custom config reference."""
        return ray.remote(self.custom_scaling_callable).remote(autoscaling_context)

    def get_decision_num_replicas(
        self, autoscaling_context: AutoscalingContext
    ) -> Optional[int]:
        """Make a decision to scale replicas.

        Returns the new number of replicas to scale to. Or None if the custom scaling
        function call is not finished yet, finished but not returning an integer, or
        errored out.
        """
        if self.custom_config_ref is None:
            self.custom_config_ref = self.get_custom_config_ref(autoscaling_context)

        finished, _ = ray.wait([self.custom_config_ref], timeout=0)
        try:
            if self.custom_config_ref in finished:
                decision_num_replicas = ray.get(self.custom_config_ref)
                self.custom_config_ref = None
                if (
                    isinstance(decision_num_replicas, int)
                    and decision_num_replicas
                    != autoscaling_context.curr_target_num_replicas
                ):
                    return decision_num_replicas
                elif not isinstance(decision_num_replicas, int):
                    logger.error(
                        "Custom scaling policy must return an integer. Received type "
                        f"{type(decision_num_replicas)}, for {decision_num_replicas}"
                    )
        except ray.exceptions.RayTaskError as e:
            logger.error(f"Error in custom scaling policy: {e}")
            self.custom_config_ref = None

        return None


class AutoscalingPolicyManager:
    """Managing autoscaling policies and the lifecycle of the scaling function calls."""

    def __init__(self, config: Optional[AutoscalingConfig]):
        self.config = config
        self.autoscaling_policy = None
        self.replica_decision_call_ref = None
        # self.replica_decision_callable = import_attr(config.autoscaling_policy)
        self.create_policy()

    def _get_replica_decision_callable(self):
        """Get the replica decision callable."""
        self.replica_decision_callable = import_attr(self.config.autoscaling_policy)

    def _get_custom_config_ref(self, autoscaling_context: AutoscalingContext):
        """Get the custom config reference."""
        return ray.remote(self.custom_scaling_callable).remote(autoscaling_context)

    def should_autoscale(self) -> bool:
        """Returns whether autoscaling should be performed."""
        return self.config is not None

    def create_policy(self):
        """Creates an autoscaling policy based on the given config.

        Args:
            config: The config to use for the policy.
        """
        if self.config is not None and self.config != DEFAULT_AUTOSCALING_POLICY:
            self.autoscaling_policy = CustomScalingPolicy(self.config)
        elif self.config is not None:
            self.autoscaling_policy = BasicAutoscalingPolicy(self.config)

    def get_decision_num_replicas(
        self, autoscaling_context: AutoscalingContext
    ) -> Optional[int]:
        """Make a decision to scale replicas.

        Returns the new number of replicas to scale to. Or None if the custom scaling
        function call is not finished yet, finished but not returning an integer, or
        errored out.
        """
        return self.autoscaling_policy.get_decision_num_replicas(autoscaling_context)
        # if self.replica_decision_call_ref is None:
        #     self.replica_decision_call_ref = self._get_custom_config_ref(
        #         autoscaling_context
        #     )
        #
        # finished, _ = ray.wait([self.replica_decision_call_ref], timeout=0)
        # try:
        #     if self.replica_decision_call_ref in finished:
        #         decision_num_replicas = ray.get(self.replica_decision_call_ref)
        #         self.replica_decision_call_ref = None
        #         if (
        #             isinstance(decision_num_replicas, int)
        #             and decision_num_replicas
        #             != autoscaling_context.curr_target_num_replicas
        #         ):
        #             return decision_num_replicas
        #         elif not isinstance(decision_num_replicas, int):
        #             logger.error(
        #                 "Custom scaling policy must return an integer. Received type "
        #                 f"{type(decision_num_replicas)}, for {decision_num_replicas}"
        #             )
        # except ray.exceptions.RayTaskError as e:
        #     logger.error(f"Error in custom scaling policy: {e}")
        #     self.replica_decision_call_ref = None
        #
        # return None
