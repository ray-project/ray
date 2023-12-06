import os
from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Any, List, Optional

import requests

from ray.serve.config import AutoscalingConfig
from ray.util.annotations import PublicAPI

PROMETHEUS_HOST = os.environ.get("RAY_PROMETHEUS_HOST", "http://localhost:9090")


@PublicAPI(stability="beta")
class TargetCapacityDirection(str, Enum):
    """Determines what direction the target capacity is scaling."""

    UP = "UP"
    DOWN = "DOWN"


@PublicAPI(stability="beta")
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
        target_capacity_scale_direction: Optional[TargetCapacityDirection] = None,
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

    @staticmethod
    def prometheus_metrics(metrics_name: str) -> List[Any]:
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


@PublicAPI(stability="beta")
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
