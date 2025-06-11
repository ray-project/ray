import abc
from dataclasses import dataclass
from typing import Dict

from ray.train.v2._internal.execution.callback import ControllerCallback
from ray.train.v2._internal.execution.worker_group import (
    WorkerGroupPollStatus,
    WorkerGroupState,
)
from ray.train.v2.api.config import ScalingConfig


@dataclass
class ScalingDecision:
    pass


@dataclass
class NoopDecision(ScalingDecision):
    pass


@dataclass
class ResizeDecision(ScalingDecision):
    num_workers: int
    resources_per_worker: Dict[str, float]


class ScalingPolicy(abc.ABC, ControllerCallback):
    """A policy that determines when and how to scale a worker group.

    This can be used to implement elasticity and fault tolerance.

    Recovery decisions are made when workers are in an inactive or unhealthy state.
    Upscale decisions are optional and are made when workers are healthy.
    """

    # TODO: Restructure these APIs to consider different TrainControllerStates
    # instead of just running and non-running worker groups.

    def __init__(self, scaling_config: ScalingConfig):
        self.scaling_config = scaling_config

    @abc.abstractmethod
    def make_decision_for_non_running_worker_group(self) -> ScalingDecision:
        """Makes a scaling decision when the worker group is initializing
        or recovering from an error."""
        raise NotImplementedError

    @abc.abstractmethod
    def make_decision_for_running_worker_group(
        self,
        worker_group_state: WorkerGroupState,
        worker_group_status: WorkerGroupPollStatus,
    ) -> ScalingDecision:
        """Makes a scaling decision when monitoring healthy, running workers."""
        raise NotImplementedError
