from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING

from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import (
        SchedulingDecision,
        Topology,
    )


@DeveloperAPI
class Autoscaler(metaclass=ABCMeta):
    """Abstract interface for Ray Data autoscaler."""

    def __init__(
        self,
        topology: "Topology",
        resource_manager: "ResourceManager",
        execution_id: str,
    ):
        self._topology = topology
        self._resource_manager = resource_manager
        self._execution_id = execution_id

    @abstractmethod
    def try_trigger_scaling(self, scheduling_decision: "SchedulingDecision"):
        """Try trigger autoscaling.

        This method will be called each time when StreamExecutor makes
        a scheduling decision.

        Args:
            scheduling_decision: The scheduling decision made by the executor.
        """
        ...

    @abstractmethod
    def on_executor_shutdown(self):
        """Callback when the StreamExecutor is shutting down."""
        ...
