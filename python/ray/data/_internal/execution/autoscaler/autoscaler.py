from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ray.data._internal.execution.resource_manager import ResourceManager
    from ray.data._internal.execution.streaming_executor_state import OpState, Topology


class ActorPoolAutoscalingHandler(metaclass=ABCMeta):

    @abstractmethod
    def actor_pool_min_size(self) -> int: ...

    @abstractmethod
    def actor_pool_max_size(self) -> int: ...

    @abstractmethod
    def actor_pool_current_size(self) -> int: ...

    @abstractmethod
    def num_running_actors(self) -> int: ...

    @abstractmethod
    def max_tasks_in_flight_per_actor(self) -> int: ...

    @abstractmethod
    def num_in_flight_tasks(self) -> int: ...

    @abstractmethod
    def scale_up_actor_pool(self, num_actors: int) -> int: ...

    @abstractmethod
    def scale_down_actor_pool(self, num_actors: int) -> int: ...


class Autoscaler(metaclass=ABCMeta):

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
    def try_trigger_scaling(self):
        pass

    @abstractmethod
    def on_executor_shutdown(self):
        pass
