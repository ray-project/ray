from typing import Dict, List

from ray.air.execution import action
from ray.air.execution.actor_request import ActorRequest, ActorInfo
from ray.air.execution.result import ExecutionResult


class Controller:
    def is_finished(self) -> bool:
        raise NotImplementedError

    def get_actor_requests(self) -> List[ActorRequest]:
        raise NotImplementedError

    def actor_started(self, actor_info: ActorInfo) -> None:
        """Register actor start."""
        raise NotImplementedError

    def actor_failed(self, actor_info: ActorInfo, exception: Exception) -> None:
        """Register actor failure."""
        raise NotImplementedError

    def actor_stopped(self, actor_info: ActorInfo) -> None:
        """Register graceful actor stop (requested by contorller)."""
        raise NotImplementedError

    def actor_results(
        self, actor_infos: List[ActorInfo], results: List[ExecutionResult]
    ):
        """Handle result."""
        raise NotImplementedError

    def get_actions(self) -> Dict[ActorInfo, List[action.Action]]:
        """Act on the available information."""
        raise NotImplementedError
