import ray.actor
from ray.air.execution.actor_request import ActorInfo
from ray.air.execution.event import (
    ExecutionEvent,
    ActorStarted,
    ActorStopped,
    FutureResult,
)


class Controller:
    def is_finished(self) -> bool:
        raise NotImplementedError

    def on_step_begin(self) -> None:
        pass

    def on_step_end(self) -> None:
        pass

    def next_event(self) -> ExecutionEvent:
        raise NotImplementedError

    def step(self):
        self.on_step_begin()

        event = self.next_event()

        if isinstance(event, ActorStarted):
            self.actor_started(actor=event.actor, actor_info=event.actor_info)
        elif isinstance(event, ActorStopped):
            if event.exception:
                self.actor_failed(
                    actor=event.actor,
                    actor_info=event.actor_info,
                    exception=event.exception,
                )
            else:
                self.actor_started(actor=event.actor, actor_info=event.actor_info)
        elif isinstance(event, FutureResult):
            self.future_result(result=event)

    def actor_started(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo
    ) -> None:
        """Register actor start."""
        raise NotImplementedError

    def actor_failed(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo, exception: Exception
    ) -> None:
        """Register actor failure."""
        raise NotImplementedError

    def actor_stopped(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo
    ) -> None:
        """Register graceful actor stop (requested by contorller)."""
        raise NotImplementedError

    def future_result(self, result: FutureResult):
        """Handle result."""
        raise NotImplementedError
