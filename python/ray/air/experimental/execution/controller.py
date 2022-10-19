import abc

import ray.actor
from ray.air.experimental.execution.actor_request import ActorInfo
from ray.air.experimental.execution.event import (
    ExecutionEvent,
    ActorStarted,
    ActorStopped,
    FutureResult,
    MultiFutureResult,
    ActorFailed,
)


class Controller(abc.ABC):
    """Convenience interface for actor/future managing libraries."""

    @abc.abstractmethod
    def is_finished(self) -> bool:
        raise NotImplementedError

    def on_step_begin(self) -> None:
        pass

    def on_step_end(self) -> None:
        pass

    @abc.abstractmethod
    def next_event(self) -> ExecutionEvent:
        raise NotImplementedError

    @abc.abstractmethod
    def actor_started(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo
    ) -> None:
        """Register actor start."""
        raise NotImplementedError

    @abc.abstractmethod
    def actor_failed(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo, exception: Exception
    ) -> None:
        """Register actor failure."""
        raise NotImplementedError

    @abc.abstractmethod
    def actor_stopped(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo
    ) -> None:
        """Register graceful actor stop (requested by contorller)."""
        raise NotImplementedError

    @abc.abstractmethod
    def future_result(self, result: FutureResult):
        """Handle result."""
        raise NotImplementedError

    @abc.abstractmethod
    def multi_future_result(self, result: MultiFutureResult):
        """Handle multiple result."""
        raise NotImplementedError

    def step(self):
        if self.is_finished():
            raise RuntimeError(
                "Cannot step through controller as it has finished already."
            )

        self.on_step_begin()

        event = self.next_event()

        if isinstance(event, ActorStarted):
            self.actor_started(actor=event.actor, actor_info=event.actor_info)
        elif isinstance(event, ActorFailed):
            self.actor_failed(
                actor=event.actor,
                actor_info=event.actor_info,
                exception=event.exception,
            )
        elif isinstance(event, ActorStopped):
            self.actor_stopped(actor=event.actor, actor_info=event.actor_info)
        elif isinstance(event, FutureResult):
            self.future_result(result=event)
        elif isinstance(event, MultiFutureResult):
            self.multi_future_result(result=event)

        self.on_step_end()

    def step_until_finished(self):
        while not self.is_finished():
            self.step()
