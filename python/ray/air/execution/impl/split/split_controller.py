import logging
from typing import Dict, Optional, Callable

import ray
from ray.air.execution.actor_manager import ActorManager
from ray.air.execution.actor_request import ActorInfo, ActorRequest
from ray.air.execution.controller import Controller
from ray.air.execution.event import (
    ExecutionEvent,
    MultiFutureResult,
    FutureFailed,
)
from ray.air.execution.impl.split.split_result import SplitResult
from ray.air.execution.resources.fixed import FixedResourceManager
from ray.air.execution.resources.request import ResourceRequest
from ray.air.execution.resources.resource_manager import ResourceManager


logger = logging.getLogger(__name__)


class _SplitActor:
    def __init__(self, run_fn: Callable, args, kwargs):
        self.run_fn = run_fn
        self.args = args
        self.kwargs = kwargs

    def run(self):
        return self.run_fn(*self.args, **self.kwargs)


class SplitController(Controller):
    def __init__(
        self,
        resource_manager: Optional[ResourceManager] = None,
    ):
        self._actor_manager = ActorManager(
            resource_manager=resource_manager or FixedResourceManager()
        )

        self._started = False
        self._live_actors = set()

        self._results = []

    @property
    def results(self):
        return self._results

    def add_split(self, run_fn: Callable, resources: Dict, *args, **kwargs):
        self._actor_manager.add_actor(
            ActorRequest(
                cls=_SplitActor,
                kwargs={"run_fn": run_fn, "args": args, "kwargs": kwargs},
                resources=ResourceRequest([resources]),
            )
        )

    def next_event(self) -> ExecutionEvent:
        return self._actor_manager.next_event(block=True)

    def is_finished(self) -> bool:
        return self._started and not self._live_actors

    def on_step_begin(self) -> None:
        pass

    def actor_started(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo
    ) -> None:
        self._live_actors.add(actor)
        self._actor_manager.track_future(
            actor=actor, future=actor.run.remote(), cls=SplitResult
        )
        self._started = True

    def actor_stopped(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo
    ) -> None:
        self._live_actors.remove(actor)

    def actor_failed(
        self, actor: ray.actor.ActorHandle, actor_info: ActorInfo, exception: Exception
    ) -> None:
        logger.error(f"Split actor failed: {exception}")
        self._live_actors.remove(actor)

    def future_result(self, result: SplitResult):
        if isinstance(result, FutureFailed):
            logger.error(f"Split actor execution failed: {result.exception}")
        else:
            logger.info("Split actor finished.")
            self._results.append(result.return_value)
            self._actor_manager.remove_actor(result.actor)

    def multi_future_result(self, result: MultiFutureResult):
        pass
