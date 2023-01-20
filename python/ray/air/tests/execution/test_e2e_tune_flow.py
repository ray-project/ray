from collections import defaultdict
from typing import Dict, List, Optional

import pytest

import ray
from ray.air import ResourceRequest
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.air.execution._internal.event_manager import RayEventManager
from ray.air.execution._internal.tracked_actor import TrackedActor
from ray.exceptions import RayActorError


@pytest.fixture(scope="module")
def ray_start_4_cpus():
    address_info = ray.init(num_cpus=4)
    yield address_info
    ray.shutdown()


@ray.remote
class Actor:
    """Simple actor for testing an execution flow.

    This actor can fail in three ways:

    1. On init if ``actor_error_init`` is passed as a kwarg
    2. On run() if ``actor_error_task`` is passed as a kwarg (RayActorError)
    3. On run() if ``task_error`` is passed as a kwarg (RayTaskError)
    """

    def __init__(self, **kwargs):
        self.kwargs = kwargs

        if self.kwargs.get("actor_error_init"):
            raise RuntimeError("INIT")

    def get_kwargs(self):
        return self.kwargs

    def run(self, value: float) -> float:
        if value == 2:
            if self.kwargs.get("actor_error_task"):
                # SystemExit will invoke a RayActorError
                raise SystemExit

            if self.kwargs.get("task_error"):
                # RuntimeError will invoke a RayTaskError
                raise RuntimeError("TASK")

        return value


class TuneFlow:
    """This is a Ray-tune like execution flow.

    - We want to run 10 actors in total ("trials")
    - Each actor collects 11 results sequentially
    - We schedule up to 6 actors at the same time
    - Every step, we see if we should add any new actors
    - Otherwise, we just yield control to the event manager and process events one
      by one
    - When an actor is started, start training flow
    - When a result comes in, schedule next future
      - If this is the 11th result, stop actor
    - When the last actor is stopped, set state to finished

    - When an actor fails, restart
    - When a task fails, stop actor, and restart
    """

    def __init__(self, event_manager: RayEventManager, errors: Optional[str] = None):
        self._event_manager = event_manager
        self._finished = False

        self._actors_to_run = 10
        self._actors_started = 0
        self._actors_stopped = 0
        self._max_pending = 6

        self._actor_to_id = {}
        self._results = defaultdict(list)

        self._errors = errors

    def maybe_add_actors(self):
        if self._actors_started >= self._actors_to_run:
            return

        if self._event_manager.num_pending_actors >= self._max_pending:
            return

        error_kwargs = {}
        if self._errors:
            error_kwargs[self._errors] = True

        actor_id = self._actors_started
        tracked_actor = (
            self._event_manager.add_actor(
                cls=Actor,
                kwargs={"id": actor_id, **error_kwargs},
                resource_request=ResourceRequest([{"CPU": 1}]),
            )
            .on_start(self.actor_started)
            .on_stop(self.actor_stopped)
            .on_error(self.actor_error)
        )
        self._actor_to_id[tracked_actor] = actor_id

        self._actors_started += 1

    def actor_started(self, tracked_actor: TrackedActor):
        self._event_manager.schedule_actor_task(
            tracked_actor, "run", kwargs={"value": 0}
        ).on_error(self.task_error).on_result(self.task_result)

    def actor_stopped(self, tracked_actor: TrackedActor):
        self._actors_stopped += 1
        self._finished = self._actors_stopped >= self._actors_to_run

    def actor_error(self, tracked_actor: TrackedActor, exception: Exception):
        actor_id = self._actor_to_id.pop(tracked_actor)

        replacement_actor = (
            self._event_manager.add_actor(
                cls=Actor,
                kwargs={
                    "id": actor_id,
                    "actor_error_init": False,
                    "actor_error_task": False,
                    "task_error": False,
                },
                resource_request=ResourceRequest([{"CPU": 1}]),
            )
            .on_start(self.actor_started)
            .on_stop(self.actor_stopped)
            .on_error(self.actor_error)
        )

        self._actor_to_id[replacement_actor] = actor_id

    def task_result(self, tracked_actor: TrackedActor, result: float):
        actor_id = self._actor_to_id[tracked_actor]
        self._results[actor_id].append(result)

        if result == 10:
            self._event_manager.remove_actor(tracked_actor)
        else:
            self._event_manager.schedule_actor_task(
                tracked_actor, "run", kwargs={"value": result + 1}
            ).on_error(self.task_error).on_result(self.task_result)

    def task_error(self, tracked_actor: TrackedActor, exception: Exception):
        if isinstance(exception, RayActorError):
            return

        self._actors_stopped -= 1  # account for extra stop
        self._event_manager.remove_actor(tracked_actor, resolve_futures=False)
        actor_id = self._actor_to_id.pop(tracked_actor)

        replacement_actor = (
            self._event_manager.add_actor(
                cls=Actor,
                kwargs={
                    "id": actor_id,
                    "actor_error_init": False,
                    "actor_error_task": False,
                    "task_error": False,
                },
                resource_request=ResourceRequest([{"CPU": 1}]),
            )
            .on_start(self.actor_started)
            .on_stop(self.actor_stopped)
            .on_error(self.actor_error)
        )
        self._actor_to_id[replacement_actor] = actor_id

    def run(self):
        while not self._finished:
            self.maybe_add_actors()
            self._event_manager.wait(num_events=1, timeout=1)

    def get_results(self) -> Dict[int, List[float]]:
        return self._results


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
@pytest.mark.parametrize(
    "errors", [None, "actor_error_init", "actor_error_task", "task_error"]
)
def test_e2e(ray_start_4_cpus, resource_manager_cls, errors):
    event_manager = RayEventManager(resource_manager=resource_manager_cls())

    flow = TuneFlow(event_manager=event_manager, errors=errors)
    flow.run()

    results = flow.get_results()

    assert all(res[-1] == 10 for res in results.values()), results


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
