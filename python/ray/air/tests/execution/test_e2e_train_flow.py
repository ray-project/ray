import random
from typing import Any, List, Optional

import pytest

import ray
from ray.air import ResourceRequest
from ray.air.execution import FixedResourceManager, PlacementGroupResourceManager
from ray.air.execution._internal import Barrier
from ray.air.execution._internal.actor_manager import RayActorManager
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

    This actor can fail in these ways:

    1. On init if ``actor_init_kill`` is passed as a kwarg
    2. On setup_1() if ``actor_setup_kill`` is passed as a kwarg (RayActorError)
    3. On setup_1() if ``actor_setup_fail`` is passed as a kwarg (RayTaskError)
    4. On train() if ``actor_train_kill`` is passed as a kwarg (RayTaskError)
    5. On train() if ``actor_train_fail`` is passed as a kwarg (RayTaskError)
    """

    def __init__(self, **kwargs):
        self.kwargs = kwargs

        if self.kwargs.get("actor_init_kill"):
            raise RuntimeError("INIT")

    def get_kwargs(self):
        return self.kwargs

    def setup_1(self):
        if self.kwargs.get("actor_setup_kill"):
            raise SystemExit

        if self.kwargs.get("actor_setup_fail"):
            raise RuntimeError("Setup")

        return True

    def setup_2(self):
        return True

    def train(self, value: float) -> float:
        if value == 4:
            if self.kwargs.get("actor_train_kill"):
                # SystemExit will invoke a RayActorError
                raise SystemExit

            if self.kwargs.get("actor_train_fail"):
                # RuntimeError will invoke a RayTaskError
                raise RuntimeError("TASK")

        return value


class TrainFlow:
    """This is a Ray Train-like execution flow.

    - We want to run 4 actors in total ("trials")
    - Each actor runs two init functions
    - We train all actors in parallel for 10 iterations
    - Errors can come up on actor construction, in the init functions,
      or during training
    - When an actor fails, restart that actor
    - When a task fails, stop actor, and restart
    """

    def __init__(
        self, actor_manager: RayActorManager, errors: Optional[List[str]] = None
    ):
        self._actor_manager = actor_manager
        self._finished = False

        self._actors_to_run = 4
        self._tracked_actors = []
        self._actors_stopped = 0

        self._actors_to_replace = set()

        self._ready_actors = set()
        self._training_barrier = Barrier(
            max_results=self._actors_to_run,
            on_completion=self.training_barrier_completed,
        )
        self._restart_training = None

        self._training_iter = 0
        self._results = []

        self._errors = errors

    def setup_actors(self):
        for actor_id in range(self._actors_to_run):
            error_kwargs = {}
            if self._errors:
                error = random.choice(self._errors)
                error_kwargs[error] = True

            print("Actor", actor_id, "will be failing with", error_kwargs)

            tracked_actor = self._actor_manager.add_actor(
                cls=Actor,
                kwargs={"id": actor_id, **error_kwargs},
                resource_request=ResourceRequest([{"CPU": 1}]),
                on_start=self.actor_started,
                on_stop=self.actor_stopped,
                on_error=self.actor_error,
            )
            self._tracked_actors.append(tracked_actor)

    def actor_started(self, tracked_actor: TrackedActor):
        self._actor_manager.schedule_actor_task(
            tracked_actor,
            "setup_1",
            on_error=self.setup_error,
            on_result=self.setup_1_result,
        )

    def actor_stopped(self, tracked_actor: TrackedActor):
        self._ready_actors.discard(tracked_actor)

        if tracked_actor in self._actors_to_replace:
            self._replace_actor(tracked_actor=tracked_actor)
        else:
            self._actors_stopped += 1
            self._finished = self._actors_stopped >= self._actors_to_run

    def actor_error(self, tracked_actor: TrackedActor, exception: Exception):
        self._ready_actors.discard(tracked_actor)
        self._replace_actor(tracked_actor=tracked_actor)

    def _replace_actor(self, tracked_actor: TrackedActor):
        actor_index = self._tracked_actors.index(tracked_actor)

        replacement_actor = self._actor_manager.add_actor(
            cls=Actor,
            kwargs={"id": actor_index},
            resource_request=ResourceRequest([{"CPU": 1}]),
            on_start=self.actor_started,
            on_stop=self.actor_stopped,
            on_error=self.actor_error,
        )

        self._tracked_actors[actor_index] = replacement_actor

    def setup_1_result(self, tracked_actor: TrackedActor, result: Any):
        self._actor_manager.schedule_actor_task(
            tracked_actor,
            "setup_2",
            on_error=self.setup_error,
            on_result=self.setup_2_result,
        )

    def setup_2_result(self, tracked_actor: TrackedActor, result: Any):
        self._ready_actors.add(tracked_actor)

        if len(self._ready_actors) == self._actors_to_run:
            self.continue_training()

    def setup_error(self, tracked_actor: TrackedActor, exception: Exception):
        if isinstance(exception, RayActorError):
            return

        self._actors_to_replace.add(tracked_actor)
        self._actor_manager.remove_actor(tracked_actor)

    def continue_training(self):
        if self._restart_training:
            self._training_iter = self._restart_training
        else:
            self._training_iter += 1

        self._training_barrier.reset()
        self._actor_manager.schedule_actor_tasks(
            self._tracked_actors,
            "train",
            args=(self._training_iter,),
            on_result=self._training_barrier.arrive,
            on_error=self.training_error,
        )

    def training_barrier_completed(self, barrier: Barrier):
        self._results.append([res for _, res in barrier.get_results()])
        self._restart_training = None

        # If less than 10 epochs, continue training
        if self._training_iter < 10:
            return self.continue_training()

        # Else, training finished
        for tracked_actor in self._tracked_actors:
            self._actor_manager.remove_actor(tracked_actor)

    def training_error(self, tracked_actor: TrackedActor, exception: Exception):
        self._restart_training = self._training_iter

        if isinstance(exception, RayActorError):
            return

        self._actors_to_replace.add(tracked_actor)
        self._ready_actors.discard(tracked_actor)
        self._actor_manager.remove_actor(tracked_actor)

    def run(self):
        self.setup_actors()

        while not self._finished:
            self._actor_manager.next()

    def get_results(self) -> List[List[float]]:
        return self._results


@pytest.mark.parametrize(
    "resource_manager_cls", [FixedResourceManager, PlacementGroupResourceManager]
)
@pytest.mark.parametrize(
    "errors",
    [
        None,
        "actor_init_kill",
        "actor_setup_kill",
        "actor_setup_fail",
        "actor_train_kill",
        "actor_train_fail",
        # Chaos - every actor fails somehow, but in different ways
        [
            "actor_init_kill",
            "actor_setup_kill",
            "actor_setup_fail",
            "actor_train_kill",
            "actor_train_fail",
        ],
    ],
)
def test_e2e(ray_start_4_cpus, resource_manager_cls, errors):
    actor_manager = RayActorManager(resource_manager=resource_manager_cls())

    if errors and isinstance(errors, str):
        errors = [errors]

    flow = TrainFlow(actor_manager=actor_manager, errors=errors)
    flow.run()

    results = flow.get_results()

    assert results == [[i] * 4 for i in range(1, 11)], results


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
