import os
import uuid
from collections import Counter
from typing import Any, Callable, Dict, Optional, Tuple, Type, Union

import ray
from ray.air.execution._internal import RayActorManager
from ray.air.execution.resources import (
    ResourceManager,
    ResourceRequest,
)

from ray.air.execution._internal.tracked_actor import TrackedActor
from ray.tune.execution.tune_controller import TuneController
from ray.tune.experiment import Trial


class NoopClassCache:
    def get(self, trainable_name: str):
        return trainable_name


class NoopResourceManager(ResourceManager):
    def __init__(self):
        self.requested_resources = []
        self.canceled_resource_requests = []
        self.currently_requested_resources = Counter()

    def request_resources(self, resource_request: ResourceRequest):
        self.requested_resources.append(resource_request)
        self.currently_requested_resources[resource_request] += 1

    def cancel_resource_request(self, resource_request: ResourceRequest):
        self.canceled_resource_requests.append(resource_request)
        self.currently_requested_resources[resource_request] -= 1

    def has_resources_ready(self, resource_request: ResourceRequest) -> bool:
        return True


class NoopActorManager(RayActorManager):
    def __init__(self, resource_manager: ResourceManager):
        super().__init__(resource_manager=resource_manager)

        self.added_actors = []
        self.removed_actors = []
        self.scheduled_futures = []

    def add_actor(
        self,
        cls: Union[Type, ray.actor.ActorClass],
        kwargs: Dict[str, Any],
        resource_request: ResourceRequest,
        *,
        on_start: Optional[Callable[[TrackedActor], None]] = None,
        on_stop: Optional[Callable[[TrackedActor], None]] = None,
        on_error: Optional[Callable[[TrackedActor, Exception], None]] = None,
    ) -> TrackedActor:
        fake_actor_ref = uuid.uuid4().int
        tracked_actor = TrackedActor(
            fake_actor_ref, on_start=on_start, on_stop=on_stop, on_error=on_error
        )
        self._live_actors_to_ray_actors_resources[tracked_actor] = (fake_actor_ref,)
        self.added_actors.append((tracked_actor, cls, kwargs))
        return tracked_actor

    def remove_actor(
        self,
        tracked_actor: TrackedActor,
        kill: bool = False,
    ) -> None:
        self.removed_actors.append(tracked_actor)

    def schedule_actor_task(
        self,
        tracked_actor: TrackedActor,
        method_name: str,
        args: Optional[Tuple] = None,
        kwargs: Optional[Dict] = None,
        on_result: Optional[Callable[[TrackedActor, Any], None]] = None,
        on_error: Optional[Callable[[TrackedActor, Exception], None]] = None,
        _return_future: bool = False,
    ) -> Optional[int]:
        fake_ref = uuid.uuid4().int
        self.scheduled_futures.append(
            (fake_ref, tracked_actor, method_name, args, kwargs, on_result, on_error)
        )
        return fake_ref

    @property
    def num_actor_tasks(self):
        return len(self.scheduled_futures)

    def get_live_actors_resources(self):
        return {}

    def next(self, timeout: Optional[Union[int, float]] = None) -> None:
        pass


class TestingTrial(Trial):
    def get_trainable_cls(self):
        return self.trainable_name

    def create_placement_group_factory(self):
        pass


def create_execution_test_objects(tmpdir, max_pending_trials: int = 8):
    os.environ["TUNE_MAX_PENDING_TRIALS_PG"] = str(max_pending_trials)

    tune_controller = TuneController(
        experiment_path=str(tmpdir),
        reuse_actors=True,
    )
    resource_manager = NoopResourceManager()
    actor_manger = NoopActorManager(resource_manager)
    tune_controller._actor_manager = actor_manger
    tune_controller._class_cache = NoopClassCache()

    return tune_controller, actor_manger, resource_manager
