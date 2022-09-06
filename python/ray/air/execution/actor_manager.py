import random
from collections import defaultdict, deque
from typing import List, Optional, Union, Type, Dict, Set, Deque, Any

import ray
from ray.air.execution.actor_request import ActorRequest, ActorInfo
from ray.air.execution.future import TypedFuture
from ray.air.execution.resources.resource_manager import ResourceManager
from ray.air.execution.event import (
    ExecutionEvent,
    FutureFailed,
    ActorStopped,
    ActorStarted,
    _ResourceReady,
    NativeResult,
    FutureResult,
)


def _resolve(
    future: Union[TypedFuture, List[TypedFuture]]
) -> Union[ExecutionEvent, List[ExecutionEvent]]:
    if isinstance(future, list):
        return _resolve_multiple_futures(future)
    else:
        return _resolve_multiple_futures([future])[0]


def _resolve_multiple_futures(futures: List[TypedFuture]) -> List[ExecutionEvent]:
    try:
        results = ray.get([f.future for f in futures])
    except Exception as e:
        return [FutureFailed(e) for i in range(len(futures))]
    return [f.convert_result(result) for f, result in zip(futures, results)]


def _convert_result(cls: FutureResult, actor: ray.actor.ActorHandle, result: Any):
    n_args = len(getattr(cls, "__dataclass_fields__", 1))
    if n_args == 0:
        return cls()
    elif n_args == 1:
        return cls(actor)
    elif n_args == 2:
        return cls(actor, result)
    else:
        return cls(actor, *result)


class ActorManager:
    def __init__(self, resource_manager: ResourceManager):
        self._resource_manager: ResourceManager = resource_manager

        self._actor_requests: List[ActorRequest] = []
        self._active_actors: Set[ray.actor.ActorHandle] = set()
        self._actor_to_info: Dict[ray.actor.ActorHandle, ActorInfo] = {}

        self._actors_to_futures: Dict[
            ray.actor.ActorHandle, Set[ray.ObjectRef]
        ] = defaultdict(set)
        self._futures_to_actors: Dict[ray.ObjectRef, ray.actor.ActorHandle] = {}
        self._futures_to_classes: Dict[ray.ObjectRef, Type[TypedFuture]] = {}

        self._actors_to_remove: Dict[ray.actor.ActorHandle, Optional[Exception]] = {}

        self._ready_future: Optional[ray.ObjectRef] = None
        self._next_events: Deque[ExecutionEvent] = deque()

    def has_ready_event(self) -> bool:
        """Return True if there is an event queued or ready to resolve."""
        return bool(self._next_events or self._ready_future)

    def wait_union(self, *actor_managers):
        """Wait for next future of any of the actor managers."""
        raise NotImplementedError

    def wait(self):
        """Wait for next future."""
        if self.has_ready_event():
            return

        self._ready_future = self._wait_for_next_future()

    def _trigger_bookkeeping(self):
        self._remove_stale_actors()
        self._start_new_actors()

    def next_event(self, block: bool = True) -> Optional[ExecutionEvent]:
        self._trigger_bookkeeping()

        if self._next_events:
            return self._next_events.popleft()

        if not self._ready_future:
            self._ready_future = self._wait_for_next_future(block=block)

        if not self._ready_future:
            # If we didn't block, this could be None
            return None

        result = self._resolve_ready_future()

        if isinstance(result, _ResourceReady):
            raise NotImplementedError("Todo: Ready resource handling")

        return result

    def _get_futures_to_await(self, shuffle: bool = True) -> List[ray.ObjectRef]:
        resource_futures = self._resource_manager.get_resource_futures()
        tracked_futures = list(self._futures_to_actors.keys())

        if shuffle:
            random.shuffle(tracked_futures)

        # Prioritize resource futures
        futures = resource_futures + tracked_futures

        return futures

    def _wait_for_next_future(self, block: bool = True) -> Optional[ray.ObjectRef]:
        futures = self._get_futures_to_await()

        ready, not_ready = ray.wait(
            futures, timeout=(None if block else 0), num_returns=1
        )
        if not ready:
            return None

        return ready[0]

    def add_actor(self, actor_request: ActorRequest):
        """Add actor to be managed by actor manager."""
        self._actor_requests.append(actor_request)
        self._resource_manager.request_resources(actor_request.resources)

    def cancel_actor_request(self, actor_request: ActorRequest):
        """Cancel actor request."""
        self._actor_requests.remove(actor_request)
        self._resource_manager.cancel_resource_request(actor_request.resources)

    def remove_actor(
        self,
        actor: ray.actor.ActorHandle,
        resolve_futures: bool = True,
        exception: Optional[Exception] = None,
    ):
        if resolve_futures and self._actors_to_futures[actor]:
            self._actors_to_remove[actor] = exception
            return

        # Clear futures
        self._clear_actor_futures(actor)

        # remove actor
        ray.kill(actor)

        info = self._actor_to_info.pop(actor)

        # Return resources
        self._resource_manager.return_resources(info.used_resource)

        self._next_events.append(
            ActorStopped(actor=actor, actor_info=info, exception=exception)
        )

    def _clear_actor_futures(self, actor: ray.actor.ActorHandle):
        # remove futures
        futures = self._actors_to_futures.pop(actor)
        for future in futures:
            self._futures_to_actors.pop(future)
            self._futures_to_classes.pop(future)

    def track_future(
        self,
        actor: ray.actor.ActorHandle,
        future: ray.ObjectRef,
        cls: Optional[Type] = None,
    ):
        self._actors_to_futures[actor].add(future)
        self._futures_to_actors[future] = actor
        self._futures_to_classes[future] = cls

    def track_sync_futures(
        self,
        actors_to_futures: Dict[ActorInfo, ray.ObjectRef],
        cls: Optional[Type] = None,
    ):
        raise NotImplementedError

    def _resolve_ready_future(self) -> ExecutionEvent:
        ready_future = self._ready_future
        self._ready_future = None

        # Todo: Handle resource ready futures here

        actor = self._futures_to_actors.pop(ready_future)
        cls = self._futures_to_classes.pop(ready_future) or NativeResult
        self._actors_to_futures[actor].remove(ready_future)

        try:
            raw_result = ray.get(ready_future)
            result = _convert_result(cls=cls, actor=actor, result=raw_result)
        except Exception as e:
            result = FutureFailed(actor=actor, exception=e)

        return result

    def _start_new_actors(self):
        new_actor_requests = []
        for actor_request in self._actor_requests:
            if self._resource_manager.has_resources_ready(actor_request.resources):
                ready_resource = self._resource_manager.acquire_resources(
                    actor_request.resources
                )
                remote_actor_cls = ray.remote(actor_request.cls)
                annotated_actor_cls = ready_resource.annotate_remote_objects(
                    [remote_actor_cls]
                )
                actor = annotated_actor_cls.remote(**actor_request.kwargs)
                actor_info = ActorInfo(
                    actor_request=actor_request, used_resource=ready_resource
                )
                self._actor_to_info[actor] = actor_info
                self._active_actors.add(actor)

                self._next_events.append(
                    ActorStarted(actor=actor, actor_info=actor_info)
                )
            else:
                new_actor_requests.append(actor_request)

        self._actor_requests = new_actor_requests

    def _remove_stale_actors(self):
        for actor, exception in list(self._actors_to_remove.items()):
            self._actors_to_remove.pop(actor)
            self.remove_actor(actor, resolve_futures=True, exception=exception)
