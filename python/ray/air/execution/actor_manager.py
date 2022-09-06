import random
from collections import defaultdict, deque
from typing import List, Optional, Type, Dict, Set, Deque, Any, Tuple, Union, Iterable

import ray
from ray.air.execution.actor_request import ActorRequest, ActorInfo
from ray.air.execution.resources.resource_manager import ResourceManager
from ray.air.execution.event import (
    ExecutionEvent,
    FutureFailed,
    ActorStopped,
    ActorStarted,
    _ResourceReady,
    FutureResult,
    MultiFutureResult,
)


def _resolve_future(
    actor: ray.actor.ActorHandle,
    future: ray.ObjectRef,
    cls: Optional[Type[FutureResult]] = None,
) -> Any:
    return _resolve_many_futures(actors=[actor], futures=[future], cls=cls)[0]


def _resolve_many_futures(
    actors: List[ray.actor.ActorHandle],
    futures: List[ray.ObjectRef],
    cls: Optional[Type[FutureResult]] = None,
) -> List[Any]:
    try:
        raw_results = ray.get(futures)
        return [
            _convert_result(actor=actor, result=raw_result, cls=cls)
            for actor, raw_result in zip(actors, raw_results)
        ]
    except Exception as e:
        return [FutureFailed(actor=actor, exception=e) for actor in actors]


def _convert_result(
    actor: ray.actor.ActorHandle,
    result: Any,
    cls: Optional[Type[FutureResult]] = None,
):
    if not cls:
        return result

    n_args = len(getattr(cls, "__dataclass_fields__", 1))
    if n_args == 0:
        return cls()
    elif n_args == 1:
        return cls(actor)
    elif n_args == 2:
        return cls(actor, result)
    else:
        return cls(actor, *result)


class _FutureCollection:
    def __init__(self):
        self._actors_to_futures: Dict[
            ray.actor.ActorHandle, Set[ray.ObjectRef]
        ] = defaultdict(set)
        self._futures_to_actors: Dict[ray.ObjectRef, ray.actor.ActorHandle] = {}

    def has_actor(self, actor: ray.actor.ActorHandle) -> bool:
        return bool(self._actors_to_futures[actor])

    def get_actors_for_futures(self) -> List[ray.actor.ActorHandle]:
        futures = self._futures_to_actors.keys()
        return [self._futures_to_actors[future] for future in futures]

    def has_future(self, future: ray.ObjectRef) -> bool:
        return future in self._futures_to_actors

    def get_futures(self) -> List[ray.ObjectRef]:
        return list(self._futures_to_actors.keys())

    def pop_future(
        self, future: ray.ObjectRef
    ) -> Tuple[ray.actor.ActorHandle, Optional[Type[FutureResult]]]:
        actor = self._futures_to_actors.pop(future)
        cls = self.future_cls(future)
        self._actors_to_futures[actor].remove(future)

        return actor, cls

    def clear_future(self, future: ray.ObjectRef) -> None:
        actor = self._futures_to_actors.pop(future)
        self._actors_to_futures[actor].discard(future)

    def clear_actor(self, actor: ray.actor.ActorHandle) -> None:
        futures = self._actors_to_futures.pop(actor)
        for future in futures:
            self.clear_future(future)

    def track_future(
        self,
        actor: ray.actor.ActorHandle,
        future: ray.ObjectRef,
        cls: Optional[Type[FutureResult]] = None,
    ):
        self._actors_to_futures[actor].add(future)
        self._futures_to_actors[future] = actor

    def future_cls(self, future: ray.ObjectRef) -> Optional[FutureResult]:
        return None


class _AsyncFutureCollection(_FutureCollection):
    def __init__(self):
        super().__init__()
        self._futures_to_classes: Dict[ray.ObjectRef, Type[FutureResult]] = {}

    def pop_future(
        self, future: ray.ObjectRef
    ) -> Tuple[ray.actor.ActorHandle, Optional[Type[FutureResult]]]:
        actor, cls = super().pop_future(future)
        self._futures_to_classes.pop(future)
        return actor, cls

    def clear_future(self, future: ray.ObjectRef) -> None:
        super().clear_future(future)
        self._futures_to_classes.pop(future)

    def future_cls(self, future: ray.ObjectRef) -> Optional[FutureResult]:
        return self._futures_to_classes.get(future)

    def track_future(
        self,
        actor: ray.actor.ActorHandle,
        future: ray.ObjectRef,
        cls: Optional[Type[FutureResult]] = None,
    ):
        super().track_future(actor=actor, future=future, cls=cls)
        self._futures_to_classes[future] = cls


class _SyncFutureCollection(_FutureCollection):
    def __init__(self, future_cls: Optional[Type[FutureResult]]):
        super().__init__()
        self._future_cls = future_cls

    def future_cls(self, future: ray.ObjectRef) -> Optional[FutureResult]:
        return self._future_cls


class ActorManager:
    def __init__(self, resource_manager: ResourceManager):
        self._resource_manager: ResourceManager = resource_manager

        self._actor_requests: List[ActorRequest] = []
        self._active_actors: Set[ray.actor.ActorHandle] = set()
        self._actor_to_info: Dict[ray.actor.ActorHandle, ActorInfo] = {}

        self._async_futures = _AsyncFutureCollection()
        self._sync_futures: List[_SyncFutureCollection] = []

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
        tracked_futures = self._async_futures.get_futures()

        for sync_futures in self._sync_futures:
            tracked_futures += sync_futures.get_futures()

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
        if resolve_futures and (
            self._async_futures.has_actor(actor)
            or any(sync_futures.has_actor(actor) for sync_futures in self._sync_futures)
        ):
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
        self._async_futures.clear_actor(actor)
        for sync_futures in self._sync_futures:
            sync_futures.clear_actor(actor)

    def track_future(
        self,
        actor: ray.actor.ActorHandle,
        future: ray.ObjectRef,
        cls: Optional[Type] = None,
    ):
        self._async_futures.track_future(actor=actor, future=future, cls=cls)

    def track_sync_futures(
        self,
        actors_to_futures: Dict[
            ray.actor.ActorHandle, Union[ray.ObjectRef, Iterable[ray.ObjectRef]]
        ],
        cls: Optional[Type] = None,
    ):
        sync_futures = _SyncFutureCollection(future_cls=cls)
        for actor, futures in actors_to_futures.items():
            if isinstance(futures, ray.ObjectRef):
                futures = [futures]

            for future in futures:
                sync_futures.track_future(actor=actor, future=future)

        self._sync_futures.append(sync_futures)

    def _resolve_ready_future(self) -> ExecutionEvent:
        ready_future = self._ready_future
        self._ready_future = None

        # Todo: Handle resource ready futures here

        if self._async_futures.has_future(ready_future):
            actor, cls = self._async_futures.pop_future(ready_future)
            return _resolve_future(actor=actor, future=ready_future, cls=cls)

        # Else, this is a sync future
        new_sync_futures = []
        multi_result = None
        for sync_futures in self._sync_futures:
            # If this is not the collection with the future, keep
            if not sync_futures.has_future(ready_future):
                new_sync_futures.append(sync_futures)
                continue

            # Otherwise, fetch all actors and futures and resolve all at once
            actors = sync_futures.get_actors_for_futures()
            futures = sync_futures.get_futures()
            cls = sync_futures.future_cls(ready_future)

            results = _resolve_many_futures(actors=actors, futures=futures, cls=cls)
            multi_result = MultiFutureResult(results=results)

        self._sync_futures = new_sync_futures
        return multi_result

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
