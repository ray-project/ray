import asyncio
from asyncio.events import AbstractEventLoop
import random
from collections import defaultdict
from dataclasses import dataclass
from enum import Enum, auto
from typing import (Any, Optional, Tuple, Callable, DefaultDict, Dict, Set,
                    Union)

import ray
from ray.serve.utils import logger


class LongPollNamespace(Enum):
    def __repr__(self):
        return f"{self.__class__.__name__}.{self.name}"

    REPLICA_HANDLES = auto()
    TRAFFIC_POLICIES = auto()
    BACKEND_CONFIGS = auto()
    ROUTE_TABLE = auto()


@dataclass
class UpdatedObject:
    object_snapshot: Any
    # The identifier for the object's version. There is not sequential relation
    # among different object's snapshot_ids.
    snapshot_id: int


# Type signature for the update state callbacks. E.g.
# async def update_state(updated_object: Any):
#     do_something(updated_object)
UpdateStateCallable = Callable[[Any], None]
KeyType = Union[str, LongPollNamespace, Tuple[LongPollNamespace, str]]


class LongPollClient:
    """The asynchronous long polling client.

    Args:
        host_actor(ray.ActorHandle): handle to actor embedding LongPollHost.
        key_listeners(Dict[str, AsyncCallable]): a dictionary mapping keys to
          callbacks to be called on state update for the corresponding keys.
        call_in_event_loop(Optional[AbstractEventLoop]): an optional event loop
          to post the callback into. The callbacks were called within a cpp
          core worker thread if the event loop is not passed in.
    """

    def __init__(
            self,
            host_actor,
            key_listeners: Dict[KeyType, UpdateStateCallable],
            call_in_event_loop: Optional[AbstractEventLoop] = None,
    ) -> None:
        assert len(key_listeners) > 0

        self.host_actor = host_actor
        self.key_listeners = key_listeners
        self.event_loop = call_in_event_loop
        self._reset()

    def _reset(self):
        self.snapshot_ids: Dict[KeyType, int] = {
            key: -1
            for key in self.key_listeners.keys()
        }
        self.object_snapshots: Dict[KeyType, Any] = dict()

        self._current_ref = None
        self._poll_next()

    def update_key_listeners(
            self,
            key_listeners: Dict[KeyType, UpdateStateCallable],
    ):
        self.key_lisners = key_listeners
        self._reset()

    def _poll_next(self):
        """Poll the update. The callback is expected to scheduler another
        _poll_next call.
        """
        self._current_ref = self.host_actor.listen_for_change.remote(
            self.snapshot_ids)
        self._current_ref._on_completed(
            lambda update: self._process_update(update))

    def _process_update(self, updates: Dict[str, UpdatedObject]):
        if isinstance(updates, (ray.exceptions.RayActorError)):
            # This can happen during shutdown where the controller is
            # intentionally killed, the client should just gracefully
            # exit.
            logger.debug("LongPollClient failed to connect to host. "
                         "Shutting down.")
            return

        if isinstance(updates, (ray.exceptions.RayTaskError)):
            # This can happen during shutdown where the controller doesn't
            # contain this key, we will just repull.
            # NOTE(simon): should we repull or just wait in the long poll
            # host?
            if not isinstance(updates.as_instanceof_cause(), ValueError):
                logger.error("LongPollHost errored\n" + updates.traceback_str)
            self._poll_next()
            return

        # Before we process the updates and calling callbacks, kick off
        # another poll so we can pipeline the polling and processing.
        self._poll_next()
        logger.debug("LongPollClient received updates for keys: "
                     f"{list(updates.keys())}.")
        for key, update in updates.items():
            self.object_snapshots[key] = update.object_snapshot
            self.snapshot_ids[key] = update.snapshot_id
            callback = self.key_listeners[key]
            if self.event_loop is None:
                callback(update.object_snapshot)
            else:
                self.event_loop.call_soon_threadsafe(
                    lambda: callback(update.object_snapshot))


class LongPollHost:
    """The server side object that manages long pulling requests.

    The desired use case is to embed this in an Ray actor. Client will be
    expected to call actor.listen_for_change.remote(...). On the host side,
    you can call host.notify_changed(key, object) to update the state and
    potentially notify whoever is polling for these values.

    Internally, we use snapshot_ids for each object to identify client with
    outdated object and immediately return the result. If the client has the
    up-to-date verison, then the listen_for_change call will only return when
    the object is updated.
    """

    def __init__(self):
        # Map object_key -> int
        self.snapshot_ids: DefaultDict[KeyType, int] = defaultdict(
            lambda: random.randint(0, 1_000_000))
        # Map object_key -> object
        self.object_snapshots: Dict[KeyType, Any] = dict()
        # Map object_key -> set(asyncio.Event waiting for updates)
        self.notifier_events: DefaultDict[KeyType, Set[
            asyncio.Event]] = defaultdict(set)

    async def listen_for_change(
            self,
            keys_to_snapshot_ids: Dict[KeyType, int],
    ) -> Dict[KeyType, UpdatedObject]:
        """Listen for changed objects.

        This method will returns a dictionary of updated objects. It returns
        immediately if the snapshot_ids are outdated, otherwise it will block
        until there's one updates.
        """
        watched_keys = keys_to_snapshot_ids.keys()
        nonexistent_keys = set(watched_keys) - set(self.snapshot_ids.keys())
        if len(nonexistent_keys) > 0:
            raise ValueError(f"Keys not found: {nonexistent_keys}.")

        # 2. If there are any outdated keys (by comparing snapshot ids)
        #    return immediately.
        client_outdated_keys = {
            key: UpdatedObject(self.object_snapshots[key],
                               self.snapshot_ids[key])
            for key in watched_keys
            if self.snapshot_ids[key] != keys_to_snapshot_ids[key]
        }
        if len(client_outdated_keys) > 0:
            return client_outdated_keys

        # 3. Otherwise, register asyncio events to be waited.
        async_task_to_watched_keys = {}
        for key in watched_keys:
            # Create a new asyncio event for this key
            event = asyncio.Event()
            task = asyncio.get_event_loop().create_task(event.wait())
            async_task_to_watched_keys[task] = key

            # Make sure future caller of notify_changed will unblock this
            # asyncio Event.
            self.notifier_events[key].add(event)

        done, not_done = await asyncio.wait(
            async_task_to_watched_keys.keys(),
            return_when=asyncio.FIRST_COMPLETED)
        [task.cancel() for task in not_done]

        updated_object_key: str = async_task_to_watched_keys[done.pop()]
        return {
            updated_object_key: UpdatedObject(
                self.object_snapshots[updated_object_key],
                self.snapshot_ids[updated_object_key])
        }

    def notify_changed(
            self,
            object_key: KeyType,
            updated_object: Any,
    ):
        self.snapshot_ids[object_key] += 1
        self.object_snapshots[object_key] = updated_object
        logger.debug(f"LongPollHost: Notify change for key {object_key}.")

        if object_key in self.notifier_events:
            for event in self.notifier_events.pop(object_key):
                event.set()
