import asyncio
from asyncio.tasks import Task
import random
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, DefaultDict, Dict, Generic, Set, Tuple, List, TypeVar, Optional

import ray
from ray.serve.utils import logger


@dataclass
class UpdatedObject:
    object_snapshot: Any
    # The identifier for the object's version. There is not sequential relation
    # among different object's snapshot_ids.
    snapshot_id: int


# Type signature for the update state callbacks. E.g.
# def update_state(updated_objects: Dict[str, Any], updated_keys: List[str]):
#     if "my_key" in updated_keys: do_something(updated_objects["my_key"])
UpdateStateCallable = Callable[[Dict[str, Any], List[str]], None]
UpdateStateAsyncCallable = Callable[[Dict[str, Any], List[str]], Awaitable[
    None]]


class BaseClient:
    """Shared functionality between LongPollerSyncClient and AsyncClient.
       This class is not expected to use this directly.
    """

    def __init__(self, host_actor, keys: List[str]) -> None:
        self.host_actor = host_actor
        self.snapshot_ids: Dict[str, int] = {key: -1 for key in keys}
        self.object_snapshots: Dict[str, Any] = dict()

    def _pull_once(self) -> ray.ObjectRef:
        object_ref = self.host_actor.listen_for_change.remote(
            self.snapshot_ids)
        return object_ref

    def _update(self, updates: Dict[str, UpdatedObject]):
        for key, update in updates.items():
            self.object_snapshots[key] = update.object_snapshot
            self.snapshot_ids[key] = update.snapshot_id

    def get_object_snapshot(self, object_key: str) -> Any:
        return self.object_snapshots[object_key]


class LongPollerSyncClient(BaseClient):
    """The synchronous long polling client.

    Internally, it uses ray.wait(timeout=0) to check if the object notification
    has arrived. When a object notification arrived, the client will invoke
    callback if supplied.
    """

    def __init__(self,
                 host_actor,
                 keys: List[str],
                 callback: Optional[UpdateStateCallable] = None) -> None:
        super().__init__(host_actor, keys)
        self.in_flight_request_ref: ray.ObjectRef = self._pull_once()
        self.callback = callback
        self.refresh(block=True)

    def refresh(self, block=False):
        """Check the inflight request once and update internal state.

        Args:
            block(bool): If block=True, then calls ray.get to wait and ensure
              the object notificaiton is received and updated.
        """
        done, _ = ray.wait([self.in_flight_request_ref], timeout=0)
        if len(done) == 1 or block:
            updates = ray.get(self.in_flight_request_ref)
            self._update(updates)
            if self.callback:
                self.callback(self.object_snapshots, list(updates.keys()))
        self.in_flight_request_ref = self._pull_once()

    def get_object_snapshot(self, object_key: str) -> Any:
        # NOTE(simon): Performing one ray.wait on get still has too high
        # overhead. Consider a batch submission scenario and how to amortize
        # the cost.
        self.refresh()
        return self.object_snapshots[object_key]


class LongPollerAsyncClient(BaseClient):
    """The asynchronous long polling client.

    Internally, it runs `await object_ref` in a `while True` loop. When a
    object notification arrived, the client will invoke callback if supplied.
    Note that this client will wait the callback to be completed before issuing
    the next poll.
    """

    def __init__(self,
                 host_actor,
                 keys: List[str],
                 callback: Optional[UpdateStateAsyncCallable] = None) -> None:
        assert asyncio.get_event_loop(
        ).is_running, "The client is only available in async context."
        super().__init__(host_actor, keys)
        asyncio.get_event_loop().create_task(self._do_long_poll())
        self.callback = callback

    async def _do_long_poll(self):
        while True:
            updates = await self._pull_once()
            self._update(updates)
            if self.callback:
                await self.callback(self.object_snapshots,
                                    list(updates.keys()))


class LongPollerHost:
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
        self.snapshot_ids: DefaultDict[str, int] = defaultdict(
            lambda: random.randint(0, 1_000_000))
        # Map object_key -> object
        self.object_snapshots: Dict[str, Any] = dict()
        # Map object_key -> set(asyncio.Event waiting for updates)
        self.notifier_events: DefaultDict[str, Set[
            asyncio.Event]] = defaultdict(set)

    async def listen_for_change(self, keys_to_snapshot_ids: Dict[str, int]
                                ) -> Dict[str, UpdatedObject]:
        """Listen for changed objects.

        This method will returns a dictionary of updated objects. It returns
        immediately if the snapshot_ids are outdated, otherwise it will block
        until there's one updates.
        """
        # 1. Figure out which keys do we care about
        watched_keys = set(self.snapshot_ids.keys()).intersection(
            keys_to_snapshot_ids.keys())
        if len(watched_keys) == 0:
            raise ValueError("Keys not found.")

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

    def notify_changed(self, object_key: str, updated_object: Any):
        self.snapshot_ids[object_key] += 1
        self.object_snapshots[object_key] = updated_object
        logger.debug(
            f"LongPollerHost: Updated object_snapshot to {self.object_snapshots}"
        )

        if object_key in self.notifier_events:
            for event in self.notifier_events.pop(object_key):
                event.set()
