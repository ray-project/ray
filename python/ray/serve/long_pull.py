import asyncio
import random
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, DefaultDict, Dict, Generic, Set, Tuple, List, TypeVar, Optional

import ray
from ray.serve.utils import logger


@dataclass
class UpdatedObject:
    object_snapshot: Any
    snapshot_id: int


class BaseClient:
    def __init__(self,
                 host_actor,
                 keys: List[str],
                 callback: Optional[Callable] = None) -> None:
        self.host_actor = host_actor
        self.keys = keys
        self.snapshot_ids: DefaultDict[str, int] = defaultdict(lambda: -1)
        self.object_snapshots: Dict[str, Any] = dict()
        self.callback = callback

        # Perform one blocking update
        self._update(ray.get(self._pull_once()))

    def _pull_once(self) -> ray.ObjectRef:
        key_to_snapshot_ids = {k: self.snapshot_ids[k] for k in self.keys}
        object_ref = self.host_actor.listen_on_changed.remote(
            key_to_snapshot_ids)
        return object_ref

    def _update(self, updates: Dict[str, UpdatedObject]):
        for key, update in updates.items():
            self.object_snapshots[key] = update.object_snapshot
            self.snapshot_ids[key] = update.snapshot_id
        if self.callback:
            self.callback(self.object_snapshots, list(updates.keys()))

    def get_object_snapshot(self, object_key: str) -> Any:
        return self.object_snapshots[object_key]


class LongPullerSyncClient(BaseClient):
    def __init__(self,
                 host_actor,
                 keys: List[str],
                 callback: Optional[Callable] = None) -> None:
        super().__init__(host_actor, keys, callback)
        self.in_flight_request_ref: ray.ObjectRef = self._pull_once()

    def refresh(self, block=False):
        done, _ = ray.wait([self.in_flight_request_ref], timeout=0)
        if len(done) == 1 or block:
            self._update(ray.get(self.in_flight_request_ref))
        self.in_flight_request_ref = self._pull_once()

    def get_object_snapshot(self, object_key: str) -> Any:
        # NOTE(simon): Performing one ray.wait on get still has too high
        # overhead. Consider a batch submission scenario.
        self.refresh()
        return self.object_snapshots[object_key]


class LongPullerAsyncClient(BaseClient):
    def __init__(self,
                 host_actor,
                 keys: List[str],
                 callback: Optional[Callable] = None) -> None:
        assert asyncio.get_event_loop().is_running
        super().__init__(host_actor, keys, callback)
        asyncio.get_event_loop().create_task(self._do_long_pull())

    async def _do_long_pull(self):
        while True:
            updates = await self._pull_once()
            self._update(updates)


class LongPullerHost:
    """The server side object that manages long pulling requests."""

    def __init__(self):
        # Map object_key -> int
        self.snapshot_ids: DefaultDict[str, int] = defaultdict(
            lambda: random.randint(0, 1_000_000))
        # Map object_key -> object
        self.object_snapshots: Dict[str, Any] = dict()
        # Map object_key -> set(asyncio.Event waiting for updates)
        self.notifier_events: DefaultDict[str, Set[
            asyncio.Event]] = defaultdict(set)

    async def listen_on_changed(self, keys_to_snapshot_ids: Dict[str, int]
                                ) -> Dict[str, UpdatedObject]:
        # 1. Figure out which keys do we care about, and whether any of those are outdated
        watched_keys = set(self.snapshot_ids.keys()).intersection(
            keys_to_snapshot_ids.keys())
        if len(watched_keys) == 0:
            raise ValueError("Keys not found.")

        client_outdated_keys = {
            key: UpdatedObject(self.object_snapshots[key],
                               self.snapshot_ids[key])
            for key in watched_keys
            if self.snapshot_ids[key] != keys_to_snapshot_ids[key]
        }
        if len(client_outdated_keys) > 0:
            return client_outdated_keys

        to_be_awaited = set()
        task_to_key = {}
        for key in watched_keys:
            event = asyncio.Event()
            task = asyncio.get_event_loop().create_task(event.wait())
            self.notifier_events[key].add(event)
            task_to_key[task] = key
            to_be_awaited.add(task)
        done, _ = await asyncio.wait(
            to_be_awaited, return_when=asyncio.FIRST_COMPLETED)
        updated_object_key: str = task_to_key[done.pop()]
        return {
            updated_object_key: UpdatedObject(
                self.object_snapshots[updated_object_key],
                self.snapshot_ids[updated_object_key])
        }

    def notify_on_changed(self, object_key: str, updated_object: Any):
        self.snapshot_ids[object_key] += 1
        self.object_snapshots[object_key] = updated_object
        logger.error(
            f"LongPullerHost: Updated object_snapshot to {self.object_snapshots}"
        )

        if object_key in self.notifier_events:
            for event in self.notifier_events.pop(object_key):
                event.set()
