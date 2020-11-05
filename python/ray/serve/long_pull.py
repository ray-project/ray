from collections import defaultdict
import random
from time import sleep
from typing import Any, DefaultDict, Dict, Set, Tuple
import asyncio

import ray


class LongPullerClient:
    def __init__(self, host_actor):
        self.epoch_ids = defaultdict(lambda: random.randint(0, 1_000_000))
        self.in_flight_requests: Dict[str, ray.ObjectRef] = dict()
        self.object_snapshots: Dict[str, Any] = dict()
        self.host_actor = host_actor

    def get_object_snapshot(self, object_key):
        if object_key not in self.in_flight_requests:
            epoch_id = self.epoch_ids[object_key]
            ref = self.host_actor.listen_on_changed.remote(
                object_key, epoch_id)
            self.in_flight_requests[object_key] = ref

        if object_key not in self.object_snapshots:
            ray.wait([self.in_flight_requests[object_key]], num_returns=1)

        self.resolve_futures()
        return self.object_snapshots[object_key]

    def resolve_futures(self):
        refs_to_keys = {v: k for k, v in self.in_flight_requests.items()}
        ready_futures, _ = ray.wait(list(refs_to_keys.keys()), timeout=0)
        ready_results = ray.get(ready_futures)
        for ref, (object_snapshot, epoch_id) in zip(ready_futures,
                                                    ready_results):
            key = refs_to_keys[ref]
            self.in_flight_requests.pop(key)
            self.object_snapshots[key] = object_snapshot
            self.epoch_ids[key] = epoch_id


class LongPullerHost:
    def __init__(self):
        self.epoch_ids = defaultdict(lambda: random.randint(0, 1_000_000))
        self.object_snapshots: Dict[str, Any] = dict()
        self.notifier_events: DefaultDict[str, Set[
            asyncio.Event]] = defaultdict(set)

    async def listen_on_changed(self, object_key: str,
                                epoch_id: int) -> Tuple[Any, int]:
        if epoch_id == self.epoch_ids[object_key]:
            event = asyncio.Event()
            self.notifier_events[object_key].add(event)
            await event.wait()
            assert epoch_id != self.epoch_ids[object_key]
        return self.object_snapshots[object_key], self.epoch_ids[object_key]

    def notify_on_changed(self, object_key: str, updated_object: Any):
        self.epoch_ids[object_key] += 1
        self.object_snapshots[object_key] = updated_object

        if object_key in self.notifier_events:
            for event in self.notifier_events.pop(object_key):
                event.set()


if __name__ == "__main__":
    ray.init()

    host = ray.remote(LongPullerHost).remote()
    client = LongPullerClient(host)

    ray.get(host.notify_on_changed.remote("obj", 1))
    assert client.get_object_snapshot("obj") == 1

    host.notify_on_changed.remote("obj", 2)
    tries = set()
    for _ in range(3):
        tries.add(client.get_object_snapshot("obj") == 2)
        sleep(0.2)
    assert True in tries