import threading
from typing import List

import ray


@ray.remote(num_cpus=0, max_restarts=-1, max_task_retries=-1)
class ActorLocationTracker:
    def __init__(self):
        self._actor_locations = {}
        self._actor_locations_lock = threading.Lock()

    def update_actor_location(self, logical_actor_id: str, node_id: str):
        with self._actor_locations_lock:
            self._actor_locations[logical_actor_id] = node_id

    def get_actor_locations(self, logical_actor_ids: List[str]):
        return {
            logical_actor_id: self._actor_locations.get(logical_actor_id, None)
            for logical_actor_id in logical_actor_ids
        }
