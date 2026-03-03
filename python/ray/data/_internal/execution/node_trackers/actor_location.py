import threading
from typing import List

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


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


def get_or_create_actor_location_tracker():

    # Pin the actor location tracker to the local node so it fate-shares with the driver.
    # NOTE: for Ray Client, the ray.get_runtime_context().get_node_id() should
    # point to the head node.
    scheduling_strategy = NodeAffinitySchedulingStrategy(
        ray.get_runtime_context().get_node_id(),
        soft=False,
    )
    return ActorLocationTracker.options(
        name="ActorLocationTracker",
        namespace="ActorLocationTracker",
        get_if_exists=True,
        lifetime="detached",
        scheduling_strategy=scheduling_strategy,
        max_concurrency=8,
    ).remote()
