import ray
from .actor_location import ActorLocationTracker
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


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


__all__ = ["get_or_create_actor_location_tracker", "ActorLocationTracker"]
