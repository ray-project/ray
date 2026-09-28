import time
from typing import Dict, Set, Tuple

import ray
import ray._private.internal_api
import ray._private.state
from ray.data._internal.execution.node_trackers.actor_location import (
    get_or_create_actor_location_tracker,
)
from ray.data._internal.utils.cache import timed_cache


@timed_cache(ttl=1)
def get_local_ongoing_lineage_reconstruction_tasks():
    # ResourceManager.update_usages() calls extra_resource_usage() on map
    # operators. Unit tests exercise that path without ray.init(); lineage
    # reconstruction can only exist with a live worker, so treat offline as empty.
    if not ray.is_initialized():
        return []
    return ray._private.internal_api.get_local_ongoing_lineage_reconstruction_tasks()


@timed_cache(ttl=1)
def get_draining_nodes() -> Dict[str, int]:
    return ray._private.state.state.get_draining_nodes()


@timed_cache(ttl=1)
def get_actor_locations(logical_actor_ids: Tuple[str, ...]) -> Dict[str, str]:
    """Get the actor locations from logical actor ids.
    NOTE: This function is not thread-safe"""
    if not logical_actor_ids:
        return {}
    return ray.get(
        get_or_create_actor_location_tracker().get_actor_locations.remote(
            logical_actor_ids
        )
    )


# If we submit a task immediately before the deadline, Ray Core might not have
# enough time to launch the task and fetch objects before the node is terminated.
# To avoid this, we stop using inputs on such nodes some time before the deadline.
DRAIN_DEADLINE_BUFFER_TIME_MS = 5000


def get_drained_nodes() -> Set[str]:
    """Returns the set of nodes that are draining and are past their deadline."""
    now_ms = time.time() * 1000
    return {
        node_id
        for node_id, deadline_ms in get_draining_nodes().items()
        if deadline_ms - DRAIN_DEADLINE_BUFFER_TIME_MS < now_ms
    }
