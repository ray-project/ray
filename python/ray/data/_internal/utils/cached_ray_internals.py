from typing import Dict, Tuple

import ray
import ray._private.internal_api
import ray._private.state
from ray.data._internal.execution.node_trackers.actor_location import (
    get_or_create_actor_location_tracker,
)
from ray.data._internal.utils.cache import timed_cache


@timed_cache(ttl=60)
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
    return ray.get(
        get_or_create_actor_location_tracker().get_actor_locations.remote(
            logical_actor_ids
        )
    )
