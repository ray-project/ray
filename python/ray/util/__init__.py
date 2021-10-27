from typing import List

import ray
from ray._private.services import get_node_ip_address
from ray.util import iter
from ray.util.annotations import PublicAPI
from ray.util.actor_pool import ActorPool
from ray.util.check_serialize import inspect_serializability
from ray.util.debug import log_once, disable_log_once_globally, \
    enable_periodic_logging
from ray.util.placement_group import (placement_group, placement_group_table,
                                      remove_placement_group,
                                      get_placement_group)
from ray.util import rpdb as pdb
from ray.util.serialization import register_serializer, deregister_serializer

from ray.util.client_connect import connect, disconnect
from ray._private.client_mode_hook import client_mode_hook


@PublicAPI(stability="beta")
@client_mode_hook(auto_init=True)
def list_named_actors(all_namespaces: bool = False) -> List[str]:
    """List all named actors in the system.

    Actors must have been created with Actor.options(name="name").remote().
    This works for both detached & non-detached actors.

    By default, only actors in the current namespace will be returned
    and the returned entries will simply be their name.

    If `all_namespaces` is set to True, all actors in the cluster will be
    returned regardless of namespace, and the returned entries will be of the
    form {"namespace": namespace, "name": name}.
    """
    worker = ray.worker.global_worker
    worker.check_connected()

    actors = worker.core_worker.list_named_actors(all_namespaces)
    if all_namespaces:
        return [{
            "name": name,
            "namespace": namespace
        } for namespace, name in actors]
    else:
        return [name for _, name in actors]


__all__ = [
    "ActorPool",
    "disable_log_once_globally",
    "enable_periodic_logging",
    "iter",
    "log_once",
    "pdb",
    "placement_group",
    "placement_group_table",
    "get_placement_group",
    "get_node_ip_address",
    "remove_placement_group",
    "inspect_serializability",
    "collective",
    "connect",
    "disconnect",
    "register_serializer",
    "deregister_serializer",
    "list_named_actors",
]
