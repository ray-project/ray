from typing import List

import ray
from ray._private.client_mode_hook import client_mode_hook
from ray._private.auto_init_hook import wrap_auto_init
from ray._private.services import get_node_instance_id, get_node_ip_address
from ray.util import iter
from ray.util import rpdb as pdb
from ray.util import debugpy as ray_debugpy
from ray.util.actor_pool import ActorPool
from ray.util import accelerators
from ray.util.annotations import PublicAPI
from ray.util.check_serialize import inspect_serializability
from ray.util.client_connect import connect, disconnect
from ray.util.debug import disable_log_once_globally, enable_periodic_logging, log_once
from ray.util.placement_group import (
    get_current_placement_group,
    get_placement_group,
    placement_group,
    placement_group_table,
    remove_placement_group,
)
from ray.util.serialization import deregister_serializer, register_serializer


@PublicAPI(stability="beta")
@wrap_auto_init
@client_mode_hook
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
    worker = ray._private.worker.global_worker
    worker.check_connected()

    actors = worker.core_worker.list_named_actors(all_namespaces)
    if all_namespaces:
        return [{"name": name, "namespace": namespace} for namespace, name in actors]
    else:
        return [name for _, name in actors]


class _ObjectRefWrapper:
    """Utility class to implement ray.util.pass_by_reference.

    When serialized, it wraps the ObjectRef in a tuple so it won't be passed by value.
    When deserialized, it resolves itself to the wrapped reference.

    *NOT* a public API.
    """

    def __init__(self, o: "ray.ObjectRef"):
        self._o = o

    def __reduce__(self):
        return lambda o: o, (self._o,)


@PublicAPI(stability="alpha")
def pass_by_reference(object_ref: "ray.ObjectRef") -> _ObjectRefWrapper:
    """Utility to pass the provided ObjectRef to another task by reference.

    Normally, when you pass an ObjectRef to a downstream task, it will be automatically
    resolved to its underlying value by Ray.

    When you pass the result of this function instead, it will be resolved to the
    ObjectRef directly.

    This is an advanced utility. In most cases, you should pass the ObjectRef directly.

    Args:
        object_ref: The ObjectRef to pass by reference.

    Example:

        .. code-block:: python

            import ray

            @ray.remote
            def f(obj_ref: ray.ObjectRef) -> str:
                # Normally, obj_ref would have been resolved to the string value,
                # but because we used `pass_by_reference`, it wasn't.
                return ray.get(obj_ref)

            obj_ref = ray.put("Hello!")
            assert ray.get(f.remote(ray.util.pass_by_reference(obj_ref))) == "Hello!"
    """
    return _ObjectRefWrapper(o)


__all__ = [
    "accelerators",
    "ActorPool",
    "disable_log_once_globally",
    "enable_periodic_logging",
    "iter",
    "log_once",
    "pdb",
    "placement_group",
    "placement_group_table",
    "get_placement_group",
    "get_current_placement_group",
    "get_node_instance_id",
    "get_node_ip_address",
    "remove_placement_group",
    "ray_debugpy",
    "inspect_serializability",
    "collective",
    "connect",
    "disconnect",
    "register_serializer",
    "deregister_serializer",
    "list_named_actors",
]
