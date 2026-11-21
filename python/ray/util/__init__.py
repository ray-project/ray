import importlib
import inspect
import types
from typing import Dict, List, Union

import ray
from ray._private.auto_init_hook import wrap_auto_init
from ray._private.client_mode_hook import client_mode_hook
from ray._private.services import get_node_instance_id, get_node_ip_address
from ray.util import (
    accelerators,
    debugpy as ray_debugpy,
    iter,
    placement_group as _placement_group_module,
    rpdb as pdb,
)
from ray.util.actor_pool import ActorPool
from ray.util.annotations import PublicAPI
from ray.util.check_serialize import inspect_serializability
from ray.util.client_connect import connect, disconnect
from ray.util.debug import disable_log_once_globally, enable_periodic_logging, log_once
from ray.util.helpers import as_completed, map_unordered
from ray.util.placement_group import (
    get_current_placement_group,
    get_placement_group,
    placement_group_table,
    remove_placement_group,
)
from ray.util.serialization import deregister_serializer, register_serializer


class _CallablePlacementGroupModule(types.ModuleType):
    """Module subclass for ``ray.util.placement_group`` that remains callable.

    Binding ``ray.util.placement_group`` to the ``placement_group`` function
    shadows the ``ray.util.placement_group`` submodule, breaking attribute
    lookups like ``ray.util.placement_group.PlacementGroup`` and
    ``unittest.mock.patch("ray.util.placement_group.placement_group")``.
    Making the module object itself callable preserves backwards compatibility
    for ``ray.util.placement_group(...)`` while exposing all submodule symbols.
    """

    def __call__(self, *args, **kwargs):
        return self.placement_group(*args, **kwargs)

    @property
    def __signature__(self):
        return inspect.signature(self.placement_group)

    def __reduce__(self):
        return (importlib.import_module, (self.__name__,))


if not isinstance(_placement_group_module, _CallablePlacementGroupModule):
    _placement_group_module.__class__ = _CallablePlacementGroupModule

if _placement_group_module.__doc__ is None:
    _placement_group_module.__doc__ = _placement_group_module.placement_group.__doc__

placement_group = _placement_group_module


@PublicAPI(stability="beta")
@wrap_auto_init
@client_mode_hook
def list_named_actors(
    all_namespaces: bool = False,
) -> Union[List[str], List[Dict[str, str]]]:
    """List all named actors in the system.

    Actors must have been created with Actor.options(name="name").remote().
    This works for both detached & non-detached actors.

    By default, only actors in the current namespace will be returned
    and the returned entries will simply be their name.

    If `all_namespaces` is set to True, all actors in the cluster will be
    returned regardless of namespace, and the returned entries will be of the
    form {"namespace": namespace, "name": name}.

    Args:
        all_namespaces: Whether to list named actors in all namespaces instead
            of only the current one.

    Returns:
        A list of actor names, or, if ``all_namespaces`` is True, a list of
        dicts with ``"name"`` and ``"namespace"`` keys.
    """
    worker = ray._private.worker.global_worker
    worker.check_connected()

    actors = worker.core_worker.list_named_actors(all_namespaces)
    if all_namespaces:
        return [{"name": name, "namespace": namespace} for namespace, name in actors]
    else:
        return [name for _, name in actors]


__all__ = [
    "accelerators",
    "ActorPool",
    "as_completed",
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
    "map_unordered",
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
