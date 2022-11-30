"""Registry of connector names for global access."""

import importlib
import sys

from typing import Any
from ray.tune.registry import RLLIB_CONNECTOR, _global_registry

from ray.util.annotations import PublicAPI
from ray.rllib.connectors.connector import Connector, ConnectorContext


ALL_CONNECTORS = set()


@PublicAPI(stability="alpha")
def register_connector(name: str, cls: Connector):
    """Register a connector for use with RLlib.

    Args:
        name: Name to register.
        cls: Callable that creates an env.
    """
    if _global_registry.contains(RLLIB_CONNECTOR, name):
        return

    if not issubclass(cls, Connector):
        raise TypeError("Can only register Connector type.", cls)

    # Record it in local registry in case we need to register everything
    # again in the global registry, for example in the event of cluster
    # restarts.
    ALL_CONNECTORS.add(cls.__module__)

    # Register in global registry.
    _global_registry.register(RLLIB_CONNECTOR, name, cls)


@PublicAPI(stability="alpha")
def get_connector(name: str, ctx: ConnectorContext, params: Any = None) -> Connector:
    # TODO(jungong) : switch the order of parameters man!!
    """Get a connector by its name and serialized config.

    Args:
        name: name of the connector.
        ctx: Connector context.
        params: serialized parameters of the connector.

    Returns:
        Constructed connector.
    """
    if not _global_registry.contains(RLLIB_CONNECTOR, name):
        raise NameError("connector not found.", name)
    return _global_registry.get(RLLIB_CONNECTOR, name).from_state(ctx, params)


def _register_all_connectors():
    """Force register all connectors again.

    In case the cluster has been restarted, and the global registry
    has to be rebuilt.
    """
    for module in ALL_CONNECTORS:
        if module in sys.modules:
            importlib.reload(sys.modules[module])
        else:
            importlib.import_module(module)
