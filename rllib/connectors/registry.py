"""Registry of connector names for global access."""
from typing import Any

from ray.rllib.connectors.connector import Connector, ConnectorContext
from ray.rllib.utils.annotations import OldAPIStack

ALL_CONNECTORS = dict()


@OldAPIStack
def register_connector(name: str, cls: Connector):
    """Register a connector for use with RLlib.

    Args:
        name: Name to register.
        cls: Callable that creates an env.
    """
    if name in ALL_CONNECTORS:
        return

    if not issubclass(cls, Connector):
        raise TypeError("Can only register Connector type.", cls)

    # Record it in local registry in case we need to register everything
    # again in the global registry, for example in the event of cluster
    # restarts.
    ALL_CONNECTORS[name] = cls


@OldAPIStack
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
    if name not in ALL_CONNECTORS:
        raise NameError("connector not found.", name)
    return ALL_CONNECTORS[name].from_state(ctx, params)
