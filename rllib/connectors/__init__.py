from typing import Tuple, Any
from ray.tune.registry import RLLIB_CONNECTOR, _global_registry

from ray.util.annotations import PublicAPI
from ray.rllib.connectors.connector import Connector, ConnectorContext


@PublicAPI(stability="alpha")
def register_connector(name: str, cls: Connector):
    """Register a connector for use with RLlib.

    Args:
        name: Name to register.
        cls: Callable that creates an env.
    """
    if not issubclass(cls, Connector):
        raise TypeError("Can only register Connector type.", cls)
    _global_registry.register(RLLIB_CONNECTOR, name, cls)


@PublicAPI(stability="alpha")
def get_connector(ctx: ConnectorContext, name: str, params: Tuple[Any]) -> Connector:
    """Get a connector by its name and serialized config.

    Args:
        name: name of the connector.
        params: serialized parameters of the connector.

    Returns:
        Constructed connector.
    """
    breakpoint()
    if not _global_registry.contains(RLLIB_CONNECTOR, name):
        raise NameError("connector not found.", name)
    cls = _global_registry.get(RLLIB_CONNECTOR, name)
    return cls.from_state(ctx, params)
