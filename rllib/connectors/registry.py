"""Registry of connector names for global access."""

import importlib

from typing import Any, Type
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
def get_registered_connector_class(name: str) -> Type[Connector]:
    """Get a connector class by its registered name.

    Args:
        name: name of the connector.

    Returns:
        Connector class type.
    """
    if not _global_registry.contains(RLLIB_CONNECTOR, name):
        raise NameError("connector not found.", name)
    return _global_registry.get(RLLIB_CONNECTOR, name)


@PublicAPI(stability="alpha")
def get_connector(name: str, ctx: ConnectorContext, params: Any = None) -> Connector:
    """Get a connector by its name and serialized config.

    Args:
        name: name of the connector.
        ctx: Connector context.
        params: serialized parameters of the connector.

    Returns:
        Constructed connector.
    """
    cls = get_registered_connector_class(name)
    return cls.from_state(ctx, params)


ACTION_CONNECTORS = {
    "clip.ClipActionsConnector",
    "immutable.ImmutableActionsConnector",
    "normalize.NormalizeActionsConnector",
    "pipeline.ActionConnectorPipeline",
    "lambdas.ConvertToNumpyConnector",
}

AGENT_CONNECTORS = {
    "clip_reward.ClipRewardAgentConnector",
    "mean_std_filter.MeanStdObservationFilterAgentConnector",
    "mean_std_filter.ConcurrentMeanStdObservationFilterAgentConnector",
    "obs_preproc.ObsPreprocessorConnector",
    "pipeline.AgentConnectorPipeline",
    "state_buffer.StateBufferConnector",
    "view_requirement.ViewRequirementAgentConnector",
    "synced_filter.SyncedFilterAgentConnector",
    "lambdas.FlattenDataAgentConnector",
}


def _register_all_connectors():
    for connector in ACTION_CONNECTORS | AGENT_CONNECTORS:
        prefix = "action" if connector in ACTION_CONNECTORS else "agent"
        full_connector_path = f"ray.rllib.connectors.{prefix}.{connector}"

        # Dynamically import the connector
        module_name, module_class = full_connector_path.rsplit(".", 1)
        module = importlib.import_module(module_name)
        connector_class = getattr(module, module_class)
        register_connector(connector_class.__name__, connector_class)


_register_all_connectors()
