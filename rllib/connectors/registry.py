"""Registry of connector names for global access."""

from typing import Tuple, Type, TYPE_CHECKING, Union, Any

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
    if not _global_registry.contains(RLLIB_CONNECTOR, name):
        raise NameError("connector not found.", name)
    cls = _global_registry.get(RLLIB_CONNECTOR, name)
    return cls.from_state(ctx, params)


###### Action Connectors ######
from ray.rllib.connectors.action.clip import ClipActionsConnector
from ray.rllib.connectors.action.immutable import ImmutableActionsConnector
from ray.rllib.connectors.action.normalize import NormalizeActionsConnector
from ray.rllib.connectors.action.pipeline import ActionConnectorPipeline

register_connector(ClipActionsConnector.__name__, ClipActionsConnector)
register_connector(ImmutableActionsConnector.__name__, ImmutableActionsConnector)
register_connector(NormalizeActionsConnector.__name__, NormalizeActionsConnector)
register_connector(ActionConnectorPipeline.__name__, ActionConnectorPipeline)
register_connector(ActionConnectorPipeline.__name__, ActionConnectorPipeline)


###### Agent Connectors ######
from ray.rllib.connectors.agent.clip_reward import ClipRewardAgentConnector
from ray.rllib.connectors.agent.mean_std_filter import (
        MeanStdObservationFilterAgentConnector, ConcurrentMeanStdObservationFilterAgentConnector
)
from ray.rllib.connectors.agent.obs_preproc import ObsPreprocessorConnector
from ray.rllib.connectors.agent.pipeline import AgentConnectorPipeline
from ray.rllib.connectors.agent.state_buffer import StateBufferConnector
from ray.rllib.connectors.agent.view_requirement import ViewRequirementAgentConnector
from ray.rllib.connectors.agent.synced_filter import SyncedFilterAgentConnector

register_connector(ClipRewardAgentConnector.__name__, ClipRewardAgentConnector)
register_connector(
    MeanStdObservationFilterAgentConnector.__name__,
    MeanStdObservationFilterAgentConnector,
)
register_connector(
    ConcurrentMeanStdObservationFilterAgentConnector.__name__,
    ConcurrentMeanStdObservationFilterAgentConnector,
)
register_connector(ObsPreprocessorConnector.__name__, ObsPreprocessorConnector)
register_connector(AgentConnectorPipeline.__name__, AgentConnectorPipeline)
register_connector(StateBufferConnector.__name__, StateBufferConnector)
register_connector(ViewRequirementAgentConnector.__name__, ViewRequirementAgentConnector)
register_connector(SyncedFilterAgentConnector.__name__, SyncedFilterAgentConnector)

