"""Registry of connector names for global access."""

from ray.rllib.connectors import register_connector

# Action Connectors #
from ray.rllib.connectors.action.clip import ClipActionsConnector
from ray.rllib.connectors.action.immutable import ImmutableActionsConnector
from ray.rllib.connectors.action.normalize import NormalizeActionsConnector
from ray.rllib.connectors.action.pipeline import ActionConnectorPipeline

# Agent Connectors #
from ray.rllib.connectors.agent.clip_reward import ClipRewardAgentConnector
from ray.rllib.connectors.agent.mean_std_filter import (
    MeanStdObservationFilterAgentConnector,
    ConcurrentMeanStdObservationFilterAgentConnector,
)
from ray.rllib.connectors.agent.obs_preproc import ObsPreprocessorConnector
from ray.rllib.connectors.agent.pipeline import AgentConnectorPipeline
from ray.rllib.connectors.agent.state_buffer import StateBufferConnector
from ray.rllib.connectors.agent.view_requirement import ViewRequirementAgentConnector
from ray.rllib.connectors.agent.synced_filter import SyncedFilterAgentConnector

ACTION_CONNECTORS = {
    ClipActionsConnector,
    ImmutableActionsConnector,
    NormalizeActionsConnector,
    ActionConnectorPipeline,
}

AGENT_CONNECTORS = {
    ClipRewardAgentConnector,
    MeanStdObservationFilterAgentConnector,
    ConcurrentMeanStdObservationFilterAgentConnector,
    ObsPreprocessorConnector,
    AgentConnectorPipeline,
    StateBufferConnector,
    ViewRequirementAgentConnector,
    SyncedFilterAgentConnector,
}

def __register_connectors():
    for connector in ACTION_CONNECTORS | AGENT_CONNECTORS:
        register_connector(connector.__name__, connector)

__register_connectors()