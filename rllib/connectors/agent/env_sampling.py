from typing import Any

from ray.rllib.connectors.connector import (
    AgentConnector,
    ConnectorContext,
)
from ray.rllib.connectors.registry import register_connector
from ray.rllib.utils.typing import AgentConnectorDataType
from ray.rllib.utils.annotations import OldAPIStack


@OldAPIStack
class EnvSamplingAgentConnector(AgentConnector):
    def __init__(self, ctx: ConnectorContext, sign=False, limit=None):
        super().__init__(ctx)
        self.observation_space = ctx.observation_space

    def transform(self, ac_data: AgentConnectorDataType) -> AgentConnectorDataType:
        # EnvSamplingAgentConnector is a no-op connector.
        return ac_data

    def to_state(self):
        return EnvSamplingAgentConnector.__name__, {}

    @staticmethod
    def from_state(ctx: ConnectorContext, params: Any):
        return EnvSamplingAgentConnector(ctx, **params)


register_connector(EnvSamplingAgentConnector.__name__, EnvSamplingAgentConnector)
