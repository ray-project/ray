from typing import Any, List

from ray.rllib.connectors.connector import (
    AgentConnector,
    Connector,
    ConnectorContext,
    ConnectorPipeline,
    get_connector,
    register_connector,
)
from ray.rllib.utils.typing import ActionConnectorDataType, AgentConnectorDataType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class AgentConnectorPipeline(ConnectorPipeline, AgentConnector):
    def __init__(self, ctx: ConnectorContext, connectors: List[Connector]):
        super().__init__(ctx)
        self.connectors = connectors

    def is_training(self, is_training: bool):
        self._is_training = is_training
        for c in self.connectors:
            c.is_training(is_training)

    def reset(self, env_id: str):
        for c in self.connectors:
            c.reset(env_id)

    def on_policy_output(self, output: ActionConnectorDataType):
        for c in self.connectors:
            c.on_policy_output(output)

    def __call__(
        self, acd_list: List[AgentConnectorDataType]
    ) -> List[AgentConnectorDataType]:
        ret = acd_list
        for c in self.connectors:
            ret = c(ret)
        return ret

    def to_config(self):
        return AgentConnectorPipeline.__name__, [c.to_config() for c in self.connectors]

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        assert (
            type(params) == list
        ), "AgentConnectorPipeline takes a list of connector params."
        connectors = [get_connector(ctx, name, subparams) for name, subparams in params]
        return AgentConnectorPipeline(ctx, connectors)


register_connector(AgentConnectorPipeline.__name__, AgentConnectorPipeline)
