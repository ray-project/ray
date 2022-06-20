import gym
from typing import Any, List

from ray.rllib.connectors.connector import (
    ActionConnector,
    Connector,
    ConnectorContext,
    ConnectorPipeline,
    get_connector,
    register_connector,
)
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.typing import (
    ActionConnectorDataType,
    AlgorithmConfigDict,
)


@DeveloperAPI
class ActionConnectorPipeline(ActionConnector, ConnectorPipeline):
    def __init__(self, ctx: ConnectorContext, connectors: List[Connector]):
        super().__init__(ctx)
        self.connectors = connectors

    def is_training(self, is_training: bool):
        self.is_training = is_training
        for c in self.connectors:
            c.is_training(is_training)

    def __call__(self, ac_data: ActionConnectorDataType) -> ActionConnectorDataType:
        for c in self.connectors:
            ac_data = c(ac_data)
        return ac_data

    def to_config(self):
        return ActionConnectorPipeline.__name__, [
            c.to_config() for c in self.connectors
        ]

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        assert (
            type(params) == list
        ), "ActionConnectorPipeline takes a list of connector params."
        connectors = [get_connector(ctx, name, subparams) for name, subparams in params]
        return ActionConnectorPipeline(ctx, connectors)


register_connector(ActionConnectorPipeline.__name__, ActionConnectorPipeline)


@DeveloperAPI
def get_action_connectors_from_algorithm_config(
    config: AlgorithmConfigDict, action_space: gym.Space
) -> ActionConnectorPipeline:
    connectors = []
    return ActionConnectorPipeline(connectors)
