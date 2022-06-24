import gym
from typing import Any, List

from ray.rllib.connectors.connector import (
    Connector,
    ConnectorContext,
    ConnectorPipeline,
    AgentConnector,
    register_connector,
    get_connector,
)
from ray.rllib.connectors.agent.clip_reward import ClipRewardAgentConnector
from ray.rllib.connectors.agent.lambdas import FlattenDataAgentConnector
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.typing import (
    ActionConnectorDataType,
    AgentConnectorDataType,
    AlgorithmConfigDict,
)


@DeveloperAPI
class AgentConnectorPipeline(AgentConnector, ConnectorPipeline):
    def __init__(self, ctx: ConnectorContext, connectors: List[Connector]):
        super().__init__(ctx)
        self.connectors = connectors

    def is_training(self, is_training: bool):
        self.is_training = is_training
        for c in self.connectors:
            c.is_training(is_training)

    def reset(self, env_id: str):
        for c in self.connectors:
            c.reset(env_id)

    def on_policy_output(self, output: ActionConnectorDataType):
        for c in self.connectors:
            c.on_policy_output(output)

    def __call__(self, ac_data: AgentConnectorDataType) -> List[AgentConnectorDataType]:
        ret = [ac_data]
        for c in self.connectors:
            # Run the list of input data through the next agent connect,
            # and collect the list of output data.
            new_ret = []
            for d in ret:
                new_ret += c(d)
            ret = new_ret
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


# TODO(jungong) : finish this.
@DeveloperAPI
def get_agent_connectors_from_config(
    config: AlgorithmConfigDict, obs_space: gym.Space
) -> AgentConnectorPipeline:
    connectors = [FlattenDataAgentConnector()]

    if config["clip_rewards"] is True:
        connectors.append(ClipRewardAgentConnector(sign=True))
    elif type(config["clip_rewards"]) == float:
        connectors.append(ClipRewardAgentConnector(limit=abs(config["clip_rewards"])))

    return AgentConnectorPipeline(connectors)
