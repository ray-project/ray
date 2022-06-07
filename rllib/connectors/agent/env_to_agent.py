from typing import Any, List

from ray.rllib.connectors.connector import (
    ConnectorContext,
    AgentConnector,
    register_connector,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.typing import AgentConnectorDataType


@DeveloperAPI
class EnvToAgentDataConnector(AgentConnector):
    """Converts per environment multi-agent obs into per agent SampleBatches."""

    def __init__(self, ctx: ConnectorContext):
        super().__init__(ctx)
        self._view_requirements = ctx.view_requirements

    def __call__(self, ac_data: AgentConnectorDataType) -> List[AgentConnectorDataType]:
        if ac_data.agent_id:
            # data is already for a single agent.
            return [ac_data]

        assert isinstance(ac_data.data, (tuple, list)) and len(ac_data.data) == 5, (
            "EnvToPerAgentDataConnector expects a tuple of "
            + "(obs, rewards, dones, infos, episode_infos)."
        )
        # episode_infos contains additional training related data bits
        # for each agent, such as SampleBatch.T, SampleBatch.AGENT_INDEX,
        # SampleBatch.ACTIONS, SampleBatch.DONES (if hitting horizon),
        # and is usually empty in inference mode.
        obs, rewards, dones, infos, training_episode_infos = ac_data.data
        for var, name in zip(
            (obs, rewards, dones, infos, training_episode_infos),
            ("obs", "rewards", "dones", "infos", "training_episode_infos"),
        ):
            assert isinstance(var, dict), (
                f"EnvToPerAgentDataConnector expects {name} "
                + "to be a MultiAgentDict."
            )

        env_id = ac_data.env_id
        per_agent_data = []
        for agent_id, obs in obs.items():
            input_dict = {
                SampleBatch.ENV_ID: env_id,
                SampleBatch.REWARDS: rewards[agent_id],
                # SampleBatch.DONES may be overridden by data from
                # training_episode_infos next.
                SampleBatch.DONES: dones[agent_id],
                SampleBatch.NEXT_OBS: obs,
            }
            if SampleBatch.INFOS in self._view_requirements:
                input_dict[SampleBatch.INFOS] = infos[agent_id]
            if agent_id in training_episode_infos:
                input_dict.update(training_episode_infos[agent_id])

            per_agent_data.append(AgentConnectorDataType(env_id, agent_id, input_dict))

        return per_agent_data

    def to_config(self):
        return EnvToAgentDataConnector.__name__, None

    @staticmethod
    def from_config(ctx: ConnectorContext, params: List[Any]):
        return EnvToAgentDataConnector(ctx)


register_connector(EnvToAgentDataConnector.__name__, EnvToAgentDataConnector)
