from typing import Any, List, Optional, Sequence

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.numpy import flatten_inputs_to_1d_tensor
from ray.rllib.utils.typing import AgentID, EpisodeType


class InterAgentRewardShaping(ConnectorV2):
    """Example multi-agent connector piece that shapes rewards in inter-agent fashion.

    Works for multi-agent environments in which there are exactly 2 agents
    (e.g. "ag1" and "ag2"), each one receiving "normal" single-agent rewards that can be
    used to optimize each agent's own (selfish) behavior. However, if the two agents
    want to further optimize their behavior, they'll have to work together. In order to
    push them to do so, this connector adds a percentage of "ag1"'s (original,
    single-agent) reward to "ag2"'s reward and vice-versa.

    Exact agent IDs and percentages for both agents are configurable.

    - Works only with a list of MultiAgentEpisodes.
    - Extracts most recent reward information from the running episodes, computes
    new rewards for each agent, and writes these new rewards to the batch.
    - Best suited for LearnerPipelines due to the fact that usually, rewards are only
    needed for learning.
    - However, should you require rewards alredy for action computations (on the
    EnvRunner side), you can also use this connector in an EnvToModule pipeline, then
    use another connector piece to write the changed rewards back into the episodes,
    which will release you then from doing this reward shaping in the LearnerPipeline
    again (b/c the information has already been written permanently into the episodes).
    - Does NOT write any rewards (or other data) to the given Episode objects. If you
    would like to permanently write rewards back into the episodes, you will have to
    add another connector piece that performs this action.

    .. testcode::

        TODO
    """

    def __init__(
        self,
        input_observation_space,
        input_action_space,
        *,
        agent_ids: Sequence[AgentID],
        pct_ag0_reward_for_ag1: float = 0.0,
        pct_ag1_reward_for_ag0: float = 0.0,
        **kwargs,
    ):
        """Initializes a FlattenObservations instance.

        Args:
            agent_ids: A sequence of exactly two AgentIDs, who are cooperating inside
                a multi-agent env. Both agents are assumed to normally only receive
                a "selfish" single-agent reward signal from the environment.
                In order for the agents' RLModules (policies) to learn a more
                productive cooperative behavior, you should set the
                `pct_ag0_reward_for_ag1` and `pct_ag1_reward_for_ag0` to values > 0.0
                such that each agent also receives a part of the other agent's rewards
                at each timestep.
            pct_ag0_reward_for_ag1: The percentage of the first agent's
                (selfish/single-agent) reward that should be added to the second agent's
                (selfish/single-agent) reward.
            pct_ag1_reward_for_ag0: The percentage of the second agent's
                (selfish/single-agent) reward that should be added to the first agent's
                (selfish/single-agent) reward.
        """
        super().__init__(input_observation_space, input_action_space, **kwargs)

        self._agent_ids = agent_ids
        self._pct_ag0_reward_for_ag1 = pct_ag0_reward_for_ag1
        self._pct_ag1_reward_for_ag0 = pct_ag1_reward_for_ag0

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Optional[Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:

        # Loop through all MultiAgentEpisodes and poll the most recent rewards from
        # them.
        for ma_episode in episodes:
            # Assuming that both agents always step at the same time.
            rewards = ma_episode.get_rewards(-1, agent_ids=self._agent_ids)

            # Compute the first agent's shaped reward.
            ag0_shaped_r = (
                rewards[self._agent_ids[0]]
                + self._pct_ag1_reward_for_ag0 * rewards[self._agent_ids[1]]
            )
            # Add it to the batch.
            self.add_batch_item(
                batch=data,
                column=SampleBatch.REWARDS,
                item_to_add=ag0_shaped_r,
                single_agent_episode=ma_episode.agent_episodes[self._agent_ids[0]],
            )

            # Compute the second agent's shaped reward.
            ag1_shaped_r = (
                    rewards[self._agent_ids[1]]
                    + self._pct_ag0_reward_for_ag1 * rewards[self._agent_ids[0]]
            )
            # Add it to the batch.
            self.add_batch_item(
                batch=data,
                column=SampleBatch.REWARDS,
                item_to_add=ag1_shaped_r,
                single_agent_episode=ma_episode.agent_episodes[self._agent_ids[1]],
            )
        return data
