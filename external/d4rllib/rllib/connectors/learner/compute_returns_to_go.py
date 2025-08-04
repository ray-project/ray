from typing import Any, List, Dict

import scipy

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.columns import Columns
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModule
from ray.rllib.utils.typing import EpisodeType


class ComputeReturnsToGo(ConnectorV2):
    """Learner ConnectorV2 piece computing discounted returns to go till end of episode.

    This ConnectorV2:
    - Operates on a list of Episode objects (single- or multi-agent).
    - Should be used only in the Learner pipeline as a preparation for an upcoming loss
    computation that requires the discounted returns to go (until the end of the
    episode).
    - For each agent, for each episode and at each timestep, sums up the rewards
    (discounted) until the end of the episode and assigns the results to a new
    column: RETURNS_TO_GO in the batch.
    """

    def __init__(
        self,
        input_observation_space=None,
        input_action_space=None,
        *,
        gamma,
    ):
        """Initializes a ComputeReturnsToGo instance.

        Args:
            gamma: The discount factor gamma.
        """
        super().__init__(input_observation_space, input_action_space)
        self.gamma = gamma

    def __call__(
        self,
        *,
        rl_module: MultiRLModule,
        episodes: List[EpisodeType],
        batch: Dict[str, Any],
        **kwargs,
    ):
        for sa_episode in self.single_agent_episode_iterator(
            episodes, agents_that_stepped_only=False
        ):
            # Reverse the rewards sequence.
            rewards_reversed = sa_episode.get_rewards()[::-1]
            # Use lfilter to compute the discounted cumulative sums.
            discounted_cumsum_reversed = scipy.signal.lfilter(
                [1], [1, -self.gamma], rewards_reversed
            )
            # Reverse the result to get the correct order.
            discounted_returns = discounted_cumsum_reversed[::-1]

            # Add the results to the batch under a new column: RETURNS_TO_GO.
            self.add_n_batch_items(
                batch=batch,
                column=Columns.RETURNS_TO_GO,
                items_to_add=discounted_returns,
                num_items=len(sa_episode),
                single_agent_episode=sa_episode,
            )

        return batch
