from functools import partial
import numpy as np
from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import EpisodeType


class _PrevRewardPrevActionConnector(ConnectorV2):
    """A connector piece that adds previous rewards and actions to the input."""

    def __init__(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
        *,
        n_prev_actions: int = 1,
        n_prev_rewards: int = 1,
        as_learner_connector: bool = False,
        **kwargs,
    ):
        """Initializes a _PrevRewardPrevActionConnector instance.

        Args:
            n_prev_actions: The number of previous actions to include in the output
                data. Discrete actions are ont-hot'd. If > 1, will concatenate the
                individual action tensors.
            n_prev_rewards: The number of previous rewards to include in the output
                data.
            as_learner_connector: Whether this connector is part of a Learner connector
                pipeline, as opposed to a env-to-module pipeline.
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )

        self.n_prev_actions = n_prev_actions
        self.n_prev_rewards = n_prev_rewards
        self._as_learner_connector = as_learner_connector

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
        # Learner connector pipeline. Episodes have been finalized/numpy'ized.
        if self._as_learner_connector:
            return self._build_learner_data(episodes, data)
        # Env-to-module connector pipeline. Episodes are not finalized (and still
        # operate on lists).
        else:
            return self._build_inference_data(episodes, data)

    def _build_learner_data(self, episodes, data):
        for sa_episode in self.single_agent_episode_iterator(episodes):
            assert sa_episode.is_finalized
            # Loop through each timestep in the episode and add the previous n
            # actions and previous m rewards (based on that timestep) to the batch.
            for ts in range(len(sa_episode)):
                if self.n_prev_actions:
                    prev_n_actions = sa_episode.get_actions(
                        # Extract n actions from `ts - n` to `ts` (excluding `ts`).
                        indices=slice(ts - self.n_prev_actions, ts),
                        # Make sure any negative indices are NOT interpreted as
                        # "counting from the end", but as absolute indices meaning
                        # they refer to timesteps before 0 (which is the lookback
                        # buffer).
                        neg_indices_left_of_zero=True,
                        # In case we are within the first n ts of the episode, e.g.
                        # ts==0, fill the left side with zero-actions.
                        fill=0.0,
                        # Return one-hot arrays for those action components that are
                        # discrete or multi-discrete, so these are easier to concatenate
                        # with the observations later.
                        one_hot_discrete=True,
                    )
                    self.add_batch_item(
                        batch=data,
                        column=SampleBatch.PREV_ACTIONS,
                        single_agent_episode=sa_episode,
                        item_to_add=prev_n_actions,
                    )
                # Do the same for rewards.
                if self.n_prev_rewards:
                    prev_n_rewards = sa_episode.get_rewards(
                        indices=slice(ts - self.n_prev_rewards, ts),
                        neg_indices_left_of_zero=True,
                        fill=0.0,
                    )
                    self.add_batch_item(
                        batch=data,
                        column=SampleBatch.PREV_REWARDS,
                        single_agent_episode=sa_episode,
                        item_to_add=prev_n_rewards,
                    )

        return data

    def _build_inference_data(self, episodes, data):
        for sa_episode in self.single_agent_episode_iterator(episodes):

            # Episode is not finalized yet and thus still operates on lists of items.
            assert not sa_episode.is_finalized

            if self.n_prev_actions:
                prev_n_actions = batch(sa_episode.get_actions(
                    indices=slice(-self.n_prev_actions, None),
                    fill=0.0,
                    one_hot_discrete=True,
                ))
                self.add_batch_item(
                    batch=data,
                    column=SampleBatch.PREV_ACTIONS,
                    single_agent_episode=sa_episode,
                    item_to_add=prev_n_actions,
                )

            if self.n_prev_rewards:
                self.add_batch_item(
                    batch=data,
                    column=SampleBatch.PREV_REWARDS,
                    single_agent_episode=sa_episode,
                    item_to_add=np.array(
                        sa_episode.get_rewards(
                            indices=slice(-self.n_prev_rewards, None),
                            fill=0.0,
                        )
                    ),
                )

        return data
