from typing import Any, Dict, List, Optional

import gymnasium as gym
from gymnasium.spaces import Box
import numpy as np

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import (
    batch as batch_fn,
    flatten_to_single_ndarray,
)
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class PrevActionsPrevRewards(ConnectorV2):
    """A connector piece that adds previous rewards and actions to the input obs.

    - Requires Columns.OBS to be already a part of the batch.
    - This connector makes the assumption that under the Columns.OBS key in batch,
    there is either a list of individual env observations to be flattened (single-agent
    case) or a dict mapping (AgentID, ModuleID)-tuples to lists of data items to be
    flattened (multi-agent case).
    - Converts Columns.OBS data into a dict (or creates a sub-dict if obs are
    already a dict), and adds "prev_rewards" and "prev_actions"
    to this dict. The original observations are stored under the self.ORIG_OBS_KEY in
    that dict.
    - If your RLModule does not handle dict inputs, you will have to plug in an
    `FlattenObservations` connector piece after this one.
    - Does NOT work in a Learner pipeline as it operates on individual observation
    items (as opposed to batched/time-ranked data).
    - Therefore, assumes that the altered (flattened) observations will be written
    back into the episode by a later connector piece in the env-to-module pipeline
    (which this piece is part of as well).
    - Only reads reward- and action information from the given list of Episode objects.
    - Does NOT write any observations (or other data) to the given Episode objects.
    """

    ORIG_OBS_KEY = "_orig_obs"
    PREV_ACTIONS_KEY = "prev_n_actions"
    PREV_REWARDS_KEY = "prev_n_rewards"

    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
    ) -> gym.Space:
        if self._multi_agent:
            ret = {}
            for agent_id, obs_space in input_observation_space.spaces.items():
                act_space = input_action_space[agent_id]
                ret[agent_id] = self._convert_individual_space(obs_space, act_space)
            return gym.spaces.Dict(ret)
        else:
            return self._convert_individual_space(
                input_observation_space, input_action_space
            )

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        multi_agent: bool = False,
        n_prev_actions: int = 1,
        n_prev_rewards: int = 1,
        **kwargs,
    ):
        """Initializes a PrevActionsPrevRewards instance.

        Args:
            multi_agent: Whether this is a connector operating on a multi-agent
                observation space mapping AgentIDs to individual agents' observations.
            n_prev_actions: The number of previous actions to include in the output
                data. Discrete actions are ont-hot'd. If > 1, will concatenate the
                individual action tensors.
            n_prev_rewards: The number of previous rewards to include in the output
                data.
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )

        self._multi_agent = multi_agent
        self.n_prev_actions = n_prev_actions
        self.n_prev_rewards = n_prev_rewards

        # TODO: Move into input_observation_space setter
        # Thus far, this connector piece only operates on discrete action spaces.
        # act_spaces = [self.input_action_space]
        # if self._multi_agent:
        #    act_spaces = self.input_action_space.spaces.values()
        # if not all(isinstance(s, gym.spaces.Discrete) for s in act_spaces):
        #    raise ValueError(
        #        f"{type(self).__name__} only works on Discrete action spaces "
        #        f"thus far (or, for multi-agent, on Dict spaces mapping AgentIDs to "
        #        f"the individual agents' Discrete action spaces)!"
        #    )

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Optional[Dict[str, Any]],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        for sa_episode in self.single_agent_episode_iterator(
            episodes, agents_that_stepped_only=True
        ):
            # Episode is not numpy'ized yet and thus still operates on lists of items.
            assert not sa_episode.is_numpy

            augmented_obs = {self.ORIG_OBS_KEY: sa_episode.get_observations(-1)}

            if self.n_prev_actions:
                augmented_obs[self.PREV_ACTIONS_KEY] = flatten_to_single_ndarray(
                    batch_fn(
                        sa_episode.get_actions(
                            indices=slice(-self.n_prev_actions, None),
                            fill=0.0,
                            one_hot_discrete=True,
                        )
                    )
                )

            if self.n_prev_rewards:
                augmented_obs[self.PREV_REWARDS_KEY] = np.array(
                    sa_episode.get_rewards(
                        indices=slice(-self.n_prev_rewards, None),
                        fill=0.0,
                    )
                )

            # Write new observation directly back into the episode.
            sa_episode.set_observations(at_indices=-1, new_data=augmented_obs)
            #  We set the Episode's observation space to ours so that we can safely
            #  set the last obs to the new value (without causing a space mismatch
            #  error).
            sa_episode.observation_space = self.observation_space

        return batch

    def _convert_individual_space(self, obs_space, act_space):
        return gym.spaces.Dict(
            {
                self.ORIG_OBS_KEY: obs_space,
                # Currently only works for Discrete action spaces.
                self.PREV_ACTIONS_KEY: Box(
                    0.0, 1.0, (act_space.n * self.n_prev_actions,), np.float32
                ),
                self.PREV_REWARDS_KEY: Box(
                    float("-inf"),
                    float("inf"),
                    (self.n_prev_rewards,),
                    np.float32,
                ),
            }
        )
