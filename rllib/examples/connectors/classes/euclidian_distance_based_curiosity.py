from collections import deque
from typing import Any, List, Optional

import gymnasium as gym
import numpy as np

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.typing import EpisodeType


class EuclidianDistanceBasedCuriosity(ConnectorV2):
    """Learner ConnectorV2 piece computing intrinsic rewards with euclidian distance.

    Add this connector piece to your Learner pipeline, through your algo config:
    ```
    config.training(
        learner_connector=lambda obs_sp, act_sp: EuclidianDistanceBasedCuriosity()
    )
    ```

    Intrinsic rewards are computed on the Learner side based on comparing the euclidian
    distance of observations vs already seen ones. A configurable number of observations
    will be stored in a FIFO buffer and all incoming observations have their distance
    measured against those.

    The minimum distance measured is the intrinsic reward for the incoming obs
    (multiplied by a fixed coeffieicnt and added to the "main" extrinsic reward):
    r(i) = intrinsic_reward_coeff * min(ED(o, o(i)) for o in stored_obs))
    where `ED` is the euclidian distance and `stored_obs` is the buffer.

    The intrinsic reward is then added to the extrinsic reward and saved back into the
    episode (under the main "rewards" key).

    Note that the computation and saving back to the episode all happens before the
    actual train batch is generated from the episode data. Thus, the Learner and the
    RLModule used do not take notice of the extra reward added.

    Only one observation per incoming episode will be stored as a new one in the buffer.
    Thereby, we pick the observation with the largest `min(ED)` value over all already
    stored observations to be stored per episode.

    If you would like to use a simpler, count-based mechanism for intrinsic reward
    computations, take a look at the `CountBasedCuriosity` connector piece
    at `ray.rllib.examples.connectors.classes.count_based_curiosity`
    """

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        intrinsic_reward_coeff: float = 1.0,
        max_buffer_size: int = 100,
        **kwargs,
    ):
        """Initializes a CountBasedCuriosity instance.

        Args:
            intrinsic_reward_coeff: The weight with which to multiply the intrinsic
                reward before adding (and saving) it back to the main (extrinsic)
                reward of the episode at each timestep.
        """
        super().__init__(input_observation_space, input_action_space)

        # Create an observation buffer
        self.obs_buffer = deque(maxlen=max_buffer_size)
        self.intrinsic_reward_coeff = intrinsic_reward_coeff

        self._test = 0

    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Any,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        if self._test > 10:
            return data
        self._test += 1
        # Loop through all episodes and change the reward to
        # [reward + intrinsic reward]
        for sa_episode in self.single_agent_episode_iterator(
            episodes=episodes, agents_that_stepped_only=False
        ):
            # Loop through all obs, except the last one.
            observations = sa_episode.get_observations(slice(None, -1))
            # Get all respective (extrinsic) rewards.
            rewards = sa_episode.get_rewards()

            max_dist_obs = None
            max_dist = float("-inf")
            for i, (obs, rew) in enumerate(zip(observations, rewards)):
                # Compare obs to all stored observations and compute euclidian distance.
                min_dist = 0.0
                if self.obs_buffer:
                    min_dist = min(
                        np.sqrt(np.sum((obs - stored_obs) ** 2))
                        for stored_obs in self.obs_buffer
                    )
                if min_dist > max_dist:
                    max_dist = min_dist
                    max_dist_obs = obs

                # Compute our euclidian distance-based intrinsic reward and add it to
                # the main (extrinsic) reward.
                rew += self.intrinsic_reward_coeff * min_dist
                # Store the new reward back to the episode (under the correct
                # timestep/index).
                sa_episode.set_rewards(new_data=rew, at_indices=i)

            # Add the one observation of this episode with the largest (min) euclidian
            # dist to all already stored obs to the buffer (maybe throwing out the
            # oldest obs in there).
            if max_dist_obs is not None:
                self.obs_buffer.append(max_dist_obs)

        return data
