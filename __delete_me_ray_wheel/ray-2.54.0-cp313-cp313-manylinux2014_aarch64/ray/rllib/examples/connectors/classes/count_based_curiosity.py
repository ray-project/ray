from collections import Counter
from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.typing import EpisodeType


class CountBasedCuriosity(ConnectorV2):
    """Learner ConnectorV2 piece to compute intrinsic rewards based on obs counts.

    Add this connector piece to your Learner pipeline, through your algo config:
    ```
    config.training(
        learner_connector=lambda obs_sp, act_sp: CountBasedCuriosity()
    )
    ```

    Intrinsic rewards are computed on the Learner side based on naive observation
    counts, which is why this connector should only be used for simple environments
    with a reasonable number of possible observations. The intrinsic reward for a given
    timestep is:
    r(i) = intrinsic_reward_coeff * (1 / C(obs(i)))
    where C is the total (lifetime) count of the obs at timestep i.

    The intrinsic reward is added to the extrinsic reward and saved back into the
    episode (under the main "rewards" key).

    Note that the computation and saving back to the episode all happens before the
    actual train batch is generated from the episode data. Thus, the Learner and the
    RLModule used do not take notice of the extra reward added.

    If you would like to use a more sophisticated mechanism for intrinsic reward
    computations, take a look at the `EuclidianDistanceBasedCuriosity` connector piece
    at `ray.rllib.examples.connectors.classes.euclidian_distance_based_curiosity`
    """

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        intrinsic_reward_coeff: float = 1.0,
        **kwargs,
    ):
        """Initializes a CountBasedCuriosity instance.

        Args:
            intrinsic_reward_coeff: The weight with which to multiply the intrinsic
                reward before adding (and saving) it back to the main (extrinsic)
                reward of the episode at each timestep.
        """
        super().__init__(input_observation_space, input_action_space)

        # Naive observation counter.
        self._counts = Counter()
        self.intrinsic_reward_coeff = intrinsic_reward_coeff

    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Any,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # Loop through all episodes and change the reward to
        # [reward + intrinsic reward]
        for sa_episode in self.single_agent_episode_iterator(
            episodes=episodes, agents_that_stepped_only=False
        ):
            # Loop through all observations, except the last one.
            observations = sa_episode.get_observations(slice(None, -1))
            # Get all respective extrinsic rewards.
            rewards = sa_episode.get_rewards()

            for i, (obs, rew) in enumerate(zip(observations, rewards)):
                # Add 1 to obs counter.
                obs = tuple(obs)
                self._counts[obs] += 1
                # Compute the count-based intrinsic reward and add it to the extrinsic
                # reward.
                rew += self.intrinsic_reward_coeff * (1 / self._counts[obs])
                # Store the new reward back to the episode (under the correct
                # timestep/index).
                sa_episode.set_rewards(new_data=rew, at_indices=i)

        return batch
