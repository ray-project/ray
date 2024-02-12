from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import EpisodeType


class AddLastObservationToBatch(ConnectorV2):
    """Gets the last observation from a running episode and adds it to the batch.

    - Operates on a list of Episode objects.
    - Gets the most recent observation(s) from all the given episodes and adds them
    to the batch under construction (as a list of individual observations).
    - Does NOT alter any observations (or other data) in the given episodes.
    - Can be used in EnvToModule and Learner connector pipelines.

    .. testcode::

        import gymnasium as gym
        import numpy as np

        from ray.rllib.connectors.env_to_module import AddLastObservationToBatch
        from ray.rllib.env.single_agent_episode import SingleAgentEpisode
        from ray.rllib.utils.test_utils import check

        # Create two dummy SingleAgentEpisodes, each containing 2 observations,
        # 1 action and 1 reward (both are length=1).
        obs_space = gym.spaces.Box(-1.0, 1.0, (2,), np.float32)
        act_space = gym.spaces.Discrete(2)

        episodes = [SingleAgentEpisode(
            observation_space=obs_space,
            observations=[obs_space.sample(), obs_space.sample()],
            actions=[act_space.sample()],
            rewards=[1.0],
            len_lookback_buffer=0,
        ) for _ in range(2)]
        eps_1_last_obs = episodes[0].get_observations(-1)
        eps_2_last_obs = episodes[1].get_observations(-1)
        print(f"1st Episode's last obs is {eps_1_last_obs}")
        print(f"2nd Episode's last obs is {eps_2_last_obs}")

        # Create an instance of this class, providing the obs- and action spaces.
        connector = AddLastObservationToBatch(obs_space, act_space)

        # Call the connector with the two created episodes.
        # Note that this particular connector works without an RLModule, so we
        # simplify here for the sake of this example.
        output_data = connector(
            rl_module=None,
            data={},
            episodes=episodes,
            explore=True,
            shared_data={},
        )
        # The output data should now contain the last observations of both episodes.
        check(output_data, {"obs": [eps_1_last_obs, eps_2_last_obs]})
    """

    def __init__(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
        *,
        as_learner_connector: bool = False,
        **kwargs,
    ):
        """Initializes a AddLastObservationToBatchConnector instance.

        Args:
            as_learner_connector: Whether this connector is part of a Learner connector
                pipeline, as opposed to a env-to-module pipeline.
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )

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

        for sa_episode in self.single_agent_episode_iterator(episodes):
            if self._as_learner_connector:
                prev_n_o = []
                for ts in range(len(sa_episode)):
                    prev_n_o.append(sa_episode.get_observations(indices=ts, fill=0.0))
                self.add_batch_item(
                    data,
                    SampleBatch.OBS,
                    batch(prev_n_o),
                    sa_episode,
                )
            else:
                assert not sa_episode.is_finalized
                self.add_batch_item(
                    data,
                    SampleBatch.OBS,
                    sa_episode.get_observations(indices=-1, fill=0.0),
                    sa_episode,
                )
        return data
