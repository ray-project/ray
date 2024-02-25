from typing import Any, List, Optional

import gymnasium as gym
import numpy as np
import tree  # pip install dm_tree

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType


class AddStateFromEpisodeToBatch(ConnectorV2):
    """Gets last STATE_OUT from running episode and adds it as STATE_IN to the batch.

    - Operates on a list of Episode objects.
    - Gets the most recent STATE_OUT from all the given episodes and adds them under
    the STATE_IN key to the batch under construction.
    - Does NOT alter any data in the given episodes.
    - Can be used in EnvToModule and Learner connector pipelines.

    .. testcode::

        import gymnasium as gym
        import numpy as np

        from ray.rllib.connectors.common import AddStateFromEpisodeToBatch
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
        connector = AddObservationFromEpisodeToBatch(obs_space, act_space)

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
        input_observation_space: gym.Space = None,
        input_action_space: gym.Space = None,
        *,
        max_seq_len: Optional[int] = None,
        as_learner_connector: bool = False,
        **kwargs,
    ):
        """Initializes a AddObservationFromEpisodeToBatch instance.

        Args:
            as_learner_connector: Whether this connector is part of a Learner connector
                pipeline, as opposed to a env-to-module pipeline. As a Learner
                connector, it will add an entire Episode's observations (each timestep)
                to the batch.
        """
        super().__init__(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            **kwargs,
        )

        self._as_learner_connector = as_learner_connector
        self.max_seq_len = max_seq_len
        if self._as_learner_connector and self.max_seq_len is None:
            raise ValueError(
                "Cannot run `AddStateFromEpisodeToBatch` as Learner connector without "
                "`max_seq_len` constructor argument!"
            )

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

        # If not stateful OR STATE_IN already in data, early out.
        if not rl_module.is_stateful() or STATE_IN in data:
            return data

        # Make all inputs (other than STATE_IN) have an additional T-axis.
        # Since data has not been batched yet (we are still operating on lists in the
        # batch), we add this time axis as 0 (not 1). When we batch, the batch axis will
        # be 0 and the time axis will be 1.
        data = tree.map_structure(lambda p, s: np.expand_dims(s, axis=0), data)
        # Let module-to-env pipeline know that we had added a single timestep
        # time rank to the data (to remove it again).
        shared_data["_added_single_ts_time_rank"] = True

        for sa_episode in self.single_agent_episode_iterator(episodes):
            if self._as_learner_connector:
                self.add_n_batch_items(
                    data,
                    STATE_IN,
                    items_to_add=[
                        sa_episode.get_extra_model_outputs(key=STATE_OUT, indices=ts)
                        for ts in range(0, len(sa_episode), self.max_seq_len)
                    ],
                    single_agent_episode=sa_episode,
                )
            else:
                assert not sa_episode.is_finalized
                self.add_batch_item(
                    data,
                    STATE_IN,
                    item_to_add=sa_episode.get_extra_model_outputs(
                        key=STATE_OUT, indices=-1
                    ),
                    single_agent_episode=sa_episode,
                )

        return data
