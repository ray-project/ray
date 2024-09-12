from typing import Any, Dict, List, Optional

import gymnasium as gym

from ray.rllib.core.columns import Columns
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class AddObservationsFromEpisodesToBatch(ConnectorV2):
    """Gets the last observation from a running episode and adds it to the batch.

    Note: This is one of the default env-to-module or Learner ConnectorV2 pieces that
    are added automatically by RLlib into every env-to-module/Learner connector
    pipeline, unless `config.add_default_connectors_to_env_to_module_pipeline` or
    `config.add_default_connectors_to_learner_pipeline ` are set to
    False.

    The default env-to-module connector pipeline is:
    [
        [0 or more user defined ConnectorV2 pieces],
        AddObservationsFromEpisodesToBatch,
        AddStatesFromEpisodesToBatch,
        AgentToModuleMapping,  # only in multi-agent setups!
        BatchIndividualItems,
        NumpyToTensor,
    ]
    The default Learner connector pipeline is:
    [
        [0 or more user defined ConnectorV2 pieces],
        AddObservationsFromEpisodesToBatch,
        AddColumnsFromEpisodesToTrainBatch,
        AddStatesFromEpisodesToBatch,
        AgentToModuleMapping,  # only in multi-agent setups!
        BatchIndividualItems,
        NumpyToTensor,
    ]

    This ConnectorV2:
    - Operates on a list of Episode objects (single- or multi-agent).
    - Gets the most recent observation(s) from all the given episodes and adds them
    to the batch under construction (as a list of individual observations).
    - Does NOT alter any observations (or other data) in the given episodes.
    - Can be used in EnvToModule and Learner connector pipelines.

    .. testcode::

        import gymnasium as gym
        import numpy as np

        from ray.rllib.connectors.common import AddObservationsFromEpisodesToBatch
        from ray.rllib.env.single_agent_episode import SingleAgentEpisode
        from ray.rllib.utils.test_utils import check

        # Create two dummy SingleAgentEpisodes, each containing 2 observations,
        # 1 action and 1 reward (both are length=1).
        obs_space = gym.spaces.Box(-1.0, 1.0, (2,), np.float32)
        act_space = gym.spaces.Discrete(2)

        episodes = [SingleAgentEpisode(
            observations=[obs_space.sample(), obs_space.sample()],
            actions=[act_space.sample()],
            rewards=[1.0],
            len_lookback_buffer=0,
        ) for _ in range(2)]
        eps_1_last_obs = episodes[0].get_observations(-1)
        eps_2_last_obs = episodes[1].get_observations(-1)
        print(f"1st Episode's last obs is {eps_1_last_obs}")
        print(f"2nd Episode's last obs is {eps_2_last_obs}")

        # Create an instance of this class.
        connector = AddObservationsFromEpisodesToBatch()

        # Call the connector with the two created episodes.
        # Note that this particular connector works without an RLModule, so we
        # simplify here for the sake of this example.
        output_batch = connector(
            rl_module=None,
            batch={},
            episodes=episodes,
            explore=True,
            shared_data={},
        )
        # The output data should now contain the last observations of both episodes,
        # in a "per-episode organized" fashion.
        check(
            output_batch,
            {
                "obs": {
                    (episodes[0].id_,): [eps_1_last_obs],
                    (episodes[1].id_,): [eps_2_last_obs],
                },
            },
        )
    """

    def __init__(
        self,
        input_observation_space: Optional[gym.Space] = None,
        input_action_space: Optional[gym.Space] = None,
        *,
        as_learner_connector: bool = False,
        **kwargs,
    ):
        """Initializes a AddObservationsFromEpisodesToBatch instance.

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

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Dict[str, Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # If "obs" already in data, early out.
        if Columns.OBS in batch:
            return batch

        for sa_episode in self.single_agent_episode_iterator(
            episodes,
            # If Learner connector, get all episodes (for train batch).
            # If EnvToModule, get only those ongoing episodes that just had their
            # agent step (b/c those are the ones we need to compute actions for next).
            agents_that_stepped_only=not self._as_learner_connector,
        ):
            if self._as_learner_connector:
                self.add_n_batch_items(
                    batch,
                    Columns.OBS,
                    items_to_add=sa_episode.get_observations(slice(0, len(sa_episode))),
                    num_items=len(sa_episode),
                    single_agent_episode=sa_episode,
                )
            else:
                assert not sa_episode.is_finalized
                self.add_batch_item(
                    batch,
                    Columns.OBS,
                    item_to_add=sa_episode.get_observations(-1),
                    single_agent_episode=sa_episode,
                )
        return batch
