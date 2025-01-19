from typing import Any, Dict, List, Optional

from ray.rllib.core.columns import Columns
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class AddNextObservationsFromEpisodesToTrainBatch(ConnectorV2):
    """Adds the NEXT_OBS column with the correct episode observations to train batch.

    - Operates on a list of Episode objects.
    - Gets all observation(s) from all the given episodes (except the very first ones)
    and adds them to the batch under construction in the NEXT_OBS column (as a list of
    individual observations).
    - Does NOT alter any observations (or other data) in the given episodes.
    - Can be used in Learner connector pipelines.

    .. testcode::

        import gymnasium as gym
        import numpy as np

        from ray.rllib.connectors.learner import (
            AddNextObservationsFromEpisodesToTrainBatch
        )
        from ray.rllib.core.columns import Columns
        from ray.rllib.env.single_agent_episode import SingleAgentEpisode
        from ray.rllib.utils.test_utils import check

        # Create two dummy SingleAgentEpisodes, each containing 3 observations,
        # 2 actions and 2 rewards (both episodes are length=2).
        obs_space = gym.spaces.Box(-1.0, 1.0, (2,), np.float32)
        act_space = gym.spaces.Discrete(2)

        episodes = [SingleAgentEpisode(
            observations=[obs_space.sample(), obs_space.sample(), obs_space.sample()],
            actions=[act_space.sample(), act_space.sample()],
            rewards=[1.0, 2.0],
            len_lookback_buffer=0,
        ) for _ in range(2)]
        eps_1_next_obses = episodes[0].get_observations([1, 2])
        eps_2_next_obses = episodes[1].get_observations([1, 2])
        print(f"1st Episode's next obses are {eps_1_next_obses}")
        print(f"2nd Episode's next obses are {eps_2_next_obses}")

        # Create an instance of this class.
        connector = AddNextObservationsFromEpisodesToTrainBatch()

        # Call the connector with the two created episodes.
        # Note that this particular connector works without an RLModule, so we
        # simplify here for the sake of this example.
        output_data = connector(
            rl_module=None,
            batch={},
            episodes=episodes,
            explore=True,
            shared_data={},
        )
        # The output data should now contain the last observations of both episodes,
        # in a "per-episode organized" fashion.
        check(
            output_data,
            {
                Columns.NEXT_OBS: {
                    (episodes[0].id_,): eps_1_next_obses,
                    (episodes[1].id_,): eps_2_next_obses,
                },
            },
        )
    """

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
        # If "obs" already in `batch`, early out.
        if Columns.NEXT_OBS in batch:
            return batch

        for sa_episode in self.single_agent_episode_iterator(
            # This is a Learner-only connector -> Get all episodes (for train batch).
            episodes,
            agents_that_stepped_only=False,
        ):
            self.add_n_batch_items(
                batch,
                Columns.NEXT_OBS,
                items_to_add=sa_episode.get_observations(slice(1, len(sa_episode) + 1)),
                num_items=len(sa_episode),
                single_agent_episode=sa_episode,
            )
        return batch
