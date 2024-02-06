from typing import Any, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType


class AddLastObservationToBatch(ConnectorV2):
    """Gets the last observation from a running episode and adds it to the batch.

    - Operates on a list of Episode objects.
    - Gets the most recent observation(s) from all the given episodes and adds them
    to the batch under construction (as a list of individual observations).
    - Does NOT alter any observations (or other data) in the given episodes.
    - Can be used in EnvToModule and Learner connector pipelines.

    .. testcode::

        TODO
    """

    def __init__(
        self,
        # Base class constructor args.
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
        *,
        # Specific prev. obs args.
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

        prev_n_o = []
        for episode in episodes:
            if self._as_learner_connector:
                for ts in range(len(episode)):
                    prev_n_o.append(episode.get_observations(indices=ts, fill=0.0))
            else:
                assert not episode.is_finalized
                prev_n_o.append(episode.get_observations(indices=-1, fill=0.0))

        data[SampleBatch.OBS] = prev_n_o
        return data
