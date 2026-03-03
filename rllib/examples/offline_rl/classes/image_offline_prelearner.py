import random
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union

import gymnasium as gym
import numpy as np

from ray.actor import ActorHandle
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.offline.offline_prelearner import SCHEMA, OfflinePreLearner
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType, ModuleID


class ImageOfflinePreLearner(OfflinePreLearner):
    """This class transforms image data to `MultiAgentBatch`es.

    While the `ImageOfflineData` class transforms raw image
    bytes to `numpy` arrays, this class maps these data in
    `SingleAgentEpisode` instances through the learner connector
    pipeline and finally outputs a >`MultiAgentBatch` ready for
    training in RLlib's `Learner`s.

    Note, the basic transformation from images to `SingleAgentEpisode`
    instances creates synthetic data that does not rely on any MDP
    and therefore no agent can learn from it. However, this example
    should show how to transform data into this form through
    overriding the `OfflinePreLearner`.
    """

    def __init__(
        self,
        config: "AlgorithmConfig",
        learner: Union[Learner, List[ActorHandle]],
        spaces: Optional[Tuple[gym.Space, gym.Space]] = None,
        module_spec: Optional[MultiRLModuleSpec] = None,
        module_state: Optional[Dict[ModuleID, Any]] = None,
        **kwargs: Dict[str, Any],
    ):
        # Set up necessary class attributes.
        self.config = config
        self.action_space = spaces[1]
        self.observation_space = spaces[0]
        self.input_read_episodes = self.config.input_read_episodes
        self.input_read_sample_batches = self.config.input_read_sample_batches
        self._policies_to_train = "default_policy"
        self._is_multi_agent = False

        # Build the `MultiRLModule` needed for the learner connector.
        self._module = module_spec.build()

        # Build the learner connector pipeline.
        self._learner_connector = self.config.build_learner_connector(
            input_observation_space=self.observation_space,
            input_action_space=self.action_space,
        )

    @override(OfflinePreLearner)
    @staticmethod
    def _map_to_episodes(
        is_multi_agent: bool,
        batch: Dict[str, Union[list, np.ndarray]],
        schema: Dict[str, str] = SCHEMA,
        to_numpy: bool = False,
        input_compress_columns: Optional[List[str]] = None,
        observation_space: gym.Space = None,
        action_space: gym.Space = None,
        **kwargs: Dict[str, Any],
    ) -> Dict[str, List[EpisodeType]]:

        # Define a container for the episodes.
        episodes = []

        # Batches come in as numpy arrays.
        for i, obs in enumerate(batch["array"]):

            # Construct your episode.
            episode = SingleAgentEpisode(
                id_=uuid.uuid4().hex,
                observations=[obs, obs],
                observation_space=observation_space,
                actions=[action_space.sample()],
                action_space=action_space,
                rewards=[random.random()],
                terminated=True,
                truncated=False,
                len_lookback_buffer=0,
                t_started=0,
            )

            # Numpy'ize, if necessary.
            if to_numpy:
                episode.to_numpy()

            # Store the episode in the container.
            episodes.append(episode)

        return {"episodes": episodes}
