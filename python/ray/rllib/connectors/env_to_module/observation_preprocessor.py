import abc
from typing import Any, Dict, List, Optional

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.env.multi_agent_episode import MultiAgentEpisode
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class SingleAgentObservationPreprocessor(ConnectorV2, abc.ABC):
    """Env-to-module connector preprocessing the most recent single-agent observation.

    This is a convenience class that simplifies the writing of few-step preprocessor
    connectors.

    Note that this class also works in a multi-agent setup, in which case RLlib
    separately calls this connector piece with each agents' observation and
    `SingleAgentEpisode` object.

    Users must implement the `preprocess()` method, which simplifies the usual procedure
    of extracting some data from a list of episodes and adding it to the batch to a mere
    "old-observation --transform--> return new-observation" step.
    """

    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
    ) -> gym.Space:
        # Users should override this method only in case the
        # `SingleAgentObservationPreprocessor` changes the observation space of the
        # pipeline. In this case, return the new observation space based on the
        # incoming one (`input_observation_space`).
        return super().recompute_output_observation_space(
            input_observation_space, input_action_space
        )

    @abc.abstractmethod
    def preprocess(self, observation, episode: SingleAgentEpisode):
        """Override to implement the preprocessing logic.

        Args:
            observation: A single (non-batched) observation item for a single agent to
                be preprocessed by this connector.
            episode: The `SingleAgentEpisode` instance, from which `observation` was
                taken. You can extract information on the particular AgentID and the
                ModuleID through `episode.agent_id` and `episode.module_id`.

        Returns:
            The new observation for the agent after `observation` has been preprocessed.
        """

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Dict[str, Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        persistent_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # We process and then replace observations inside the episodes directly.
        # Thus, all following connectors will only see and operate on the already
        # processed observation (w/o having access anymore to the original
        # observations).
        for sa_episode in self.single_agent_episode_iterator(episodes):
            observation = sa_episode.get_observations(-1)

            # Process the observation and write the new observation back into the
            # episode.
            new_observation = self.preprocess(
                observation=observation,
                episode=sa_episode,
            )
            sa_episode.set_observations(at_indices=-1, new_data=new_observation)
            #  We set the Episode's observation space to ours so that we can safely
            #  set the last obs to the new value (without causing a space mismatch
            #  error).
            sa_episode.observation_space = self.observation_space

        # Leave `batch` as is. RLlib's default connector automatically populates
        # the OBS column therein from the episodes' now transformed observations.
        return batch


@PublicAPI(stability="alpha")
class MultiAgentObservationPreprocessor(ConnectorV2, abc.ABC):
    """Env-to-module connector preprocessing the most recent multi-agent observation.

    The observation is always a dict of individual agents' observations.

    This is a convenience class that simplifies the writing of few-step preprocessor
    connectors.

    Users must implement the `preprocess()` method, which simplifies the usual procedure
    of extracting some data from a list of episodes and adding it to the batch to a mere
    "old-observation --transform--> return new-observation" step.
    """

    @override(ConnectorV2)
    def recompute_output_observation_space(
        self,
        input_observation_space: gym.Space,
        input_action_space: gym.Space,
    ) -> gym.Space:
        # Users should override this method only in case the
        # `MultiAgentObservationPreprocessor` changes the observation space of the
        # pipeline. In this case, return the new observation space based on the
        # incoming one (`input_observation_space`).
        return super().recompute_output_observation_space(
            input_observation_space, input_action_space
        )

    @abc.abstractmethod
    def preprocess(self, observations, episode: MultiAgentEpisode):
        """Override to implement the preprocessing logic.

        Args:
            observations: An observation dict containing each stepping agents'
                (non-batched) observation to be preprocessed by this connector.
            episode: The MultiAgentEpisode instance, where the `observation` dict
                originated from.

        Returns:
            The new multi-agent observation dict after `observations` has been
            preprocessed.
        """

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        batch: Dict[str, Any],
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        persistent_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # We process and then replace observations inside the episodes directly.
        # Thus, all following connectors will only see and operate on the already
        # processed observation (w/o having access anymore to the original
        # observations).
        for ma_episode in episodes:
            observations = ma_episode.get_observations(-1)

            # Process the observation and write the new observation back into the
            # episode.
            new_observation = self.preprocess(
                observations=observations,
                episode=ma_episode,
            )
            # TODO (sven): Implement set_observations API for multi-agent episodes.
            #  For now, we'll hack it through the single agent APIs.
            # ma_episode.set_observations(at_indices=-1, new_data=new_observation)
            for agent_id, obs in new_observation.items():
                ma_episode.agent_episodes[agent_id].set_observations(
                    at_indices=-1,
                    new_data=obs,
                )
            #  We set the Episode's observation space to ours so that we can safely
            #  set the last obs to the new value (without causing a space mismatch
            #  error).
            ma_episode.observation_space = self.observation_space

        # Leave `batch` as is. RLlib's default connector automatically populates
        # the OBS column therein from the episodes' now transformed observations.
        return batch


# Backward compatibility
ObservationPreprocessor = SingleAgentObservationPreprocessor
