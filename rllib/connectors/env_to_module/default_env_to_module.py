from typing import Any, List

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.core.models.base import STATE_IN
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class DefaultEnvToModule(ConnectorV2):
    """Default env-to-module-connector always in the pipeline at the very end.

    Makes sure that there is at least an observation (the most recent one) for each
    agent as well as a state - in case the RLModule is recurrent. Doesn't do anything
    in case other pieces in the pipeline already take care of populating these fields.

    TODO: Generalize to MultiAgentEpisodes.
    """

    @override(ConnectorV2)
    def __call__(
        self,
        input_: Any,
        episodes: List[EpisodeType],
        ctx: ConnectorContextV2,
        **kwargs,
    ):
        # If obs are not already part of the input, add the most recent ones (from all
        # single-agent episodes).
        if SampleBatch.OBS not in input_:
            observations = []
            for episode in episodes:
                # Make sure we have a single agent episode.
                assert isinstance(episode, EpisodeType)
                # Make sure, we have at least one observation in the episode.
                assert len(episode.observations) > 0
                observations.append(episode.observations[-1])
            input_[SampleBatch.OBS] = batch(observations)

        # If our module is recurrent, also add the most recent states to the inputs.
        if ctx.rl_module.is_stateful():
            states = []
            for episode in episodes:
                # Make sure we have a single agent episode.
                assert isinstance(episodes, EpisodeType)
                # Make sure, we have at least one observation in the episode.
                # TODO: Generalize to MultiAgentEpisodes.
                assert episode.state is not None
                states.append(episode.state)
            input_[STATE_IN] = batch(states)
