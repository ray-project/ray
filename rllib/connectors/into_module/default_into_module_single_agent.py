from typing import Any, List

from ray.rllib.connectors.connector import Connector, ConnectorContextV2
from ray.rllib.connectors.input_output_types import INPUT_OUTPUT_TYPE
from ray.rllib.core.models.base import STATE_IN
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.replay_buffers.episode_replay_buffer import _Episode
from ray.rllib.utils.spaces.space_utils import batch
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class DefaultIntoModuleSingleAgent(Connector):
    """Default into-module-connector used if no connectors are configured by the user.

    Extracts the last observation of all agents from the list of episodes.
    TODO: Generalize to MultiAgentEpisodes.
    Also, if the module is recurrent, will add the most recent state to its output.
    """

    def __init__(self, **kwargs):
        # Data dict in (possibly empty), data dict out
        # (obs and - if applicable - states).
        super().__init__(
            input_type=INPUT_OUTPUT_TYPE.DATA,
            output_type=INPUT_OUTPUT_TYPE.DATA,
        )

    @override(Connector)
    def __call__(self, input_: Any, episodes: List[_Episode], ctx: ConnectorContextV2):
        # If obs are not already part of the input, add the most recent ones (from all
        # single-agent episodes).
        if SampleBatch.OBS not in input_:
            observations = []
            for episode in episodes:
                # Make sure we have a single agent episode.
                assert isinstance(episodes, _Episode)
                # Make sure, we have at least one observation in the episode.
                assert len(episode.observations) > 0
                observations.append(episode.observations[-1])
            input_[SampleBatch.OBS] = batch(observations)

        # If our module is recurrent, also add the most recent states to the inputs.
        if ctx.rl_module.is_stateful():
            states = []
            for episode in episodes:
                # Make sure we have a single agent episode.
                assert isinstance(episodes, _Episode)
                # Make sure, we have at least one observation in the episode.
                assert episode.state is not None
                states.append(episode.state)
            input_[STATE_IN] = batch(states)
