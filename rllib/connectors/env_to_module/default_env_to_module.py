from typing import Any, List

import numpy as np

import tree
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.connectors.connector_context_v2 import ConnectorContextV2
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
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
                # Make sure, we have at least one observation in the episode.
                assert len(episode.observations) > 0
                observations.append(episode.observations[-1])
            input_[SampleBatch.OBS] = batch(observations)

        # If our module is recurrent:
        # - Add the most recent states to the inputs.
        # - Make all inputs have T=1.
        if ctx.rl_module.is_stateful():
            states = []
            for episode in episodes:
                # Make sure, we have at least one observation in the episode.
                assert episode.observations

                # TODO: Generalize to MultiAgentEpisodes.
                # Episode just started, get initial state from our RLModule.
                if len(episode) == 0:
                    state = ctx.rl_module.get_initial_state()
                else:
                    state = episode.extra_model_outputs[STATE_OUT][-1]
                states.append(state)

            # Make all other inputs have an additional T=1 axis.
            input_ = tree.map_structure(lambda s: np.expand_dims(s, axis=1), input_)

            # Batch states (from list of individual vector sub-env states).
            # Note that state ins should NOT have the extra time dimension.
            input_[STATE_IN] = batch(states)

        return input_
