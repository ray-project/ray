from typing import Any, List, Optional

import numpy as np

import tree
from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.spaces.space_utils import batch
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import PublicAPI


_, tf, _ = try_import_tf()


@PublicAPI(stability="alpha")
class DefaultEnvToModule(ConnectorV2):
    """Default connector piece added by RLlib to the end of any env-to-module pipeline.

    Makes sure that the output data will have at the minimum:
    - An observation (the most recent one returned by `env.step()`) under the
    SampleBatch.OBS key for each agent and
    - In case the RLModule is stateful, a STATE_IN key populated with the most recently
    computed STATE_OUT.
    - The output data is in the correct tensor format (torch or tf tensors), ready for
    the RLModule.

    The connector will not add any new data in case other connector pieces in the
    pipeline already take care of populating these fields (obs and state in).
    """

    @override(ConnectorV2)
    def __call__(
        self,
        *,
        rl_module: RLModule,
        data: Optional[Any] = None,
        episodes: List[EpisodeType],
        explore: Optional[bool] = None,
        shared_data: Optional[dict] = None,
        **kwargs,
    ) -> Any:
        # If observations cannot be found in `input`, add the most recent ones (from all
        # episodes).
        if SampleBatch.OBS not in data:
            # Collect all most-recent observations from given episodes.
            observations = []
            for episode in episodes:
                observations.append(episode.get_observations(indices=-1))
            # Batch all collected observations together.
            data[SampleBatch.OBS] = batch(observations)

        # If our module is stateful:
        # - Add the most recent STATE_OUTs to `data`.
        # - Make all data in `data` have a time rank (T=1).
        if rl_module.is_stateful():
            # Collect all most recently computed STATE_OUT (or use initial states from
            # RLModule if at beginning of episode).
            states = []
            for episode in episodes:
                # Make sure, we have at least one observation in the episode.
                assert episode.observations

                # TODO (sven): Generalize to MultiAgentEpisodes.
                # Episode just started -> Get initial state from our RLModule.
                if len(episode) == 0:
                    state = rl_module.get_initial_state()
                # Episode is already ongoing -> Use most recent STATE_OUT.
                else:
                    state = episode.extra_model_outputs[STATE_OUT][-1]
                states.append(state)

            # Make all other inputs have an additional T=1 axis.
            data = tree.map_structure(lambda s: np.expand_dims(s, axis=1), data)

            # Batch states (from list of individual vector sub-env states).
            # Note that state ins should NOT have the extra time dimension.
            data[STATE_IN] = batch(states)

        # Convert data to proper tensor formats, depending on framework used by the
        # RLModule.
        # TODO (sven): Support GPU-based EnvRunners + RLModules for sampling. Right
        #  now we assume EnvRunners have their RLModule always placed on the CPU.
        if rl_module.framework == "torch":
            data = convert_to_torch_tensor(data)
        elif rl_module.framework == "tf2":
            data = tree.map_structure(lambda s: tf.convert_to_tensor(s), data)

        return data
