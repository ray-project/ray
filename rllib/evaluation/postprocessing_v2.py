import numpy as np
import tree

from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.replay_buffers.episode_replay_buffer import _Episode
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_tf

# TODO (simon): Make framework-agnostic.
_, tf, _ = try_import_tf()

@DeveloperAPI
class Postprocessing:
    """Constant definitions for postprocessing."""

    ADVANTAGES = "advantages"
    VALUE_TARGETS = "value_targets"

@DeveloperAPI
def compute_gae_for_episode(
    episode: _Episode,
):
    """Adds GAE to a trajectory."""

    episode = compute_bootstrap_value(episode)



def compute_bootstrap_value(episodes: _Episode, module: MultiAgentRLModule) -> _Episode:

    if episodes[0].is_terminated:
        last_r = 0.0
    else:
        # TODO (simon): This has to be made multi-agent ready.
        initial_states = module[DEFAULT_POLICY_ID].get_initial_state()

        states = {
            k: np.stack(
                [
                    initial_states[k] if eps.states is None else eps.states[k]
                    for eps in episodes
                ]
            )
            for k in initial_states.keys()
        }
        input_dict = {
            STATE_IN: tree.map_structure(
                lambda s: tf.convert_to_tensor(s),
                states,
            ),
            SampleBatch.OBS: tree.map_structure(
                lambda s: tf.convert_to_tensor(s),
                [eps.observations[-1] for eps in episodes]
            )
        }

        # TODO (simon): Torch might need the correct device.

        # TODO (sven): If we want to get rid of the