import numpy as np

from ray.rllib.core.models.base import STATE_IN
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.replay_buffers.episode_replay_buffer import _Episode
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy

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
    # TODO (simon): All of this can be batched over multiple episodes.
    # This should increase performance.
    # TODO (sven): Shall do postprocessing in the training_step or
    # in the env_runner? Here we could batch over episodes as we have
    # them now in the training_step.
    episode = compute_bootstrap_value(episode)


def compute_bootstrap_value(episode: _Episode, module: MultiAgentRLModule) -> _Episode:
    if episode.is_terminated:
        last_r = 0.0
    else:
        # TODO (simon): This has to be made multi-agent ready.
        initial_states = module[DEFAULT_POLICY_ID].get_initial_state()
        state = {
            k: initial_states[k] if episode.states is None else episode.states[k]
            for k in initial_states.keys()
        }

        input_dict = {
            STATE_IN: tf.convert_to_tensor(state),
            SampleBatch.OBS: tf.convert_to_tensor(),
        }

        # TODO (simon): Torch might need the correct device.

        # TODO (sven): If we want to get rid of the policy in the future
        # what should we do for adding the time dimension?
        # TODO (simon): Add support for recurrent models.

        input_dict = NestedDict(input_dict)
        fwd_out = module[DEFAULT_POLICY_ID].forward_exploration(input_dict)
        # TODO (simon): Remove time dimension in case of recurrent model.
        last_r = fwd_out[SampleBatch.VF_PREDS][-1]

    vf_preds = np.array(fwd_out[SampleBatch.VF_PREDS])
    # TODO (simon): Squeeze out the time dimension in case of recurrent model.
    episode.extra_model_outputs[SampleBatch.VALUES_BOOTSTRAPPED] = np.concatenate(
        [
            convert_to_numpy(vf_preds[1:]),
            np.array([convert_to_numpy(last_r)], dtype=np.float32),
        ],
        axis=0,
    )

    # TODO (simon): Unsqueeze in case of recurrent model.

    return episode
