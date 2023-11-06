from typing import List

import numpy as np
import tree  # pip install dm_tree

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.models.base import STATE_IN
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.evaluation.postprocessing import discount_cumsum, Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import DeveloperAPI
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.nested_dict import NestedDict
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
from ray.rllib.utils.typing import TensorType

_, tf, _ = try_import_tf()


@DeveloperAPI
def compute_gae_for_episode(
    episode: SingleAgentEpisode,
    gamma: float,
    lambda_: float,
    use_gae: bool,
    use_critic: bool,
    rl_module: RLModule,
):
    """Adds GAE to a trajectory."""
    # TODO (simon): All of this can be batched over multiple episodes.
    # This should increase performance.
    # TODO (sven): Shall do postprocessing in the training_step or
    # in the env_runner? Here we could batch over episodes as we have
    # them now in the training_step.
    episode = compute_bootstrap_value_for_episode(episode, rl_module)

    vf_preds = episode.extra_model_outputs[SampleBatch.VF_PREDS]
    rewards = episode.rewards

    # TODO (simon): In case of recurrent models sequeeze out time dimension.

    episode = compute_advantages_for_episode(
        episode,
        last_r=episode.extra_model_outputs[SampleBatch.VALUES_BOOTSTRAPPED][-1],
        gamma=gamma,
        lambda_=lambda_,
        use_gae=use_gae,
        use_critic=use_critic,
        vf_preds=vf_preds,
        rewards=rewards,
    )

    # TODO (simon): Add dimension in case of recurrent model.
    return episode


def compute_bootstrap_value_for_episode(
    episode: SingleAgentEpisode, rl_module: RLModule
) -> SingleAgentEpisode:
    if episode.is_terminated:
        last_r = 0.0
    else:
        # TODO (simon): This has to be made multi-agent ready.
        initial_states = rl_module.get_initial_state()
        state = {
            k: initial_states[k] if episode.states is None else episode.states[k]
            for k in initial_states.keys()
        }

        input_dict = {
            STATE_IN: tree.map_structure(
                lambda s: convert_to_torch_tensor(s)
                if rl_module.framework == "torch"
                else tf.convert_to_tensor(s),
                state,
            ),
            SampleBatch.OBS: convert_to_torch_tensor(
                np.expand_dims(episode.observations[-1], axis=0)
            )
            if rl_module.framework == "torch"
            else tf.convert_to_tensor(np.expand_dims(episode.observations[-1], axis=0)),
        }

        # TODO (simon): Torch might need the correct device.
        # TODO (simon): Add support for recurrent models.

        fwd_out = rl_module.forward_exploration(input_dict)
        # TODO (simon): Remove time dimension in case of recurrent model.
        last_r = fwd_out[SampleBatch.VF_PREDS][-1]

    vf_preds = episode.extra_model_outputs[SampleBatch.VF_PREDS]
    # TODO (simon): Squeeze out the time dimension in case of recurrent model.
    episode.extra_model_outputs[SampleBatch.VALUES_BOOTSTRAPPED] = np.concatenate(
        [
            vf_preds[1:],
            np.array([convert_to_numpy(last_r)], dtype=np.float32),
        ],
        axis=0,
    )

    # TODO (simon): Unsqueeze in case of recurrent model.

    return episode


def compute_advantages_for_episode(
    episode: SingleAgentEpisode,
    last_r: float,
    gamma: float = 0.9,
    lambda_: float = 1.0,
    use_critic: bool = True,
    use_gae: bool = True,
    rewards: TensorType = None,
    vf_preds: TensorType = None,
):
    assert (
        SampleBatch.VF_PREDS in episode.extra_model_outputs or not use_critic
    ), "use_critic=True but values not found"
    assert use_critic or not use_gae, "Can't use gae without using a value function."
    # TODO (simon): Check if we need conversion here.
    last_r = convert_to_numpy(last_r)

    if rewards is None:
        rewards = episode.rewards
    if vf_preds is None:
        vf_preds = episode.extra_model_outs[SampleBatch.VF_PREDS]

    if use_gae:
        vpred_t = np.concatenate([vf_preds, np.array([last_r])])
        delta_t = rewards + gamma * vpred_t[1:] - vpred_t[:-1]
        # This formula for the advantage comes from:
        # Generalized Advantage Estimation": https://arxiv.org/abs/1506.02438
        episode.extra_model_outputs[Postprocessing.ADVANTAGES] = discount_cumsum(
            delta_t, gamma * lambda_
        )
        episode.extra_model_outputs[Postprocessing.VALUE_TARGETS] = (
            episode.extra_model_outputs[Postprocessing.ADVANTAGES] + vf_preds
        ).astype(np.float32)
    else:
        rewards_plus_v = np.concatenate([rewards, np.array([last_r])])
        discounted_returns = discount_cumsum(rewards_plus_v, gamma)[:-1].astype(
            np.float32
        )

        if use_critic:
            episode.extra_model_outputs[Postprocessing.ADVANTAGES] = (
                discounted_returns - vf_preds
            )
            episode.extra_model_outputs[
                Postprocessing.VALUE_TARGETS
            ] = discounted_returns
        else:
            episode.extra_model_outputs[Postprocessing.ADVANTAGES] = discounted_returns
            episode.extra_model_outputs[Postprocessing.VALUE_TARGETS] = np.zeros_like(
                episode.extra_model_outputs[Postprocessing.ADVANTAGES]
            )

    episode.extra_model_outputs[
        Postprocessing.ADVANTAGES
    ] = episode.extra_model_outputs[Postprocessing.ADVANTAGES].astype(np.float32)

    return episode
