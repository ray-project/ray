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


def compute_advantages(
    values,
    rewards,
    terminateds,
    gamma: float = 0.9,
    lambda_: float = 1.0,
    standardize_advantages: bool = True,
):
    orig_shape = values.shape

    # Data has an extra time rank -> reshape everything into single sequence.
    time_rank = False
    if len(orig_shape) == 2:
        time_rank = True
        values = values.reshape((-1,))
        rewards = rewards.reshape((-1,))
        terminateds = terminateds.reshape((-1,))

    # Fix terminateds, which are shifted by one to the right (all episodes)
    # have been made one ts longer artificially.
    #terminateds = np.append(terminateds[1:], False)

    print("vpred_t=", values)  # TODO
    deltas = rewards + (1.0 - terminateds) * gamma * np.append(values[1:], 0.0) - values
    print("deltas=", deltas)
    advantages = np.zeros_like(rewards)

    #last_gae = 0.0
    #for t in reversed(range(len(rewards))):
    #    last_gae = deltas[t] + gamma * lambda_ * (1.0 - terminateds[t]) * last_gae
    #    advantages[t] = last_gae
    advantages = discount_cumsum(deltas, gamma * lambda_)


    # Standardize advantages.
    if standardize_advantages:
        advantages = (advantages - advantages.mean()) / max(1e-4, advantages.std())

    # Reshape `advantages` back to shape=(B, T) if necessary.
    if time_rank:
        advantages = advantages.reshape(orig_shape)

    return advantages


import scipy.signal
import numpy as np

def discount_cumsum_2(x, gamma, terminations):
    result = np.zeros_like(x)
    start_idx = 0

    for i, term in enumerate(terminations):
        if term or i == len(terminations) - 1:
            # Apply cumsum from start_idx to i (inclusive)
            segment = x[start_idx:i+1]
            cumsum_segment = scipy.signal.lfilter([1], [1, float(-gamma)], segment[::-1], axis=0)[::-1]
            result[start_idx:i+1] = cumsum_segment

            # Update start index
            start_idx = i + 1

    return result
