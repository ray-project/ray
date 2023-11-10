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


def compute_advantages_old(
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

    # Reshape `advantages` back to shape=(B, T) if necessary.
    if time_rank:
        advantages = advantages.reshape(orig_shape)

    return advantages


def compute_advantages(values, rewards, terminateds, truncateds, gamma, lambda_):
    # The first reward is irrelevant (not used for any VF target).
    #rewards_t1_to_H_BxT = rewards_t0_to_H_BxT[1:]
    values *= (1.0 - terminateds)

    # In all the following, when building value targets for t=1 to T=H,
    # exclude rewards & continues for t=1 b/c we don't need r1 or c1.
    # The target (R1) for V1 is built from r2, c2, and V2/R2.
    continues = (1.0 - terminateds[1:])
    discount = continues * gamma  # shape=[2-16, BxT]
    #truncateds = c
    intermediates = (rewards[:-1] + discount * (1 - lambda_) * values[1:])
    # intermediates.shape=[2-16, BxT]

    # Loop through reversed timesteps (axis=1) from T+1 to t=2.
    Rs = [values[-1]]  # Rs indices=[16]
    for t in reversed(range(discount.shape[0])):
        Rs.append(intermediates[t] + (1.0 - truncateds[1:][t]) * discount[t] * lambda_ * Rs[-1])

    # Reverse along time axis and cut the last entry (value estimate at very end
    # cannot be learnt from as it's the same as the ... well ... value estimate).
    targets = np.stack(list(reversed(Rs)), axis=0)
    # targets.shape=[t0 to H-1,BxT]
    advantages = targets - values

    return advantages, targets
