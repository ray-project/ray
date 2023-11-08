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
    batch,
    gamma: float = 0.9,
    lambda_: float = 1.0,
    standardize_advantages: bool = True,
):
    values = batch[SampleBatch.VF_PREDS]
    rewards = batch[SampleBatch.REWARDS]
    terminals = batch[SampleBatch.TERMINATEDS]
    deltas = rewards + (1.0 - terminals) * gamma * np.append(values[1:], 0.0) - values
    advantages = np.zeros_like(rewards)

    last_gae = 0.0
    for t in reversed(range(len(rewards))):
        last_gae = deltas[t] + gamma * lambda_ * (1.0 - terminals[t]) * last_gae
        advantages[t] = last_gae

    # Standardize advantages.
    if standardize_advantages:
        advantages = (advantages - advantages.mean()) / max(1e-4, advantages.std())

    return advantages
