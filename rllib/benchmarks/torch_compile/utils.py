from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.algorithms.ppo.torch.ppo_torch_rl_module import PPOTorchRLModule
from ray.rllib.algorithms.ppo.ppo_catalog import PPOCatalog
from ray.rllib.core.rl_module.rl_module import SingleAgentRLModuleSpec
from ray.rllib.core.rl_module.torch.torch_rl_module import TorchCompileConfig
from ray.rllib.models.catalog import MODEL_DEFAULTS
from ray.rllib.core.learner.learner import (
    FrameworkHyperparameters,
    LearnerHyperparameters,
)
from ray.rllib.core.learner.scaling_config import LearnerGroupScalingConfig
from ray.rllib.core.testing.utils import get_module_spec
from ray.rllib.algorithms.ppo.torch.ppo_torch_learner import PPOTorchLearner
from ray.rllib.utils.test_utils import check
from ray.rllib.algorithms.ppo.ppo import PPOConfig
from ray.rllib.env.wrappers.atari_wrappers import wrap_deepmind
from ray.rllib.utils.torch_utils import convert_to_torch_tensor
import numpy as np
import matplotlib.pyplot as plt
import numpy as np
from typing import Union
import pandas as pd
import gymnasium as gym
import torch
import seaborn as sns
import torch._dynamo as dynamo


def get_ppo_batch_for_env(env: Union[str, gym.Env], batch_size):
    """Create a dummy sample batch for the given environment.

    Args:
        env: The environment to create a sample batch for. If a string is given,
        it is assumed to be a gym environment ID.
        batch_size: The batch size to use.

    Returns:
        A sample batch for the given environment.
    """
    env.reset()
    action = env.action_space.sample()
    if type(env.action_space) is gym.spaces.Box:
        action_inputs = [0.5, 0.5]
    else:
        action_inputs = [0.5]
    obs, reward, truncated, terminated, info = env.step(action)

    def batchify(x):
        x = np.array(x)
        return np.repeat(x[np.newaxis], batch_size, axis=0)

    # Fake CartPole episode of n time steps.
    return SampleBatch(
        {
            SampleBatch.OBS: batchify(obs),
            SampleBatch.NEXT_OBS: batchify(obs),
            SampleBatch.ACTIONS: batchify([action]),
            SampleBatch.PREV_ACTIONS: batchify([action]),
            SampleBatch.REWARDS: batchify([reward]),
            SampleBatch.PREV_REWARDS: batchify([reward]),
            SampleBatch.TERMINATEDS: batchify([terminated]),
            SampleBatch.TRUNCATEDS: batchify([truncated]),
            SampleBatch.VF_PREDS: batchify(0.0),
            SampleBatch.ACTION_DIST_INPUTS: batchify(action_inputs),
            SampleBatch.ACTION_LOGP: batchify(0.0),
            SampleBatch.EPS_ID: batchify(0),
            "advantages": batchify(0.0),
            "value_targets": batchify(0.0),
            SampleBatch.AGENT_INDEX: batchify(0),
        }
    )


def timed(fn, no_grad=True):
    start = torch.cuda.Event(enable_timing=True)
    end = torch.cuda.Event(enable_timing=True)
    start.record()
    if no_grad:
        with torch.no_grad():
            result = fn()
    else:
        result = fn()
    end.record()
    torch.cuda.synchronize()
    return result, start.elapsed_time(end) / 1000
