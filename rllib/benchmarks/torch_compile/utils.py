import time
from typing import Union

import gymnasium as gym
import numpy as np
import torch

from ray.rllib.policy.sample_batch import SampleBatch


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


def timed(fn, no_grad=True, use_cuda=True):
    def run_fn():
        if no_grad:
            with torch.no_grad():
                result = fn()
        else:
            result = fn()
        return result

    if use_cuda:
        start = torch.cuda.Event(enable_timing=True)
        end = torch.cuda.Event(enable_timing=True)
        start.record()
        result = run_fn()
        end.record()
        torch.cuda.synchronize()
        delta_t = start.elapsed_time(end) / 1000
    else:
        start = time.time()
        result = run_fn()
        end = time.time()
        delta_t = end - start

    return result, delta_t
