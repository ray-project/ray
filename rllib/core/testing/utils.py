from typing import List, Tuple
import gym
import tensorflow as tf
import torch
import numpy as np


from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.numpy import convert_to_numpy


def do_rollouts(
    env: gym.Env,
    module_for_inference: RLModule,
    num_rollouts: int,
    framework: str = "tf",
) -> Tuple[float, List[float], List[SampleBatch]]:
    """Do rollouts of moduke_for_inference in env.

    Args:
        env: The environment to rollout in.
        module_for_inference: The module to use for inference.
        num_rollouts: The number of rollouts to do.
        framework: The framework to use for inference.

    Returns:
        The mean return across num_rollouts, the return at each timestep, and
            the rollouts.
    """
    _returns = []
    _rollouts = []
    for _ in range(num_rollouts):
        obs = env.reset()
        _obs, _next_obs, _actions, _rewards, _dones = [], [], [], [], []
        _return = -0
        for _ in range(env._max_episode_steps):
            _obs.append(obs)
            if framework == "tf":
                fwd_out = module_for_inference.forward_inference(
                    {"obs": tf.convert_to_tensor([obs], dtype=tf.float32)}
                )
            elif framework == "torch":
                fwd_out = module_for_inference.forward_inference(
                    {"obs": torch.tensor(obs)[None]}
                )
            else:
                raise ValueError(f"Unknown framework: {framework}")
            action = convert_to_numpy(fwd_out["action_dist"].sample())[0]
            next_obs, reward, done, _ = env.step(action)
            _next_obs.append(next_obs)
            _actions.append([action])
            _rewards.append([reward])
            _dones.append([done])
            _return += reward
            if done:
                break
            obs = next_obs
        batch = SampleBatch(
            {
                "obs": _obs,
                "next_obs": _next_obs,
                "actions": _actions,
                "rewards": _rewards,
                "dones": _dones,
            }
        )
        _returns.append(_return)
        _rollouts.append(batch)
    return np.mean(_returns), _returns, _rollouts
