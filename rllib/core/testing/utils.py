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
    returns = []
    rollouts = []
    for _ in range(num_rollouts):
        obs = env.reset()
        obs_list, next_obs_list, actions_list, rewards_list, dones_list = (
            [],
            [],
            [],
            [],
            [],
        )
        ret = -0
        for _ in range(env._max_episode_steps):
            obs_list.append(obs)
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
            next_obs_list.append(next_obs)
            actions_list.append([action])
            rewards_list.append([reward])
            dones_list.append([done])
            ret += reward
            if done:
                break
            obs = next_obs
        batch = SampleBatch(
            {
                "obs": obs_list,
                "next_obs": next_obs_list,
                "actions": actions_list,
                "rewards": rewards_list,
                "dones": dones_list,
            }
        )
        returns.append(ret)
        rollouts.append(batch)
    return np.mean(returns), returns, rollouts
