from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray

from ray.rllib.ppo.utils import concatenate


def rollouts(policy, env, horizon, observation_filter, reward_filter):
    """Perform a batch of rollouts of a policy in an environment.

    Args:
        policy: The policy that will be rollout out. Can be an arbitrary object
            that supports a compute_actions(observation) function.
        env: The environment the rollout is computed in. Needs to support the
            OpenAI gym API and needs to support batches of data.
        horizon: Upper bound for the number of timesteps for each rollout in
            the batch.
        observation_filter: Function that is applied to each of the
            observations.
        reward_filter: Function that is applied to each of the rewards.

    Returns:
        A trajectory, which is a dictionary with keys "observations",
            "rewards", "orig_rewards", "actions", "logprobs", "dones". Each
            value is an array of shape (num_timesteps, env.batchsize, shape).
    """

    observation = observation_filter(env.reset())
    done = np.array(env.batchsize * [False])
    t = 0
    observations = []  # Filtered observations
    raw_rewards = []   # Empirical rewards
    actions = []  # Actions sampled by the policy
    logprobs = []  # Last layer of the policy network
    vf_preds = []  # Value function predictions
    dones = []  # Has this rollout terminated?

    while True:
        action, logprob, vfpred = policy.compute(observation)
        vf_preds.append(vfpred)
        observations.append(observation[None])
        actions.append(action[None])
        logprobs.append(logprob[None])
        observation, raw_reward, done = env.step(action)
        observation = observation_filter(observation)
        raw_rewards.append(raw_reward[None])
        dones.append(done[None])
        t += 1
        if done.all() or t >= horizon:
            break

    return {"observations": np.vstack(observations),
            "raw_rewards": np.vstack(raw_rewards),
            "actions": np.vstack(actions),
            "logprobs": np.vstack(logprobs),
            "vf_preds": np.vstack(vf_preds),
            "dones": np.vstack(dones)}


def add_return_values(trajectory, gamma, reward_filter):
    rewards = trajectory["raw_rewards"]
    dones = trajectory["dones"]
    returns = np.zeros_like(rewards)
    last_return = np.zeros(rewards.shape[1], dtype="float32")

    for t in reversed(range(len(rewards) - 1)):
        last_return = rewards[t, :] * (1 - dones[t, :]) + gamma * last_return
        returns[t, :] = last_return
        reward_filter(returns[t, :])

    trajectory["returns"] = returns


def add_advantage_values(trajectory, gamma, lam, reward_filter):
    rewards = trajectory["raw_rewards"]
    vf_preds = trajectory["vf_preds"]
    dones = trajectory["dones"]
    advantages = np.zeros_like(rewards)
    last_advantage = np.zeros(rewards.shape[1], dtype="float32")

    for t in reversed(range(len(rewards) - 1)):
        delta = rewards[t, :] * (1 - dones[t, :]) + \
            gamma * vf_preds[t+1, :] * (1 - dones[t+1, :]) - vf_preds[t, :]
        last_advantage = \
            delta + gamma * lam * last_advantage * (1 - dones[t+1, :])
        advantages[t, :] = last_advantage
        reward_filter(advantages[t, :])

    trajectory["advantages"] = advantages
    trajectory["td_lambda_returns"] = \
        trajectory["advantages"] + trajectory["vf_preds"]


def collect_samples(agents,
                    config,
                    observation_filter,
                    reward_filter):
    num_timesteps_so_far = 0
    trajectories = []
    total_rewards = []
    trajectory_lengths = []
    # This variable maps the object IDs of trajectories that are currently
    # computed to the agent that they are computed on; we start some initial
    # tasks here.
    agent_dict = {agent.compute_steps.remote(
                      config["gamma"], config["lambda"],
                      config["horizon"], config["min_steps_per_task"],
                      observation_filter, reward_filter):
                  agent for agent in agents}
    while num_timesteps_so_far < config["timesteps_per_batch"]:
        # TODO(pcm): Make wait support arbitrary iterators and remove the
        # conversion to list here.
        [next_trajectory], waiting_trajectories = ray.wait(
            list(agent_dict.keys()))
        agent = agent_dict.pop(next_trajectory)
        # Start task with next trajectory and record it in the dictionary.
        agent_dict[agent.compute_steps.remote(
                       config["gamma"], config["lambda"],
                       config["horizon"], config["min_steps_per_task"],
                       observation_filter, reward_filter)] = (
            agent)
        trajectory, rewards, lengths, obs_f, rew_f = ray.get(next_trajectory)
        total_rewards.extend(rewards)
        trajectory_lengths.extend(lengths)
        num_timesteps_so_far += len(trajectory["dones"])
        trajectories.append(trajectory)
        observation_filter.update(obs_f)
        reward_filter.update(rew_f)
    return (concatenate(trajectories), np.mean(total_rewards),
            np.mean(trajectory_lengths))
