from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray

from ray.rllib.ppo.filter import NoFilter
from ray.rllib.ppo.utils import concatenate
from ray.rllib.ppo.env import BatchedEnv


def rollouts(policy, env, horizon, observation_filter=NoFilter(),
             reward_filter=NoFilter()):
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


def partial_rollouts(policy, env, last_observation,
                     steps, observation_filter=NoFilter(),
                     reward_filter=NoFilter()):
    """Perform a batch of rollouts of a policy in an environment.

    Args:
        policy: The policy that will be rollout out. Can be an arbitrary object
            that supports a compute_actions(observation) function.
        env: The environment the rollout is computed in. Needs to support the
            OpenAI gym API and needs to support batches of data.
        last_observation: For continuing truncated rollout
        steps: Number of steps to proceed in rollout
        observation_filter: Function that is applied to each of the
            observations.
        reward_filter: Function that is applied to each of the rewards.

    Returns:
        A trajectory, which is a dictionary with keys "observations",
            "rewards", "orig_rewards", "actions", "logprobs", "dones". Each
            value is an array of shape (num_timesteps, env.batchsize, shape).
    """
    # TODO (rliaw): Would be nice to have as an iterator to store intermediate
    # TODO (rliaw): Can also take in horizon to prevent
    assert False
    if type(env) == BatchedEnv and env.batchsize > 1:
        # Only Last_observation in batched setting is not implemented
        assert False, "No support for multi-batch case"
    observation = env.reset() if last_observation is None else last_observation
    observation = observation_filter(observation)
    # done = np.array(env.batchsize * [False])
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
        if (done.all()) or t >= steps:
            break

    last_observation = observation if not done.all() else None
    truncation_vf = policy.compute(observation)[2] * (1 - done)
    vf_preds.append(truncation_vf)

    # to make add_advantage work without weird edge cases
    dones.append(done[None])
    raw_rewards.append(raw_reward[None])

    return {"observations": np.vstack(observations),
            "raw_rewards": np.vstack(raw_rewards),
            "actions": np.vstack(actions),
            "logprobs": np.vstack(logprobs),
            "vf_preds": np.vstack(vf_preds),
            "dones": np.vstack(dones),
            "last_observation": last_observation}


def continuous_partial_rollouts(policy, env, observation,
                     steps, observation_filter=NoFilter(),
                     reward_filter=NoFilter()):
    """Perform a batch of rollouts of a policy in an environment.

    Args:
        policy: The policy that will be rollout out. Can be an arbitrary object
            that supports a compute_actions(observation) function.
        env: The environment the rollout is computed in. Needs to support the
            OpenAI gym API and needs to support batches of data.
        last_observation: For continuing truncated rollout
        steps: Number of steps to proceed in rollout
        observation_filter: Function that is applied to each of the
            observations.
        reward_filter: Function that is applied to each of the rewards.

    Returns:
        A trajectory, which is a dictionary with keys "observations",
            "rewards", "orig_rewards", "actions", "logprobs", "dones". Each
            value is an array of shape (num_timesteps, env.batchsize, shape).
    """
    # TODO (rliaw): Would be nice to have as an iterator to store intermediate
    # TODO (rliaw): PREVENT BatCHED ENVS

    if type(env) == BatchedEnv and env.batchsize > 1:
        # Only Last_observation in batched setting is not implemented
        assert False, "No support for multi-batch case"
    # done = np.array(env.batchsize * [False])
    observations = []  # Filtered observations
    raw_rewards = []   # Empirical rewards
    actions = []  # Actions sampled by the policy
    logprobs = []  # Last layer of the policy network
    vf_preds = []  # Value function predictions
    dones = []  # Has this rollout terminated?

    for t in range(steps):
        observation = env.reset() if observation is None else observation
        observation = observation_filter(observation)

        action, logprob, vfpred = policy.compute(observation)
        vf_preds.append(vfpred)
        observations.append(observation[None])
        actions.append(action[None])
        logprobs.append(logprob[None])

        observation, raw_reward, done = env.step(action)
        raw_rewards.append(raw_reward[None])
        dones.append(done[None])
        if (done[0]):
            observation = None

    last_observation = observation
    truncation_vf = policy.compute(observation)[2] if observation is not None else 0
    vf_preds.append(truncation_vf)

    return {"observations": np.vstack(observations),
            "raw_rewards": np.vstack(raw_rewards),
            "actions": np.vstack(actions),
            "logprobs": np.vstack(logprobs),
            "vf_preds": np.vstack(vf_preds),
            "dones": np.vstack(dones),
            "last_observation": last_observation}

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


def add_trunc_advantage_values(trajectory, gamma, lam, reward_filter):
    assert False
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

    trajectory["dones"] = dones[:-1, :]  # hack to get bootstrap running
    trajectory["raw_rewards"] = rewards[:-1, :]  # hack
    trajectory["vf_preds"] = vf_preds[:-1, :]  # hack to get bootstrap running
    trajectory["advantages"] = advantages[:-1, :] # just a shape change
    trajectory["td_lambda_returns"] = \
        trajectory["advantages"] + trajectory["vf_preds"]

def add_multitrunc_values(trajectory, gamma, lam, reward_filter):
    rewards = trajectory["raw_rewards"]
    vf_preds = trajectory["vf_preds"]
    dones = trajectory["dones"]
    advantages = np.zeros_like(rewards)
    assert len(rewards) < len(vf_preds)
    # Indices where trajectories Terminate. Len(rewards) - 1 = "end of trajectory"
    idxs = [-1] + np.where(trajectory['dones'])[0].tolist() + [len(rewards) - 1]

    # NOTE! The usage of dones here is very different from before!
    # if trajectory['dones'][-1, :].all(): # Test for edge case!
    #     import ipdb; ipdb.set_trace()
    for i in range(len(idxs) - 1):
        last_advantage = np.zeros(rewards.shape[1], dtype="float32")
        # trajectories start 1 after the last DONE
        for t in reversed(range(idxs[i]+1, idxs[i+1] + 1)):
            delta = rewards[t, :] + (1 - dones[t, :]) * gamma * vf_preds[t+1, :]  - vf_preds[t, :]
            last_advantage = delta + gamma * lam * last_advantage
            advantages[t, :] = last_advantage
            reward_filter(advantages[t, :])

    trajectory["vf_preds"] = trajectory["vf_preds"][:-1, :]
    trajectory["advantages"] = advantages
    trajectory["td_lambda_returns"] = \
        trajectory["advantages"] + trajectory["vf_preds"]


def collect_partial(agents,
                    config,
                    observation_filter=NoFilter(),
                    reward_filter=NoFilter()):
    num_timesteps_so_far = 0
    trajectories = []
    total_rewards = []
    trajectory_lengths = []
    # This variable maps the object IDs of trajectories that are currently
    # computed to the agent that they are computed on; we start some initial
    # tasks here.
    agent_dict = {agent.compute_partial_steps.remote(
                      config["gamma"], config["lambda"],
                      config["trunc_nstep"]):
                  agent for agent in agents}
    while num_timesteps_so_far < config["timesteps_per_batch"]:
        # TODO(pcm): Make wait support arbitrary iterators and remove the
        # conversion to list here.
        [next_trajectory], waiting_trajectories = ray.wait(
            list(agent_dict.keys()))
        agent = agent_dict.pop(next_trajectory)
        # Start task with next trajectory and record it in the dictionary.
        agent_dict[agent.compute_partial_steps.remote(
                       config["gamma"], config["lambda"],
                       config["trunc_nstep"])] = (
            agent)
        trajectory, rewards, lengths = ray.get(next_trajectory)

        # TODO(rliaw): make "total_reward" to only recall full rollouts?
        total_rewards.extend(rewards)
        trajectory_lengths.extend(lengths)
        num_timesteps_so_far += len(trajectory["observations"])
        trajectories.append(trajectory)
    return (concatenate(trajectories), np.nanmean(total_rewards),
            np.nanmean(trajectory_lengths))


def collect_samples(agents,
                    config,
                    observation_filter=NoFilter(),
                    reward_filter=NoFilter()):
    # TODO(rliaw): observation, reward filters are UNUSED
    num_timesteps_so_far = 0
    trajectories = []
    total_rewards = []
    trajectory_lengths = []
    # This variable maps the object IDs of trajectories that are currently
    # computed to the agent that they are computed on; we start some initial
    # tasks here.
    agent_dict = {agent.compute_steps.remote(
                      config["gamma"], config["lambda"],
                      config["horizon"], config["min_steps_per_task"]):
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
                       config["horizon"], config["min_steps_per_task"])] = (
            agent)
        trajectory, rewards, lengths = ray.get(next_trajectory)
        total_rewards.extend(rewards)
        trajectory_lengths.extend(lengths)
        num_timesteps_so_far += len(trajectory["dones"])
        trajectories.append(trajectory)
    return (concatenate(trajectories), np.mean(total_rewards),
            np.mean(trajectory_lengths))
