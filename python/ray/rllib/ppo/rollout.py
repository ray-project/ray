from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray

from ray.rllib.optimizers import SampleBatch


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
                      config, observation_filter, reward_filter):
                  agent for agent in agents}
    while num_timesteps_so_far < config["timesteps_per_batch"]:
        # TODO(pcm): Make wait support arbitrary iterators and remove the
        # conversion to list here.
        [next_trajectory], _ = ray.wait(list(agent_dict))
        agent = agent_dict.pop(next_trajectory)
        # Start task with next trajectory and record it in the dictionary.
        agent_dict[agent.compute_steps.remote(
                      config, observation_filter, reward_filter)] = agent
        trajectory, rewards, lengths, obs_f, rew_f = ray.get(next_trajectory)
        total_rewards.extend(rewards)
        trajectory_lengths.extend(lengths)
        num_timesteps_so_far += sum(lengths)
        trajectories.append(trajectory)
        observation_filter.update(obs_f)
        reward_filter.update(rew_f)
    return (SampleBatch.concat_samples(trajectories), np.mean(total_rewards),
            np.mean(trajectory_lengths))
