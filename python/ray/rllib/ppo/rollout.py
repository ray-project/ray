from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray

from ray.rllib.ppo.utils import concatenate


def collect_samples(agents,
                    config,
                    local_evaluator):
    num_timesteps_so_far = 0
    trajectories = []
    total_rewards = []
    trajectory_lengths = []
    # This variable maps the object IDs of trajectories that are currently
    # computed to the agent that they are computed on; we start some initial
    # tasks here.

    agent_dict = {}

    for agent in agents:
        future_sample = agent.sample.remote()
        future_filters = agent.get_filters.remote(flush_after=True)
        agent_dict[future_sample] = (agent, future_filters)

    while num_timesteps_so_far < config["timesteps_per_batch"]:
        # TODO(pcm): Make wait support arbitrary iterators and remove the
        # conversion to list here.
        [next_sample], _ = ray.wait(list(agent_dict))
        agent, future_filters = agent_dict.pop(next_sample)
        obs_filter, rew_filter = ray.get(future_filters)
        # Start task with next trajectory and record it in the dictionary.
        local_evaluator.merge_filters(obs_filter, rew_filter)
        agent.sync_filters.remote(*local_evaluator.get_filters())
        future_sample = agent.sample.remote()
        future_filters = agent.get_filters.remote(flush_after=True)
        agent_dict[future_sample] = (agent, future_filters)

        trajectory = ray.get(next_sample)
        # TODO(rliaw)
        print("implement trajectory counting")
        raise NotImplementedError
        num_timesteps_so_far += sum(length)
        trajectories.append(trajectory)
    return (concatenate(trajectories))
