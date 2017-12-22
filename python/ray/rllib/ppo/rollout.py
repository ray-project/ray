from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import ray

from ray.rllib.optimizers import SampleBatch


def collect_samples(agents,
                    config,
                    local_evaluator):
    num_timesteps_so_far = 0
    trajectories = []
    # This variable maps the object IDs of trajectories that are currently
    # computed to the agent that they are computed on; we start some initial
    # tasks here.

    agent_dict = {}

    for agent in agents:
        fut_sample, fut_info = agent.sample.remote()
        agent_dict[fut_sample] = (agent, fut_info)

    while num_timesteps_so_far < config["timesteps_per_batch"]:
        # TODO(pcm): Make wait support arbitrary iterators and remove the
        # conversion to list here.
        [fut_sample], _ = ray.wait(list(agent_dict))
        agent, fut_info = agent_dict.pop(fut_sample)
        info = ray.get(fut_info)
        # Start task with next trajectory and record it in the dictionary.
        local_evaluator.merge_filters(info["obs_filter"], info["rew_filter"])
        agent.sync_filters.remote(*local_evaluator.get_filters())
        fut_sample, fut_info = agent.sample.remote()
        agent_dict[fut_sample] = (agent, fut_info)

        next_sample = ray.get(fut_sample)
        num_timesteps_so_far += next_sample.count
        trajectories.append(next_sample)
    return SampleBatch.concat(trajectories)
