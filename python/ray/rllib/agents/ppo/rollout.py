from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.evaluation.sample_batch import SampleBatch


def collect_samples(agents, train_batch_size, wait_for_stragglers):
    num_timesteps_so_far = 0
    trajectories = []
    # This variable maps the object IDs of trajectories that are currently
    # computed to the agent that they are computed on; we start some initial
    # tasks here.

    agent_dict = {}

    for agent in agents:
        fut_sample = agent.sample.remote()
        agent_dict[fut_sample] = agent

    while agents:
        [fut_sample], _ = ray.wait(list(agent_dict))
        agent = agent_dict.pop(fut_sample)
        next_sample = ray.get(fut_sample)
        num_timesteps_so_far += next_sample.count
        trajectories.append(next_sample)

        # Always wait for at least the first wave to finish
        if num_timesteps_so_far >= train_batch_size and \
                len(trajectories) >= len(agents):
            break

        if num_timesteps_so_far < train_batch_size:
            # Start task with next trajectory and record it in the dictionary.
            fut_sample2 = agent.sample.remote()
            agent_dict[fut_sample2] = agent

    return SampleBatch.concat_samples(trajectories)
