from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import ray
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.memory import ray_get_and_free

logger = logging.getLogger(__name__)


def collect_samples(agents, sample_batch_size, num_envs_per_worker,
                    train_batch_size):
    """Collects at least train_batch_size samples, never discarding any."""

    num_timesteps_so_far = 0
    trajectories = []
    agent_dict = {}

    for agent in agents:
        fut_sample = agent.sample.remote()
        agent_dict[fut_sample] = agent

    while agent_dict:
        [fut_sample], _ = ray.wait(list(agent_dict))
        agent = agent_dict.pop(fut_sample)
        next_sample = ray_get_and_free(fut_sample)
        assert next_sample.count >= sample_batch_size * num_envs_per_worker
        num_timesteps_so_far += next_sample.count
        trajectories.append(next_sample)

        # Only launch more tasks if we don't already have enough pending
        pending = len(agent_dict) * sample_batch_size * num_envs_per_worker
        if num_timesteps_so_far + pending < train_batch_size:
            fut_sample2 = agent.sample.remote()
            agent_dict[fut_sample2] = agent

    return SampleBatch.concat_samples(trajectories)


def collect_samples_straggler_mitigation(agents, train_batch_size):
    """Collects at least train_batch_size samples.

    This is the legacy behavior as of 0.6, and launches extra sample tasks to
    potentially improve performance but can result in many wasted samples.
    """

    num_timesteps_so_far = 0
    trajectories = []
    agent_dict = {}

    for agent in agents:
        fut_sample = agent.sample.remote()
        agent_dict[fut_sample] = agent

    while num_timesteps_so_far < train_batch_size:
        # TODO(pcm): Make wait support arbitrary iterators and remove the
        # conversion to list here.
        [fut_sample], _ = ray.wait(list(agent_dict))
        agent = agent_dict.pop(fut_sample)
        # Start task with next trajectory and record it in the dictionary.
        fut_sample2 = agent.sample.remote()
        agent_dict[fut_sample2] = agent

        next_sample = ray_get_and_free(fut_sample)
        num_timesteps_so_far += next_sample.count
        trajectories.append(next_sample)

    logger.info("Discarding {} sample tasks".format(len(agent_dict)))
    return SampleBatch.concat_samples(trajectories)
