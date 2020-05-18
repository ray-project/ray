import logging

import ray
from ray.rllib.policy.sample_batch import SampleBatch

logger = logging.getLogger(__name__)


def collect_samples(agents, rollout_fragment_length, num_envs_per_worker,
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
        next_sample = ray.get(fut_sample)
        num_timesteps_so_far += next_sample.count
        trajectories.append(next_sample)

        # Only launch more tasks if we don't already have enough pending
        pending = len(
            agent_dict) * rollout_fragment_length * num_envs_per_worker
        if num_timesteps_so_far + pending < train_batch_size:
            fut_sample2 = agent.sample.remote()
            agent_dict[fut_sample2] = agent

    return SampleBatch.concat_samples(trajectories)
