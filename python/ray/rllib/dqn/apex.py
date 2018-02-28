from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.dqn.dqn import DQNAgent, DEFAULT_CONFIG as DQN_CONFIG

APEX_DEFAULT_CONFIG = dict(DQN_CONFIG, **dict(
    optimizer_class="ApexOptimizer",
    optimizer_config=dict(DQN_CONFIG["optimizer_config"], **dict(
        max_weight_sync_delay=400,
        num_replay_buffer_shards=4,
    )),
    n_step=3,
    num_workers=32,
    buffer_size=2000000,
    learning_starts=50000,
    train_batch_size=512,
    sample_batch_size=50,
    max_weight_sync_delay=400,
    target_network_update_freq=500000,  # consider reducing for small clusters
    timesteps_per_iteration=25000,
    per_worker_exploration=True,
    worker_side_prioritization=True,
    force_evaluators_remote=False,  # consider enabling for large clusters
))


class ApexAgent(DQNAgent):
    """DQN variant that uses the Ape-X distributed policy optimizer.

    By default, this is configured for a large single node (32 cores). For
    running in a large cluster, increase `num_workers` and consider setting
    `force_evaluators_remote` to move workers off of the head node.
    """

    _agent_name = "APEX"
    _default_config = APEX_DEFAULT_CONFIG
