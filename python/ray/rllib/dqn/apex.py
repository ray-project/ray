from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import copy
from ray.rllib.dqn.dqn import DQNAgent, DEFAULT_CONFIG as DQN_CONFIG

APEX_DEFAULT_CONFIG = copy.deepcopy(DQN_CONFIG)
APEX_DEFAULT_CONFIG.update(dict(
    apex_optimizer=True,
    buffer_size=2000000,
    force_evaluators_remote=False,  # good to set to True on large clusters
    learning_starts=50000,
    max_weight_sync_delay=400,
    n_step=3,
    num_replay_buffer_shards=4,
    num_workers=32,
    sample_batch_size=50,
    target_network_update_freq=500000,  # consider reducing for small clusters
    timesteps_per_iteration=25000,
    train_batch_size=512,
))


class ApexAgent(DQNAgent):
    """DQN agent configured to run in Ape-X mode by default.

    By default, this is configured for a large single node (32 cores). For
    running in a large cluster, increase `num_workers` and consider setting
    `force_evaluators_remote` to move workers off of the head node.
    """

    _agent_name = "APEX"
    _default_config = APEX_DEFAULT_CONFIG
