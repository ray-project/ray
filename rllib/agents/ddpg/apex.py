from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.dqn.apex import APEX_TRAINER_PROPERTIES
from ray.rllib.agents.ddpg.ddpg import DDPGTrainer, \
    DEFAULT_CONFIG as DDPG_CONFIG
from ray.rllib.utils import merge_dicts

APEX_DDPG_DEFAULT_CONFIG = merge_dicts(
    DDPG_CONFIG,  # see also the options in ddpg.py, which are also supported
    {
        "optimizer": merge_dicts(
            DDPG_CONFIG["optimizer"], {
                "max_weight_sync_delay": 400,
                "num_replay_buffer_shards": 4,
                "debug": False
            }),
        "n_step": 3,
        "num_gpus": 0,
        "num_workers": 32,
        "buffer_size": 2000000,
        "learning_starts": 50000,
        "train_batch_size": 512,
        "sample_batch_size": 50,
        "target_network_update_freq": 500000,
        "timesteps_per_iteration": 25000,
        "per_worker_exploration": True,
        "worker_side_prioritization": True,
        "min_iter_time_s": 30,
    },
)

ApexDDPGTrainer = DDPGTrainer.with_updates(
    name="APEX_DDPG",
    default_config=APEX_DDPG_DEFAULT_CONFIG,
    **APEX_TRAINER_PROPERTIES)
