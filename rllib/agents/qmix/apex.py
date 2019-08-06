"""Experimental: scalable Ape-X variant of QMIX"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.dqn.apex import APEX_TRAINER_PROPERTIES
from ray.rllib.agents.qmix.qmix import QMixTrainer, \
    DEFAULT_CONFIG as QMIX_CONFIG
from ray.rllib.utils import merge_dicts

APEX_QMIX_DEFAULT_CONFIG = merge_dicts(
    QMIX_CONFIG,  # see also the options in qmix.py, which are also supported
    {
        "optimizer": merge_dicts(
            QMIX_CONFIG["optimizer"],
            {
                "max_weight_sync_delay": 400,
                "num_replay_buffer_shards": 4,
                "batch_replay": True,  # required for RNN. Disables prio.
                "debug": False
            }),
        "num_gpus": 0,
        "num_workers": 32,
        "buffer_size": 2000000,
        "learning_starts": 50000,
        "train_batch_size": 512,
        "sample_batch_size": 50,
        "target_network_update_freq": 500000,
        "timesteps_per_iteration": 25000,
        "per_worker_exploration": True,
        "min_iter_time_s": 30,
    },
)

ApexQMixTrainer = QMixTrainer.with_updates(
    name="APEX_QMIX",
    default_config=APEX_QMIX_DEFAULT_CONFIG,
    **APEX_TRAINER_PROPERTIES)
