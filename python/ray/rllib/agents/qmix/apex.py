"""Experimental: scalable Ape-X variant of QMIX"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.qmix.qmix import QMixAgent, DEFAULT_CONFIG as QMIX_CONFIG
from ray.rllib.utils.annotations import override
from ray.rllib.utils import merge_dicts

APEX_QMIX_DEFAULT_CONFIG = merge_dicts(
    QMIX_CONFIG,  # see also the options in qmix.py, which are also supported
    {
        "optimizer_class": "AsyncReplayOptimizer",
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


class ApexQMixAgent(QMixAgent):
    """QMIX variant that uses the Ape-X distributed policy optimizer.

    By default, this is configured for a large single node (32 cores). For
    running in a large cluster, increase the `num_workers` config var.
    """

    _agent_name = "APEX_QMIX"
    _default_config = APEX_QMIX_DEFAULT_CONFIG

    @override(QMixAgent)
    def update_target_if_needed(self):
        # Ape-X updates based on num steps trained, not sampled
        if self.optimizer.num_steps_trained - self.last_target_update_ts > \
                self.config["target_network_update_freq"]:
            self.local_evaluator.foreach_trainable_policy(
                lambda p, _: p.update_target())
            self.last_target_update_ts = self.optimizer.num_steps_trained
            self.num_target_updates += 1
