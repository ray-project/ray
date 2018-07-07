from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.ddpg.ddpg import DDPGAgent, DEFAULT_CONFIG as DDPG_CONFIG
from ray.utils import merge_dicts

APEX_DDPG_DEFAULT_CONFIG = merge_dicts(
    DDPG_CONFIG,
    {
        "optimizer_class": "AsyncSamplesOptimizer",
        "optimizer":
            merge_dicts(
                DDPG_CONFIG["optimizer"], {
                    "max_weight_sync_delay": 400,
                    "num_replay_buffer_shards": 4,
                    "debug": False
                }),
        "n_step": 3,
        "num_workers": 32,
        "buffer_size": 2000000,
        "learning_starts": 50000,
        "train_batch_size": 512,
        "sample_batch_size": 50,
        "max_weight_sync_delay": 400,
        "target_network_update_freq": 500000,
        "timesteps_per_iteration": 25000,
        "per_worker_exploration": True,
        "worker_side_prioritization": True,
    },
)


class ApexDDPGAgent(DDPGAgent):
    """DDPG variant that uses the Ape-X distributed policy optimizer.

    By default, this is configured for a large single node (32 cores). For
    running in a large cluster, increase the `num_workers` config var.
    """

    _agent_name = "APEX_DDPG"
    _default_config = APEX_DDPG_DEFAULT_CONFIG

    def update_target_if_needed(self):
        # Ape-X updates based on num steps trained, not sampled
        if self.optimizer.num_steps_trained - self.last_target_update_ts > \
                self.config["target_network_update_freq"]:
            self.local_evaluator.for_policy(lambda p: p.update_target())
            self.last_target_update_ts = self.optimizer.num_steps_trained
            self.num_target_updates += 1
