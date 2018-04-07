from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.dqn.dqn import DQNAgent, DEFAULT_CONFIG as DQN_CONFIG
from ray.tune.trial import Resources

APEX_DEFAULT_CONFIG = dict(DQN_CONFIG, **dict(
    optimizer_class="ApexOptimizer",
    optimizer_config=dict(DQN_CONFIG["optimizer_config"], **dict(
        max_weight_sync_delay=400,
        num_replay_buffer_shards=4,
        debug=False,
    )),
    n_step=3,
    gpu=True,
    num_workers=32,
    buffer_size=2000000,
    learning_starts=50000,
    train_batch_size=512,
    sample_batch_size=50,
    max_weight_sync_delay=400,
    target_network_update_freq=500000,
    timesteps_per_iteration=25000,
    per_worker_exploration=True,
    worker_side_prioritization=True,
))


class ApexAgent(DQNAgent):
    """DQN variant that uses the Ape-X distributed policy optimizer.

    By default, this is configured for a large single node (32 cores). For
    running in a large cluster, increase the `num_workers` config var.
    """

    _agent_name = "APEX"
    _default_config = APEX_DEFAULT_CONFIG

    @classmethod
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        return Resources(
            cpu=1 + cf["optimizer_config"]["num_replay_buffer_shards"],
            gpu=cf["gpu"] and 1 or 0,
            extra_cpu=cf["num_cpus_per_worker"] * cf["num_workers"],
            extra_gpu=cf["num_gpus_per_worker"] * cf["num_workers"])

    def update_target_if_needed(self):
        # Ape-X updates based on num steps trained, not sampled
        if self.optimizer.num_steps_trained - self.last_target_update_ts > \
                self.config["target_network_update_freq"]:
            self.local_evaluator.update_target()
            self.last_target_update_ts = self.optimizer.num_steps_trained
            self.num_target_updates += 1
