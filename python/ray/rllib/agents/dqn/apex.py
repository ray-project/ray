from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.dqn.dqn import DQNTrainer, DEFAULT_CONFIG as DQN_CONFIG
from ray.rllib.utils import merge_dicts

# yapf: disable
# __sphinx_doc_begin__
APEX_DEFAULT_CONFIG = merge_dicts(
    DQN_CONFIG,  # see also the options in dqn.py, which are also supported
    {
        "optimizer_class": "AsyncReplayOptimizer",
        "optimizer": merge_dicts(
            DQN_CONFIG["optimizer"], {
                "max_weight_sync_delay": 400,
                "num_replay_buffer_shards": 4,
                "debug": False
            }),
        "n_step": 3,
        "num_gpus": 1,
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
# __sphinx_doc_end__
# yapf: enable


def update_target_based_on_num_steps_trained(trainer, fetches):
    # Ape-X updates based on num steps trained, not sampled
    if (trainer.optimizer.num_steps_trained -
            trainer.state["last_target_update_ts"] >
            trainer.config["target_network_update_freq"]):
        trainer.workers.local_worker().foreach_trainable_policy(
            lambda p, _: p.update_target())
        trainer.state["last_target_update_ts"] = (
            trainer.optimizer.num_steps_trained)
        trainer.state["num_target_updates"] += 1


ApexTrainer = DQNTrainer.with_updates(
    name="APEX",
    default_config=APEX_DEFAULT_CONFIG,
    after_optimizer_step=update_target_based_on_num_steps_trained)
