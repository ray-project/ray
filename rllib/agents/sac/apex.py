from ray.rllib.agents.dqn.apex import apex_execution_plan
from ray.rllib.agents.sac.sac import DEFAULT_CONFIG as SAC_CONFIG, SACTrainer
from ray.rllib.utils import merge_dicts

# yapf: disable
# __sphinx_doc_begin__

APEX_SAC_DEFAULT_CONFIG = merge_dicts(
    SAC_CONFIG,  # see also the options in dqn.py, which are also supported
    {
        "optimizer": merge_dicts(
            SAC_CONFIG["optimizer"], {
                "max_weight_sync_delay": 400,
                "num_replay_buffer_shards": 4,
                "debug": False
            }),
        "n_step": 1,
        "num_gpus": 0,
        "num_workers": 32,
        "buffer_size": 2000000,
        "learning_starts": 50000,
        "train_batch_size": 512,
        "rollout_fragment_length": 50,
        "target_network_update_freq": 500000,
        "timesteps_per_iteration": 25000,
        "exploration_config": {"type": "StochasticSampling"},
        "worker_side_prioritization": True,
        "min_iter_time_s": 30,
        # If set, this will fix the ratio of sampled to replayed timesteps.
        # Otherwise, replay will proceed as fast as possible.
        "training_intensity": None,
    },
)


# __sphinx_doc_end__
# yapf: enable

ApexSACTrainer = SACTrainer.with_updates(
    name="APEX_SAC", default_config=APEX_SAC_DEFAULT_CONFIG, execution_plan=apex_execution_plan
)
