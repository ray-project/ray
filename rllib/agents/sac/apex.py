from ray.rllib.agents.dqn.apex import apex_execution_plan
from ray.rllib.agents.sac.sac import DEFAULT_CONFIG as SAC_CONFIG, SACTrainer

# yapf: disable
# __sphinx_doc_begin__

APEX_SAC_DEFAULT_CONFIG = SACTrainer.merge_trainer_configs(
    SAC_CONFIG,  # see also the options in sac.py, which are also supported
    {
        "optimizer": {
            "max_weight_sync_delay": 400,
            "num_replay_buffer_shards": 4,
            "debug": False,
        },
        "n_step": 1,
        "num_gpus": 0,
        "num_workers": 32,
        "buffer_size": 200000,
        "learning_starts": 5000,
        "train_batch_size": 512,
        "rollout_fragment_length": 50,
        "target_network_update_freq": 0,
        "timesteps_per_iteration": 1000,
        "exploration_config": {"type": "StochasticSampling"},
        "worker_side_prioritization": True,
        "min_iter_time_s": 10,
        "prioritized_replay": True,
        # If set, this will fix the ratio of sampled to replayed timesteps.
        # Otherwise, replay will proceed as fast as possible.
        "training_intensity": None,
        # Which mode to use in the ParallelRollouts operator used to collect
        # samples. For more details check the operator in rollout_ops module.
        "parallel_rollouts_mode": "async",
        # This only applies if async mode is used (above config setting).
        # Controls the max number of async requests in flight per actor
        "parallel_rollouts_num_async": 2,
    },
)


# __sphinx_doc_end__
# yapf: enable


ApexSACTrainer = SACTrainer.with_updates(
    name="APEX_SAC",
    default_config=APEX_SAC_DEFAULT_CONFIG,
    execution_plan=apex_execution_plan,
)
