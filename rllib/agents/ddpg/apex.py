from ray.rllib.agents.dqn.apex import apex_execution_plan
from ray.rllib.agents.ddpg.ddpg import DDPGTrainer, \
    DEFAULT_CONFIG as DDPG_CONFIG

APEX_DDPG_DEFAULT_CONFIG = DDPGTrainer.merge_trainer_configs(
    DDPG_CONFIG,  # see also the options in ddpg.py, which are also supported
    {
        "optimizer": {
            "max_weight_sync_delay": 400,
            "num_replay_buffer_shards": 4,
            "debug": False
        },
        "exploration_config": {
            "type": "PerWorkerOrnsteinUhlenbeckNoise"
        },
        "n_step": 3,
        "num_gpus": 0,
        "num_workers": 32,
        "buffer_size": 2000000,
        "learning_starts": 50000,
        "train_batch_size": 512,
        "rollout_fragment_length": 50,
        "target_network_update_freq": 500000,
        "timesteps_per_iteration": 25000,
        "worker_side_prioritization": True,
        "min_iter_time_s": 30,
    },
)

ApexDDPGTrainer = DDPGTrainer.with_updates(
    name="APEX_DDPG",
    default_config=APEX_DDPG_DEFAULT_CONFIG,
    execution_plan=apex_execution_plan)
