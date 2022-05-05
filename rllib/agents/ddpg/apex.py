from ray.rllib.agents.dqn.apex import ApexTrainer
from ray.rllib.agents.ddpg.ddpg import DDPGTrainer, DEFAULT_CONFIG as DDPG_CONFIG
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import PartialTrainerConfigDict, ResultDict,\
    TrainerConfigDict

APEX_DDPG_DEFAULT_CONFIG = DDPGTrainer.merge_trainer_configs(
    DDPG_CONFIG,  # see also the options in ddpg.py, which are also supported
    {
        "optimizer": {
            "max_weight_sync_delay": 400,
            "num_replay_buffer_shards": 4,
            "debug": False,
        },
        "exploration_config": {"type": "PerWorkerOrnsteinUhlenbeckNoise"},
        "n_step": 3,
        "num_gpus": 0,
        "num_workers": 32,
        "replay_buffer_config": {
            # For now we don't use the new ReplayBuffer API here
            "_enable_replay_buffer_api": False,
            "no_local_replay_buffer": True,
            "type": "MultiAgentReplayBuffer",
            "capacity": 2000000,
        },
        # Whether all shards of the replay buffer must be co-located
        # with the learner process (running the execution plan).
        # This is preferred b/c the learner process should have quick
        # access to the data from the buffer shards, avoiding network
        # traffic each time samples from the buffer(s) are drawn.
        # Set this to False for relaxing this constraint and allowing
        # replay shards to be created on node(s) other than the one
        # on which the learner is located.
        "replay_buffer_shards_colocated_with_driver": True,
        "learning_starts": 50000,

        "train_batch_size": 512,

        "rollout_fragment_length": 50,
        "worker_side_prioritization": True,

        "target_network_update_freq": 500000,

        "min_sample_timesteps_per_reporting": 25000,
        "min_time_s_per_reporting": 30,
    },
    _allow_unknown_configs=True,
)


class ApexDDPGTrainer(DDPGTrainer):
    @classmethod
    @override(DDPGTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return APEX_DDPG_DEFAULT_CONFIG

    @override(DDPGTrainer)
    def setup(self, config: PartialTrainerConfigDict):
        return ApexTrainer.setup(self, config)

    @override(DDPGTrainer)
    def training_iteration(self) -> ResultDict:
        return ApexTrainer.training_iteration(self)
