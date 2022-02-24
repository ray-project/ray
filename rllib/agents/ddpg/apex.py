from ray.rllib.agents.dqn.apex import ApexTrainer
from ray.rllib.agents.ddpg.ddpg import DDPGTrainer, DEFAULT_CONFIG as DDPG_CONFIG
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict
from ray.util.iter import LocalIterator

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
        "buffer_size": 2000000,
        # TODO(jungong) : update once Apex supports replay_buffer_config.
        "replay_buffer_config": None,
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
        "target_network_update_freq": 500000,
        "timesteps_per_iteration": 25000,
        "worker_side_prioritization": True,
        "min_time_s_per_reporting": 30,
        # For each stack of multi-GPU towers, how many slots should we reserve for
        # parallel data loading? Set this to >1 to load data into GPUs in
        # parallel. This will increase GPU memory usage proportionally with the
        # number of stacks.
        # Example:
        # 2 GPUs and `num_multi_gpu_tower_stacks=3`:
        # - One tower stack consists of 2 GPUs, each with a copy of the
        #   model/graph.
        # - Each of the stacks will create 3 slots for batch data on each of its
        #   GPUs, increasing memory requirements on each GPU by 3x.
        # - This enables us to preload data into these stacks while another stack
        #   is performing gradient calculations.
        "num_multi_gpu_tower_stacks": 1,
        # Max queue size for train batches feeding into the learner.
        "learner_queue_size": 16,
        # Wait for train batches to be available in minibatch buffer queue
        # this many seconds. This may need to be increased e.g. when training
        # with a slow environment.
        "learner_queue_timeout": 300,
    },
    _allow_unknown_configs=True,
)


class ApexDDPGTrainer(DDPGTrainer):
    @classmethod
    @override(DDPGTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return APEX_DDPG_DEFAULT_CONFIG

    @staticmethod
    @override(DDPGTrainer)
    def execution_plan(
        workers: WorkerSet, config: dict, **kwargs
    ) -> LocalIterator[dict]:
        """Use APEX-DQN's execution plan."""
        return ApexTrainer.execution_plan(workers, config, **kwargs)
