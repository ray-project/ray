from typing import List

from ray.actor import ActorHandle
from ray.rllib.agents import Trainer
from ray.rllib.agents.dqn.apex import ApexTrainer
from ray.rllib.algorithms.ddpg.ddpg import DDPGConfig, DDPGTrainer
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict
from ray.util.iter import LocalIterator
from ray.rllib.utils.typing import PartialTrainerConfigDict
from ray.rllib.utils.typing import ResultDict
from ray.rllib.utils.deprecation import DEPRECATED_VALUE

APEX_DDPG_DEFAULT_CONFIG = DDPGTrainer.merge_trainer_configs(
    DDPGConfig().to_dict(),  # see also the options in ddpg.py, which are also supported
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
            "capacity": 2000000,
            "no_local_replay_buffer": True,
            # Specify prioritized replay by supplying a buffer type that supports
            # prioritization, for example: MultiAgentPrioritizedReplayBuffer.
            "prioritized_replay": DEPRECATED_VALUE,
            "learning_starts": 50000,
            # Whether all shards of the replay buffer must be co-located
            # with the learner process (running the execution plan).
            # This is preferred b/c the learner process should have quick
            # access to the data from the buffer shards, avoiding network
            # traffic each time samples from the buffer(s) are drawn.
            # Set this to False for relaxing this constraint and allowing
            # replay shards to be created on node(s) other than the one
            # on which the learner is located.
            "replay_buffer_shards_colocated_with_driver": True,
            "worker_side_prioritization": True,
        },
        "train_batch_size": 512,
        "rollout_fragment_length": 50,
        # Update the target network every `target_network_update_freq` sample timesteps.
        "target_network_update_freq": 500000,
        "min_sample_timesteps_per_reporting": 25000,
        "min_time_s_per_reporting": 30,
        "training_intensity": 1,
        # max number of inflight requests to each sampling worker
        # see the AsyncRequestsManager class for more details
        # Tuning these values is important when running experimens with large sample
        # batches. If the sample batches are large in size, then there is the risk that
        # the object store may fill up, causing the store to spill objects to disk.
        # This can cause any asynchronous requests to become very slow, making your
        # experiment run slowly. You can inspect the object store during your
        # experiment via a call to ray memory on your headnode, and by using the ray
        # dashboard. If you're seeing that the object store is filling up, turn down
        # the number of remote requests in flight, or enable compression in your
        # experiment of timesteps.
        "max_requests_in_flight_per_sampler_worker": 2,
        "max_requests_in_flight_per_replay_worker": float("inf"),
        "timeout_s_sampler_manager": 0.0,
        "timeout_s_replay_manager": 0.0,
    },
    _allow_unknown_configs=True,
)


class ApexDDPGTrainer(DDPGTrainer, ApexTrainer):
    @classmethod
    @override(DDPGTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return APEX_DDPG_DEFAULT_CONFIG

    @override(DDPGTrainer)
    def setup(self, config: PartialTrainerConfigDict):
        return ApexTrainer.setup(self, config)

    @override(DDPGTrainer)
    def training_iteration(self) -> ResultDict:
        """Use APEX-DQN's training iteration function."""
        return ApexTrainer.training_iteration(self)

    @override(Trainer)
    def on_worker_failures(
        self, removed_workers: List[ActorHandle], new_workers: List[ActorHandle]
    ):
        """Handle the failures of remote sampling workers

        Args:
            removed_workers: removed worker ids.
            new_workers: ids of newly created workers.
        """
        self._sampling_actor_manager.remove_workers(removed_workers)
        self._sampling_actor_manager.add_workers(new_workers)

    @staticmethod
    @override(DDPGTrainer)
    def execution_plan(
        workers: WorkerSet, config: dict, **kwargs
    ) -> LocalIterator[dict]:
        """Use APEX-DQN's execution plan."""
        return ApexTrainer.execution_plan(workers, config, **kwargs)
