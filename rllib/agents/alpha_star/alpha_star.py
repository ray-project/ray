"""
Multi-Agent, League-Based Asynch. PPO (MALB-APPO)
================================================
"""
import ray
from ray.rllib.agents.trainer import Trainer
import ray.rllib.agents.ppo.appo as appo
from ray.rllib.execution.buffers.multi_agent_replay_buffer import \
    MultiAgentReplayBuffer
from ray.rllib.utils.actors import create_colocated
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import PartialTrainerConfigDict, TrainerConfigDict
from ray.tune.utils.placement_groups import PlacementGroupFactory

# yapf: disable
# __sphinx_doc_begin__

# Adds the following updates to the `IMPALATrainer` config in
# rllib/agents/impala/impala.py.
DEFAULT_CONFIG = Trainer.merge_trainer_configs(
    appo.DEFAULT_CONFIG,  # See keys in appo.py, which are also supported.
    {
        "num_gpus_per_policy_learner": 0.1,
    },
    _allow_unknown_configs=True,
)

# __sphinx_doc_end__
# yapf: enable


class AlphaStarTrainer(appo.APPOTrainer):
    @classmethod
    @override(Trainer)
    def default_resource_request(cls, config):
        cf = dict(cls.get_default_config(), **config)

        eval_config = cf["evaluation_config"]

        # Return PlacementGroupFactory containing all needed resources
        # (already properly defined as device bundles).
        return PlacementGroupFactory(
            bundles=[{
                # Driver (no GPUs).
                "CPU": cf["num_cpus_for_driver"],
            }] + [
                {
                    # RolloutWorkers.
                    "CPU": cf["num_cpus_per_worker"],
                } for _ in range(cf["num_workers"])
            ] + [
                {
                    # Policy learners (and Replay buffer shards).
                    "CPU": 1,
                    "GPU": cf["num_gpus_per_policy_learner"],
                } for _ in range(cf["num_policy_learners"])
            ] + ([
                {
                    # Evaluation (remote) workers.
                    # Note: The local eval worker is located on the driver
                    # CPU or not even created iff >0 eval workers.
                    "CPU": eval_config.get("num_cpus_per_worker",
                                           cf["num_cpus_per_worker"]),
                    "GPU": eval_config.get("num_gpus_per_worker",
                                           cf["num_gpus_per_worker"]),
                } for _ in range(cf["evaluation_num_workers"])
            ] if cf["evaluation_interval"] else []),
            strategy=config.get("placement_strategy", "PACK"))

    @classmethod
    @override(appo.APPOTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(appo.APPOTrainer)
    def setup(self, config: PartialTrainerConfigDict):
        super().setup(config)

        # Map from policy ID to a) policy actor handle and b) replay actor
        # handle.
        self._policy_learners = {
            pid: [None, None]
            for pid in config["multiagent"]["policies"].keys()
        }

        # Single CPU replay shard (co-located with GPUs so we can place the
        # policies on the same machine(s)).

        ReplayActor = ray.remote(num_cpus=1)(MultiAgentReplayBuffer)

        # Setup remote replay buffer shards (located on any GPU machine
        # in the cluster, together with a subset of the policy learner
        # actors).

        num_shards: int = 1,
        learning_starts: int = 1000,
        capacity: int = 10000,
        replay_batch_size: int = 1,
        prioritized_replay_alpha: float = 0.6,
        prioritized_replay_beta: float = 0.4,
        prioritized_replay_eps: float = 1e-6,
        replay_mode: str = "independent",
        replay_sequence_length: int = 1,

        self._replay_actors = [ReplayActor.remote(
            config["num_replay_buffer_shards"],
            config["learning_starts"],
            config["buffer_size"],
            config["train_batch_size"],
            config["prioritized_replay_alpha"],
            config["prioritized_replay_beta"],
            config["prioritized_replay_eps"],
            config["multiagent"]["replay_mode"],
            config.get("replay_sequence_length", 1),
        ) for _ in range(config["num_replay_buffer_shards"])]

        # Setup remote policy learners.
        
        policy_class
        print("here")
