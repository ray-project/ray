"""
Multi-Agent, League-Based Asynch. PPO (MALB-APPO)
================================================
"""
from collections import defaultdict

import ray
from ray.rllib.agents.trainer import Trainer
import ray.rllib.agents.ppo.appo as appo
from ray.rllib.execution.buffers.multi_agent_replay_buffer import \
    MultiAgentReplayBuffer
from ray.rllib.utils.actors import create_colocated_actors
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import PartialTrainerConfigDict, \
    TrainerConfigDict, ResultDict
from ray.tune.utils.placement_groups import PlacementGroupFactory

# yapf: disable
# __sphinx_doc_begin__

# Adds the following updates to the `IMPALATrainer` config in
# rllib/agents/impala/impala.py.
DEFAULT_CONFIG = Trainer.merge_trainer_configs(
    appo.DEFAULT_CONFIG,  # See keys in appo.py, which are also supported.
    {
        "num_gpus_per_policy_learner": 0.1,

        # TODO: Unify the buffer API, then clean up our existing
        #  implementations of different buffers.
        "replay_buffer_num_slots": 10,

        # Use the `training_iteration` method instead of an execution-plan.
        "_disable_execution_plan_api": True,
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
                    # RolloutWorkers (no GPUs).
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

        ma_cfg = self.config["multiagent"]

        # TODO: For now, fix the number of policies to those that are already
        #  defined in the multiagent config dict. However, later, make the pool
        #  of policy learners dynamically grow with more policies being added
        #  on-the-fly.

        # Map from policy ID to a) respective policy learner actor handle
        # and b) the co-located replay actor handle for that policy.
        # We'll keep this map synched across the algorithm's workers.
        self._policy_learners = {}
        for pid, policy_spec in ma_cfg["policies"].items():
            if pid in self.workers.local_worker().policies_to_train:
                self._policy_learners[pid] = [None, None]

        # Examples:
        # 4 GPUs 2 Policies -> 2 shards.
        # 2 GPUs 4 Policies -> 2 shards.
        if self.config["num_gpus"]:
            num_learner_shards = max(
                self.config["num_gpus"] / len(self._policy_learners),
                self.config["num_gpus"])
            num_gpus_per_shard = self.config["num_gpus"] / num_learner_shards
        else:
            num_learner_shards = self.config.get("num_replay_buffer_shards", 1)
            num_gpus_per_shard = 0

        num_policies_per_shard = len(self._policy_learners) / num_learner_shards
        num_gpus_per_policy = num_gpus_per_shard / num_policies_per_shard

        # Single CPU replay shard (co-located with GPUs so we can place the
        # policies on the same machine(s)).
        ReplayActor = ray.remote(num_cpus=1, num_gpus=0.01)(
            MultiAgentReplayBuffer)

        # Setup remote replay buffer shards and policy learner actors
        # (located on any GPU machine in the cluster):
        replay_actor_args = [
            num_learner_shards,
            self.config["train_batch_size"],  # learning starts
            self.config["replay_buffer_num_slots"],
            self.config["train_batch_size"],
            0.0,  # no prioritization
        ]
        self._replay_actors = []
        policy_remote_kwargs = {}
        #if
        #    "num_gpus": ,
        #    "num_cpus": 1,
        #}

        for shard_idx in range(num_learner_shards):
            policies = list(self._policy_learners.keys())[
                slice(int(shard_idx * num_policies_per_shard),
                      int((shard_idx+1) * num_policies_per_shard))
            ]
            colocated = create_colocated_actors(actor_specs=[
                (ReplayActor, replay_actor_args, {}, 1),
            ] + [(
                ray.remote(num_cpus=1, num_gpus=num_gpus_per_policy)(
                    ma_cfg["policies"][pid].policy_class),
                # Policy c'tor args.
                (ma_cfg["policies"][pid].observation_space,
                 ma_cfg["policies"][pid].action_space,
                 ma_cfg["policies"][pid].config),
                # Policy c'tor kwargs={}.
                {},
                # Count=1,
                1
            ) for pid in policies])

            replay_actor = colocated[0][0]
            self._replay_actors.append(replay_actor)
            self._policy_learners

        print("here")

    def training_iteration(self) -> ResultDict:
        # - trigger rollouts on all workers (w/ timeout for asynch)
        # - rollout results are sent directly to correct replay buffer
        #   shards
        evaluation_metrics = ray.get([worker.sample_to_buffer.remote() for worker in self.workers.remote_workers()])

        # - trigger one update on each policy.
        learner_metrics = ray.get([pol.learn_on_batch.remote() for pol in self._policy_learners])

        return results
