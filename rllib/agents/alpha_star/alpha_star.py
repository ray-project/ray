"""
A multi-agent, distributed multi-GPU, league-capable asynch. PPO
================================================================
"""
import ray
from ray.rllib.agents.trainer import Trainer
import ray.rllib.agents.ppo.appo as appo
from ray.rllib.execution.rollout_ops import asynchronous_parallel_sample
from ray.rllib.execution.buffers.multi_agent_replay_buffer import \
    MultiAgentReplayBuffer
from ray.rllib.policy.sample_batch import MultiAgentBatch
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
        #"num_gpus_per_policy_learner": 0.1,

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

        num_policies = len(ma_cfg["policies"])
        if cf["num_gpus"]:
            num_learner_shards = max(
                cf["num_gpus"] / num_policies, cf["num_gpus"])
            num_gpus_per_shard = cf["num_gpus"] / num_learner_shards
        else:
            num_learner_shards = cf.get("num_replay_buffer_shards", 1)
            num_gpus_per_shard = 0

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
                    "GPU": num_gpus_per_shard,
                } for _ in range(num_learner_shards)
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
        _policy_learners = {}
        for pid, policy_spec in ma_cfg["policies"].items():
            if pid in self.workers.local_worker().policies_to_train:
                _policy_learners[pid] = [None, None]

        # Examples:
        # 4 GPUs 2 Policies -> 2 shards.
        # 2 GPUs 4 Policies -> 2 shards.
        if self.config["num_gpus"]:
            num_learner_shards = max(
                self.config["num_gpus"] / len(_policy_learners),
                self.config["num_gpus"])
            num_gpus_per_shard = self.config["num_gpus"] / num_learner_shards
        else:
            num_learner_shards = self.config.get("num_replay_buffer_shards", 1)
            num_gpus_per_shard = 0

        num_policies_per_shard = len(_policy_learners) / num_learner_shards
        num_gpus_per_policy = num_gpus_per_shard / num_policies_per_shard

        # Single CPU replay shard (co-located with GPUs so we can place the
        # policies on the same machine(s)).
        ReplayActor = ray.remote(
            num_cpus=1,
            num_gpus=0.01 if (self.config["num_gpus"] and
                              not self.config["_fake_gpus"]) else 0)(
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

        for shard_idx in range(num_learner_shards):
            policies = list(_policy_learners.keys())[slice(
                int(shard_idx * num_policies_per_shard),
                int((shard_idx + 1) * num_policies_per_shard))]
            configs = [
                self.merge_trainer_configs(self.config,
                                           ma_cfg["policies"][pid].config)
                for pid in policies
            ]
            colocated = create_colocated_actors(
                actor_specs=[
                    (ReplayActor, replay_actor_args, {}, 1),
                ] + [
                    (
                        ray.remote(
                            num_cpus=1,
                            num_gpus=num_gpus_per_policy if
                            not self.config["_fake_gpus"] else 0)(
                            ma_cfg["policies"][pid].policy_class),
                        # Policy c'tor args.
                        (ma_cfg["policies"][pid].observation_space,
                         ma_cfg["policies"][pid].action_space, cfg),
                        # Policy c'tor kwargs={}.
                        {},
                        # Count=1,
                        1) for pid, cfg in zip(policies, configs)
                ],
                node=None)  # None

            # Store replay actor (shard) in our list.
            replay_actor = colocated[0][0]
            self._replay_actors.append(replay_actor)
            # Store policy actors together with their respective (co-located)
            # replay actor.
            for co, pid in zip(colocated[1:], policies):
                policy_actor = co[0]
                _policy_learners[pid] = [policy_actor, replay_actor]

        # Store policy actor -> replay actor mapping on each RolloutWorker
        # so they know, to which replay shard to send samples to.

        def _set_policy_learners(worker):
            worker._policy_learners = _policy_learners

        ray.get([
            w.apply.remote(_set_policy_learners)
            for w in self.workers.remote_workers()
        ])

        self._policy_learners = _policy_learners

    def training_iteration(self) -> ResultDict:
        # - Rollout results are sent directly to correct replay buffer
        #   shards, instead of here (to the local worker).

        def _sample_to_buffer(worker):
            # Generate a sample.
            sample = worker.sample()
            # Send the per-agent SampleBatches to the correct buffer(s),
            # depending on which policies participated in the episode.
            assert isinstance(sample, MultiAgentBatch)
            for pid, batch in sample.policy_batches.items():
                replay_actor = worker._policy_learners[pid][1]
                ma_batch = MultiAgentBatch({pid: batch}, batch.count)
                replay_actor.add_batch.remote(ma_batch)

        # Trigger rollouts on all RolloutWorkers (w/ timeout for asynch).
        eval_results = asynchronous_parallel_sample(
            trainer=self,
            worker_set=self.workers,
            timeout_s=0.1,
            max_remote_requests_in_flight_per_worker=2,
            remote_fn=_sample_to_buffer,
        )

        # Trigger one update on each learning policy.
        ray.get([
            pol_actor.learn_on_batch_from_replay_buffer.remote(
                replay_actor=replay_actor, policy_id=pid)
            for pid, (pol_actor,
                      replay_actor) in self._policy_learners.items()
        ])

        return {}
