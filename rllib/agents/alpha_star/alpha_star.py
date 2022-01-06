"""
A multi-agent, distributed multi-GPU, league-capable asynch. PPO
================================================================
"""
import ray
from ray.rllib.agents.trainer import Trainer
import ray.rllib.agents.ppo.appo as appo
from ray.rllib.execution.common import WORKER_UPDATE_TIMER
from ray.rllib.execution.rollout_ops import asynchronous_parallel_sample
from ray.rllib.execution.buffers.mixin_replay_buffer import \
    MixInMultiAgentReplayBuffer
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
        # TODO: Unify the buffer API, then clean up our existing
        #  implementations of different buffers.
        # This is num batches held at any time for each policy.
        "replay_buffer_capacity": 20,
        # e.g. ratio=0.2 -> 20% of samples are old (replayed) ones.
        "replay_buffer_replay_ratio": 0.33,

        # Use the `training_iteration` method instead of an execution plan.
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

        num_policies = len(cf["multiagent"]["policies"])
        if cf["num_gpus"]:
            num_learner_shards = max(cf["num_gpus"] / num_policies,
                                     cf["num_gpus"])
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
            num_learner_shards = int(
                max(self.config["num_gpus"] / len(_policy_learners),
                    self.config["num_gpus"]))
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
            num_gpus=0.01
            if (self.config["num_gpus"] and not self.config["_fake_gpus"]) else
            0)(MixInMultiAgentReplayBuffer)

        # Setup remote replay buffer shards and policy learner actors
        # (located on any GPU machine in the cluster):
        replay_actor_args = [
            self.config["replay_buffer_capacity"],
            self.config["replay_buffer_replay_ratio"]
        ]
        self._replay_actors = []

        for shard_idx in range(num_learner_shards):
            # Which policies should be placed in this learner shard?
            policies = list(_policy_learners.keys())[slice(
                int(shard_idx * num_policies_per_shard),
                int((shard_idx + 1) * num_policies_per_shard))]
            # Merge the policies config overrides with the main config.
            # Also, wdjust `num_gpus` (to indicate an individual policy's
            # num_gpus, not the total number of GPUs).
            configs = [
                self.merge_trainer_configs(
                    self.config,
                    dict(ma_cfg["policies"][pid].config,
                         **{"num_gpus": num_gpus_per_policy}))
                for pid in policies
            ]

            colocated = create_colocated_actors(
                actor_specs=[
                    (ReplayActor, replay_actor_args, {}, 1),
                ] + [
                    (
                        ray.remote(
                            num_cpus=1,
                            num_gpus=num_gpus_per_policy
                            if not self.config["_fake_gpus"] else 0)(
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
        # Trigger asynchronous rollouts on all RolloutWorkers.
        # - Rollout results are sent directly to correct replay buffer
        #   shards, instead of here (to the driver).
        asynchronous_parallel_sample(
            trainer=self,
            actors=self.workers.remote_workers()
            or [self.workers.local_worker()],
            ray_wait_timeout_s=0.1,
            max_remote_requests_in_flight_per_actor=2,
            remote_fn=self._sample_and_send_to_buffer,
        )

        # Trigger asynchronous training update requests on all learning
        # policies.
        idx_to_pol_actor_and_pid = {}
        args = []
        for i, (pid, (pol_actor,
                      repl_actor)) in enumerate(self._policy_learners.items()):
            idx_to_pol_actor_and_pid[i] = (pol_actor, pid)
            args.append([repl_actor, pid])
        train_results = asynchronous_parallel_sample(
            trainer=self,
            actors=[act for (act, _) in idx_to_pol_actor_and_pid.values()],
            ray_wait_timeout_s=0.1,
            max_remote_requests_in_flight_per_actor=2,
            remote_fn=self._update_policy,
            remote_args=args,
        )

        # For those policies that have been updated in this iteration
        # (not all policies may have undergone an updated as we are
        # requesting updates asynchronously):
        # - Gather train infos.
        # - Update weights to those remote rollout workers that contain
        #   the respective policy.
        train_infos = {}
        policy_weights = {}
        with self._timers[WORKER_UPDATE_TIMER]:
            for i, policy_result in enumerate(train_results):
                if policy_result:
                    pol_actor, pid = idx_to_pol_actor_and_pid[i]
                    train_infos[pid] = policy_result
                    policy_weights[pid] = self._policy_learners[pid][
                        0].get_weights.remote()

            policy_weights_ref = ray.put(policy_weights)

            for worker in self.workers.remote_workers():
                worker.set_weights.remote(policy_weights_ref)

        return train_infos

    @staticmethod
    def _sample_and_send_to_buffer(worker):
        # Generate a sample.
        sample = worker.sample()
        # Send the per-agent SampleBatches to the correct buffer(s),
        # depending on which policies participated in the episode.
        assert isinstance(sample, MultiAgentBatch)
        for pid, batch in sample.policy_batches.items():
            replay_actor = worker._policy_learners[pid][1]
            ma_batch = MultiAgentBatch({pid: batch}, batch.count)
            replay_actor.add_batch.remote(ma_batch)

    @staticmethod
    def _update_policy(policy, replay_actor, pid):
        return policy.learn_on_batch_from_replay_buffer(
            replay_actor=replay_actor, policy_id=pid)
