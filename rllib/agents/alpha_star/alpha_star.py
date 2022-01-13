"""
A multi-agent, distributed multi-GPU, league-capable asynch. PPO
================================================================
"""
from typing import Dict, Set

import ray
from ray.actor import ActorHandle
from ray.rllib.agents.trainer import Trainer
import ray.rllib.agents.ppo.appo as appo
from ray.rllib.agents.alpha_star.league_building import league_building_fn
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.execution.parallel_requests import asynchronous_parallel_requests
from ray.rllib.execution.buffers.mixin_replay_buffer import \
    MixInMultiAgentReplayBuffer
from ray.rllib.policy.policy import Policy, PolicySpec
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.actors import create_colocated_actors
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import LAST_TARGET_UPDATE_TS, \
    LEARN_ON_BATCH_TIMER, NUM_AGENT_STEPS_SAMPLED, NUM_AGENT_STEPS_TRAINED, \
    NUM_ENV_STEPS_SAMPLED, NUM_ENV_STEPS_TRAINED, NUM_TARGET_UPDATES, \
    SAMPLE_TIMER, SYNCH_WORKER_WEIGHTS_TIMER, TARGET_NET_UPDATE_TIMER
from ray.rllib.utils.typing import PartialTrainerConfigDict, PolicyID, \
    TrainerConfigDict, ResultDict
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.util.timer import _Timer

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
        # e.g. ratio=0.2 -> 20% of samples in each train batch are
        # old (replayed) ones.
        "replay_buffer_replay_ratio": 0.5,

        # League-building parameters.
        # The league building function defines the core logic for constructing
        # the league. It is called after each `step` and takes the Trainer
        # instance and the `step()` ResultDict (including the "evaluation"
        # results) as args. It may then analyze the results and decide on adding
        # new policies to the league.
        "league_building_fn": league_building_fn,
        # Minimum win-rate (between 0.0 = 0% and 1.0 = 100%) of any policy to
        # be considered for snapshotting (cloning). The cloned copy may then
        # be frozen (no further learning) or keep learning (independent of
        # its ancestor policy).
        # Set this to lower values to speed up league growth.
        "win_rate_threshold_for_new_snapshot": 0.9,
        # If we took a new snapshot of any given policy, what's the probability
        # that this snapshot will continue to be trainable (rather than become
        # frozen/non-trainable)?
        "keep_new_snapshot_training_prob": 0.3,

        # Basic "multiagent" setup for league-building AlphaStar:
        # Start with "main_0" (the policy we would like to use in the very end),
        # "league_exploiter_0" (a random policy) and "main_exploiter_0"
        # (also random).
        # Once "main_0" reaches n% win-rate, we'll create a snapshot of it
        # (main_1) as well as create learning "league_exploiter_1" and
        # "main_exploiter_1" policies (cloned off "main_0").
        "multiagent": {
            # Initial policy map. This will be expanded
            # to more policy snapshots inside the `league_building_fn`.
            "policies": {
                # Our main policy, we'd like to optimize.
                "main_0": PolicySpec(),
                # Initial main exploiter (random).
                "main_exploiter_0": PolicySpec(policy_class=RandomPolicy),
                # Initial league exploiter (random).
                "league_exploiter_0": PolicySpec(policy_class=RandomPolicy),
            },
            "policy_mapping_fn":
                (lambda aid, ep, worker, **kw: "main_0" if
                ep.episode_id % 2 == aid else "main_exploiter_0"),
            # At first, only train main_0 (until good enough to win against
            # random).
            "policies_to_train": ["main_0"],
        },

        # Reporting interval.
        "min_time_s_per_reporting": 2,

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

        # Map from policy ID to a) respective policy learner actor handle
        # and b) the co-located replay actor handle for that policy.
        # We'll keep this map synched across the algorithm's workers.
        _policy_learners = {}
        _pol_actor_to_pid = {}
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
                _pol_actor_to_pid[policy_actor] = pid

        # Store policy actor -> replay actor mapping on each RolloutWorker
        # so they know, to which replay shard to send samples to.

        def _set_policy_learners(worker):
            worker._policy_learners = _policy_learners

        ray.get([
            w.apply.remote(_set_policy_learners)
            for w in self.workers.remote_workers()
        ])

        # Setup important properties for this trainer:
        # Map of PolicyID -> Tuple[policy actor, corresponding replay actor]
        self._policy_learners = _policy_learners
        # Map of policy actor -> PolicyID.
        self._pol_actor_to_pid = _pol_actor_to_pid

        # Policy groups (main, main_exploit, league_exploit):
        self.main_policies: Set[PolicyID] = set()
        self.main_exploiters: Set[PolicyID] = set()
        self.league_exploiters: Set[PolicyID] = set()
        # Sets of currently trainable/non-trainable policies in the league.
        self.trainable_policies: Set[PolicyID] = set()
        self.non_trainable_policies: Set[PolicyID] = set()
        for pid in self.config["multiagent"]["policies"]:
            if pid in self.config["multiagent"]["policies_to_train"]:
                self.trainable_policies.add(pid)
            else:
                self.non_trainable_policies.add(pid)
        # Store the win rates for league overview printouts.
        self.win_rates: Dict[PolicyID, float] = {}

    @override(Trainer)
    def step(self) -> ResultDict:
        # Perform a full step (including evaluation).
        result = super().step()

        # Based on the (train + evaluate) results, perform a step of
        # league building.
        self.config["league_building_fn"](
            trainer=self,
            result=result,
            config=self.config,
            main_policies=self.main_policies,
            main_exploiters=self.main_exploiters,
            league_exploiters=self.league_exploiters,
            trainable_policies=self.trainable_policies,
            non_trainable_policies=self.non_trainable_policies,
        )

        return result

    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        # Trigger asynchronous rollouts on all RolloutWorkers.
        # - Rollout results are sent directly to correct replay buffer
        #   shards, instead of here (to the driver).
        with self._timers[SAMPLE_TIMER]:
            sample_results = asynchronous_parallel_requests(
                trainer=self,
                actors=self.workers.remote_workers()
                or [self.workers.local_worker()],
                ray_wait_timeout_s=0.01,
                max_remote_requests_in_flight_per_actor=2,
                remote_fn=self._sample_and_send_to_buffer,
            )
        # Update sample counters.
        for (env_steps, agent_steps) in sample_results.values():
            self._counters[NUM_ENV_STEPS_SAMPLED] += env_steps
            self._counters[NUM_AGENT_STEPS_SAMPLED] += agent_steps

        # Trigger asynchronous training update requests on all learning
        # policies.
        with self._timers[LEARN_ON_BATCH_TIMER]:
            pol_actors = []
            args = []
            for i, (pid, (pol_actor, repl_actor)) in enumerate(
                    self._policy_learners.items()):
                pol_actors.append(pol_actor)
                args.append([repl_actor, pid])
            train_results = asynchronous_parallel_requests(
                trainer=self,
                actors=pol_actors,
                ray_wait_timeout_s=0.1,
                max_remote_requests_in_flight_per_actor=2,
                remote_fn=self._update_policy,
                remote_args=args,
            )

        # Update sample counters.
        for result in train_results.values():
            if NUM_AGENT_STEPS_TRAINED in result:
                self._counters[NUM_AGENT_STEPS_TRAINED] += result[
                    NUM_AGENT_STEPS_TRAINED]

        # For those policies that have been updated in this iteration
        # (not all policies may have undergone an updated as we are
        # requesting updates asynchronously):
        # - Gather train infos.
        # - Update weights to those remote rollout workers that contain
        #   the respective policy.
        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            train_infos = {}
            policy_weights = {}
            for pol_actor, policy_result in train_results.items():
                if policy_result:
                    pid = self._pol_actor_to_pid[pol_actor]
                    train_infos[pid] = policy_result
                    policy_weights[pid] = self._policy_learners[pid][
                        0].get_weights.remote()

            policy_weights_ref = ray.put(policy_weights)

            for worker in self.workers.remote_workers():
                worker.set_weights.remote(policy_weights_ref)

        return train_infos

    @staticmethod
    def _sample_and_send_to_buffer(worker: RolloutWorker):
        # Generate a sample.
        sample = worker.sample()
        # Send the per-agent SampleBatches to the correct buffer(s),
        # depending on which policies participated in the episode.
        assert isinstance(sample, MultiAgentBatch)
        for pid, batch in sample.policy_batches.items():
            # Don't send data, if policy is not trainable.
            if pid in worker._policy_learners:
                replay_actor = worker._policy_learners[pid][1]
                ma_batch = MultiAgentBatch({pid: batch}, batch.count)
                replay_actor.add_batch.remote(ma_batch)
        # Return counts (env-steps, agent-steps).
        return sample.count, sample.agent_steps()

    @staticmethod
    def _update_policy(policy: Policy, replay_actor: ActorHandle,
                       pid: PolicyID):
        if not hasattr(policy, "_target_and_kl_stats"):
            policy._target_and_kl_stats = {
                LAST_TARGET_UPDATE_TS: 0,
                NUM_TARGET_UPDATES: 0,
                NUM_AGENT_STEPS_TRAINED: 0,
                TARGET_NET_UPDATE_TIMER: _Timer()
            }

        train_results = policy.learn_on_batch_from_replay_buffer(
            replay_actor=replay_actor, policy_id=pid)

        if not train_results:
            return train_results

        # Update target net and KL.
        with policy._target_and_kl_stats[TARGET_NET_UPDATE_TIMER]:
            policy._target_and_kl_stats[
                NUM_AGENT_STEPS_TRAINED] += train_results[
                    NUM_AGENT_STEPS_TRAINED]
            target_update_freq = policy.config["num_sgd_iter"] * policy.config["replay_buffer_capacity"] * policy.config["train_batch_size"]
            cur_ts = policy._target_and_kl_stats[NUM_AGENT_STEPS_TRAINED]
            last_update = policy._target_and_kl_stats[LAST_TARGET_UPDATE_TS]

            # Update target networks on all policy learners.
            if cur_ts - last_update > target_update_freq:
                policy._target_and_kl_stats[NUM_TARGET_UPDATES] += 1
                policy._target_and_kl_stats[LAST_TARGET_UPDATE_TS] = cur_ts
                policy.update_target()
                # Also update Policy's current KL coeff.
                if policy.config["use_kl_loss"]:
                    policy.update_kl(TODO)

        return train_results
