"""
A multi-agent, distributed multi-GPU, league-capable asynch. PPO
================================================================
"""
from collections import defaultdict
import gym
from typing import DefaultDict, Optional, Type

import ray
from ray.actor import ActorHandle
from ray.rllib.agents.alpha_star.distributed_learners import DistributedLearners
from ray.rllib.agents.alpha_star.league_builder import AlphaStarLeagueBuilder
from ray.rllib.agents.trainer import Trainer
import ray.rllib.agents.ppo.appo as appo
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.execution.parallel_requests import asynchronous_parallel_requests
from ray.rllib.execution.buffers.mixin_replay_buffer import MixInMultiAgentReplayBuffer
from ray.rllib.policy.policy import Policy, PolicySpec
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.from_config import from_config
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    LEARN_ON_BATCH_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_TARGET_UPDATES,
    SAMPLE_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
    TARGET_NET_UPDATE_TIMER,
)
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.typing import (
    PartialTrainerConfigDict,
    PolicyID,
    PolicyState,
    TrainerConfigDict,
    ResultDict,
)
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
        # The LeagueBuilder class to be used for league building logic.
        "league_builder_config": {
            "type": AlphaStarLeagueBuilder,
            # Minimum win-rate (between 0.0 = 0% and 1.0 = 100%) of any policy to
            # be considered for snapshotting (cloning). The cloned copy may then
            # be frozen (no further learning) or keep learning (independent of
            # its ancestor policy).
            # Set this to lower values to speed up league growth.
            "win_rate_threshold_for_new_snapshot": 0.9,
            # If we took a new snapshot of any given policy, what's the probability
            # that this snapshot will continue to be trainable (rather than become
            # frozen/non-trainable)? By default, only keep those policies trainable
            # that have been trainable from the very beginning.
            "keep_new_snapshot_training_prob": 0.0,
            # Probabilities of different match-types:
            # LE: Learning league_exploiter vs any.
            # ME: Learning main exploiter vs any main.
            # M: Main self-play (p=1.0 - LE - ME).
            "prob_league_exploiter_match": 0.33,
            "prob_main_exploiter_match": 0.33,
            # Only for ME matches: Prob to play against learning
            # main (vs a snapshot main).
            "prob_main_exploiter_playing_against_learning_main": 0.5,
        },

        # The maximum number of trainable policies for this Trainer.
        # Each trainable policy will exist as a independent remote actor, co-located
        # with a replay buffer. This is besides its existence inside
        # the RolloutWorkers for training and evaluation.
        # Set to None for automatically inferring this value from the number of
        # trainable policies found in the `multiagent` config.
        "max_num_policies_to_train": None,

        # By default, don't drop last timestep.
        # TODO: We should do the same for IMPALA and APPO at some point.
        "vtrace_drop_last_ts": False,

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
    _allow_unknown_subkeys = appo.APPOTrainer._allow_unknown_subkeys + [
        "league_builder_config",
    ]
    _override_all_subkeys_if_type_changes = (
        appo.APPOTrainer._override_all_subkeys_if_type_changes
        + [
            "league_builder_config",
        ]
    )

    @classmethod
    @override(Trainer)
    def default_resource_request(cls, config):
        cf = dict(cls.get_default_config(), **config)
        # Construct a dummy LeagueBuilder, such that it gets the opportunity to
        # adjust the multiagent config, according to its setup, and we can then
        # properly infer the resources to allocate.
        from_config(cf["league_builder_config"], trainer=None, trainer_config=cf)

        max_num_policies_to_train = cf["max_num_policies_to_train"] or len(
            cf["multiagent"].get("policies_to_train") or cf["multiagent"]["policies"]
        )
        num_learner_shards = min(
            cf["num_gpus"] or max_num_policies_to_train, max_num_policies_to_train
        )
        num_gpus_per_shard = cf["num_gpus"] / num_learner_shards
        num_policies_per_shard = max_num_policies_to_train / num_learner_shards

        fake_gpus = cf["_fake_gpus"]

        eval_config = cf["evaluation_config"]

        # Return PlacementGroupFactory containing all needed resources
        # (already properly defined as device bundles).
        return PlacementGroupFactory(
            bundles=[
                {
                    # Driver (no GPUs).
                    "CPU": cf["num_cpus_for_driver"],
                }
            ]
            + [
                {
                    # RolloutWorkers (no GPUs).
                    "CPU": cf["num_cpus_per_worker"],
                }
                for _ in range(cf["num_workers"])
            ]
            + [
                {
                    # Policy learners (and Replay buffer shards).
                    # 1 CPU for the replay buffer.
                    # 1 CPU (or fractional GPU) for each learning policy.
                    "CPU": 1 + (num_policies_per_shard if fake_gpus else 0),
                    "GPU": 0 if fake_gpus else num_gpus_per_shard,
                }
                for _ in range(num_learner_shards)
            ]
            + (
                [
                    {
                        # Evaluation (remote) workers.
                        # Note: The local eval worker is located on the driver
                        # CPU or not even created iff >0 eval workers.
                        "CPU": eval_config.get(
                            "num_cpus_per_worker", cf["num_cpus_per_worker"]
                        ),
                    }
                    for _ in range(cf["evaluation_num_workers"])
                ]
                if cf["evaluation_interval"]
                else []
            ),
            strategy=config.get("placement_strategy", "PACK"),
        )

    @classmethod
    @override(appo.APPOTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(appo.APPOTrainer)
    def validate_config(self, config: TrainerConfigDict):
        # Create the LeagueBuilder object, allowing it to build the multiagent
        # config as well.
        self.league_builder = from_config(
            config["league_builder_config"], trainer=self, trainer_config=config
        )
        super().validate_config(config)

    @override(appo.APPOTrainer)
    def setup(self, config: PartialTrainerConfigDict):
        # Call super's setup to validate config, create RolloutWorkers
        # (train and eval), etc..
        num_gpus_saved = config["num_gpus"]
        config["num_gpus"] = min(config["num_gpus"], 1)
        super().setup(config)
        self.config["num_gpus"] = num_gpus_saved

        # - Create n policy learner actors (@ray.remote-converted Policies) on
        #   one or more GPU nodes.
        # - On each such node, also locate one replay buffer shard.

        ma_cfg = self.config["multiagent"]
        # By default, set max_num_policies_to_train to the number of policy IDs
        # provided in the multiagent config.
        if self.config["max_num_policies_to_train"] is None:
            self.config["max_num_policies_to_train"] = len(
                self.workers.local_worker().get_policies_to_train()
            )

        # Single CPU replay shard (co-located with GPUs so we can place the
        # policies on the same machine(s)).
        num_gpus = (
            0.01 if (self.config["num_gpus"] and not self.config["_fake_gpus"]) else 0
        )
        ReplayActor = ray.remote(
            num_cpus=1,
            num_gpus=num_gpus,
        )(MixInMultiAgentReplayBuffer)

        # Setup remote replay buffer shards and policy learner actors
        # (located on any GPU machine in the cluster):
        replay_actor_args = [
            self.config["replay_buffer_capacity"],
            self.config["replay_buffer_replay_ratio"],
        ]

        # Create a DistributedLearners utility object and set it up with
        # the initial first n learnable policies (found in the config).
        distributed_learners = DistributedLearners(
            config=self.config,
            max_num_policies_to_train=self.config["max_num_policies_to_train"],
            replay_actor_class=ReplayActor,
            replay_actor_args=replay_actor_args,
        )
        for pid, policy_spec in ma_cfg["policies"].items():
            if pid in self.workers.local_worker().get_policies_to_train():
                distributed_learners.add_policy(pid, policy_spec)

        # Store distributed_learners on all RolloutWorkers
        # so they know, to which replay shard to send samples to.

        def _set_policy_learners(worker):
            worker._distributed_learners = distributed_learners

        ray.get(
            [
                w.apply.remote(_set_policy_learners)
                for w in self.workers.remote_workers()
            ]
        )

        self.distributed_learners = distributed_learners

        # Store the win rates for league overview printouts.
        self.win_rates: DefaultDict[PolicyID, float] = defaultdict(float)

    @override(Trainer)
    def step(self) -> ResultDict:
        # Perform a full step (including evaluation).
        result = super().step()

        # Based on the (train + evaluate) results, perform a step of
        # league building.
        self.league_builder.build_league(result=result)

        return result

    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        # Trigger asynchronous rollouts on all RolloutWorkers.
        # - Rollout results are sent directly to correct replay buffer
        #   shards, instead of here (to the driver).
        with self._timers[SAMPLE_TIMER]:
            sample_results = asynchronous_parallel_requests(
                remote_requests_in_flight=self.remote_requests_in_flight,
                actors=self.workers.remote_workers() or [self.workers.local_worker()],
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
            for pid, pol_actor, repl_actor in self.distributed_learners:
                pol_actors.append(pol_actor)
                args.append([repl_actor, pid])
            train_results = asynchronous_parallel_requests(
                remote_requests_in_flight=self.remote_requests_in_flight,
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
                    NUM_AGENT_STEPS_TRAINED
                ]

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
                    pid = self.distributed_learners.get_policy_id(pol_actor)
                    train_infos[pid] = policy_result
                    policy_weights[pid] = pol_actor.get_weights.remote()

            policy_weights_ref = ray.put(policy_weights)

            global_vars = {
                "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
                "win_rates": self.win_rates,
            }

            for worker in self.workers.remote_workers():
                worker.set_weights.remote(policy_weights_ref, global_vars)

        return train_infos

    @override(Trainer)
    def add_policy(
        self,
        policy_id: PolicyID,
        policy_cls: Type[Policy],
        *,
        observation_space: Optional[gym.spaces.Space] = None,
        action_space: Optional[gym.spaces.Space] = None,
        config: Optional[PartialTrainerConfigDict] = None,
        policy_state: Optional[PolicyState] = None,
        **kwargs,
    ) -> Policy:
        # Add the new policy to all our train- and eval RolloutWorkers
        # (including the local worker).
        new_policy = super().add_policy(
            policy_id,
            policy_cls,
            observation_space=observation_space,
            action_space=action_space,
            config=config,
            policy_state=policy_state,
            **kwargs,
        )

        # Do we have to create a policy-learner actor from it as well?
        if policy_id in kwargs.get("policies_to_train", []):
            new_policy_actor = self.distributed_learners.add_policy(
                policy_id,
                PolicySpec(
                    policy_cls,
                    new_policy.observation_space,
                    new_policy.action_space,
                    self.config,
                ),
            )
            # Set state of new policy actor, if provided.
            if policy_state is not None:
                ray.get(new_policy_actor.set_state.remote(policy_state))

        return new_policy

    @override(Trainer)
    def cleanup(self) -> None:
        super().cleanup()
        # Stop all policy- and replay actors.
        self.distributed_learners.stop()

    @staticmethod
    def _sample_and_send_to_buffer(worker: RolloutWorker):
        # Generate a sample.
        sample = worker.sample()
        # Send the per-agent SampleBatches to the correct buffer(s),
        # depending on which policies participated in the episode.
        assert isinstance(sample, MultiAgentBatch)
        for pid, batch in sample.policy_batches.items():
            # Don't send data, if policy is not trainable.
            replay_actor, _ = worker._distributed_learners.get_replay_and_policy_actors(
                pid
            )
            if replay_actor is not None:
                ma_batch = MultiAgentBatch({pid: batch}, batch.count)
                replay_actor.add_batch.remote(ma_batch)
        # Return counts (env-steps, agent-steps).
        return sample.count, sample.agent_steps()

    @staticmethod
    def _update_policy(policy: Policy, replay_actor: ActorHandle, pid: PolicyID):
        if not hasattr(policy, "_target_and_kl_stats"):
            policy._target_and_kl_stats = {
                LAST_TARGET_UPDATE_TS: 0,
                NUM_TARGET_UPDATES: 0,
                NUM_AGENT_STEPS_TRAINED: 0,
                TARGET_NET_UPDATE_TIMER: _Timer(),
            }

        train_results = policy.learn_on_batch_from_replay_buffer(
            replay_actor=replay_actor, policy_id=pid
        )

        if not train_results:
            return train_results

        # Update target net and KL.
        with policy._target_and_kl_stats[TARGET_NET_UPDATE_TIMER]:
            policy._target_and_kl_stats[NUM_AGENT_STEPS_TRAINED] += train_results[
                NUM_AGENT_STEPS_TRAINED
            ]
            target_update_freq = (
                policy.config["num_sgd_iter"]
                * policy.config["replay_buffer_capacity"]
                * policy.config["train_batch_size"]
            )
            cur_ts = policy._target_and_kl_stats[NUM_AGENT_STEPS_TRAINED]
            last_update = policy._target_and_kl_stats[LAST_TARGET_UPDATE_TS]

            # Update target networks on all policy learners.
            if cur_ts - last_update > target_update_freq:
                policy._target_and_kl_stats[NUM_TARGET_UPDATES] += 1
                policy._target_and_kl_stats[LAST_TARGET_UPDATE_TS] = cur_ts
                policy.update_target()
                # Also update Policy's current KL coeff.
                if policy.config["use_kl_loss"]:
                    kl = train_results[LEARNER_STATS_KEY].get("kl")
                    assert kl is not None, train_results
                    # Make the actual `Policy.update_kl()` call.
                    policy.update_kl(kl)

        return train_results
