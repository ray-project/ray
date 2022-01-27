"""
A multi-agent, distributed multi-GPU, league-capable asynch. PPO
================================================================
"""
import gym
import numpy as np
import re
from typing import Dict, Optional, Type

import ray
from ray.actor import ActorHandle
from ray.rllib.agents.alpha_star.distributed_learners import DistributedLearners
from ray.rllib.agents.trainer import Trainer
import ray.rllib.agents.ppo.appo as appo
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.examples.policy.random_policy import RandomPolicy
from ray.rllib.execution.parallel_requests import asynchronous_parallel_requests
from ray.rllib.execution.buffers.mixin_replay_buffer import \
    MixInMultiAgentReplayBuffer
from ray.rllib.policy.policy import Policy, PolicySpec
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import LAST_TARGET_UPDATE_TS, \
    LEARN_ON_BATCH_TIMER, NUM_AGENT_STEPS_SAMPLED, NUM_AGENT_STEPS_TRAINED, \
    NUM_ENV_STEPS_SAMPLED, NUM_TARGET_UPDATES, \
    SAMPLE_TIMER, SYNCH_WORKER_WEIGHTS_TIMER, TARGET_NET_UPDATE_TIMER
from ray.rllib.utils.numpy import softmax
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.typing import PartialTrainerConfigDict,\
    PolicyID, PolicyState, TrainerConfigDict, ResultDict
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

        # The maximum number of trainable policies for this Trainer.
        # Each trainable policy will exist as a independent remote actor, co-locate
        # with a replay buffer. This is besides its existence inside
        # the RolloutWorkers for training and evaluation.
        "max_num_policies_to_train": 11,

        # Basic "multiagent" setup for league-building AlphaStar:
        # Start with "main_0" (the policy we would like to use in the very end),
        # "league_exploiter_0" (a random policy) and "main_exploiter_0"
        # (also random).
        # Once "main_0" reaches n% win-rate, we'll create a snapshot of it
        # (main_1) as well as create learning "league_exploiter_1" and
        # "main_exploiter_1" policies (cloned off "main_0").
        "multiagent": {
            # Initial policy map. This will be expanded
            # to more policy snapshots inside the `build_league` method.
            "policies": {
                # Our main policy, we'd like to optimize.
                "main_0": PolicySpec(),
                # Initial main exploiters (1 of these is a random policy).
                "main_exploiter_0": PolicySpec(policy_class=RandomPolicy),
                "main_exploiter_1": PolicySpec(),
                "main_exploiter_2": PolicySpec(),
                "main_exploiter_3": PolicySpec(),
                "main_exploiter_4": PolicySpec(),
                # Initial league exploiter (1 of these is a random policy).
                "league_exploiter_0": PolicySpec(policy_class=RandomPolicy),
                "league_exploiter_1": PolicySpec(),
                "league_exploiter_2": PolicySpec(),
                "league_exploiter_3": PolicySpec(),
                "league_exploiter_4": PolicySpec(),
            },
            "policy_mapping_fn":
                (lambda aid, ep, worker, **kw: "main_0" if
                 ep.episode_id % 2 == aid else "main_exploiter_0"),
            # Train all non-random policies that exist at beginning.
            "policies_to_train": [
                "main_0", "main_exploiter_1", "main_exploiter_2",
                "main_exploiter_3", "main_exploiter_4", "league_exploiter_1",
                "league_exploiter_2", "league_exploiter_3",
                "league_exploiter_4",
            ],
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

        num_policies = len(cf["max_num_policies_to_train"])
        if cf["num_gpus"]:
            num_learner_shards = min(cf["num_gpus"], num_policies)
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
        # Call super's setup to validate config, create RolloutWorkers
        # (train and eval), etc..
        super().setup(config)

        # AlphaStar specific setup:
        # - Create n policy learner actors (@ray.remote-converted Policies) on
        #   one or more GPU nodes.
        # - On each such node, also locate one replay buffer shard.
        # -

        ma_cfg = self.config["multiagent"]
        max_num_policies_to_train = self.config["max_num_policies_to_train"]

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

        # Create a DistributedLearners utility object and set it up with
        # the initial first n learnable policies (found in the config).
        distributed_learners = DistributedLearners(
            config=self.config,
            max_num_policies=max_num_policies_to_train,
            replay_actor_class=ReplayActor,
            replay_actor_args=replay_actor_args,
        )
        for pid, policy_spec in ma_cfg["policies"].items():
            if pid in self.workers.local_worker().policies_to_train:
                distributed_learners.add_policy(pid, policy_spec)

        # Store distributed_learners on all RolloutWorkers
        # so they know, to which replay shard to send samples to.

        def _set_policy_learners(worker):
            worker._distributed_learners = distributed_learners

        ray.get([
            w.apply.remote(_set_policy_learners)
            for w in self.workers.remote_workers()
        ])

        self.distributed_learners = distributed_learners

        # Policy groups (main, main_exploit, league_exploit):
        self.main_policies = 1
        self.main_exploiters = 1
        self.league_exploiters = 1
        # Store the win rates for league overview printouts.
        self.win_rates: Dict[PolicyID, float] = {}

    @override(Trainer)
    def step(self) -> ResultDict:
        # Perform a full step (including evaluation).
        result = super().step()

        # Based on the (train + evaluate) results, perform a step of
        # league building.
        self.build_league(result=result)

        return result

    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        # Trigger asynchronous rollouts on all RolloutWorkers.
        # - Rollout results are sent directly to correct replay buffer
        #   shards, instead of here (to the driver).
        with self._timers[SAMPLE_TIMER]:
            sample_results = asynchronous_parallel_requests(
                remote_requests_in_flight=self.remote_requests_in_flight,
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
            for i, (pid, pol_actor,
                    repl_actor) in enumerate(self.distributed_learners):
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
                    pid = self.distributed_learners.get_policy_id(pol_actor)
                    train_infos[pid] = policy_result
                    policy_weights[pid] = pol_actor.get_weights.remote()

            policy_weights_ref = ray.put(policy_weights)

            for worker in self.workers.remote_workers():
                worker.set_weights.remote(policy_weights_ref)

        return train_infos

    def build_league(self, result: ResultDict) -> None:
        """Method containing league-building logic. Called after train step.

        Args:
            result: The most recent result dict with all necessary stats in
                it (e.g. episode rewards) to perform league building
                operations.
        """

        # If no evaluation results -> Use hist data gathered for training.
        if "evaluation" in result:
            hist_stats = result["evaluation"]["hist_stats"]
        else:
            hist_stats = result["hist_stats"]

        trainable_policies = self.workers.local_worker().get_policies_to_train(
        )
        non_trainable_policies = \
            set(self.workers.local_worker().policy_map.keys()) - \
            trainable_policies

        # Calculate current win-rates.
        for policy_id, rew in hist_stats.items():
            mo = re.match("^policy_(.+)_reward$", policy_id)
            if mo is None:
                continue
            policy_id = mo.group(1)

            # Calculate this policy's win rate.
            won = 0
            for r in rew:
                if r > 0.0:  # win = 1.0; loss = -1.0
                    won += 1
            win_rate = won / len(rew)
            # TODO: This should probably be a running average
            #  (instead of hard-overriding it with the most recent data).
            self.win_rates[policy_id] = win_rate

            # Policy is a snapshot (frozen) -> Ignore.
            if policy_id not in trainable_policies:
                continue

            print(
                f"Iter={self.iteration} {policy_id}'s "
                f"win-rate={win_rate} -> ",
                end="")

            # If win rate is good enough -> Snapshot current policy and decide,
            # whether to freeze the new snapshot or not.
            if win_rate >= self.config["win_rate_threshold_for_new_snapshot"]:
                is_main = re.match("^main(_\\d+)?$", policy_id)

                # Probability that the new snapshot is trainable.
                keep_training_p = \
                    self.config["keep_new_snapshot_training_prob"]
                # For main, new snapshots are never trainable, for all others
                # use `config.keep_new_snapshot_training_prob` (default: 0.0!).
                keep_training = False if is_main else np.random.choice(
                    [True, False], p=[keep_training_p, 1.0 - keep_training_p])
                # New league-exploiter policy.
                if policy_id.startswith("league_ex"):
                    new_pol_id = re.sub("_\\d+$", f"_{self.league_exploiters}",
                                        policy_id)
                    self.league_exploiters += 1
                # New main-exploiter policy.
                elif policy_id.startswith("main_ex"):
                    new_pol_id = re.sub("_\\d+$", f"_{self.main_exploiters}",
                                        policy_id)
                    self.main_exploiters += 1
                # New main policy snapshot.
                else:
                    new_pol_id = re.sub("_\\d+$", f"_{self.main_policies}",
                                        policy_id)
                    self.main_policies += 1

                if keep_training:
                    trainable_policies.add(new_pol_id)
                else:
                    non_trainable_policies.add(new_pol_id)

                print(f"adding new opponents to the mix ({new_pol_id}; "
                      f"trainable={keep_training}).")

                num_main_policies = self.main_policies
                probs_match_types = [
                    self.config["prob_league_exploiter_match"],
                    self.config["prob_main_exploiter_match"],
                    1.0 - self.config["prob_league_exploiter_match"] -
                    self.config["prob_main_exploiter_match"]
                ]
                prob_playing_learning_main = \
                    self.config["prob_main_exploiter_playing_against_learning_main"]

                # Update our mapping function accordingly.
                def policy_mapping_fn(agent_id, episode, worker, **kwargs):

                    # Pick, whether this is:
                    # LE: league-exploiter vs snapshot.
                    # ME: main-exploiter vs (any) main.
                    # M: Learning main vs itself.
                    type_ = np.random.choice(
                        ["LE", "ME", "M"], p=probs_match_types)

                    # Learning league exploiter vs a snapshot.
                    # Opponent snapshots should be selected based on a win-rate-
                    # derived probability.
                    if type_ == "LE":
                        league_exploiter = np.random.choice([
                            p for p in trainable_policies
                            if p.startswith("league_ex")
                        ])
                        # Play against any non-trainable policy (excluding itself).
                        all_opponents = list(non_trainable_policies)
                        probs = softmax(
                            [worker.win_rates[pid] for pid in all_opponents])
                        opponent = np.random.choice(all_opponents, p=probs)
                        print(
                            f"{league_exploiter} (training) vs {opponent} (frozen)"
                        )
                        return league_exploiter if \
                            episode.episode_id % 2 == agent_id else opponent

                    # Learning main exploiter vs (learning main OR snapshot main).
                    elif type_ == "ME":
                        main_exploiter = np.random.choice([
                            p for p in trainable_policies
                            if p.startswith("main_ex")
                        ])
                        # n% of the time, play against the learning main.
                        # Also always play againt learning main if no
                        # non-learning mains have been created yet.
                        if num_main_policies == 1 or \
                                (np.random.random() <
                                 prob_playing_learning_main):
                            main = "main_0"
                            training = "training"
                        # 100-n% of the time, play against a non-learning
                        # main. Opponent main snapshots should be selected
                        # based on a win-rate-derived probability.
                        else:
                            all_opponents = [
                                f"main_{p}"
                                for p in list(range(1, num_main_policies))
                            ]
                            probs = softmax([
                                worker.win_rates[pid] for pid in all_opponents
                            ])
                            main = np.random.choice(all_opponents, p=probs)
                            training = "frozen"
                        print(
                            f"{main_exploiter} (training) vs {main} ({training})"
                        )
                        return main_exploiter if \
                            episode.episode_id % 2 == agent_id else main

                    # Main policy: Self-play.
                    else:
                        print("main_0 (training) vs main_0 (training)")
                        return "main_0"

                # Add and set the weights of the new polic(y/ies).
                state = self.get_policy(policy_id).get_state()
                self.add_policy(
                    policy_id=new_pol_id,
                    policy_cls=type(self.get_policy(policy_id)),
                    policy_state=state,
                    policy_mapping_fn=policy_mapping_fn,
                    policies_to_train=trainable_policies,
                )

            else:
                print("not good enough; will keep learning ...")

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
                PolicySpec(policy_cls, new_policy.observation_space,
                           new_policy.action_space, self.config))
            # Set state of new policy actor, if provided.
            if policy_state is not None:
                ray.get(new_policy_actor.set_state.remote(policy_state))

        return new_policy

    @staticmethod
    def _sample_and_send_to_buffer(worker: RolloutWorker):
        # Generate a sample.
        sample = worker.sample()
        # Send the per-agent SampleBatches to the correct buffer(s),
        # depending on which policies participated in the episode.
        assert isinstance(sample, MultiAgentBatch)
        assert len(sample.policy_batches) == 2
        for pid, batch in sample.policy_batches.items():
            # Don't send data, if:
            # - Policy is not trainable.
            # - Data was generated by a main-exploiter playing against
            #   a league-exploiter.
            replay_actor, _ = worker._distributed_learners.get_replay_and_policy_actors(
                pid)
            if replay_actor is not None and (
                    not pid.startswith("main_exploiter_")
                    or next(iter(set(sample.policy_batches.keys()) -
                                 {pid})).startswith("main_")):
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
            target_update_freq = \
                policy.config["num_sgd_iter"] * \
                policy.config["replay_buffer_capacity"] * \
                policy.config["train_batch_size"]
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
