"""
A multi-agent, distributed multi-GPU, league-capable asynch. PPO
================================================================
"""
from typing import Any, Dict, Optional, Type

import gym
import tree

import ray
import ray.rllib.algorithms.appo.appo as appo
from ray.actor import ActorHandle
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.alpha_star.distributed_learners import DistributedLearners
from ray.rllib.algorithms.alpha_star.league_builder import AlphaStarLeagueBuilder
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.execution.buffers.mixin_replay_buffer import MixInMultiAgentReplayBuffer
from ray.rllib.execution.parallel_requests import AsyncRequestsManager
from ray.rllib.policy.policy import Policy, PolicySpec
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils import deep_update
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
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
    PartialAlgorithmConfigDict,
    PolicyID,
    PolicyState,
    ResultDict,
)
from ray.tune.execution.placement_groups import PlacementGroupFactory
from ray.util.timer import _Timer


class AlphaStarConfig(appo.APPOConfig):
    """Defines a configuration class from which an AlphaStar Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.alpha_star import AlphaStarConfig
        >>> config = AlphaStarConfig().training(lr=0.0003, train_batch_size=512)\
        ...     .resources(num_gpus=4)\
        ...     .rollouts(num_rollout_workers=64)
        >>> print(config.to_dict()) # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build(env="CartPole-v1")
        >>> algo.train() # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.alpha_star import AlphaStarConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = AlphaStarConfig()
        >>> # Print out some default values.
        >>> print(config.vtrace) # doctest: +SKIP
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.0001, 0.0003]), grad_clip=20.0)
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner( # doctest: +SKIP
        ...     "AlphaStar",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean": 200}),
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a AlphaStarConfig instance."""
        super().__init__(algo_class=algo_class or AlphaStar)

        # fmt: off
        # __sphinx_doc_begin__

        # AlphaStar specific settings:
        self.replay_buffer_capacity = 20
        self.replay_buffer_replay_ratio = 0.5
        # Tuning max_requests_in_flight_per_sampler_worker and
        # max_requests_in_flight_per_learner_worker is important so backpressure is
        # created on the remote workers and the object store doesn't fill up
        # unexpectedly. If the workers spend time idle, consider increasing these.
        self.max_requests_in_flight_per_sampler_worker = 2
        self.max_requests_in_flight_per_learner_worker = 2

        self.timeout_s_sampler_manager = 0.0
        self.timeout_s_learner_manager = 0.0

        # League-building parameters.
        # The LeagueBuilder class to be used for league building logic.
        self.league_builder_config = {
            # Specify the sub-class of the `LeagueBuilder` API to use.
            "type": AlphaStarLeagueBuilder,

            # Any any number of constructor kwargs to pass to this class:

            # The number of random policies to add to the league. This must be an
            # even number (including 0) as these will be evenly distributed
            # amongst league- and main- exploiters.
            "num_random_policies": 2,
            # The number of initially learning league-exploiters to create.
            "num_learning_league_exploiters": 4,
            # The number of initially learning main-exploiters to create.
            "num_learning_main_exploiters": 4,
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
        }
        self.max_num_policies_to_train = None

        # Override some of APPOConfig's default values with AlphaStar-specific
        # values.
        self.vtrace_drop_last_ts = False
        self.min_time_s_per_iteration = 2
        self.policies = None
        self.simple_optimizer = True
        # __sphinx_doc_end__
        # fmt: on

    @override(appo.APPOConfig)
    def training(
        self,
        *,
        replay_buffer_capacity: Optional[int] = NotProvided,
        replay_buffer_replay_ratio: Optional[float] = NotProvided,
        max_requests_in_flight_per_sampler_worker: Optional[int] = NotProvided,
        max_requests_in_flight_per_learner_worker: Optional[int] = NotProvided,
        timeout_s_sampler_manager: Optional[float] = NotProvided,
        timeout_s_learner_manager: Optional[float] = NotProvided,
        league_builder_config: Optional[Dict[str, Any]] = NotProvided,
        max_num_policies_to_train: Optional[int] = NotProvided,
        **kwargs,
    ) -> "AlphaStarConfig":
        """Sets the training related configuration.

        Args:
            replay_buffer_capacity: This is num batches held at any time for each
                policy.
            replay_buffer_replay_ratio: For example, ratio=0.2 -> 20% of samples in
                each train batch are old (replayed) ones.
            timeout_s_sampler_manager: Timeout to use for `ray.wait()` when waiting for
                samplers to have placed new data into the buffers. If no samples are
                ready within the timeout, the buffers used for mixin-sampling will
                return only older samples.
            timeout_s_learner_manager: Timeout to use for `ray.wait()` when waiting for
                the policy learner actors to have performed an update and returned
                learning stats. If no learner actors have produced any learning
                results in the meantime, their learner-stats in the results will be
                empty for that iteration.
            max_requests_in_flight_per_sampler_worker: Maximum number of ray remote
                calls that can be run in parallel for each sampler worker. This is
                particularly important when dealing with many sampler workers or
                sample batches that are large, and when could potentially fill up
                the object store.
            max_requests_in_flight_per_learner_worker: Maximum number of ray remote
                calls that can be run in parallel for each learner worker. This is
                important to tune when dealing with many learner workers so that the
                object store doesn't fill up and so that learner actors don't become
                backed up with too many requests that could become stale if not
                attended to in a timely manner.
            league_builder_config: League-building config dict.
                The dict Must contain a `type` key indicating the LeagueBuilder class
                to be used for league building logic. All other keys (that are not
                `type`) will be used as constructor kwargs on the given class to
                construct the LeagueBuilder instance. See the
                `ray.rllib.algorithms.alpha_star.league_builder::AlphaStarLeagueBuilder`
                (used by default by this algo) as an example.
            max_num_policies_to_train: The maximum number of trainable policies for this
                Algorithm. Each trainable policy will exist as a independent remote
                actor, co-located with a replay buffer. This is besides its existence
                inside the RolloutWorkers for training and evaluation. Set to None for
                automatically inferring this value from the number of trainable
                policies found in the `multiagent` config.

        Returns:
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        # TODO: Unify the buffer API, then clean up our existing
        #  implementations of different buffers.
        if replay_buffer_capacity is not NotProvided:
            self.replay_buffer_capacity = replay_buffer_capacity
        if replay_buffer_replay_ratio is not NotProvided:
            self.replay_buffer_replay_ratio = replay_buffer_replay_ratio
        if timeout_s_sampler_manager is not NotProvided:
            self.timeout_s_sampler_manager = timeout_s_sampler_manager
        if timeout_s_learner_manager is not NotProvided:
            self.timeout_s_learner_manager = timeout_s_learner_manager
        if league_builder_config is not NotProvided:
            # Override entire `league_builder_config` if `type` key changes.
            # Update, if `type` key remains the same or is not specified.
            new_league_builder_config = deep_update(
                {"league_builder_config": self.league_builder_config},
                {"league_builder_config": league_builder_config},
                False,
                ["league_builder_config"],
                ["league_builder_config"],
            )
            self.league_builder_config = new_league_builder_config[
                "league_builder_config"
            ]
        if max_num_policies_to_train is not NotProvided:
            self.max_num_policies_to_train = max_num_policies_to_train
        if max_requests_in_flight_per_sampler_worker is not NotProvided:
            self.max_requests_in_flight_per_sampler_worker = (
                max_requests_in_flight_per_sampler_worker
            )
        if max_requests_in_flight_per_learner_worker is not NotProvided:
            self.max_requests_in_flight_per_learner_worker = (
                max_requests_in_flight_per_learner_worker
            )

        return self


class AlphaStar(appo.APPO):
    _allow_unknown_subkeys = appo.APPO._allow_unknown_subkeys + [
        "league_builder_config",
    ]
    _override_all_subkeys_if_type_changes = (
        appo.APPO._override_all_subkeys_if_type_changes
        + [
            "league_builder_config",
        ]
    )

    @classmethod
    @override(Algorithm)
    def default_resource_request(cls, config):
        cf = dict(cls.get_default_config(), **config)
        # Construct a dummy LeagueBuilder, such that it gets the opportunity to
        # adjust the multiagent config, according to its setup, and we can then
        # properly infer the resources to allocate.
        from_config(cf["league_builder_config"], algo=None, algo_config=cf)

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
    @override(appo.APPO)
    def get_default_config(cls) -> AlgorithmConfig:
        return AlphaStarConfig()

    @override(appo.APPO)
    def setup(self, config: AlphaStarConfig):
        # Create the LeagueBuilder object, allowing it to build the multiagent
        # config as well.
        self.league_builder = from_config(
            self.config.league_builder_config, algo=self, algo_config=self.config
        )

        # Call super's setup to validate config, create RolloutWorkers
        # (train and eval), etc..
        super().setup(config)

        local_worker = self.workers.local_worker()

        # - Create n policy learner actors (@ray.remote-converted Policies) on
        #   one or more GPU nodes.
        # - On each such node, also locate one replay buffer shard.

        # Single CPU replay shard (co-located with GPUs so we can place the
        # policies on the same machine(s)).
        num_gpus = 0.01 if (self.config.num_gpus and not self.config._fake_gpus) else 0
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
            # By default, set max_num_policies_to_train to the number of policy IDs
            # provided in the multiagent config.
            max_num_policies_to_train=(
                self.config.max_num_policies_to_train
                or len(self.workers.local_worker().get_policies_to_train())
            ),
            replay_actor_class=ReplayActor,
            replay_actor_args=replay_actor_args,
        )
        policies, _ = self.config.get_multi_agent_setup(
            spaces=local_worker.spaces,
            default_policy_class=local_worker.default_policy_class,
        )
        for pid, policy_spec in policies.items():
            if (
                local_worker.is_policy_to_train is None
                or local_worker.is_policy_to_train(pid)
            ):
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
        self._sampling_actor_manager = AsyncRequestsManager(
            self.workers.remote_workers(),
            max_remote_requests_in_flight_per_worker=self.config[
                "max_requests_in_flight_per_sampler_worker"
            ],
            ray_wait_timeout_s=self.config.timeout_s_sampler_manager,
        )
        policy_actors = [policy_actor for _, policy_actor, _ in distributed_learners]
        self._learner_worker_manager = AsyncRequestsManager(
            workers=policy_actors,
            max_remote_requests_in_flight_per_worker=self.config[
                "max_requests_in_flight_per_learner_worker"
            ],
            ray_wait_timeout_s=self.config.timeout_s_learner_manager,
        )

    @override(Algorithm)
    def step(self) -> ResultDict:
        # Perform a full step (including evaluation).
        result = super().step()

        # Based on the (train + evaluate) results, perform a step of
        # league building.
        self.league_builder.build_league(result=result)

        return result

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        # Trigger asynchronous rollouts on all RolloutWorkers.
        # - Rollout results are sent directly to correct replay buffer
        #   shards, instead of here (to the driver).
        with self._timers[SAMPLE_TIMER]:
            # if there are no remote workers (e.g. num_workers=0)
            if not self.workers.remote_workers():
                worker = self.workers.local_worker()
                statistics = worker.apply(self._sample_and_send_to_buffer)
                sample_results = {worker: [statistics]}
            else:
                self._sampling_actor_manager.call_on_all_available(
                    self._sample_and_send_to_buffer
                )
                sample_results = self._sampling_actor_manager.get_ready()
        # Update sample counters.
        for sample_result in sample_results.values():
            for (env_steps, agent_steps) in sample_result:
                self._counters[NUM_ENV_STEPS_SAMPLED] += env_steps
                self._counters[NUM_AGENT_STEPS_SAMPLED] += agent_steps

        # Trigger asynchronous training update requests on all learning
        # policies.
        with self._timers[LEARN_ON_BATCH_TIMER]:
            for pid, pol_actor, repl_actor in self.distributed_learners:
                if pol_actor not in self._learner_worker_manager.workers:
                    self._learner_worker_manager.add_workers(pol_actor)
                self._learner_worker_manager.call(
                    self._update_policy, actor=pol_actor, fn_args=[repl_actor, pid]
                )
            train_results = self._learner_worker_manager.get_ready()

        # Update sample counters.
        for train_result in train_results.values():
            for result in train_result:
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
            for pol_actor, policy_results in train_results.items():
                results_have_same_structure = True
                for result1, result2 in zip(policy_results, policy_results[1:]):
                    try:
                        tree.assert_same_structure(result1, result2)
                    except (ValueError, TypeError):
                        results_have_same_structure = False
                        break
                if len(policy_results) > 1 and results_have_same_structure:
                    policy_result = tree.map_structure(
                        lambda *_args: sum(_args) / len(policy_results), *policy_results
                    )
                else:
                    policy_result = policy_results[-1]
                if policy_result:
                    pid = self.distributed_learners.get_policy_id(pol_actor)
                    train_infos[pid] = policy_result
                    policy_weights[pid] = pol_actor.get_weights.remote()

            policy_weights_ref = ray.put(policy_weights)

            global_vars = {
                "timestep": self._counters[NUM_ENV_STEPS_SAMPLED],
                "league_builder": self.league_builder.__getstate__(),
            }

            for worker in self.workers.remote_workers():
                worker.set_weights.remote(policy_weights_ref, global_vars)

        return train_infos

    @override(Algorithm)
    def add_policy(
        self,
        policy_id: PolicyID,
        policy_cls: Type[Policy],
        *,
        observation_space: Optional[gym.spaces.Space] = None,
        action_space: Optional[gym.spaces.Space] = None,
        config: Optional[PartialAlgorithmConfigDict] = None,
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

    @override(Algorithm)
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
                replay_actor.add.remote(ma_batch)
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

    @override(appo.APPO)
    def __getstate__(self) -> dict:
        state = super().__getstate__()
        state.update(
            {
                "league_builder": self.league_builder.__getstate__(),
            }
        )
        return state

    @override(appo.APPO)
    def __setstate__(self, state: dict) -> None:
        state_copy = state.copy()
        self.league_builder.__setstate__(state.pop("league_builder", {}))
        super().__setstate__(state_copy)


# Deprecated: Use ray.rllib.algorithms.alpha_star.AlphaStarConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(AlphaStarConfig().to_dict())

    @Deprecated(
        old="ray.rllib.algorithms.alpha_star.alpha_star.DEFAULT_CONFIG",
        new="ray.rllib.algorithms.alpha_star.alpha_star.AlphaStarConfig(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
