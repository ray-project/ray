"""
Distributed Prioritized Experience Replay (Ape-X)
=================================================

This file defines a DQN algorithm using the Ape-X architecture.

Ape-X uses a single GPU learner and many CPU workers for experience collection.
Experience collection can scale to hundreds of CPU workers due to the
distributed prioritization of experience prior to storage in replay buffers.

Detailed documentation:
https://docs.ray.io/en/master/rllib-algorithms.html#distributed-prioritized-experience-replay-ape-x
"""  # noqa: E501
import copy
import platform
import random
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

import ray
from ray._private.dict import merge_dicts
from ray.actor import ActorHandle
from ray.rllib.algorithms import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.dqn.dqn import DQN, DQNConfig
from ray.rllib.algorithms.dqn.learner_thread import LearnerThread
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.evaluation.worker_set import handle_remote_call_result_errors
from ray.rllib.utils.actor_manager import FaultTolerantActorManager
from ray.rllib.utils.actors import create_colocated_actors
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import DEPRECATED_VALUE, Deprecated
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
    NUM_TARGET_UPDATES,
    SAMPLE_TIMER,
    SYNCH_WORKER_WEIGHTS_TIMER,
    TARGET_NET_UPDATE_TIMER,
)
from ray.rllib.utils.typing import (
    ResultDict,
    SampleBatchType,
)
from ray.tune.trainable import Trainable
from ray.tune.execution.placement_groups import PlacementGroupFactory


class ApexDQNConfig(DQNConfig):
    """Defines a configuration class from which an ApexDQN Algorithm can be built.

    Example:
        >>> from ray.rllib.algorithms.apex_dqn.apex_dqn import ApexDQNConfig
        >>> config = ApexDQNConfig()
        >>> print(config.replay_buffer_config) # doctest: +SKIP
        >>> replay_config = config.replay_buffer_config.update( # doctest: +SKIP
        ...     {
        ...         "capacity": 100000,
        ...         "prioritized_replay_alpha": 0.45,
        ...         "prioritized_replay_beta": 0.55,
        ...         "prioritized_replay_eps": 3e-6,
        ...     }
        ... )
        >>> config = config.training(replay_buffer_config=replay_config) #doctest: +SKIP
        >>> config = config.resources(num_gpus=1)  # doctest: +SKIP
        >>> config = config.rollouts(num_rollout_workers=30)  # doctest: +SKIP
        >>> config = config.environment("CartPole-v1")  # doctest: +SKIP
        >>> algo = config.build() # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.apex_dqn.apex_dqn import ApexDQNConfig
        >>> from ray import air
        >>> from ray import tune
        >>> config = ApexDQNConfig()
        >>> config.training(  # doctest: +SKIP
        ...     num_atoms=tune.grid_search(list(range(1, 11)))
        >>> config.environment(env="CartPole-v1")  # doctest: +SKIP
        >>> tune.Tuner( # doctest: +SKIP
        ...     "APEX",
        ...     run_config=air.RunConfig(stop={"episode_reward_mean":200}),
        ...     param_space=config.to_dict()
        ... ).fit()

    Example:
        >>> from ray.rllib.algorithms.apex_dqn.apex_dqn import ApexDQNConfig
        >>> config = ApexDQNConfig()
        >>> print(config.exploration_config)  # doctest: +SKIP
        >>> explore_config = config.exploration_config.update(  # doctest: +SKIP
        ...     {
        ...         "type": "EpsilonGreedy",
        ...         "initial_epsilon": 0.96,
        ...         "final_epsilon": 0.01,
        ...         "epsilone_timesteps": 5000,
        ...     }
        ... )
        >>> config = config.training(  # doctest: +SKIP
        ...     lr_schedule=[[1, 1e-3, [500, 5e-3]]
        ... )
        >>> config = config.exploration(  # doctest: +SKIP
        ...     exploration_config=explore_config
        ... )

    Example:
        >>> from ray.rllib.algorithms.apex_dqn.apex_dqn import ApexDQNConfig
        >>> config = ApexDQNConfig()
        >>> print(config.exploration_config)  # doctest: +SKIP
        >>> explore_config = config.exploration_config.update(  # doctest: +SKIP
        ...     {
        ...         "type": "SoftQ",
        ...         "temperature": [1.0],
        ...     }
        ... )
        >>> config = config.training(  # doctest: +SKIP
        ...     lr_schedule=[[1, 1e-3, [500, 5e-3]]
        ... )
        >>> config = config.exploration(  # doctest: +SKIP
        ...     exploration_config=explore_config
        ... )
    """

    def __init__(self, algo_class=None):
        """Initializes a ApexConfig instance."""
        super().__init__(algo_class=algo_class or ApexDQN)

        # fmt: off
        # __sphinx_doc_begin__
        # APEX-DQN settings overriding DQN ones:
        # .training()
        self.optimizer = merge_dicts(
            DQNConfig().optimizer, {
                "max_weight_sync_delay": 400,
                "num_replay_buffer_shards": 4,
                "debug": False
            })
        self.n_step = 3
        self.train_batch_size = 512
        self.target_network_update_freq = 500000
        self.training_intensity = 1
        # Number of timesteps to collect from rollout workers before we start
        # sampling from replay buffers for learning. Whether we count this in agent
        # steps  or environment steps depends on config["multiagent"]["count_steps_by"].
        self.num_steps_sampled_before_learning_starts = 50000

        self.max_requests_in_flight_per_replay_worker = float("inf")
        self.timeout_s_sampler_manager = 0.0
        self.timeout_s_replay_manager = 0.0
        # APEX-DQN is using a distributed (non local) replay buffer.
        self.replay_buffer_config = {
            "no_local_replay_buffer": True,
            # Specify prioritized replay by supplying a buffer type that supports
            # prioritization
            "type": "MultiAgentPrioritizedReplayBuffer",
            "capacity": 2000000,
            # Alpha parameter for prioritized replay buffer.
            "prioritized_replay_alpha": 0.6,
            # Beta parameter for sampling from prioritized replay buffer.
            "prioritized_replay_beta": 0.4,
            # Epsilon to add to the TD errors when updating priorities.
            "prioritized_replay_eps": 1e-6,
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
            # Deprecated key.
            "prioritized_replay": DEPRECATED_VALUE,
        }

        # .rollouts()
        self.num_rollout_workers = 32
        self.rollout_fragment_length = 50
        self.exploration_config = {
            "type": "PerWorkerEpsilonGreedy",
        }

        # .resources()
        self.num_gpus = 1

        # .reporting()
        self.min_time_s_per_iteration = 30
        self.min_sample_timesteps_per_iteration = 25000

        # fmt: on
        # __sphinx_doc_end__

    def training(
        self,
        *,
        max_requests_in_flight_per_replay_worker: Optional[int] = NotProvided,
        timeout_s_sampler_manager: Optional[float] = NotProvided,
        timeout_s_replay_manager: Optional[float] = NotProvided,
        **kwargs,
    ) -> "ApexDQNConfig":
        """Sets the training related configuration.

        Args:
            num_atoms: Number of atoms for representing the distribution of return.
                When this is greater than 1, distributional Q-learning is used.
            v_min: Minimum value estimation
            v_max: Maximum value estimation
            noisy: Whether to use noisy network to aid exploration. This adds parametric
                noise to the model weights.
            sigma0: Control the initial parameter noise for noisy nets.
            dueling: Whether to use dueling DQN.
            hiddens: Dense-layer setup for each the advantage branch and the value
                branch
            double_q: Whether to use double DQN.
            n_step: N-step for Q-learning.
            before_learn_on_batch: Callback to run before learning on a multi-agent
                batch of experiences.
            training_intensity: The intensity with which to update the model (vs
                collecting samples from the env).
                If None, uses "natural" values of:
                `train_batch_size` / (`rollout_fragment_length` x `num_workers` x
                `num_envs_per_worker`).
                If not None, will make sure that the ratio between timesteps inserted
                into and sampled from the buffer matches the given values.
                Example:
                training_intensity=1000.0
                train_batch_size=250
                rollout_fragment_length=1
                num_workers=1 (or 0)
                num_envs_per_worker=1
                -> natural value = 250 / 1 = 250.0
                -> will make sure that replay+train op will be executed 4x asoften as
                rollout+insert op (4 * 250 = 1000).
                See: rllib/algorithms/dqn/dqn.py::calculate_rr_weights for further
                details.
            replay_buffer_config: Replay buffer config.
                Examples:
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentReplayBuffer",
                "capacity": 50000,
                "replay_sequence_length": 1,
                }
                - OR -
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentPrioritizedReplayBuffer",
                "capacity": 50000,
                "prioritized_replay_alpha": 0.6,
                "prioritized_replay_beta": 0.4,
                "prioritized_replay_eps": 1e-6,
                "replay_sequence_length": 1,
                }
                - Where -
                prioritized_replay_alpha: Alpha parameter controls the degree of
                prioritization in the buffer. In other words, when a buffer sample has
                a higher temporal-difference error, with how much more probability
                should it drawn to use to update the parametrized Q-network. 0.0
                corresponds to uniform probability. Setting much above 1.0 may quickly
                result as the sampling distribution could become heavily “pointy” with
                low entropy.
                prioritized_replay_beta: Beta parameter controls the degree of
                importance sampling which suppresses the influence of gradient updates
                from samples that have higher probability of being sampled via alpha
                parameter and the temporal-difference error.
                prioritized_replay_eps: Epsilon parameter sets the baseline probability
                for sampling so that when the temporal-difference error of a sample is
                zero, there is still a chance of drawing the sample.
            max_requests_in_flight_per_replay_worker: Max number of inflight requests
                to each replay (shard) worker. See the FaultTolerantActorManager class
                for more details.
                Tuning these values is important when running experimens
                with large sample batches, where there is the risk that the object store
                may fill up, causing spilling of objects to disk. This can cause any
                asynchronous requests to become very slow, making your experiment run
                slow as well. You can inspect the object store during your experiment
                via a call to ray memory on your headnode, and by using the ray
                dashboard. If you're seeing that the object store is filling up,
                turn down the number of remote requests in flight, or enable compression
                in your experiment of timesteps.
            timeout_s_sampler_manager: The timeout for waiting for sampling results
                for workers -- typically if this is too low, the manager won't be able
                to retrieve ready sampling results.
            timeout_s_replay_manager: The timeout for waiting for replay worker
                results -- typically if this is too low, the manager won't be able to
                retrieve ready replay requests.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if max_requests_in_flight_per_replay_worker is not NotProvided:
            self.max_requests_in_flight_per_replay_worker = (
                max_requests_in_flight_per_replay_worker
            )
        if timeout_s_sampler_manager is not NotProvided:
            self.timeout_s_sampler_manager = timeout_s_sampler_manager
        if timeout_s_replay_manager is not NotProvided:
            self.timeout_s_replay_manager = timeout_s_replay_manager

        return self

    @override(DQNConfig)
    def validate(self) -> None:
        if self.num_gpus > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for APEX-DQN!")
        # Call DQN's validation method.
        super().validate()


class ApexDQN(DQN):
    @override(Trainable)
    def setup(self, config: AlgorithmConfig):
        super().setup(config)

        num_replay_buffer_shards = self.config.optimizer["num_replay_buffer_shards"]

        # Create copy here so that we can modify without breaking other logic
        replay_actor_config = copy.deepcopy(self.config.replay_buffer_config)

        replay_actor_config["capacity"] = (
            self.config.replay_buffer_config["capacity"] // num_replay_buffer_shards
        )

        ReplayActor = ray.remote(num_cpus=0, max_restarts=-1)(
            replay_actor_config["type"]
        )

        # Place all replay buffer shards on the same node as the learner
        # (driver process that runs this execution plan).
        if replay_actor_config["replay_buffer_shards_colocated_with_driver"]:
            _replay_actors = create_colocated_actors(
                actor_specs=[  # (class, args, kwargs={}, count)
                    (
                        ReplayActor,
                        None,
                        replay_actor_config,
                        num_replay_buffer_shards,
                    )
                ],
                node=platform.node(),  # localhost
            )[
                0
            ]  # [0]=only one item in `actor_specs`.
        # Place replay buffer shards on any node(s).
        else:
            _replay_actors = [
                ReplayActor.remote(*replay_actor_config)
                for _ in range(num_replay_buffer_shards)
            ]
        self._replay_actor_manager = FaultTolerantActorManager(
            _replay_actors,
            max_remote_requests_in_flight_per_actor=(
                self.config.max_requests_in_flight_per_replay_worker
            ),
        )
        self._replay_req_timeout_s = self.config.timeout_s_replay_manager
        self._sample_req_tiemeout_s = self.config.timeout_s_sampler_manager
        self.learner_thread = LearnerThread(self.workers.local_worker())
        self.learner_thread.start()
        self.steps_since_update = defaultdict(int)
        weights = self.workers.local_worker().get_weights()
        self.curr_learner_weights = ray.put(weights)
        self.curr_num_samples_collected = 0
        self._num_ts_trained_since_last_target_update = 0

    @classmethod
    @override(DQN)
    def get_default_config(cls) -> AlgorithmConfig:
        return ApexDQNConfig()

    @override(Algorithm)
    def _remote_worker_ids_for_metrics(self) -> List[int]:
        # Tag those workers (top 1/3rd indices) that we should collect episodes from
        # for metrics due to `PerWorkerEpsilonGreedy` exploration strategy.
        num_remote_workers_for_metrics = self.config["num_workers"] // 3
        return self.workers.healthy_worker_ids()[-num_remote_workers_for_metrics:]

    @override(DQN)
    def training_step(self) -> ResultDict:
        num_samples_ready = self.get_samples_and_store_to_replay_buffers()
        num_worker_samples_collected = defaultdict(int)

        for worker_id, samples_info in num_samples_ready:
            self._counters[NUM_AGENT_STEPS_SAMPLED] += samples_info["agent_steps"]
            self._counters[NUM_ENV_STEPS_SAMPLED] += samples_info["env_steps"]
            num_worker_samples_collected[worker_id] += samples_info["agent_steps"]

        # Update the weights of the workers that returned samples.
        # Only do this if there are remote workers (config["num_workers"] > 1).
        # Also, only update those policies that were actually trained.
        if self.workers.num_remote_workers() > 0:
            self.update_workers(num_worker_samples_collected)

        # Update target network every `target_network_update_freq` sample steps.
        cur_ts = self._counters[
            NUM_AGENT_STEPS_SAMPLED
            if self.config.count_steps_by == "agent_steps"
            else NUM_ENV_STEPS_SAMPLED
        ]

        if cur_ts > self.config.num_steps_sampled_before_learning_starts:
            # trigger a sample from the replay actors and enqueue operation to the
            # learner thread.
            self.sample_from_replay_buffer_place_on_learner_queue_non_blocking(
                num_worker_samples_collected
            )
            self.update_replay_sample_priority()

        # Training step done. Try to bring replay actors back to life if necessary.
        # Replay actors can start fresh, so we do not need to restore any state.
        self._replay_actor_manager.probe_unhealthy_actors()

        return copy.deepcopy(self.learner_thread.learner_info)

    def get_samples_and_store_to_replay_buffers(self):
        # in the case the num_workers = 0
        if self.workers.num_remote_workers() <= 0:
            with self._timers[SAMPLE_TIMER]:
                local_sampling_worker = self.workers.local_worker()
                batch = local_sampling_worker.sample()
                actor_id = random.choice(self._replay_actor_manager.healthy_actor_ids())
                self._replay_actor_manager.foreach_actor(
                    lambda actor: actor.add_batch(batch),
                    remote_actor_ids=[actor_id],
                    timeout_seconds=0,
                )
                batch_statistics = [
                    (
                        0,
                        {
                            "agent_steps": batch.agent_steps(),
                            "env_steps": batch.env_steps(),
                        },
                    )
                ]
                return batch_statistics

        replay_actor_manager = self._replay_actor_manager

        def remote_worker_sample_and_store(worker: RolloutWorker):
            # This function is run as a remote function on sampling workers,
            # and should only be used with the RolloutWorker's apply function ever.
            # It is used to gather samples, and trigger the operation to store them to
            # replay actors from the rollout worker instead of returning the obj
            # refs for the samples to the driver process and doing the sampling
            # operation on there.
            _batch = worker.sample()
            _actor = random.choice(replay_actor_manager.healthy_actor_ids())
            replay_actor_manager.foreach_actor(
                lambda actor: actor.add(_batch),
                remote_actor_ids=[_actor],
                timeout_seconds=0,
            )
            _batch_statistics = {
                "agent_steps": _batch.agent_steps(),
                "env_steps": _batch.env_steps(),
            }
            return _batch_statistics

        # Sample and Store in the Replay Actors on the sampling workers.
        with self._timers[SAMPLE_TIMER]:
            self.workers.foreach_worker_async(
                func=remote_worker_sample_and_store,
                healthy_only=True,
            )
            num_samples_ready = self.workers.fetch_ready_async_reqs(
                timeout_seconds=self._sample_req_tiemeout_s
            )
        return num_samples_ready

    def update_workers(self, _num_samples_ready: Dict[ActorHandle, int]) -> int:
        """Update the remote workers that have samples ready.

        Args:
            _num_samples_ready: A mapping from ActorHandle (RolloutWorker) to
                the number of samples returned by the remote worker.

        Returns:
            The number of remote workers whose weights were updated.
        """
        max_steps_weight_sync_delay = self.config.optimizer["max_weight_sync_delay"]
        # Update our local copy of the weights if the learner thread has updated
        # the learner worker's weights
        policy_ids_updated = self.learner_thread.policy_ids_updated.copy()
        self.learner_thread.policy_ids_updated.clear()
        if policy_ids_updated:
            weights = self.workers.local_worker().get_weights(
                policies=policy_ids_updated
            )
            self.curr_learner_weights = ray.put(weights)

        num_workers_updated = 0

        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            curr_weights = self.curr_learner_weights
            timestep = self._counters[
                NUM_AGENT_STEPS_TRAINED
                if self.config.count_steps_by == "agent_steps"
                else NUM_ENV_STEPS_TRAINED
            ]
            for (
                remote_sampler_worker_id,
                num_samples_collected,
            ) in _num_samples_ready.items():
                self.steps_since_update[
                    remote_sampler_worker_id
                ] += num_samples_collected
                if (
                    self.steps_since_update[remote_sampler_worker_id]
                    >= max_steps_weight_sync_delay
                ):
                    self.workers.foreach_worker(
                        func=lambda w: w.set_weights(
                            ray.get(curr_weights), {"timestep": timestep}
                        ),
                        healthy_only=True,
                        local_worker=False,
                        timeout_seconds=0,  # Do not wait for results.
                    )
                    self.steps_since_update[remote_sampler_worker_id] = 0
                    num_workers_updated += 1

                self._counters["num_weight_syncs"] += 1

        return num_workers_updated

    def sample_from_replay_buffer_place_on_learner_queue_non_blocking(
        self, num_worker_samples_collected: Dict[ActorHandle, int]
    ) -> None:
        """Get samples from the replay buffer and place them on the learner queue.

        Args:
            num_worker_samples_collected: A mapping from ActorHandle (RolloutWorker) to
                number of samples returned by the remote worker. This is used to
                implement training intensity which is the concept of triggering a
                certain amount of training based on the number of samples that have
                been collected since the last time that training was triggered.

        """

        def wait_on_replay_actors() -> List[Tuple[int, SampleBatchType]]:
            """Wait for the replay actors to finish sampling for timeout seconds.

            If the timeout is None, then block on the actors indefinitely.
            """
            results = self._replay_actor_manager.fetch_ready_async_reqs(
                timeout_seconds=self._replay_req_timeout_s
            )
            handle_remote_call_result_errors(
                results, self.config["ignore_worker_failures"]
            )
            return [(r.actor_id, r.get()) for r in results.ignore_errors()]

        num_samples_collected = sum(num_worker_samples_collected.values())
        self.curr_num_samples_collected += num_samples_collected
        # Fetch replayed batched from last round.
        replay_sample_batches = wait_on_replay_actors()
        if (
            self.curr_num_samples_collected >= self.config.train_batch_size
            and
            # There are at least 1 healthy replay actor.
            self._replay_actor_manager.num_healthy_actors() > 0
        ):
            training_intensity = int(self.config.training_intensity or 1)
            num_requests_to_launch = (
                self.curr_num_samples_collected / self.config.train_batch_size
            ) * training_intensity
            num_requests_to_launch = max(1, round(num_requests_to_launch))

            self.curr_num_samples_collected = 0
            train_batch_size = self.config.train_batch_size
            healthy_worker_ids = self._replay_actor_manager.healthy_actor_ids()

            # Make num_requests_to_launch calls to the underlying replay actors.
            worker_ids_to_call = [
                random.choice(healthy_worker_ids) for _ in range(num_requests_to_launch)
            ]
            self._replay_actor_manager.foreach_actor_async(
                func=lambda actor: actor.sample(train_batch_size),
                remote_actor_ids=worker_ids_to_call,
            )
            # Fetch anything that is already ready.
            replay_sample_batches.extend(wait_on_replay_actors())

        # Add all the tuples of (ActorHandle, SampleBatchType) to the learner queue.
        for item in replay_sample_batches:
            # Setting block = True prevents the learner thread,
            # the main thread, and the gpu loader threads from
            # thrashing when there are more samples than the
            # learner can reasonable process.
            # see https://github.com/ray-project/ray/pull/26581#issuecomment-1187877674  # noqa
            self.learner_thread.inqueue.put(item, block=True)
        del replay_sample_batches

    def update_replay_sample_priority(self) -> None:
        """Update the priorities of the sample batches with new priorities that are
        computed by the learner thread.
        """
        num_samples_trained_this_itr = 0
        for _ in range(self.learner_thread.outqueue.qsize()):
            if self.learner_thread.is_alive():
                (
                    replay_actor_id,
                    priority_dict,
                    env_steps,
                    agent_steps,
                ) = self.learner_thread.outqueue.get(timeout=0.001)
                if self.config.replay_buffer_config.get("prioritized_replay_alpha") > 0:
                    self._replay_actor_manager.foreach_actor(
                        func=lambda actor: actor.update_priorities(priority_dict),
                        remote_actor_ids=[replay_actor_id],
                        timeout_seconds=0,  # Do not wait for results.
                    )
                num_samples_trained_this_itr += env_steps
                self.update_target_networks(env_steps)
                self._counters[NUM_ENV_STEPS_TRAINED] += env_steps
                self._counters[NUM_AGENT_STEPS_TRAINED] += agent_steps
                self.workers.local_worker().set_global_vars(
                    {"timestep": self._counters[NUM_ENV_STEPS_TRAINED]}
                )
            else:
                raise RuntimeError("The learner thread died while training")

        self._timers["learner_dequeue"] = self.learner_thread.queue_timer
        self._timers["learner_grad"] = self.learner_thread.grad_timer
        self._timers["learner_overall"] = self.learner_thread.overall_timer

    def update_target_networks(self, num_new_trained_samples) -> None:
        """Update the target networks."""
        self._num_ts_trained_since_last_target_update += num_new_trained_samples
        if (
            self._num_ts_trained_since_last_target_update
            >= self.config.target_network_update_freq
        ):
            self._num_ts_trained_since_last_target_update = 0
            with self._timers[TARGET_NET_UPDATE_TIMER]:
                to_update = self.workers.local_worker().get_policies_to_train()
                self.workers.local_worker().foreach_policy_to_train(
                    lambda p, pid: pid in to_update and p.update_target()
                )
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = self._counters[
                NUM_AGENT_STEPS_TRAINED
                if self.config.count_steps_by == "agent_steps"
                else NUM_ENV_STEPS_TRAINED
            ]

    def _get_shard0_replay_stats(self) -> Dict[str, Any]:
        """Get replay stats from the replay actor shard 0.

        The first healthy replay actor is picked to fetch stats from.
        TODO(jungong) : figure out why not collecting data from all
        replay actors?

        Returns:
            A dictionary of replay stats.
        """
        healthy_actor_ids = self._replay_actor_manager.healthy_actor_ids()
        if not healthy_actor_ids:
            return {}

        healthy_actor_id = healthy_actor_ids[0]
        debug = self.config.optimizer.get("debug")
        results = list(
            self._replay_actor_manager.foreach_actor(
                func=lambda actor: actor.stats(debug),
                remote_actor_ids=[healthy_actor_id],
            )
        )
        if not results:
            return {}
        if not results[0].ok:
            raise results[0].get()
        return results[0].get()

    @override(Algorithm)
    def _compile_iteration_results(self, *args, **kwargs):
        result = super()._compile_iteration_results(*args, **kwargs)
        replay_stats = self._get_shard0_replay_stats()
        exploration_infos_list = self.workers.foreach_policy_to_train(
            lambda p, pid: {pid: p.get_exploration_state()}
        )
        exploration_infos = {}
        for info in exploration_infos_list:
            # we're guaranteed that each info has policy ids that are unique
            exploration_infos.update(info)
        other_results = {
            "exploration_infos": exploration_infos,
            "learner_queue": self.learner_thread.learner_queue_size.stats(),
            "replay_shard_0": replay_stats,
        }

        result["info"].update(other_results)
        return result

    @classmethod
    @override(Algorithm)
    def default_resource_request(cls, config):
        cf = dict(cls.get_default_config(), **config)

        eval_config = cf["evaluation_config"]

        # Return PlacementGroupFactory containing all needed resources
        # (already properly defined as device bundles).
        return PlacementGroupFactory(
            bundles=[
                {
                    # Local worker + replay buffer actors.
                    # Force replay buffers to be on same node to maximize
                    # data bandwidth between buffers and the learner (driver).
                    # Replay buffer actors each contain one shard of the total
                    # replay buffer and use 1 CPU each.
                    "CPU": cf["num_cpus_for_driver"]
                    + cf["optimizer"]["num_replay_buffer_shards"],
                    "GPU": 0 if cf["_fake_gpus"] else cf["num_gpus"],
                }
            ]
            + [
                {
                    # RolloutWorkers.
                    "CPU": cf["num_cpus_per_worker"],
                    "GPU": cf["num_gpus_per_worker"],
                    **cf["custom_resources_per_worker"],
                }
                for _ in range(cf["num_workers"])
            ]
            + (
                [
                    {
                        # Evaluation workers.
                        # Note: The local eval worker is located on the driver
                        # CPU.
                        "CPU": eval_config.get(
                            "num_cpus_per_worker", cf["num_cpus_per_worker"]
                        ),
                        "GPU": eval_config.get(
                            "num_gpus_per_worker", cf["num_gpus_per_worker"]
                        ),
                        **eval_config.get(
                            "custom_resources_per_worker",
                            cf["custom_resources_per_worker"],
                        ),
                    }
                    for _ in range(cf["evaluation_num_workers"])
                ]
                if cf["evaluation_interval"]
                else []
            ),
            strategy=config.get("placement_strategy", "PACK"),
        )


# Deprecated: Use ray.rllib.algorithms.apex_dqn.ApexDQNConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(ApexDQNConfig().to_dict())

    @Deprecated(
        old="ray.rllib.agents.dqn.apex.APEX_DEFAULT_CONFIG",
        new="ray.rllib.algorithms.apex_dqn.apex_dqn.ApexDQNConfig(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


APEX_DEFAULT_CONFIG = _deprecated_default_config()
