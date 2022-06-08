"""
Distributed Prioritized Experience Replay (Ape-X)
=================================================

This file defines a DQN trainer using the Ape-X architecture.

Ape-X uses a single GPU learner and many CPU workers for experience collection.
Experience collection can scale to hundreds of CPU workers due to the
distributed prioritization of experience prior to storage in replay buffers.

Detailed documentation:
https://docs.ray.io/en/master/rllib-algorithms.html#distributed-prioritized-experience-replay-ape-x
"""  # noqa: E501
import queue
from collections import defaultdict
import copy
import platform
import random
from typing import Dict, List, Type, Optional, Callable

import ray
from ray.actor import ActorHandle
from ray.rllib import Policy
from ray.rllib.agents import Trainer
from ray.rllib.algorithms.dqn.dqn import DQN, DQNConfig
from ray.rllib.algorithms.dqn.learner_thread import LearnerThread
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.execution.common import (
    STEPS_TRAINED_COUNTER,
    STEPS_TRAINED_THIS_ITER_COUNTER,
)
from ray.rllib.execution.parallel_requests import (
    AsyncRequestsManager,
)
from ray.rllib.policy.sample_batch import MultiAgentBatch
from ray.rllib.utils.actors import create_colocated_actors
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, DEPRECATED_VALUE
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
    TrainerConfigDict,
    ResultDict,
    PartialTrainerConfigDict,
)
from ray.tune.trainable import Trainable
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.util.ml_utils.dict import merge_dicts


class ApexDQNConfig(DQNConfig):
    """Defines a configuration class from which an ApexDQN Trainer can be built.

    Example:
        >>> from ray.rllib.algorithms.apex_dqn.apex_dqn import ApexDQNConfig
        >>> config = ApexDQNConfig()
        >>> print(config.replay_buffer_config)
        >>> replay_config = config.replay_buffer_config.update(
        >>>     {
        >>>         "capacity": 100000,
        >>>         "prioritized_replay_alpha": 0.45,
        >>>         "prioritized_replay_beta": 0.55,
        >>>         "prioritized_replay_eps": 3e-6,
        >>>     }
        >>> )
        >>> config.training(replay_buffer_config=replay_config)\
        >>>       .resources(num_gpus=1)\
        >>>       .rollouts(num_rollout_workers=30)\
        >>>       .environment("CartPole-v1")
        >>> trainer = config.build()
        >>> while True:
        >>>     trainer.train()

    Example:
        >>> from ray.rllib.algorithms.apex_dqn.apex_dqn import ApexDQNConfig
        >>> from ray import tune
        >>> config = ApexDQNConfig()
        >>> config.training(num_atoms=tune.grid_search(list(range(1, 11)))
        >>> config.environment(env="CartPole-v1")
        >>> tune.run(
        >>>     "APEX",
        >>>     stop={"episode_reward_mean":200},
        >>>     config=config.to_dict()
        >>> )

    Example:
        >>> from ray.rllib.algorithms.apex_dqn.apex_dqn import ApexDQNConfig
        >>> config = ApexDQNConfig()
        >>> print(config.exploration_config)
        >>> explore_config = config.exploration_config.update(
        >>>     {
        >>>         "type": "EpsilonGreedy",
        >>>         "initial_epsilon": 0.96,
        >>>         "final_epsilon": 0.01,
        >>>         "epsilone_timesteps": 5000,
        >>>     }
        >>> )
        >>> config.training(lr_schedule=[[1, 1e-3, [500, 5e-3]])\
        >>>       .exploration(exploration_config=explore_config)

    Example:
        >>> from ray.rllib.algorithms.apex_dqn.apex_dqn import ApexDQNConfig
        >>> config = ApexDQNConfig()
        >>> print(config.exploration_config)
        >>> explore_config = config.exploration_config.update(
        >>>     {
        >>>         "type": "SoftQ",
        >>>         "temperature": [1.0],
        >>>     }
        >>> )
        >>> config.training(lr_schedule=[[1, 1e-3, [500, 5e-3]])\
        >>>       .exploration(exploration_config=explore_config)
    """

    def __init__(self, trainer_class=None):
        """Initializes a ApexConfig instance."""
        super().__init__(trainer_class=trainer_class or ApexDQN)

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
        self.max_requests_in_flight_per_sampler_worker = 2
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
            # Deprecated key.
            "prioritized_replay": DEPRECATED_VALUE,
        }

        # .rollouts()
        self.num_workers = 32
        self.rollout_fragment_length = 50
        self.exploration_config = {
            "type": "PerWorkerEpsilonGreedy",
        }

        # .resources()
        self.num_gpus = 1

        # .reporting()
        self.min_time_s_per_reporting = 30
        self.min_sample_timesteps_per_reporting = 25000

        # fmt: on
        # __sphinx_doc_end__

    def training(
        self,
        *,
        num_atoms: Optional[int] = None,
        v_min: Optional[float] = None,
        v_max: Optional[float] = None,
        noisy: Optional[bool] = None,
        sigma0: Optional[float] = None,
        dueling: Optional[bool] = None,
        hiddens: Optional[int] = None,
        double_q: Optional[bool] = None,
        n_step: Optional[int] = None,
        before_learn_on_batch: Callable[
            [Type[MultiAgentBatch], List[Type[Policy]], Type[int]],
            Type[MultiAgentBatch],
        ] = None,
        training_intensity: Optional[float] = None,
        replay_buffer_config: Optional[dict] = None,
        max_requests_in_flight_per_sampler_worker: Optional[int] = None,
        max_requests_in_flight_per_replay_worker: Optional[int] = None,
        timeout_s_sampler_manager: Optional[float] = None,
        timeout_s_replay_manager: Optional[float] = None,
        **kwargs,
    ) -> "ApexDQNConfig":
        """Sets the training related configuration.

        Args:
            num_atoms: Number of atoms for representing the distribution of return.
                When this is greater than 1, distributional Q-learning is used.
            v_min: Minimum value estimation
            v_max: Maximum value estimation
            noisy: Whether to use noisy network to aid exploration. This adds
                parametric noise to the model weights.
            sigma0: Control the initial parameter noise for noisy nets.
            dueling: Whether to use dueling DQN policy.
            hiddens: Dense-layer setup for each the advantage branch and the value
                branch
            double_q: Whether to use double DQN for the policy.
            n_step: N-step for Q-learning.
            before_learn_on_batch: Callback to run before learning on a multi-agent
                batch of experiences.
            training_intensity: The ratio of timesteps to train on for every
                timestep that is sampled. This must be greater than 0.
            replay_buffer_config: Replay buffer config.
                Examples:
                {
                "_enable_replay_buffer_api": True,
                "type": "MultiAgentReplayBuffer",
                "learning_starts": 1000,
                "capacity": 50000,
                "replay_batch_size": 32,
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
            max_requests_in_flight_per_sampler_worker: Max number of inflight requests
                to each sampling worker. See the AsyncRequestsManager class for more
                details. Tuning these values is important when running experimens with
                large sample batches, where there is the risk that the object store may
                fill up, causing spilling of objects to disk. This can cause any
                asynchronous requests to become very slow, making your experiment run
                slow as well. You can inspect the object store during your experiment
                via a call to ray memory on your headnode, and by using the ray
                dashboard. If you're seeing that the object store is filling up,
                turn down the number of remote requests in flight, or enable compression
                in your experiment of timesteps.
            max_requests_in_flight_per_replay_worker: Max number of inflight requests
                to each replay (shard) worker. See the AsyncRequestsManager class for
                more details. Tuning these values is important when running experimens
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

        if num_atoms is not None:
            self.num_atoms = num_atoms
        if v_min is not None:
            self.v_min = v_min
        if v_max is not None:
            self.v_max = v_max
        if noisy is not None:
            self.noisy = noisy
        if sigma0 is not None:
            self.sigma0 = sigma0
        if dueling is not None:
            self.dueling = dueling
        if hiddens is not None:
            self.hiddens = hiddens
        if double_q is not None:
            self.double_q = double_q
        if n_step is not None:
            self.n_step = n_step
        if before_learn_on_batch is not None:
            self.before_learn_on_batch = before_learn_on_batch
        if training_intensity is not None:
            self.training_intensity = training_intensity
        if replay_buffer_config is not None:
            self.replay_buffer_config = replay_buffer_config
        if max_requests_in_flight_per_sampler_worker is not None:
            self.max_requests_in_flight_per_sampler_worker = (
                max_requests_in_flight_per_sampler_worker
            )
        if max_requests_in_flight_per_replay_worker is not None:
            self.max_requests_in_flight_per_replay_worker = (
                max_requests_in_flight_per_replay_worker
            )
        if timeout_s_sampler_manager is not None:
            self.timeout_s_sampler_manager = timeout_s_sampler_manager
        if timeout_s_replay_manager is not None:
            self.timeout_s_replay_manager = timeout_s_replay_manager

        return self


class ApexDQN(DQN):
    @override(Trainable)
    def setup(self, config: PartialTrainerConfigDict):
        super().setup(config)

        # Shortcut: If execution_plan, thread and buffer will be created in there.
        if self.config["_disable_execution_plan_api"] is False:
            return

        # Tag those workers (top 1/3rd indices) that we should collect episodes from
        # for metrics due to `PerWorkerEpsilonGreedy` exploration strategy.
        if self.workers.remote_workers():
            self._remote_workers_for_metrics = self.workers.remote_workers()[
                -len(self.workers.remote_workers()) // 3 :
            ]

        num_replay_buffer_shards = self.config["optimizer"]["num_replay_buffer_shards"]

        # Create copy here so that we can modify without breaking other logic
        replay_actor_config = copy.deepcopy(self.config["replay_buffer_config"])

        replay_actor_config["capacity"] = (
            self.config["replay_buffer_config"]["capacity"] // num_replay_buffer_shards
        )

        ReplayActor = ray.remote(num_cpus=0)(replay_actor_config["type"])

        # Place all replay buffer shards on the same node as the learner
        # (driver process that runs this execution plan).
        if replay_actor_config["replay_buffer_shards_colocated_with_driver"]:
            self._replay_actors = create_colocated_actors(
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
            self._replay_actors = [
                ReplayActor.remote(*replay_actor_config)
                for _ in range(num_replay_buffer_shards)
            ]
        self._replay_actor_manager = AsyncRequestsManager(
            self._replay_actors,
            max_remote_requests_in_flight_per_worker=self.config[
                "max_requests_in_flight_per_replay_worker"
            ],
            ray_wait_timeout_s=self.config["timeout_s_replay_manager"],
        )
        self._sampling_actor_manager = AsyncRequestsManager(
            self.workers.remote_workers(),
            max_remote_requests_in_flight_per_worker=self.config[
                "max_requests_in_flight_per_sampler_worker"
            ],
            ray_wait_timeout_s=self.config["timeout_s_sampler_manager"],
        )
        self.learner_thread = LearnerThread(self.workers.local_worker())
        self.learner_thread.start()
        self.steps_since_update = defaultdict(int)
        weights = self.workers.local_worker().get_weights()
        self.curr_learner_weights = ray.put(weights)
        self.curr_num_samples_collected = 0
        self.replay_sample_batches = []
        self._num_ts_trained_since_last_target_update = 0

    @classmethod
    @override(DQN)
    def get_default_config(cls) -> TrainerConfigDict:
        return ApexDQNConfig().to_dict()

    @override(DQN)
    def validate_config(self, config):
        if config["num_gpus"] > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for APEX-DQN!")
        # Call DQN's validation method.
        super().validate_config(config)

    @override(Trainable)
    def training_iteration(self) -> ResultDict:
        num_samples_ready_dict = self.get_samples_and_store_to_replay_buffers()
        worker_samples_collected = defaultdict(int)

        for worker, samples_infos in num_samples_ready_dict.items():
            for samples_info in samples_infos:
                self._counters[NUM_AGENT_STEPS_SAMPLED] += samples_info["agent_steps"]
                self._counters[NUM_ENV_STEPS_SAMPLED] += samples_info["env_steps"]
                worker_samples_collected[worker] += samples_info["agent_steps"]

        # update the weights of the workers that returned samples
        # only do this if there are remote workers (config["num_workers"] > 1)
        if self.workers.remote_workers():
            self.update_workers(worker_samples_collected)
        # trigger a sample from the replay actors and enqueue operation to the
        # learner thread.
        self.sample_from_replay_buffer_place_on_learner_queue_non_blocking(
            worker_samples_collected
        )
        self.update_replay_sample_priority()

        return copy.deepcopy(self.learner_thread.learner_info)

    def get_samples_and_store_to_replay_buffers(self):
        # in the case the num_workers = 0
        if not self.workers.remote_workers():
            with self._timers[SAMPLE_TIMER]:
                local_sampling_worker = self.workers.local_worker()
                batch = local_sampling_worker.sample()
                actor = random.choice(self._replay_actors)
                ray.get(actor.add.remote(batch))
                batch_statistics = {
                    local_sampling_worker: [
                        {
                            "agent_steps": batch.agent_steps(),
                            "env_steps": batch.env_steps(),
                        }
                    ]
                }
                return batch_statistics

        def remote_worker_sample_and_store(
            worker: RolloutWorker, replay_actors: List[ActorHandle]
        ):
            # This function is run as a remote function on sampling workers,
            # and should only be used with the RolloutWorker's apply function ever.
            # It is used to gather samples, and trigger the operation to store them to
            # replay actors from the rollout worker instead of returning the obj
            # refs for the samples to the driver process and doing the sampling
            # operation on there.
            _batch = worker.sample()
            _actor = random.choice(replay_actors)
            _actor.add.remote(_batch)
            _batch_statistics = {
                "agent_steps": _batch.agent_steps(),
                "env_steps": _batch.env_steps(),
            }
            return _batch_statistics

        # Sample and Store in the Replay Actors on the sampling workers.
        with self._timers[SAMPLE_TIMER]:
            self._sampling_actor_manager.call_on_all_available(
                remote_worker_sample_and_store,
                fn_kwargs={"replay_actors": self._replay_actors},
            )
            num_samples_ready_dict = self._sampling_actor_manager.get_ready()
        return num_samples_ready_dict

    def update_workers(self, _num_samples_ready: Dict[ActorHandle, int]) -> int:
        """Update the remote workers that have samples ready.

        Args:
            _num_samples_ready: A mapping from ActorHandle (RolloutWorker) to
                the number of samples returned by the remote worker.
        Returns:
            The number of remote workers whose weights were updated.
        """
        max_steps_weight_sync_delay = self.config["optimizer"]["max_weight_sync_delay"]
        # Update our local copy of the weights if the learner thread has updated
        # the learner worker's weights
        if self.learner_thread.weights_updated:
            self.learner_thread.weights_updated = False
            weights = self.workers.local_worker().get_weights()
            self.curr_learner_weights = ray.put(weights)
        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            for (
                remote_sampler_worker,
                num_samples_collected,
            ) in _num_samples_ready.items():
                self.steps_since_update[remote_sampler_worker] += num_samples_collected
                if (
                    self.steps_since_update[remote_sampler_worker]
                    >= max_steps_weight_sync_delay
                ):
                    remote_sampler_worker.set_weights.remote(
                        self.curr_learner_weights,
                        {"timestep": self._counters[STEPS_TRAINED_COUNTER]},
                    )
                    self.steps_since_update[remote_sampler_worker] = 0
                self._counters["num_weight_syncs"] += 1

    def sample_from_replay_buffer_place_on_learner_queue_non_blocking(
        self, num_samples_collected: Dict[ActorHandle, int]
    ) -> None:
        """Get samples from the replay buffer and place them on the learner queue.

        Args:
            num_samples_collected: A mapping from ActorHandle (RolloutWorker) to
                number of samples returned by the remote worker. This is used to
                implement training intensity which is the concept of triggering a
                certain amount of training based on the number of samples that have
                been collected since the last time that training was triggered.

        """

        def wait_on_replay_actors() -> None:
            """Wait for the replay actors to finish sampling for timeout seconds.
            If the timeout is None, then block on the actors indefinitely.
            """
            _replay_samples_ready = self._replay_actor_manager.get_ready()

            for _replay_actor, _sample_batches in _replay_samples_ready.items():
                for _sample_batch in _sample_batches:
                    self.replay_sample_batches.append((_replay_actor, _sample_batch))

        num_samples_collected = sum(num_samples_collected.values())
        self.curr_num_samples_collected += num_samples_collected
        wait_on_replay_actors()
        if self.curr_num_samples_collected >= self.config["train_batch_size"]:
            training_intensity = int(self.config["training_intensity"] or 1)
            num_requests_to_launch = (
                self.curr_num_samples_collected / self.config["train_batch_size"]
            ) * training_intensity
            num_requests_to_launch = max(1, round(num_requests_to_launch))
            self.curr_num_samples_collected = 0
            for _ in range(num_requests_to_launch):
                self._replay_actor_manager.call(
                    lambda actor, num_items: actor.sample(num_items),
                    fn_args=[self.config["train_batch_size"]],
                )
            wait_on_replay_actors()

        # add the sample batches to the learner queue
        while self.replay_sample_batches:
            try:
                item = self.replay_sample_batches[0]
                # the replay buffer returns none if it has not been filled to
                # the minimum threshold yet.
                if item:
                    self.learner_thread.inqueue.put(
                        self.replay_sample_batches[0], timeout=0.001
                    )
                    self.replay_sample_batches.pop(0)
            except queue.Full:
                break

    def update_replay_sample_priority(self) -> None:
        """Update the priorities of the sample batches with new priorities that are
        computed by the learner thread.
        """
        num_samples_trained_this_itr = 0
        for _ in range(self.learner_thread.outqueue.qsize()):
            if self.learner_thread.is_alive():
                (
                    replay_actor,
                    priority_dict,
                    env_steps,
                    agent_steps,
                ) = self.learner_thread.outqueue.get(timeout=0.001)
                if (
                    self.config["replay_buffer_config"].get("prioritized_replay_alpha")
                    > 0
                ):
                    replay_actor.update_priorities.remote(priority_dict)
                num_samples_trained_this_itr += env_steps
                self.update_target_networks(env_steps)
                self._counters[NUM_ENV_STEPS_TRAINED] += env_steps
                self._counters[NUM_AGENT_STEPS_TRAINED] += agent_steps
                self.workers.local_worker().set_global_vars(
                    {"timestep": self._counters[NUM_ENV_STEPS_TRAINED]}
                )
            else:
                raise RuntimeError("The learner thread died in while training")

        self._counters[STEPS_TRAINED_THIS_ITER_COUNTER] = num_samples_trained_this_itr
        self._timers["learner_dequeue"] = self.learner_thread.queue_timer
        self._timers["learner_grad"] = self.learner_thread.grad_timer
        self._timers["learner_overall"] = self.learner_thread.overall_timer

    def update_target_networks(self, num_new_trained_samples) -> None:
        """Update the target networks."""
        self._num_ts_trained_since_last_target_update += num_new_trained_samples
        if (
            self._num_ts_trained_since_last_target_update
            >= self.config["target_network_update_freq"]
        ):
            self._num_ts_trained_since_last_target_update = 0
            with self._timers[TARGET_NET_UPDATE_TIMER]:
                to_update = self.workers.local_worker().get_policies_to_train()
                self.workers.local_worker().foreach_policy_to_train(
                    lambda p, pid: pid in to_update and p.update_target()
                )
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = self._counters[
                STEPS_TRAINED_COUNTER
            ]

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

    @override(Trainer)
    def _compile_step_results(self, *, step_ctx, step_attempt_results=None):
        result = super()._compile_step_results(
            step_ctx=step_ctx, step_attempt_results=step_attempt_results
        )
        replay_stats = ray.get(
            self._replay_actors[0].stats.remote(self.config["optimizer"].get("debug"))
        )
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
    @override(Trainable)
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
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


APEX_DEFAULT_CONFIG = _deprecated_default_config()
