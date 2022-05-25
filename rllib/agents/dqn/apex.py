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
from typing import Dict, List, DefaultDict, Set

import ray
from ray.actor import ActorHandle
from ray.rllib.agents import Trainer
from ray.rllib.algorithms.dqn.dqn import DQNConfig, DQNTrainer
from ray.rllib.algorithms.dqn.learner_thread import LearnerThread
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.execution.common import (
    STEPS_TRAINED_COUNTER,
    STEPS_TRAINED_THIS_ITER_COUNTER,
)
from ray.rllib.execution.parallel_requests import (
    asynchronous_parallel_requests,
    wait_asynchronous_requests,
)
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
    T,
)
from ray.tune.trainable import Trainable
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.util.ml_utils.dict import merge_dicts


class ApexConfig(DQNConfig):
    """Defines a configuration class from which an ApexTrainer can be built.

    Example:
        >>> from ray.rllib.agents.dqn.apex import ApexConfig
        >>> config = ApexConfig()
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
        >>> trainer = ApexTrainer(config=config)
        >>> while True:
        >>>     trainer.train()

    Example:
        >>> from ray.rllib.agents.dqn.apex import ApexConfig
        >>> from ray import tune
        >>> config = ApexConfig()
        >>> config.training(num_atoms=tune.grid_search(list(range(1, 11)))
        >>> config.environment(env="CartPole-v1")
        >>> tune.run(
        >>>     "APEX",
        >>>     stop={"episode_reward_mean":200},
        >>>     config=config.to_dict()
        >>> )

    Example:
        >>> from ray.rllib.agents.dqn.apex import ApexConfig
        >>> config = ApexConfig()
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
        >>> from ray.rllib.agents.dqn.apex import ApexConfig
        >>> config = ApexConfig()
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
        super().__init__(trainer_class=trainer_class or ApexTrainer)

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


class ApexTrainer(DQNTrainer):
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
            self.replay_actors = create_colocated_actors(
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
            self.replay_actors = [
                ReplayActor.remote(*replay_actor_config)
                for _ in range(num_replay_buffer_shards)
            ]
        self.learner_thread = LearnerThread(self.workers.local_worker())
        self.learner_thread.start()
        self.steps_since_update = defaultdict(int)
        weights = self.workers.local_worker().get_weights()
        self.curr_learner_weights = ray.put(weights)
        self.remote_sampling_requests_in_flight: DefaultDict[
            ActorHandle, Set[ray.ObjectRef]
        ] = defaultdict(set)
        self.remote_replay_requests_in_flight: DefaultDict[
            ActorHandle, Set[ray.ObjectRef]
        ] = defaultdict(set)
        self.curr_num_samples_collected = 0
        self.replay_sample_batches = []
        self._num_ts_trained_since_last_target_update = 0

    @classmethod
    @override(DQNTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return ApexConfig().to_dict()

    @override(DQNTrainer)
    def validate_config(self, config):
        if config["num_gpus"] > 1:
            raise ValueError("`num_gpus` > 1 not yet supported for APEX-DQN!")
        # Call DQN's validation method.
        super().validate_config(config)
        # if config["_disable_execution_plan_api"]:
        #     if not config.get("training_intensity", 1.0) > 0:
        #         raise ValueError("training_intensity must be > 0")

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
                actor = random.choice(self.replay_actors)
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
            # Results are a mapping from ActorHandle (RolloutWorker) to their
            # returned gradient calculation results.
            num_samples_ready_dict: Dict[
                ActorHandle, T
            ] = asynchronous_parallel_requests(
                remote_requests_in_flight=self.remote_sampling_requests_in_flight,
                actors=self.workers.remote_workers(),
                ray_wait_timeout_s=0.1,
                max_remote_requests_in_flight_per_actor=4,
                remote_fn=remote_worker_sample_and_store,
                remote_kwargs=[{"replay_actors": self.replay_actors}]
                * len(self.workers.remote_workers()),
            )
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

        def wait_on_replay_actors(timeout: float) -> None:
            """Wait for the replay actors to finish sampling for timeout seconds.
            If the timeout is None, then block on the actors indefinitely.
            """
            replay_samples_ready: Dict[ActorHandle, T] = wait_asynchronous_requests(
                remote_requests_in_flight=self.remote_replay_requests_in_flight,
                ray_wait_timeout_s=timeout,
            )

            for replay_actor, sample_batches in replay_samples_ready.items():
                for sample_batch in sample_batches:
                    self.replay_sample_batches.append((replay_actor, sample_batch))

        num_samples_collected = sum(num_samples_collected.values())
        self.curr_num_samples_collected += num_samples_collected
        if self.curr_num_samples_collected >= self.config["train_batch_size"]:
            wait_on_replay_actors(None)
            training_intensity = int(self.config["training_intensity"] or 1)
            num_requests_to_launch = (
                self.curr_num_samples_collected / self.config["train_batch_size"]
            ) * training_intensity
            num_requests_to_launch = max(1, round(num_requests_to_launch))
            self.curr_num_samples_collected = 0
            for _ in range(num_requests_to_launch):
                rand_actor = random.choice(self.replay_actors)
                replay_samples_ready: Dict[
                    ActorHandle, T
                ] = asynchronous_parallel_requests(
                    remote_requests_in_flight=self.remote_replay_requests_in_flight,
                    actors=[rand_actor],
                    ray_wait_timeout_s=0.1,
                    max_remote_requests_in_flight_per_actor=num_requests_to_launch,
                    remote_args=[[self.config["train_batch_size"]]],
                    remote_fn=lambda actor, num_items: actor.sample(num_items),
                )
            for replay_actor, sample_batches in replay_samples_ready.items():
                for sample_batch in sample_batches:
                    self.replay_sample_batches.append((replay_actor, sample_batch))

        wait_on_replay_actors(0.1)

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
    def _compile_step_results(self, *, step_ctx, step_attempt_results=None):
        result = super()._compile_step_results(
            step_ctx=step_ctx, step_attempt_results=step_attempt_results
        )
        replay_stats = ray.get(
            self.replay_actors[0].stats.remote(self.config["optimizer"].get("debug"))
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
                    }
                    for _ in range(cf["evaluation_num_workers"])
                ]
                if cf["evaluation_interval"]
                else []
            ),
            strategy=config.get("placement_strategy", "PACK"),
        )


# Deprecated: Use ray.rllib.algorithms.dqn.DQNConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(ApexConfig().to_dict())

    @Deprecated(
        old="ray.rllib.agents.dqn.apex.APEX_DEFAULT_CONFIG",
        new="ray.rllib.agents.dqn.apex.ApexConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


APEX_DEFAULT_CONFIG = _deprecated_default_config()
