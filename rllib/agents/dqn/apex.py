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

import collections
import copy
from typing import Tuple

import ray
from ray.rllib.agents.dqn.dqn import calculate_rr_weights, \
    DEFAULT_CONFIG as DQN_CONFIG, DQNTrainer, validate_config
from ray.rllib.agents.dqn.learner_thread import LearnerThread
from ray.rllib.agents.trainer import Trainer
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import (STEPS_TRAINED_COUNTER,
                                        _get_global_vars, _get_shared_metrics)
from ray.rllib.execution.concurrency_ops import Concurrently, Dequeue, Enqueue
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_buffer import ReplayActor, VanillaReplayActor
from ray.rllib.execution.replay_ops import Replay, StoreToReplayBuffer
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.train_ops import UpdateTargetNetwork
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.actors import create_colocated
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import SampleBatchType
from ray.tune.trainable import Trainable
from ray.tune.utils.placement_groups import PlacementGroupFactory
from ray.util.iter import LocalIterator

# yapf: disable
# __sphinx_doc_begin__
APEX_DEFAULT_CONFIG = DQNTrainer.merge_trainer_configs(
    DQN_CONFIG,  # see also the options in dqn.py, which are also supported
    {
        "optimizer": merge_dicts(
            DQN_CONFIG["optimizer"], {
                "max_weight_sync_delay": 400,
                "num_replay_buffer_shards": 4,
                "debug": False
            }),
        "n_step": 3,
        "num_gpus": 1,
        "num_workers": 32,
        "buffer_size": 2000000,
        "learning_starts": 50000,
        "train_batch_size": 512,
        "rollout_fragment_length": 50,
        "target_network_update_freq": 500000,
        "timesteps_per_iteration": 25000,
        "exploration_config": {"type": "PerWorkerEpsilonGreedy"},
        "worker_side_prioritization": True,
        "min_iter_time_s": 30,
        # If set, this will fix the ratio of replayed from a buffer and learned
        # on timesteps to sampled from an environment and stored in the replay
        # buffer timesteps. Otherwise, replay will proceed as fast as possible.
        "training_intensity": None,
        # Which mode to use in the ParallelRollouts operator used to collect
        # samples. For more details check the operator in rollout_ops module.
        "parallel_rollouts_mode": "async",
        # This only applies if async mode is used (above config setting).
        # Controls the max number of async requests in flight per actor
        "parallel_rollouts_num_async": 2,
        # If this setting is different than None
        # we report metrics from the workers with the lowest
        # 1/worker_amount_to_collect_metrics_from of epsilons
        "worker_amount_to_collect_metrics_from": 3,
        "custom_resources_per_replay_buffer": {},
    },
    _allow_unknown_configs=True,
    _allow_unknown_subkeys=["custom_resources_per_replay_buffer"],
)
# __sphinx_doc_end__
# yapf: enable


class OverrideDefaultResourceRequest:
    @classmethod
    @override(Trainable)
    def default_resource_request(cls, config):
        cf = dict(cls._default_config, **config)
        Trainer._validate_config(cf)

        eval_config = cf["evaluation_config"]

        # Return PlacementGroupFactory containing all needed resources
        # (already properly defined as device bundles).
        return PlacementGroupFactory(
            bundles=[{
                # Local worker + replay buffer actors.
                # Force replay buffers to be on same node to maximize
                # data bandwidth between buffers and the learner (driver).
                # Replay buffer actors each contain one shard of the total
                # replay buffer and use 1 CPU each.
                "CPU": cf["num_cpus_for_driver"] +
                cf["optimizer"]["num_replay_buffer_shards"],
                "GPU": cf["num_gpus"]
            }] + [
                {
                    # RolloutWorkers.
                    "CPU": cf["num_cpus_per_worker"],
                    "GPU": cf["num_gpus_per_worker"],
                } for _ in range(cf["num_workers"])
            ] + ([
                {
                    # Evaluation workers.
                    # Note: The local eval worker is located on the driver CPU.
                    "CPU": eval_config.get("num_cpus_per_worker",
                                           cf["num_cpus_per_worker"]),
                    "GPU": eval_config.get("num_gpus_per_worker",
                                           cf["num_gpus_per_worker"]),
                } for _ in range(cf["evaluation_num_workers"])
            ] if cf["evaluation_interval"] else []),
            strategy=config.get("placement_strategy", "PACK"))


# Update worker weights as they finish generating experiences.
class UpdateWorkerWeights:
    def __init__(self, learner_thread: LearnerThread, workers: WorkerSet,
                 max_weight_sync_delay: int):
        self.learner_thread = learner_thread
        self.workers = workers
        self.steps_since_update = collections.defaultdict(int)
        self.max_weight_sync_delay = max_weight_sync_delay
        self.weights = None

    def __call__(self, item: Tuple["ActorHandle", SampleBatchType]):
        actor, batch = item
        self.steps_since_update[actor] += batch.count
        if self.steps_since_update[actor] >= self.max_weight_sync_delay:
            # Note that it's important to pull new weights once
            # updated to avoid excessive correlation between actors.
            if self.weights is None or self.learner_thread.weights_updated:
                self.learner_thread.weights_updated = False
                self.weights = ray.put(
                    self.workers.local_worker().get_weights())
            actor.set_weights.remote(self.weights, _get_global_vars())
            self.steps_since_update[actor] = 0
            # Update metrics.
            metrics = _get_shared_metrics()
            metrics.counters["num_weight_syncs"] += 1


def apex_execution_plan(workers: WorkerSet,
                        config: dict) -> LocalIterator[dict]:
    # Create a number of replay buffer actors.
    num_replay_buffer_shards = config["optimizer"]["num_replay_buffer_shards"]
    replay_actor_cls = ReplayActor if config[
        "prioritized_replay"] else VanillaReplayActor
    custom_resources = config.get("custom_resources_per_replay_buffer")
    if custom_resources:
        replay_actors = [
            replay_actor_cls.options(resources=custom_resources).remote(
                num_replay_buffer_shards,
                config["learning_starts"],
                config["buffer_size"],
                config["train_batch_size"],
                config["prioritized_replay_alpha"],
                config["prioritized_replay_beta"],
                config["prioritized_replay_eps"],
                config["multiagent"]["replay_mode"],
                config.get("replay_sequence_length", 1),
            )
            for _ in range(num_replay_buffer_shards)
        ]
    else:
        replay_actors = create_colocated(
            replay_actor_cls,
            [
                num_replay_buffer_shards,
                config["learning_starts"],
                config["buffer_size"],
                config["train_batch_size"],
                config["prioritized_replay_alpha"],
                config["prioritized_replay_beta"],
                config["prioritized_replay_eps"],
                config["multiagent"]["replay_mode"],
                config.get("replay_sequence_length", 1),
            ], num_replay_buffer_shards)

    # Start the learner thread.
    learner_thread = LearnerThread(workers.local_worker())
    learner_thread.start()

    # Update experience priorities post learning.
    def update_prio_and_stats(item: Tuple["ActorHandle", dict, int]) -> None:
        actor, prio_dict, count = item
        if config["prioritized_replay"]:
            actor.update_priorities.remote(prio_dict)
        metrics = _get_shared_metrics()
        # Manually update the steps trained counter since the learner thread
        # is executing outside the pipeline.
        metrics.counters[STEPS_TRAINED_COUNTER] += count
        metrics.timers["learner_dequeue"] = learner_thread.queue_timer
        metrics.timers["learner_grad"] = learner_thread.grad_timer
        metrics.timers["learner_overall"] = learner_thread.overall_timer

    # We execute the following steps concurrently:
    # (1) Generate rollouts and store them in one of our replay buffer actors. Update
    # the weights of the worker that generated the batch.
    parallel_rollouts_mode = config.get("parallel_rollouts_mode", "async")
    num_async = config.get("parallel_rollouts_num_async")
    # This could be set to None explicitly
    if not num_async:
        num_async = 2
    rollouts = ParallelRollouts(workers, mode=parallel_rollouts_mode, num_async=num_async)
    store_op = rollouts \
        .for_each(StoreToReplayBuffer(actors=replay_actors))
    if config.get("execution_plan_custom_store_ops"):
        custom_store_ops = config["execution_plan_custom_store_ops"]
        store_op = store_op.for_each(custom_store_ops(workers, config))
    # Only need to update workers if there are remote workers.
    if workers.remote_workers():
        store_op = store_op.zip_with_source_actor() \
            .for_each(UpdateWorkerWeights(
                learner_thread, workers,
                max_weight_sync_delay=(
                    config["optimizer"]["max_weight_sync_delay"])
            ))

    # (2) Read experiences from one of the replay buffer actors and send to the
    # the learner thread via its in-queue.
    if config.get("before_learn_on_batch"):
        before_learn_on_batch = config["before_learn_on_batch"]
        before_learn_on_batch = before_learn_on_batch(workers, config)
    else:
        before_learn_on_batch = lambda b: b
    replay_op = Replay(actors=replay_actors, num_async=4) \
        .for_each(before_learn_on_batch) \
        .zip_with_source_actor() \
        .for_each(Enqueue(learner_thread.inqueue))

    # (3) Get priorities back from learner thread and apply them to the
    # replay buffer actors.
    update_op = Dequeue(
            learner_thread.outqueue, check=learner_thread.is_alive) \
        .for_each(update_prio_and_stats) \
        .for_each(UpdateTargetNetwork(
            workers, config["target_network_update_freq"],
            by_steps_trained=True))

    if config["training_intensity"]:
        # Execute (1), (2) with a fixed intensity ratio.
        rr_weights = calculate_rr_weights(config) + ["*"]
        merged_op = Concurrently(
            [store_op, replay_op, update_op],
            mode="round_robin",
            output_indexes=[2],
            round_robin_weights=rr_weights,
            strict=True)
    else:
        # Execute (1), (2), (3) asynchronously as fast as possible. Only output
        # items from (3) since metrics aren't available before then.
        merged_op = Concurrently(
            [store_op, replay_op, update_op],
            mode="async",
            output_indexes=[2],
            strict=True)

    # Add in extra replay and learner metrics to the training result.
    def add_apex_metrics(result: dict) -> dict:
        replay_stats = ray.get(replay_actors[0].stats.remote(
            config["optimizer"].get("debug")))
        exploration_infos = workers.foreach_trainable_policy(
            lambda p, _: p.get_exploration_info())
        result["info"].update({
            "exploration_infos": exploration_infos,
            "learner_queue": learner_thread.learner_queue_size.stats(),
            "learner": copy.deepcopy(learner_thread.stats),
            "replay_shard_0": replay_stats,
        })
        return result

    # Only report metrics from the workers with the lowest 1/3 of epsilons.
    selected_workers = None
    worker_amount_to_collect_metrics_from = config.get("worker_amount_to_collect_metrics_from")
    if worker_amount_to_collect_metrics_from:
        selected_workers = workers.remote_workers()[
            -len(workers.remote_workers()) // worker_amount_to_collect_metrics_from:]

    return StandardMetricsReporting(
        merged_op, workers, config,
        selected_workers=selected_workers).for_each(add_apex_metrics)


def apex_validate_config(config):
    if config["num_gpus"] > 1:
        raise ValueError("`num_gpus` > 1 not yet supported for APEX-DQN!")
    validate_config(config)


ApexTrainer = DQNTrainer.with_updates(
    name="APEX",
    default_config=APEX_DEFAULT_CONFIG,
    validate_config=apex_validate_config,
    execution_plan=apex_execution_plan,
    mixins=[OverrideDefaultResourceRequest],
    allow_unknown_subkeys=["custom_resources_per_replay_buffer"]
)
