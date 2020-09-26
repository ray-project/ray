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
from ray.rllib.agents.dqn.dqn import DEFAULT_CONFIG as DQN_CONFIG
from ray.rllib.agents.dqn.dqn import DQNTrainer, calculate_rr_weights
from ray.rllib.agents.dqn.learner_thread import LearnerThread
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import (STEPS_TRAINED_COUNTER,
                                        _get_global_vars, _get_shared_metrics)
from ray.rllib.execution.concurrency_ops import Concurrently, Dequeue, Enqueue
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.replay_buffer import ReplayActor
from ray.rllib.execution.replay_ops import Replay, StoreToReplayBuffer
from ray.rllib.execution.rollout_ops import ParallelRollouts
from ray.rllib.execution.train_ops import UpdateTargetNetwork
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.actors import create_colocated
from ray.rllib.utils.typing import SampleBatchType
from ray.util.iter import LocalIterator

# yapf: disable
# __sphinx_doc_begin__
APEX_DEFAULT_CONFIG = merge_dicts(
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
    },
)
# __sphinx_doc_end__
# yapf: enable


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
    replay_actors = create_colocated(ReplayActor, [
        num_replay_buffer_shards,
        config["learning_starts"],
        config["buffer_size"],
        config["train_batch_size"],
        config["prioritized_replay_alpha"],
        config["prioritized_replay_beta"],
        config["prioritized_replay_eps"],
        config["multiagent"]["replay_mode"],
        config["replay_sequence_length"],
    ], num_replay_buffer_shards)

    # Start the learner thread.
    learner_thread = LearnerThread(workers.local_worker())
    learner_thread.start()

    # Update experience priorities post learning.
    def update_prio_and_stats(item: Tuple["ActorHandle", dict, int]) -> None:
        actor, prio_dict, count = item
        actor.update_priorities.remote(prio_dict)
        metrics = _get_shared_metrics()
        # Manually update the steps trained counter since the learner thread
        # is executing outside the pipeline.
        metrics.counters[STEPS_TRAINED_COUNTER] += count
        metrics.timers["learner_dequeue"] = learner_thread.queue_timer
        metrics.timers["learner_grad"] = learner_thread.grad_timer
        metrics.timers["learner_overall"] = learner_thread.overall_timer

    # We execute the following steps concurrently:
    # (1) Generate rollouts and store them in our replay buffer actors. Update
    # the weights of the worker that generated the batch.
    rollouts = ParallelRollouts(workers, mode="async", num_async=2)
    store_op = rollouts \
        .for_each(StoreToReplayBuffer(actors=replay_actors))
    # Only need to update workers if there are remote workers.
    if workers.remote_workers():
        store_op = store_op.zip_with_source_actor() \
            .for_each(UpdateWorkerWeights(
                learner_thread, workers,
                max_weight_sync_delay=(
                    config["optimizer"]["max_weight_sync_delay"])
            ))

    # (2) Read experiences from the replay buffer actors and send to the
    # learner thread via its in-queue.
    post_fn = config.get("before_learn_on_batch") or (lambda b, *a: b)
    replay_op = Replay(actors=replay_actors, num_async=4) \
        .for_each(lambda x: post_fn(x, workers, config)) \
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
            round_robin_weights=rr_weights)
    else:
        # Execute (1), (2), (3) asynchronously as fast as possible. Only output
        # items from (3) since metrics aren't available before then.
        merged_op = Concurrently(
            [store_op, replay_op, update_op], mode="async", output_indexes=[2])

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
    selected_workers = workers.remote_workers()[
        -len(workers.remote_workers()) // 3:]

    return StandardMetricsReporting(
        merged_op, workers, config,
        selected_workers=selected_workers).for_each(add_apex_metrics)


ApexTrainer = DQNTrainer.with_updates(
    name="APEX",
    default_config=APEX_DEFAULT_CONFIG,
    execution_plan=apex_execution_plan)
