"""Experimental distributed execution API.

TODO(ekl): describe the concepts."""

import logging
from typing import List, Any, Tuple, Union
import numpy as np
import queue
import random
import time

import ray
from ray.util.iter import from_actors, LocalIterator, _NextValueNotReady
from ray.util.iter_metrics import SharedMetrics
from ray.rllib.optimizers.replay_buffer import PrioritizedReplayBuffer, \
    ReplayBuffer
from ray.rllib.evaluation.metrics import collect_episodes, \
    summarize_episodes, get_learner_stats
from ray.rllib.evaluation.rollout_worker import get_global_worker
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch, \
    DEFAULT_POLICY_ID
from ray.rllib.utils.compression import pack_if_needed

logger = logging.getLogger(__name__)

# Counters for training progress (keys for metrics.counters).
STEPS_SAMPLED_COUNTER = "num_steps_sampled"
STEPS_TRAINED_COUNTER = "num_steps_trained"

# Counters to track target network updates.
LAST_TARGET_UPDATE_TS = "last_target_update_ts"
NUM_TARGET_UPDATES = "num_target_updates"

# Performance timers (keys for metrics.timers).
APPLY_GRADS_TIMER = "apply_grad"
COMPUTE_GRADS_TIMER = "compute_grads"
WORKER_UPDATE_TIMER = "update"
GRAD_WAIT_TIMER = "grad_wait"
SAMPLE_TIMER = "sample"
LEARN_ON_BATCH_TIMER = "learn"

# Instant metrics (keys for metrics.info).
LEARNER_INFO = "learner"

# Type aliases.
GradientType = dict
SampleBatchType = Union[SampleBatch, MultiAgentBatch]


# Asserts that an object is a type of SampleBatch.
def _check_sample_batch_type(batch):
    if not isinstance(batch, SampleBatchType.__args__):
        raise ValueError("Expected either SampleBatch or MultiAgentBatch, "
                         "got {}: {}".format(type(batch), batch))


# Returns pipeline global vars that should be periodically sent to each worker.
def _get_global_vars():
    metrics = LocalIterator.get_metrics()
    return {"timestep": metrics.counters[STEPS_SAMPLED_COUNTER]}


def ParallelRollouts(workers: WorkerSet, mode="bulk_sync",
                     async_queue_depth=1) -> LocalIterator[SampleBatch]:
    """Operator to collect experiences in parallel from rollout workers.

    If there are no remote workers, experiences will be collected serially from
    the local worker instance instead.

    Arguments:
        workers (WorkerSet): set of rollout workers to use.
        mode (str): One of {'async', 'bulk_sync'}.
            - In 'async' mode, batches are returned as soon as they are
              computed by rollout workers with no order guarantees.
            - In 'bulk_sync' mode, we collect one batch from each worker
              and concatenate them together into a large batch to return.
        async_queue_depth (int): In async mode, the max number of async
            requests in flight per actor.

    Returns:
        A local iterator over experiences collected in parallel.

    Examples:
        >>> rollouts = ParallelRollouts(workers, mode="async")
        >>> batch = next(rollouts)
        >>> print(batch.count)
        50  # config.rollout_fragment_length

        >>> rollouts = ParallelRollouts(workers, mode="bulk_sync")
        >>> batch = next(rollouts)
        >>> print(batch.count)
        200  # config.rollout_fragment_length * config.num_workers

    Updates the STEPS_SAMPLED_COUNTER counter in the local iterator context.
    """

    # Ensure workers are initially in sync.
    workers.sync_weights()

    def report_timesteps(batch):
        metrics = LocalIterator.get_metrics()
        metrics.counters[STEPS_SAMPLED_COUNTER] += batch.count
        return batch

    if not workers.remote_workers():
        # Handle the serial sampling case.
        def sampler(_):
            while True:
                yield workers.local_worker().sample()

        return (LocalIterator(sampler, SharedMetrics())
                .for_each(report_timesteps))

    # Create a parallel iterator over generated experiences.
    rollouts = from_actors(workers.remote_workers())

    if mode == "bulk_sync":
        return rollouts \
            .batch_across_shards() \
            .for_each(lambda batches: SampleBatch.concat_samples(batches)) \
            .for_each(report_timesteps)
    elif mode == "async":
        return rollouts.gather_async(
            async_queue_depth=async_queue_depth).for_each(report_timesteps)
    else:
        raise ValueError(
            "mode must be one of 'bulk_sync', 'async', got '{}'".format(mode))


def AsyncGradients(
        workers: WorkerSet) -> LocalIterator[Tuple[GradientType, int]]:
    """Operator to compute gradients in parallel from rollout workers.

    Arguments:
        workers (WorkerSet): set of rollout workers to use.

    Returns:
        A local iterator over policy gradients computed on rollout workers.

    Examples:
        >>> grads_op = AsyncGradients(workers)
        >>> print(next(grads_op))
        {"var_0": ..., ...}, 50  # grads, batch count

    Updates the STEPS_SAMPLED_COUNTER counter and LEARNER_INFO field in the
    local iterator context.
    """

    # Ensure workers are initially in sync.
    workers.sync_weights()

    # This function will be applied remotely on the workers.
    def samples_to_grads(samples):
        return get_global_worker().compute_gradients(samples), samples.count

    # Record learner metrics and pass through (grads, count).
    class record_metrics:
        def _on_fetch_start(self):
            self.fetch_start_time = time.perf_counter()

        def __call__(self, item):
            (grads, info), count = item
            metrics = LocalIterator.get_metrics()
            metrics.counters[STEPS_SAMPLED_COUNTER] += count
            metrics.info[LEARNER_INFO] = get_learner_stats(info)
            metrics.timers[GRAD_WAIT_TIMER].push(time.perf_counter() -
                                                 self.fetch_start_time)
            return grads, count

    rollouts = from_actors(workers.remote_workers())
    grads = rollouts.for_each(samples_to_grads)
    return grads.gather_async().for_each(record_metrics())


def StandardMetricsReporting(train_op: LocalIterator[Any], workers: WorkerSet,
                             config: dict) -> LocalIterator[dict]:
    """Operator to periodically collect and report metrics.

    Arguments:
        train_op (LocalIterator): Operator for executing training steps.
            We ignore the output values.
        workers (WorkerSet): Rollout workers to collect metrics from.
        config (dict): Trainer configuration, used to determine the frequency
            of stats reporting.

    Returns:
        A local iterator over training results.

    Examples:
        >>> train_op = ParallelRollouts(...).for_each(TrainOneStep(...))
        >>> metrics_op = StandardMetricsReporting(train_op, workers, config)
        >>> next(metrics_op)
        {"episode_reward_max": ..., "episode_reward_mean": ..., ...}
    """

    output_op = train_op \
        .filter(OncePerTimeInterval(max(2, config["min_iter_time_s"]))) \
        .for_each(CollectMetrics(
            workers, min_history=config["metrics_smoothing_episodes"],
            timeout_seconds=config["collect_metrics_timeout"]))
    return output_op


class ConcatBatches:
    """Callable used to merge batches into larger batches for training.

    This should be used with the .combine() operator.

    Examples:
        >>> rollouts = ParallelRollouts(...)
        >>> rollouts = rollouts.combine(ConcatBatches(min_batch_size=10000))
        >>> print(next(rollouts).count)
        10000
    """

    def __init__(self, min_batch_size: int):
        self.min_batch_size = min_batch_size
        self.buffer = []
        self.count = 0
        self.batch_start_time = None

    def _on_fetch_start(self):
        if self.batch_start_time is None:
            self.batch_start_time = time.perf_counter()

    def __call__(self, batch: SampleBatchType) -> List[SampleBatchType]:
        _check_sample_batch_type(batch)
        self.buffer.append(batch)
        self.count += batch.count
        if self.count >= self.min_batch_size:
            out = SampleBatch.concat_samples(self.buffer)
            timer = LocalIterator.get_metrics().timers[SAMPLE_TIMER]
            timer.push(time.perf_counter() - self.batch_start_time)
            timer.push_units_processed(self.count)
            self.batch_start_time = None
            self.buffer = []
            self.count = 0
            return [out]
        return []


class TrainOneStep:
    """Callable that improves the policy and updates workers.

    This should be used with the .for_each() operator.

    Examples:
        >>> rollouts = ParallelRollouts(...)
        >>> train_op = rollouts.for_each(TrainOneStep(workers))
        >>> print(next(train_op))  # This trains the policy on one batch.
        None

    Updates the STEPS_TRAINED_COUNTER counter and LEARNER_INFO field in the
    local iterator context.
    """

    def __init__(self, workers: WorkerSet):
        self.workers = workers

    def __call__(self, batch: SampleBatchType) -> List[dict]:
        _check_sample_batch_type(batch)
        metrics = LocalIterator.get_metrics()
        learn_timer = metrics.timers[LEARN_ON_BATCH_TIMER]
        with learn_timer:
            info = self.workers.local_worker().learn_on_batch(batch)
            learn_timer.push_units_processed(batch.count)
        metrics.counters[STEPS_TRAINED_COUNTER] += batch.count
        metrics.info[LEARNER_INFO] = get_learner_stats(info)
        if self.workers.remote_workers():
            with metrics.timers[WORKER_UPDATE_TIMER]:
                weights = ray.put(self.workers.local_worker().get_weights())
                for e in self.workers.remote_workers():
                    e.set_weights.remote(weights, _get_global_vars())
        # Also update global vars of the local worker.
        self.workers.local_worker().set_global_vars(_get_global_vars())
        return info


class CollectMetrics:
    """Callable that collects metrics from workers.

    The metrics are smoothed over a given history window.

    This should be used with the .for_each() operator. For a higher level
    API, consider using StandardMetricsReporting instead.

    Examples:
        >>> output_op = train_op.for_each(CollectMetrics(workers))
        >>> print(next(output_op))
        {"episode_reward_max": ..., "episode_reward_mean": ..., ...}
    """

    def __init__(self, workers, min_history=100, timeout_seconds=180):
        self.workers = workers
        self.episode_history = []
        self.to_be_collected = []
        self.min_history = min_history
        self.timeout_seconds = timeout_seconds

    def __call__(self, _):
        # Collect worker metrics.
        episodes, self.to_be_collected = collect_episodes(
            self.workers.local_worker(),
            self.workers.remote_workers(),
            self.to_be_collected,
            timeout_seconds=self.timeout_seconds)
        orig_episodes = list(episodes)
        missing = self.min_history - len(episodes)
        if missing > 0:
            episodes.extend(self.episode_history[-missing:])
            assert len(episodes) <= self.min_history
        self.episode_history.extend(orig_episodes)
        self.episode_history = self.episode_history[-self.min_history:]
        res = summarize_episodes(episodes, orig_episodes)

        # Add in iterator metrics.
        metrics = LocalIterator.get_metrics()
        timers = {}
        counters = {}
        info = {}
        info.update(metrics.info)
        for k, counter in metrics.counters.items():
            counters[k] = counter
        for k, timer in metrics.timers.items():
            timers["{}_time_ms".format(k)] = round(timer.mean * 1000, 3)
            if timer.has_units_processed():
                timers["{}_throughput".format(k)] = round(
                    timer.mean_throughput, 3)
        res.update({
            "num_healthy_workers": len(self.workers.remote_workers()),
            "timesteps_total": metrics.counters[STEPS_SAMPLED_COUNTER],
        })
        res["timers"] = timers
        res["info"] = info
        res["info"].update(counters)
        return res


class OncePerTimeInterval:
    """Callable that returns True once per given interval.

    This should be used with the .filter() operator to throttle / rate-limit
    metrics reporting. For a higher-level API, consider using
    StandardMetricsReporting instead.

    Examples:
        >>> throttled_op = train_op.filter(OncePerTimeInterval(5))
        >>> start = time.time()
        >>> next(throttled_op)
        >>> print(time.time() - start)
        5.00001  # will be greater than 5 seconds
    """

    def __init__(self, delay):
        self.delay = delay
        self.last_called = 0

    def __call__(self, item):
        now = time.time()
        if now - self.last_called > self.delay:
            self.last_called = now
            return True
        return False


class ComputeGradients:
    """Callable that computes gradients with respect to the policy loss.

    This should be used with the .for_each() operator.

    Examples:
        >>> grads_op = rollouts.for_each(ComputeGradients(workers))
        >>> print(next(grads_op))
        {"var_0": ..., ...}, 50  # grads, batch count

    Updates the LEARNER_INFO info field in the local iterator context.
    """

    def __init__(self, workers):
        self.workers = workers

    def __call__(self, samples: SampleBatchType):
        _check_sample_batch_type(samples)
        metrics = LocalIterator.get_metrics()
        with metrics.timers[COMPUTE_GRADS_TIMER]:
            grad, info = self.workers.local_worker().compute_gradients(samples)
        metrics.info[LEARNER_INFO] = get_learner_stats(info)
        return grad, samples.count


class ApplyGradients:
    """Callable that applies gradients and updates workers.

    This should be used with the .for_each() operator.

    Examples:
        >>> apply_op = grads_op.for_each(ApplyGradients(workers))
        >>> print(next(apply_op))
        None

    Updates the STEPS_TRAINED_COUNTER counter in the local iterator context.
    """

    def __init__(self, workers, update_all=True):
        """Creates an ApplyGradients instance.

        Arguments:
            workers (WorkerSet): workers to apply gradients to.
            update_all (bool): If true, updates all workers. Otherwise, only
                update the worker that produced the sample batch we are
                currently processing (i.e., A3C style).
        """
        self.workers = workers
        self.update_all = update_all

    def __call__(self, item):
        if not isinstance(item, tuple) or len(item) != 2:
            raise ValueError(
                "Input must be a tuple of (grad_dict, count), got {}".format(
                    item))
        gradients, count = item
        metrics = LocalIterator.get_metrics()
        metrics.counters[STEPS_TRAINED_COUNTER] += count

        apply_timer = metrics.timers[APPLY_GRADS_TIMER]
        with apply_timer:
            self.workers.local_worker().apply_gradients(gradients)
            apply_timer.push_units_processed(count)

        # Also update global vars of the local worker.
        self.workers.local_worker().set_global_vars(_get_global_vars())

        if self.update_all:
            if self.workers.remote_workers():
                with metrics.timers[WORKER_UPDATE_TIMER]:
                    weights = ray.put(
                        self.workers.local_worker().get_weights())
                    for e in self.workers.remote_workers():
                        e.set_weights.remote(weights, _get_global_vars())
        else:
            if metrics.current_actor is None:
                raise ValueError(
                    "Could not find actor to update. When "
                    "update_all=False, `current_actor` must be set "
                    "in the iterator context.")
            with metrics.timers[WORKER_UPDATE_TIMER]:
                weights = self.workers.local_worker().get_weights()
                metrics.current_actor.set_weights.remote(
                    weights, _get_global_vars())


class AverageGradients:
    """Callable that averages the gradients in a batch.

    This should be used with the .for_each() operator after a set of gradients
    have been batched with .batch().

    Examples:
        >>> batched_grads = grads_op.batch(32)
        >>> avg_grads = batched_grads.for_each(AverageGradients())
        >>> print(next(avg_grads))
        {"var_0": ..., ...}, 1600  # averaged grads, summed batch count
    """

    def __call__(self, gradients):
        acc = None
        sum_count = 0
        for grad, count in gradients:
            if acc is None:
                acc = grad
            else:
                acc = [a + b for a, b in zip(acc, grad)]
            sum_count += count
        logger.info("Computing average of {} microbatch gradients "
                    "({} samples total)".format(len(gradients), sum_count))
        return acc, sum_count


class StoreToReplayBuffer:
    """Callable that stores data into a local replay buffer.

    This should be used with the .for_each() operator on a rollouts iterator.
    The batch that was stored is returned.

    Examples:
        >>> buf = ReplayBuffer(1000)
        >>> rollouts = ParallelRollouts(...)
        >>> store_op = rollouts.for_each(StoreToReplayBuffer(buf))
        >>> next(store_op)
        SampleBatch(...)
    """

    def __init__(self, replay_buffer: ReplayBuffer):
        assert isinstance(replay_buffer, ReplayBuffer)
        self.replay_buffers = {DEFAULT_POLICY_ID: replay_buffer}

    def __call__(self, batch: SampleBatchType):
        # Handle everything as if multiagent
        if isinstance(batch, SampleBatch):
            batch = MultiAgentBatch({DEFAULT_POLICY_ID: batch}, batch.count)

        for policy_id, s in batch.policy_batches.items():
            for row in s.rows():
                self.replay_buffers[policy_id].add(
                    pack_if_needed(row["obs"]),
                    row["actions"],
                    row["rewards"],
                    pack_if_needed(row["new_obs"]),
                    row["dones"],
                    weight=None)
        return batch


class StoreToReplayActors:
    """Callable that stores data into a replay buffer actors.

    This should be used with the .for_each() operator on a rollouts iterator.
    The batch that was stored is returned.

    Examples:
        >>> actors = [ReplayActor.remote() for _ in range(4)]
        >>> rollouts = ParallelRollouts(...)
        >>> store_op = rollouts.for_each(StoreToReplayActors(actors))
        >>> next(store_op)
        SampleBatch(...)
    """

    def __init__(self, replay_actors: List["ActorHandle"]):
        self.replay_actors = replay_actors

    def __call__(self, batch: SampleBatchType):
        actor = random.choice(self.replay_actors)
        actor.add_batch.remote(batch)
        return batch


def ParallelReplay(replay_actors: List["ActorHandle"], async_queue_depth=4):
    """Replay experiences in parallel from the given actors.

    This should be combined with the StoreToReplayActors operation using the
    Concurrently() operator.

    Arguments:
        replay_actors (list): List of replay actors.
        async_queue_depth (int): In async mode, the max number of async
            requests in flight per actor.

    Examples:
        >>> actors = [ReplayActor.remote() for _ in range(4)]
        >>> replay_op = ParallelReplay(actors)
        >>> next(replay_op)
        SampleBatch(...)
    """
    replay = from_actors(replay_actors)
    return replay.gather_async(
        async_queue_depth=async_queue_depth).filter(lambda x: x is not None)


def LocalReplay(replay_buffer: ReplayBuffer, train_batch_size: int):
    """Replay experiences from a local buffer instance.

    This should be combined with the StoreToReplayBuffer operation using the
    Concurrently() operator.

    Arguments:
        replay_buffer (ReplayBuffer): Buffer to replay experiences from.
        train_batch_size (int): Batch size of fetches from the buffer.

    Examples:
        >>> actors = [ReplayActor.remote() for _ in range(4)]
        >>> replay_op = ParallelReplay(actors)
        >>> next(replay_op)
        SampleBatch(...)
    """
    assert isinstance(replay_buffer, ReplayBuffer)
    replay_buffers = {DEFAULT_POLICY_ID: replay_buffer}
    # TODO(ekl) support more options, or combine with ParallelReplay (?)
    synchronize_sampling = False
    prioritized_replay_beta = None

    def gen_replay(timeout):
        while True:
            samples = {}
            idxes = None
            for policy_id, replay_buffer in replay_buffers.items():
                if synchronize_sampling:
                    if idxes is None:
                        idxes = replay_buffer.sample_idxes(train_batch_size)
                else:
                    idxes = replay_buffer.sample_idxes(train_batch_size)

                if isinstance(replay_buffer, PrioritizedReplayBuffer):
                    metrics = LocalIterator.get_metrics()
                    num_steps_trained = metrics.counters[STEPS_TRAINED_COUNTER]
                    (obses_t, actions, rewards, obses_tp1, dones, weights,
                     batch_indexes) = replay_buffer.sample_with_idxes(
                         idxes,
                         beta=prioritized_replay_beta.value(num_steps_trained))
                else:
                    (obses_t, actions, rewards, obses_tp1,
                     dones) = replay_buffer.sample_with_idxes(idxes)
                    weights = np.ones_like(rewards)
                    batch_indexes = -np.ones_like(rewards)
                samples[policy_id] = SampleBatch({
                    "obs": obses_t,
                    "actions": actions,
                    "rewards": rewards,
                    "new_obs": obses_tp1,
                    "dones": dones,
                    "weights": weights,
                    "batch_indexes": batch_indexes
                })
            yield MultiAgentBatch(samples, train_batch_size)

    return LocalIterator(gen_replay, SharedMetrics())


def Concurrently(ops: List[LocalIterator], mode="round_robin"):
    """Operator that runs the given parent iterators concurrently.

    Arguments:
        mode (str): One of {'round_robin', 'async'}.
            - In 'round_robin' mode, we alternate between pulling items from
              each parent iterator in order deterministically.
            - In 'async' mode, we pull from each parent iterator as fast as
              they are produced. This is non-deterministic.

        >>> sim_op = ParallelRollouts(...).for_each(...)
        >>> replay_op = LocalReplay(...).for_each(...)
        >>> combined_op = Concurrently([sim_op, replay_op])
    """

    if len(ops) < 2:
        raise ValueError("Should specify at least 2 ops.")
    if mode == "round_robin":
        deterministic = True
    elif mode == "async":
        deterministic = False
    else:
        raise ValueError("Unknown mode {}".format(mode))
    return ops[0].union(*ops[1:], deterministic=deterministic)


class UpdateTargetNetwork:
    """Periodically call policy.update_target() on all trainable policies.

    This should be used with the .for_each() operator after training step
    has been taken.

    Examples:
        >>> train_op = ParallelRollouts(...).for_each(TrainOneStep(...))
        >>> update_op = train_op.for_each(
        ...     UpdateTargetIfNeeded(workers, target_update_freq=500))
        >>> print(next(update_op))
        None

    Updates the LAST_TARGET_UPDATE_TS and NUM_TARGET_UPDATES counters in the
    local iterator context. The value of the last update counter is used to
    track when we should update the target next.
    """

    def __init__(self, workers, target_update_freq, by_steps_trained=False):
        self.workers = workers
        self.target_update_freq = target_update_freq
        if by_steps_trained:
            self.metric = STEPS_TRAINED_COUNTER
        else:
            self.metric = STEPS_SAMPLED_COUNTER

    def __call__(self, _):
        metrics = LocalIterator.get_metrics()
        cur_ts = metrics.counters[self.metric]
        last_update = metrics.counters[LAST_TARGET_UPDATE_TS]
        if cur_ts - last_update > self.target_update_freq:
            self.workers.local_worker().foreach_trainable_policy(
                lambda p, _: p.update_target())
            metrics.counters[NUM_TARGET_UPDATES] += 1
            metrics.counters[LAST_TARGET_UPDATE_TS] = cur_ts


class Enqueue:
    """Enqueue data items into a queue.Queue instance.

    The enqueue is non-blocking, so Enqueue operations can executed with
    Dequeue via the Concurrently() operator.

    Examples:
        >>> queue = queue.Queue(100)
        >>> write_op = ParallelRollouts(...).for_each(Enqueue(queue))
        >>> read_op = Dequeue(queue)
        >>> combined_op = Concurrently([write_op, read_op], mode="async")
        >>> next(combined_op)
        SampleBatch(...)
    """

    def __init__(self, output_queue: queue.Queue):
        if not isinstance(output_queue, queue.Queue):
            raise ValueError("Expected queue.Queue, got {}".format(
                type(output_queue)))
        self.queue = output_queue

    def __call__(self, x):
        try:
            self.queue.put_nowait(x)
        except queue.Full:
            return _NextValueNotReady()


def Dequeue(input_queue: queue.Queue, check=lambda: True):
    """Dequeue data items from a queue.Queue instance.

    The dequeue is non-blocking, so Dequeue operations can executed with
    Enqueue via the Concurrently() operator.

    Arguments:
        input_queue (Queue): queue to pull items from.
        check (fn): liveness check. When this function returns false,
            Dequeue() will raise an error to halt execution.

    Examples:
        >>> queue = queue.Queue(100)
        >>> write_op = ParallelRollouts(...).for_each(Enqueue(queue))
        >>> read_op = Dequeue(queue)
        >>> combined_op = Concurrently([write_op, read_op], mode="async")
        >>> next(combined_op)
        SampleBatch(...)
    """
    if not isinstance(input_queue, queue.Queue):
        raise ValueError("Expected queue.Queue, got {}".format(
            type(input_queue)))

    def base_iterator(timeout=None):
        while check():
            try:
                item = input_queue.get_nowait()
                yield item
            except queue.Empty:
                yield _NextValueNotReady()
        raise RuntimeError("Error raised reading from queue")

    return LocalIterator(base_iterator, SharedMetrics())
