"""Experimental operators for defining distributed training pipelines.

TODO(ekl): describe the concepts."""

from typing import List, Any
import time

import ray
from ray.util.iter import from_actors, LocalIterator
from ray.rllib.evaluation.metrics import collect_episodes, summarize_episodes
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.policy.sample_batch import SampleBatch


def ParallelRollouts(workers: WorkerSet,
                     mode="bulk_sync") -> LocalIterator[SampleBatch]:
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

    Returns:
        A local iterator over experiences collected in parallel.

    Examples:
        >>> rollouts = ParallelRollouts(workers, mode="async")
        >>> batch = next(rollouts)
        >>> print(batch.count)
        50  # config.sample_batch_size

        >>> rollouts = ParallelRollouts(workers, mode="bulk_sync")
        >>> batch = next(rollouts)
        >>> print(batch.count)
        200  # config.sample_batch_size * config.num_workers
    """

    if not workers.remote_workers():
        # Handle the serial sampling case.
        def sampler(_):
            while True:
                yield workers.local_worker().sample()

        return LocalIterator(sampler)

    # Create a parallel iterator over generated experiences.
    rollouts = from_actors(workers.remote_workers())

    if mode == "bulk_sync":
        return rollouts \
            .batch_across_shards() \
            .for_each(lambda batches: SampleBatch.concat_samples(batches))
    elif mode == "async":
        return rollouts.gather_async()
    else:
        raise ValueError(
            "mode must be one of 'bulk_sync', 'async', got '{}'".format(mode))


def StandardMetricsReporting(train_op: LocalIterator[Any], workers: WorkerSet,
                             config: dict):
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
        .filter(OncePerTimeInterval(config["min_iter_time_s"])) \
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

    def __call__(self, batch: SampleBatch) -> List[SampleBatch]:
        if not isinstance(batch, SampleBatch):
            raise ValueError("Expected type SampleBatch, got {}: {}".format(
                type(batch), batch))
        self.buffer.append(batch)
        self.count += batch.count
        if self.count >= self.min_batch_size:
            out = SampleBatch.concat_samples(self.buffer)
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
        {"learner_stats": {"policy_loss": ...}}
    """

    def __init__(self, workers: WorkerSet):
        self.workers = workers

    def __call__(self, batch: SampleBatch) -> List[dict]:
        info = self.workers.local_worker().learn_on_batch(batch)
        if self.workers.remote_workers():
            weights = ray.put(self.workers.local_worker().get_weights())
            for e in self.workers.remote_workers():
                e.set_weights.remote(weights)
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

    def __call__(self, info):
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
        res.update(info=info)
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
        {"var_0": ..., ...}, {"learner_stats": ...}  # grads, learner info
    """

    def __init__(self, workers):
        self.workers = workers

    def __call__(self, samples):
        grad, info = self.workers.local_worker().compute_gradients(samples)
        return grad, info


class ApplyGradients:
    """Callable that applies gradients and updates workers.

    This should be used with the .for_each() operator.

    Examples:
        >>> apply_op = grads_op.for_each(ApplyGradients(workers))
        >>> print(next(apply_op))
        {"learner_stats": ...}  # learner info
    """

    def __init__(self, workers):
        self.workers = workers

    def __call__(self, item):
        gradients, info = item
        self.workers.local_worker().apply_gradients(gradients)
        if self.workers.remote_workers():
            weights = ray.put(self.workers.local_worker().get_weights())
            for e in self.workers.remote_workers():
                e.set_weights.remote(weights)
        return info


class AverageGradients:
    """Callable that averages the gradients in a batch.

    This should be used with the .for_each() operator after a set of gradients
    have been batched with .batch().

    Examples:
        >>> batched_grads = grads_op.batch(32)
        >>> avg_grads = batched_grads.for_each(AverageGradients())
        >>> print(next(avg_grads))
        {"var_0": ..., ...}, {"learner_stats": ...}  # avg grads, last info
    """

    def __call__(self, gradients):
        acc = None
        for grad, info in gradients:
            if acc is None:
                acc = grad
            else:
                acc = [a + b for a, b in zip(acc, grad)]
        return acc, info
