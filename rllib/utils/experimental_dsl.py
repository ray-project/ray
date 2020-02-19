from typing import List, Any
import time

import ray
from ray.util.iter import from_actors, LocalIterator
from ray.rllib.evaluation.metrics import collect_episodes, summarize_episodes
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.policy.sample_batch import SampleBatch


def ParallelRollouts(workers: WorkerSet,
                     mode="batch_sync") -> LocalIterator[SampleBatch]:
    rollouts = from_actors(workers.remote_workers())
    if mode == "batch_sync":
        return rollouts \
            .batch_across_shards() \
            .for_each(lambda batches: SampleBatch.concat_samples(batches))
    elif mode == "async":
        return rollouts.gather_async()
    else:
        raise ValueError(
            "mode must be one of 'batch_sync', 'async', got '{}'".format(mode))


def StandardMetricsReporting(train_op: LocalIterator[Any], workers: WorkerSet,
                             config: dict):
    output_op = train_op \
        .filter(OncePerTimeInterval(config["min_iter_time_s"])) \
        .for_each(CollectMetrics(workers))
    return output_op


class ConcatBatches(object):
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


class TrainOneStep(object):
    def __init__(self, workers):
        self.workers = workers

    def __call__(self, batch):
        info = self.workers.local_worker().learn_on_batch(batch)
        if self.workers.remote_workers():
            weights = ray.put(self.workers.local_worker().get_weights())
            for e in self.workers.remote_workers():
                e.set_weights.remote(weights)
        return info


class CollectMetrics(object):
    def __init__(self, workers, min_history=100):
        self.workers = workers
        self.episode_history = []
        self.to_be_collected = []
        self.min_history = min_history

    def __call__(self, info):
        episodes, self.to_be_collected = collect_episodes(
            self.workers.local_worker(),
            self.workers.remote_workers(),
            self.to_be_collected,
            timeout_seconds=60)
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


class OncePerTimeInterval(object):
    def __init__(self, delay):
        self.delay = delay
        self.last_called = 0

    def __call__(self, item):
        now = time.time()
        if now - self.last_called > self.delay:
            self.last_called = now
            return True
        return False


class ComputeGradients(object):
    def __init__(self, workers):
        self.workers = workers

    def __call__(self, samples):
        grad, info = self.workers.local_worker().compute_gradients(samples)
        return grad, info


class ApplyGradients(object):
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


class AverageGradients(object):
    def __call__(self, gradients):
        acc = None
        for grad, info in gradients:
            if acc is None:
                acc = grad
            else:
                acc = [a + b for a, b in zip(acc, grad)]
        return acc, info
