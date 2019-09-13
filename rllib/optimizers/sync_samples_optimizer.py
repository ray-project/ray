from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import random
from collections import defaultdict

import ray
from ray.rllib.evaluation.metrics import LEARNER_STATS_KEY
from ray.rllib.optimizers.multi_gpu_optimizer import _averaged
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.policy.sample_batch import SampleBatch, DEFAULT_POLICY_ID, \
    MultiAgentBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.filter import RunningStat
from ray.rllib.utils.timer import TimerStat
from ray.rllib.utils.memory import ray_get_and_free

logger = logging.getLogger(__name__)


class SyncSamplesOptimizer(PolicyOptimizer):
    """A simple synchronous RL optimizer.

    In each step, this optimizer pulls samples from a number of remote
    workers, concatenates them, and then updates a local model. The updated
    model weights are then broadcast to all remote workers.
    """

    def __init__(self,
                 workers,
                 num_sgd_iter=1,
                 train_batch_size=1,
                 sgd_minibatch_size=0,
                 standardize_fields=frozenset([])):
        PolicyOptimizer.__init__(self, workers)

        self.update_weights_timer = TimerStat()
        self.standardize_fields = standardize_fields
        self.sample_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.throughput = RunningStat()
        self.num_sgd_iter = num_sgd_iter
        self.sgd_minibatch_size = sgd_minibatch_size
        self.train_batch_size = train_batch_size
        self.learner_stats = {}
        self.policies = dict(self.workers.local_worker()
                             .foreach_trainable_policy(lambda p, i: (i, p)))
        logger.debug("Policies to train: {}".format(self.policies))

    @override(PolicyOptimizer)
    def step(self):
        with self.update_weights_timer:
            if self.workers.remote_workers():
                weights = ray.put(self.workers.local_worker().get_weights())
                for e in self.workers.remote_workers():
                    e.set_weights.remote(weights)

        with self.sample_timer:
            samples = []
            while sum(s.count for s in samples) < self.train_batch_size:
                if self.workers.remote_workers():
                    samples.extend(
                        ray_get_and_free([
                            e.sample.remote()
                            for e in self.workers.remote_workers()
                        ]))
                else:
                    samples.append(self.workers.local_worker().sample())
            samples = SampleBatch.concat_samples(samples)
            self.sample_timer.push_units_processed(samples.count)

        # Handle everything as if multiagent
        if isinstance(samples, SampleBatch):
            samples = MultiAgentBatch({
                DEFAULT_POLICY_ID: samples
            }, samples.count)

        fetches = {}
        with self.grad_timer:
            for policy_id, policy in self.policies.items():
                if policy_id not in samples.policy_batches:
                    continue

                batch = samples.policy_batches[policy_id]
                for field in self.standardize_fields:
                    value = batch[field]
                    standardized = (value - value.mean()) / max(
                        1e-4, value.std())
                    batch[field] = standardized

                for i in range(self.num_sgd_iter):
                    iter_extra_fetches = defaultdict(list)
                    for minibatch in self._minibatches(batch):
                        batch_fetches = (
                            self.workers.local_worker().learn_on_batch(
                                MultiAgentBatch({
                                    policy_id: minibatch
                                }, minibatch.count)))[policy_id]
                        for k, v in batch_fetches[LEARNER_STATS_KEY].items():
                            iter_extra_fetches[k].append(v)
                    logger.debug("{} {}".format(i,
                                                _averaged(iter_extra_fetches)))
                fetches[policy_id] = _averaged(iter_extra_fetches)

        self.grad_timer.push_units_processed(samples.count)
        if len(fetches) == 1 and DEFAULT_POLICY_ID in fetches:
            self.learner_stats = fetches[DEFAULT_POLICY_ID]
        else:
            self.learner_stats = fetches
        self.num_steps_sampled += samples.count
        self.num_steps_trained += samples.count
        return self.learner_stats

    @override(PolicyOptimizer)
    def stats(self):
        return dict(
            PolicyOptimizer.stats(self), **{
                "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
                "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
                "update_time_ms": round(1000 * self.update_weights_timer.mean,
                                        3),
                "opt_peak_throughput": round(self.grad_timer.mean_throughput,
                                             3),
                "sample_peak_throughput": round(
                    self.sample_timer.mean_throughput, 3),
                "opt_samples": round(self.grad_timer.mean_units_processed, 3),
                "learner": self.learner_stats,
            })

    def _minibatches(self, samples):
        if not self.sgd_minibatch_size:
            yield samples
            return

        if isinstance(samples, MultiAgentBatch):
            raise NotImplementedError(
                "Minibatching not implemented for multi-agent in simple mode")

        if "state_in_0" in samples.data:
            logger.warn("Not shuffling RNN data for SGD in simple mode")
        else:
            samples.shuffle()

        i = 0
        slices = []
        while i < samples.count:
            slices.append((i, i + self.sgd_minibatch_size))
            i += self.sgd_minibatch_size
        random.shuffle(slices)

        for i, j in slices:
            yield samples.slice(i, j)
