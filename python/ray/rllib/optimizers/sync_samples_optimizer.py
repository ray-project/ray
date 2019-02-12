from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import logging
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.filter import RunningStat
from ray.rllib.utils.timer import TimerStat

logger = logging.getLogger(__name__)


class SyncSamplesOptimizer(PolicyOptimizer):
    """A simple synchronous RL optimizer.

    In each step, this optimizer pulls samples from a number of remote
    evaluators, concatenates them, and then updates a local model. The updated
    model weights are then broadcast to all remote evaluators.
    """

    @override(PolicyOptimizer)
    def _init(self, num_sgd_iter=1, train_batch_size=1):
        self.update_weights_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.throughput = RunningStat()
        self.num_sgd_iter = num_sgd_iter
        self.train_batch_size = train_batch_size
        self.learner_stats = {}

    @override(PolicyOptimizer)
    def step(self):
        with self.update_weights_timer:
            if self.remote_evaluators:
                weights = ray.put(self.local_evaluator.get_weights())
                for e in self.remote_evaluators:
                    e.set_weights.remote(weights)

        with self.sample_timer:
            samples = []
            while sum(s.count for s in samples) < self.train_batch_size:
                if self.remote_evaluators:
                    samples.extend(
                        ray.get([
                            e.sample.remote() for e in self.remote_evaluators
                        ]))
                else:
                    samples.append(self.local_evaluator.sample())
            samples = SampleBatch.concat_samples(samples)
            self.sample_timer.push_units_processed(samples.count)

        with self.grad_timer:
            for i in range(self.num_sgd_iter):
                fetches = self.local_evaluator.learn_on_batch(samples)
                if "stats" in fetches:
                    self.learner_stats = fetches["stats"]
                if self.num_sgd_iter > 1:
                    logger.debug("{} {}".format(i, fetches))
            self.grad_timer.push_units_processed(samples.count)

        self.num_steps_sampled += samples.count
        self.num_steps_trained += samples.count
        return fetches

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
