from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.utils.filter import RunningStat
from ray.rllib.utils.timer import TimerStat


class LocalSyncOptimizer(Optimizer):
    """A simple synchronous RL optimizer.

    In each step, this optimizer pulls samples from a number of remote
    evaluators, concatenates them, and then updates a local model. The updated
    model weights are then broadcast to all remote evaluators.
    """

    def _init(self, batch_size=32):
        self.update_weights_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.throughput = RunningStat()
        self.batch_size = batch_size

    def step(self):
        with self.update_weights_timer:
            if self.remote_evaluators:
                weights = ray.put(self.local_evaluator.get_weights())
                for e in self.remote_evaluators:
                    e.set_weights.remote(weights)

        with self.sample_timer:
            if self.remote_evaluators:
                samples = SampleBatch.concat_samples(
                    ray.get(
                        [e.sample.remote() for e in self.remote_evaluators]))
            else:
                samples = self.local_evaluator.sample()

        with self.grad_timer:
            grad = self.local_evaluator.compute_gradients(samples)
            self.local_evaluator.apply_gradients(grad)
            self.grad_timer.push_units_processed(samples.count)

        self.num_steps_sampled += samples.count
        self.num_steps_trained += samples.count

    def stats(self):
        return dict(Optimizer.stats(self), **{
            "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
            "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
            "update_time_ms": round(1000 * self.update_weights_timer.mean, 3),
            "opt_peak_throughput": round(self.grad_timer.mean_throughput, 3),
            "opt_samples": round(self.grad_timer.mean_units_processed, 3),
        })
