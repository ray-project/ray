from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.evaluation.sample_batch import SampleBatch
from ray.rllib.utils.filter import RunningStat
from ray.rllib.utils.timer import TimerStat


class SyncSamplesOptimizer(PolicyOptimizer):
    """A simple synchronous RL optimizer.

    In each step, this optimizer pulls samples from a number of remote
    evaluators, concatenates them, and then updates a local model. The updated
    model weights are then broadcast to all remote evaluators.
    """

    def _init(self, num_sgd_iter=1, timesteps_per_batch=1):
        self.update_weights_timer = TimerStat()
        self.sample_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.throughput = RunningStat()
        self.num_sgd_iter = num_sgd_iter
        self.timesteps_per_batch = timesteps_per_batch

    def step(self):
        with self.update_weights_timer:
            if self.remote_evaluators:
                weights = ray.put(self.local_evaluator.get_weights())
                for e in self.remote_evaluators:
                    e.set_weights.remote(weights)

        with self.sample_timer:
            samples = []
            while sum(s.count for s in samples) < self.timesteps_per_batch:
                if self.remote_evaluators:
                    samples.extend(
                        ray.get([
                            e.sample.remote() for e in self.remote_evaluators
                        ]))
                else:
                    samples.append(self.local_evaluator.sample())
            samples = SampleBatch.concat_samples(samples)

        with self.grad_timer:
            for i in range(self.num_sgd_iter):
                fetches = self.local_evaluator.compute_apply(samples)
                if self.num_sgd_iter > 1:
                    print(i, fetches)
            self.grad_timer.push_units_processed(samples.count)

        self.num_steps_sampled += samples.count
        self.num_steps_trained += samples.count
        return fetches

    def stats(self):
        return dict(
            PolicyOptimizer.stats(self), **{
                "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
                "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
                "update_time_ms": round(1000 * self.update_weights_timer.mean,
                                        3),
                "opt_peak_throughput": round(self.grad_timer.mean_throughput,
                                             3),
                "opt_samples": round(self.grad_timer.mean_units_processed, 3),
            })
