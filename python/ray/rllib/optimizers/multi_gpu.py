from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from functools import reduce
import numpy as np
import tensorflow as tf

import ray
from ray.rllib.evaluator import TFMultiGPUSupport
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.parallel import LocalSyncParallelOptimizer
from ray.rllib.utils.timer import TimerStat


class LocalMultiGPUOptimizer(Optimizer):
    def _init(self):
        assert isinstance(self.local_evaluator, TFMultiGPUSupport)
        self.batch_size = self.config.get("sgd_batchsize", 128)
        self.devices = ["/cpu:0", "/cpu:1"]
        self.per_device_batch_size = self.batch_size // len(self.devices)
        self.sample_timer = TimerStat()
        self.load_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.update_weights_timer = TimerStat()

        # per-GPU graph copies created below must share vars with the policy
        tf.get_variable_scope().reuse_variables()
        self.par_opt = LocalSyncParallelOptimizer(
            tf.train.AdamOptimizer(self.config.get("sgd_stepsize", 5e-5)),
            self.devices,
            self.local_evaluator.tf_loss_inputs(),
            self.per_device_batch_size,
            lambda *ph: self.local_evaluator.build_tf_loss(ph),
            self.config.get("logdir", "/tmp/ray"))

    def step(self):
        with self.update_weights_timer:
            if self.remote_evaluators:
                weights = ray.put(self.local_evaluator.get_weights())
                for e in self.remote_evaluators:
                    e.set_weights.remote(weights)

        with self.sample_timer:
            if self.remote_evaluators:
                samples = reduce(
                    lambda a, b: a.concat(b),
                    ray.get(
                        [e.sample.remote() for e in self.remote_evaluators]))
            else:
                samples = self.local_evaluator.sample()
            assert isinstance(samples, SampleBatch)

        with self.load_timer:
            tuples_per_device = self.par_opt.load_data(
                self.local_evaluator.sess, samples.feature_columns())

        with self.grad_timer:
            for i in range(self.config.get("num_sgd_iter", 10)):
                batch_index = 0
                num_batches = (
                    int(tuples_per_device) // int(self.per_device_batch_size))
                permutation = np.random.permutation(num_batches)
                while batch_index < num_batches:
                    self.par_opt.optimize(
                        permutation[batch_index] * self.per_device_batch_size)
                    batch_index += 1

    def stats(self):
        return {
            "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
            "load_time_ms": round(1000 * self.load_timer.mean, 3),
            "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
            "update_time_ms": round(1000 * self.update_weights_timer.mean, 3),
        }
