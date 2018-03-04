from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
import os
import tensorflow as tf

import ray
from ray.rllib.optimizers.evaluator import TFMultiGPUSupport
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.optimizers.sample_batch import SampleBatch
from ray.rllib.optimizers.multi_gpu_impl import LocalSyncParallelOptimizer
from ray.rllib.utils.timer import TimerStat


class LocalMultiGPUOptimizer(Optimizer):
    """A synchronous optimizer that uses multiple local GPUs.

    Samples are pulled synchronously from multiple remote evaluators,
    concatenated, and then split across the memory of multiple local GPUs.
    A number of SGD passes are then taken over the in-memory data. For more
    details, see `multi_gpu_impl.LocalSyncParallelOptimizer`.

    This optimizer is Tensorflow-specific and require evaluators to implement
    the TFMultiGPUSupport API.
    """

    def _init(self, sgd_batch_size=128, sgd_stepsize=5e-5, num_sgd_iter=10):
        assert isinstance(self.local_evaluator, TFMultiGPUSupport)
        self.batch_size = sgd_batch_size
        self.sgd_stepsize = sgd_stepsize
        self.num_sgd_iter = num_sgd_iter
        gpu_ids = ray.get_gpu_ids()
        if not gpu_ids:
            self.devices = ["/cpu:0"]
        else:
            self.devices = ["/gpu:{}".format(i) for i in range(len(gpu_ids))]
        assert self.batch_size > len(self.devices), "batch size too small"
        self.per_device_batch_size = self.batch_size // len(self.devices)
        self.sample_timer = TimerStat()
        self.load_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.update_weights_timer = TimerStat()

        print("LocalMultiGPUOptimizer devices", self.devices)
        print("LocalMultiGPUOptimizer batch size", self.batch_size)

        # List of (feature name, feature placeholder) tuples
        self.loss_inputs = self.local_evaluator.tf_loss_inputs()

        # per-GPU graph copies created below must share vars with the policy
        tf.get_variable_scope().reuse_variables()

        self.par_opt = LocalSyncParallelOptimizer(
            tf.train.AdamOptimizer(self.sgd_stepsize),
            self.devices,
            [ph for _, ph in self.loss_inputs],
            self.per_device_batch_size,
            lambda *ph: self.local_evaluator.build_tf_loss(ph),
            os.getcwd())

        self.sess = self.local_evaluator.sess
        self.sess.run(tf.global_variables_initializer())

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
            assert isinstance(samples, SampleBatch)

        with self.load_timer:
            tuples_per_device = self.par_opt.load_data(
                self.local_evaluator.sess,
                samples.columns([key for key, _ in self.loss_inputs]))

        with self.grad_timer:
            for i in range(self.num_sgd_iter):
                batch_index = 0
                num_batches = (
                    int(tuples_per_device) // int(self.per_device_batch_size))
                permutation = np.random.permutation(num_batches)
                while batch_index < num_batches:
                    # TODO(ekl) support ppo's debugging features, e.g.
                    # printing the current loss and tracing
                    self.par_opt.optimize(
                        self.sess,
                        permutation[batch_index] * self.per_device_batch_size)
                    batch_index += 1

        self.num_steps_sampled += samples.count
        self.num_steps_trained += samples.count

    def stats(self):
        return dict(Optimizer.stats(), **{
            "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
            "load_time_ms": round(1000 * self.load_timer.mean, 3),
            "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
            "update_time_ms": round(1000 * self.update_weights_timer.mean, 3),
        })
