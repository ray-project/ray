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
from ray.rllib.utils.filter import RunningStat
from collections import OrderedDict



class LocalMultiGPUOptimizer(Optimizer):
    """A synchronous optimizer that uses multiple local GPUs.

    Samples are pulled synchronously from multiple remote evaluators,
    concatenated, and then split across the memory of multiple local GPUs.
    A number of SGD passes are then taken over the in-memory data. For more
    details, see `multi_gpu_impl.LocalSyncParallelOptimizer`.

    This optimizer is Tensorflow-specific and require evaluators to implement
    the TFMultiGPUSupport API.
    """

    def _init(self):
        assert isinstance(self.local_evaluator, TFMultiGPUSupport)
        self.batch_size = self.config.get("sgd_batch_size", 128)
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
        self._init_extra_ops()
        # per-GPU graph copies created below must share vars with the policy
        tf.get_variable_scope().reuse_variables()

        self.par_opt = LocalSyncParallelOptimizer(
            tf.train.AdamOptimizer(self.config.get("sgd_stepsize", 5e-5)),
            self.devices,
            [ph for _, ph in self.loss_inputs],
            self.per_device_batch_size,
            lambda *ph: self.local_evaluator.build_tf_loss(ph),
            self.config.get("logdir", os.getcwd()))

        self.sess = self.local_evaluator.sess
        self.sess.run(tf.global_variables_initializer())

    def _init_extra_ops(self):
        self.extra_ops, extra_ops_keys = self.local_evaluator.tf_extra_ops(
            self.par_opt.get_device_losses)
        self.extra_stats = OrderedDict(
            [(k, RunningStat(())) for k in extra_ops_keys])

    def step(self, postprocess_samples=None):
        with self.update_weights_timer:
            if self.remote_evaluators:
                weights = ray.put(self.local_evaluator.get_weights())
                for e in self.remote_evaluators:
                    e.set_weights.remote(weights)

        with self.sample_timer:
            if self.remote_evaluators:
                samples = collect_samples(
                    self.remote_evaluators,
                    self.config["timesteps_per_batch"],
                    self.local_evaluator)
            else:
                samples = self.local_evaluator.sample()
            assert isinstance(samples, SampleBatch)

        if postprocess_samples:
            samples = postprocess_samples(samples)

        with self.load_timer:
            # TODO(rliaw): introduce support for printing out trace
            tuples_per_device = self.par_opt.load_data(
                self.local_evaluator.sess,
                samples.columns([key for key, _ in self.loss_inputs]))

        with self.grad_timer:
            # TODO(rliaw): consider making this fail
            for i in range(self.config.get("num_sgd_iter", 10)):
                batch_index = 0
                num_batches = (
                    int(tuples_per_device) // int(self.per_device_batch_size))
                permutation = np.random.permutation(num_batches)
                for batch_index in num_batches:
                    # TODO(ekl) support ppo's debugging features, e.g.
                    # printing the current loss and tracing
                    outputs = self.par_opt.optimize(
                        self.sess,
                        permutation[batch_index] * self.per_device_batch_size,
                        extra_ops=self.extra_ops,
                        extra_feed_dict=self.local_evaluator.extra_feed_dict())
                    for output, (k, stat) in zip(outputs, self.extra_stats):
                        stat(output)

    def stats(self, clear=False):
        info = {k: round(stat.mean, 3) for k, stat in self.extra_stats.items()}

        info.update({
            "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
            "load_time_ms": round(1000 * self.load_timer.mean, 3),
            "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
            "update_time_ms": round(1000 * self.update_weights_timer.mean, 3)
        })

        if clear:
            self.extra_stats = OrderedDict(
                [(k, RunningStat(())) for k in extra_ops_keys])
            self.sample_timer = TimerStat()
            self.load_timer = TimerStat()
            self.grad_timer = TimerStat()
            self.update_weights_timer = TimerStat()
        return info
