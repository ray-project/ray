from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np
from collections import defaultdict
import os
import tensorflow as tf

import ray
from ray.rllib.evaluation.tf_policy_graph import TFPolicyGraph
from ray.rllib.optimizers.policy_optimizer import PolicyOptimizer
from ray.rllib.optimizers.multi_gpu_impl import LocalSyncParallelOptimizer
from ray.rllib.utils.timer import TimerStat


class LocalMultiGPUOptimizer(PolicyOptimizer):
    """A synchronous optimizer that uses multiple local GPUs.

    Samples are pulled synchronously from multiple remote evaluators,
    concatenated, and then split across the memory of multiple local GPUs.
    A number of SGD passes are then taken over the in-memory data. For more
    details, see `multi_gpu_impl.LocalSyncParallelOptimizer`.

    This optimizer is Tensorflow-specific and require the underlying
    PolicyGraph to be a TFPolicyGraph instance that support `.copy()`.

    Note that all replicas of the TFPolicyGraph will merge their
    extra_compute_grad and apply_grad feed_dicts and fetches. This
    may result in unexpected behavior.
    """

    def _init(self, sgd_batch_size=128, sgd_stepsize=5e-5, num_sgd_iter=10,
              timesteps_per_batch=1024):
        self.batch_size = sgd_batch_size
        self.sgd_stepsize = sgd_stepsize
        self.num_sgd_iter = num_sgd_iter
        self.timesteps_per_batch = timesteps_per_batch
        gpu_ids = ray.get_gpu_ids()
        if not gpu_ids:
            self.devices = ["/cpu:0"]
        else:
            self.devices = ["/gpu:{}".format(i) for i in range(len(gpu_ids))]
        self.batch_size = int(
                sgd_batch_size / len(self.devices)) * len(self.devices)
        assert self.batch_size % len(self.devices) == 0
        assert self.batch_size >= len(self.devices), "batch size too small"
        self.per_device_batch_size = int(self.batch_size / len(self.devices))
        self.sample_timer = TimerStat()
        self.load_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.update_weights_timer = TimerStat()

        print("LocalMultiGPUOptimizer devices", self.devices)
        print("LocalMultiGPUOptimizer batch size", self.batch_size)

        assert set(self.local_evaluator.policy_map.keys()) == {"default"}, \
            "Multi-agent is not supported"
        self.policy = self.local_evaluator.policy_map["default"]
        assert isinstance(self.policy, TFPolicyGraph), \
            "Only TF policies are supported"

        # per-GPU graph copies created below must share vars with the policy
        # reuse is set to AUTO_REUSE because Adam nodes are created after
        # all of the device copies are created.
        with self.local_evaluator.tf_sess.graph.as_default():
            with self.local_evaluator.tf_sess.as_default():
                with tf.variable_scope("default", reuse=tf.AUTO_REUSE):
                    self.par_opt = LocalSyncParallelOptimizer(
                        tf.train.AdamOptimizer(self.sgd_stepsize),
                        self.devices,
                        self.policy.loss_inputs(),
                        self.per_device_batch_size,
                        self.policy.copy,
                        os.getcwd())

                self.sess = self.local_evaluator.tf_sess
                self.sess.run(tf.global_variables_initializer())

    def step(self, postprocess_fn=None):
        with self.update_weights_timer:
            if self.remote_evaluators:
                weights = ray.put(self.local_evaluator.get_weights())
                for e in self.remote_evaluators:
                    e.set_weights.remote(weights)

        with self.sample_timer:
            if self.remote_evaluators:
                # TODO(rliaw): remove when refactoring
                from ray.rllib.agents.ppo.rollout import collect_samples
                samples = collect_samples(self.remote_evaluators,
                                          self.timesteps_per_batch)
            else:
                samples = self.local_evaluator.sample()
            self._check_not_multiagent(samples)

            if postprocess_fn:
                postprocess_fn(samples)

        with self.load_timer:
            tuples_per_device = self.par_opt.load_data(
                self.sess,
                samples.columns([key for key, _ in self.policy.loss_inputs()]))

        with self.grad_timer:
            all_extra_fetches = defaultdict(list)
            num_batches = (
                int(tuples_per_device) // int(self.per_device_batch_size))
            for i in range(self.num_sgd_iter):
                iter_extra_fetches = defaultdict(list)
                permutation = np.random.permutation(num_batches)
                for batch_index in range(num_batches):
                    # TODO(ekl) support ppo's debugging features, e.g.
                    # printing the current loss and tracing
                    batch_fetches = self.par_opt.optimize(
                        self.sess,
                        permutation[batch_index] * self.per_device_batch_size)
                    for k, v in batch_fetches.items():
                        iter_extra_fetches[k] += [v]
                for k, v in iter_extra_fetches.items():
                    all_extra_fetches[k] += [v]

        self.num_steps_sampled += samples.count
        self.num_steps_trained += samples.count
        return all_extra_fetches

    def stats(self):
        return dict(PolicyOptimizer.stats(self), **{
            "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
            "load_time_ms": round(1000 * self.load_timer.mean, 3),
            "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
            "update_time_ms": round(1000 * self.update_weights_timer.mean, 3),
        })
