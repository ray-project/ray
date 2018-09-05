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

    def _init(self,
              sgd_batch_size=128,
              num_sgd_iter=10,
              timesteps_per_batch=1024,
              num_gpus=0,
              standardize_fields=[]):
        self.batch_size = sgd_batch_size
        self.num_sgd_iter = num_sgd_iter
        self.timesteps_per_batch = timesteps_per_batch
        if not num_gpus:
            self.devices = ["/cpu:0"]
        else:
            self.devices = ["/gpu:{}".format(i) for i in range(num_gpus)]
        self.batch_size = int(sgd_batch_size / len(self.devices)) * len(
            self.devices)
        assert self.batch_size % len(self.devices) == 0
        assert self.batch_size >= len(self.devices), "batch size too small"
        self.per_device_batch_size = int(self.batch_size / len(self.devices))
        self.sample_timer = TimerStat()
        self.load_timer = TimerStat()
        self.grad_timer = TimerStat()
        self.update_weights_timer = TimerStat()
        self.standardize_fields = standardize_fields

        print("LocalMultiGPUOptimizer devices", self.devices)

        if set(self.local_evaluator.policy_map.keys()) != {"default"}:
            raise ValueError(
                "Multi-agent is not supported with multi-GPU. Try using the "
                "simple optimizer instead.")
        self.policy = self.local_evaluator.policy_map["default"]
        if not isinstance(self.policy, TFPolicyGraph):
            raise ValueError(
                "Only TF policies are supported with multi-GPU. Try using the "
                "simple optimizer instead.")

        # per-GPU graph copies created below must share vars with the policy
        # reuse is set to AUTO_REUSE because Adam nodes are created after
        # all of the device copies are created.
        with self.local_evaluator.tf_sess.graph.as_default():
            with self.local_evaluator.tf_sess.as_default():
                with tf.variable_scope("default", reuse=tf.AUTO_REUSE):
                    if self.policy._state_inputs:
                        rnn_inputs = self.policy._state_inputs + [
                            self.policy._seq_lens
                        ]
                    else:
                        rnn_inputs = []
                    self.par_opt = LocalSyncParallelOptimizer(
                        self.policy.optimizer(), self.devices,
                        [v for _, v in self.policy.loss_inputs()], rnn_inputs,
                        self.per_device_batch_size, self.policy.copy,
                        os.getcwd())

                self.sess = self.local_evaluator.tf_sess
                self.sess.run(tf.global_variables_initializer())

    def step(self):
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

        for field in self.standardize_fields:
            value = samples[field]
            standardized = (value - value.mean()) / max(1e-4, value.std())
            samples[field] = standardized
        samples.shuffle()

        with self.load_timer:
            tuples = self.policy._get_loss_inputs_dict(samples)
            data_keys = [ph for _, ph in self.policy.loss_inputs()]
            if self.policy._state_inputs:
                state_keys = (
                    self.policy._state_inputs + [self.policy._seq_lens])
            else:
                state_keys = []
            tuples_per_device = self.par_opt.load_data(
                self.sess, [tuples[k] for k in data_keys],
                [tuples[k] for k in state_keys])

        with self.grad_timer:
            num_batches = (
                int(tuples_per_device) // int(self.per_device_batch_size))
            print("== sgd epochs ==")
            for i in range(self.num_sgd_iter):
                iter_extra_fetches = defaultdict(list)
                permutation = np.random.permutation(num_batches)
                for batch_index in range(num_batches):
                    batch_fetches = self.par_opt.optimize(
                        self.sess,
                        permutation[batch_index] * self.per_device_batch_size)
                    for k, v in batch_fetches.items():
                        iter_extra_fetches[k].append(v)
                print(i, _averaged(iter_extra_fetches))

        self.num_steps_sampled += samples.count
        self.num_steps_trained += samples.count
        return _averaged(iter_extra_fetches)

    def stats(self):
        return dict(
            PolicyOptimizer.stats(self), **{
                "sample_time_ms": round(1000 * self.sample_timer.mean, 3),
                "load_time_ms": round(1000 * self.load_timer.mean, 3),
                "grad_time_ms": round(1000 * self.grad_timer.mean, 3),
                "update_time_ms": round(1000 * self.update_weights_timer.mean,
                                        3),
            })


def _averaged(kv):
    out = {}
    for k, v in kv.items():
        if v[0] is not None:
            out[k] = np.mean(v)
    return out
