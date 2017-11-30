from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from functools import reduce
import tensorflow as tf
import time

from ray.rllib.evaluator import TFMultiGpuSupport
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.parallel import LocalSyncParallelOptimizer
from ray.rllib.ppo.filter import RunningStat


class LocalMultiGPUOptimizer(Optimizer):
    def _init(self):
        assert isinstance(self.local_evaluator, TFMultiGpuSupport)
        self.batch_size = self.config.get("sgd_batchsize", 128)
        self.devices = ["/cpu:0", "/cpu:1"]
        self.per_device_batch_size = self.batch_size // len(self.devices)
        self.sample_time = RunningStat(())
        self.load_time = RunningStat(())
        self.grad_time = RunningStat(())
        self.update_weights_time = RunningStat(())

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
        t0 = time.time()
        if self.remote_evaluators:
            weights = ray.put(self.local_evaluator.get_weights())
            for e in self.remote_evaluators:
                e.set_weights.remote(weights)
        self.update_weights_time.push(time.time() - t0)

        t1 = time.time()
        if self.remote_evaluators:
            samples = reduce(
                lambda a, b: a.concat(b),
                ray.get([e.sample.remote() for e in self.remote_evaluators]))
        else:
            samples = self.local_evaluator.sample()
        assert isinstance(samples, SampleBatch)
        self.sample_time.push(time.time() - t1)

        t2 = time.time()
        tuples_per_device = self.par_opt.load_data(
            self.local_evaluator.sess, samples.feature_columns())
        self.load_time.push(time.time() - t2)

        t3 = time.time()
        for i in range(self.config.get("num_sgd_iter", 10)):
            batch_index = 0
            num_batches = (
                int(tuples_per_device) // int(self.per_device_batch_size))
            permutation = np.random.permutation(num_batches)
            while batch_index < num_batches:
                model.run_sgd_minibatch(
                    permutation[batch_index] * self.per_device_batch_size,
                    [], False, None)
                batch_index += 1

        t4 = time.time()
        grad = self.local_evaluator.compute_gradients(samples)
        self.local_evaluator.apply_gradients(grad)
        self.grad_time.push(time.time() - t4)

    def stats(self):
        return {
            "sample_time_ms": round(1000 * self.sample_time.mean, 3),
            "load_time_ms": round(1000 * self.load_time.mean, 3),
            "grad_time_ms": round(1000 * self.grad_time.mean, 3),
            "update_time_ms": round(1000 * self.update_weights_time.mean, 3),
        }


# TODO(ekl) this should be implemented by some sample batch class
def _concat(samples):
    result = []
    for s in samples:
        result.extend(s)
    return result
