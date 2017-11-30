from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import time

import ray
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.ppo.filter import RunningStat


class AsyncOptimizer(Optimizer):
    """An asynchronous RL optimizer, e.g. for implementing A3C.

    This optimizer asynchronously pulls and applies gradients from remote
    evaluators, sending updated weights back as needed. This pipelines the
    gradient computations on the remote workers.
    """
    def _init(self):
        self.apply_time = RunningStat(())
        self.wait_time = RunningStat(())
        self.dispatch_time = RunningStat(())
        self.grads_per_step = self.config.get("grads_per_step", 100)

    def step(self):
        weights = ray.put(self.local_evaluator.get_weights())
        gradient_queue = []
        num_gradients = 0

        # Kick off the first wave of async tasks
        for e in self.remote_evaluators:
            e.set_weights.remote(weights)
            fut = e.compute_gradients.remote(e.sample.remote())
            gradient_queue.append((fut, e))
            num_gradients += 1

        # Note: can't use wait: https://github.com/ray-project/ray/issues/1128
        while gradient_queue:
            t0 = time.time()
            fut, e = gradient_queue[0]
            gradient_queue = gradient_queue[1:]
            gradient = ray.get(fut)
            self.wait_time.push(time.time() - t0)

            if gradient is not None:
                t1 = time.time()
                self.local_evaluator.apply_gradients(gradient)
                self.apply_time.push(time.time() - t1)

            if num_gradients < self.grads_per_step:
                t2 = time.time()
                e.set_weights.remote(self.local_evaluator.get_weights())
                fut = e.compute_gradients.remote(e.sample.remote())
                gradient_queue.append((fut, e))
                num_gradients += 1
                self.dispatch_time.push(time.time() - t2)

    def stats(self):
        return {
            "wait_time_ms": round(1000 * self.wait_time.mean, 3),
            "apply_time_ms": round(1000 * self.apply_time.mean, 3),
            "dispatch_time_ms": round(1000 * self.dispatch_time.mean, 3),
        }
