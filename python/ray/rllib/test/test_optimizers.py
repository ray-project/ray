from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.rllib.optimizers.optimizer import Optimizer
from ray.rllib.utils.timer import TimerStat

import ray
import unittest




class AsyncOptimizerTest(unittest.TestCase):

    def tearDown(self):
        ray.worker.cleanup()

    def testBasic(self):
        ray.init(num_cpus=4)
        local = MockEvaluator()
        remotes = ray.remote(MockEvaluator)
        remote_evaluators = [remotes.remote() for i in range(5)]
        test_optimizer = AsyncOptimizer({"grads_per_step": 10}, local, remote_evaluators)
        test_optimizer.step()

    def testFilters(self):
        ray.init(num_cpus=4)
        local = MeanStdFilterEvaluator()
        remotes = ray.remote(MeanStdFilterEvaluator)
        remote_evaluators = [remotes.remote() for i in range(1)]
        test_optimizer = AsyncOptimizer({"grads_per_step": 10}, local, remote_evaluators)
        test_optimizer.step()


