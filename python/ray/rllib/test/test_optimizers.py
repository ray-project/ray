from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import ray
from ray.rllib.optimizers.evaluator import (
    _MockEvaluator, _MeanStdFilterEvaluator)
from ray.rllib.optimizers import AsyncOptimizer


class AsyncOptimizerTest(unittest.TestCase):

    def tearDown(self):
        ray.worker.cleanup()

    def testBasic(self):
        ray.init(num_cpus=4)
        local = _MockEvaluator()
        remotes = ray.remote(_MockEvaluator)
        remote_evaluators = [remotes.remote() for i in range(5)]
        test_optimizer = AsyncOptimizer(
            {"grads_per_step": 10}, local, remote_evaluators)
        test_optimizer.step()
        # TODO(rliaw)

    def testFilters(self):
        ray.init(num_cpus=4)
        local = _MeanStdFilterEvaluator()
        remotes = ray.remote(_MeanStdFilterEvaluator)
        remote_evaluators = [remotes.remote() for i in range(1)]
        test_optimizer = AsyncOptimizer(
            {"grads_per_step": 10}, local, remote_evaluators)
        test_optimizer.step()
        # TODO(rliaw)
