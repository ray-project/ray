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
        self.assertTrue(all(local.get_weights() == 0))

    def testFilters(self):
        ray.init(num_cpus=4)
        local = _MeanStdFilterEvaluator()
        remotes = ray.remote(_MeanStdFilterEvaluator)
        remote_evaluators = [remotes.remote() for i in range(1)]
        test_optimizer = AsyncOptimizer(
            {"grads_per_step": 10}, local, remote_evaluators)
        test_optimizer.step()
        obs_f, rew_f = local.get_filters()
        self.assertEqual(obs_f.buffer.n, 0)
        self.assertEqual(rew_f.buffer.n, 0)
        self.assertEqual(obs_f.rs.n, 100)
        self.assertEqual(rew_f.rs.n, 100)


if __name__ == '__main__':
    unittest.main(verbosity=2)
