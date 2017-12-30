from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import ray
from ray.rllib.test.mock_evaluator import _MockEvaluator
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


if __name__ == '__main__':
    unittest.main(verbosity=2)
