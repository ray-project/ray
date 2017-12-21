from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import time
import unittest
import numpy as np

from ray.rllib.optimizers import Evaluator, SampleBatch
from ray.rllib.utils.filter import NoFilter

# from ray.rllib.models import ModelCatalog
# from ray.rllib.models.preprocessors import Preprocessor

# from ray.rllib.ppo import PPOAgent
# from ray.rllib.a3c import A3CAgent

# TODO(rliaw)
class MockEvaluator(Evaluator):
    def __init__(self):
        self._weights = np.array([-10, -10, -10, -10])
        self._grad = np.array([1,1,1,1])
        self.obs_filter = NoFilter()
        self.rew_filter = NoFilter()

    def sample(self):
        obs_filter, rew_filter = self.get_filters()
        info = {"obs_filter": obs_filter, "rew_filter": rew_filter}
        return SampleBatch({"observations": [1]}), info

    def compute_gradients(self, samples):
        return self._grad * samples.count, {}

    def apply_gradients(self, grads):
        self._weights += self._grad

    def get_weights(self):
        return self._weights

    def set_weights(self, weights):
        self._weights = weights


class MeanStdFilterEvaluator(MockEvaluator):
    def __init__(self):
        pass

class EvaluatorTest(unittest.TestCase):
    def testMergeFilter(self):
        pass

    def testSyncFilter(self):
        pass

    def testCopyFilter(self):
        pass


class OptimizerTest(unittest.TestCase):
    def testLocalSync(self):
        pass

    def testAsync(self):
        pass

