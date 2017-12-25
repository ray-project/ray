from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
from ray.rllib.optimizers import _MeanStdFilterEvaluator
from ray.rllib.utils.filters import MeanStdFilter


class EvaluatorTest(unittest.TestCase):
    def testMergeFilter(self):
        e = _MeanStdFilterEvaluator()
        e.sample()
        e.get_filters()
        obs_filter = MeanStdFilter((4,))
        rew_filter = MeanStdFilter(())
        # TODO(rliaw)

    def testSyncFilter(self):
        e = _MeanStdFilterEvaluator()
        e.sample()
        e.get_filters()
        obs_filter = MeanStdFilter((4,))
        rew_filter = MeanStdFilter(())
        # TODO(rliaw)

    def testCopyFilter(self):
        e = _MeanStdFilterEvaluator()
        e.sample()
        e.get_filters()
        obs_filter = MeanStdFilter((4,))
        rew_filter = MeanStdFilter(())
        # TODO(rliaw)
