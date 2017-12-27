from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
from ray.rllib.optimizers.evaluator import _MeanStdFilterEvaluator


class EvaluatorTest(unittest.TestCase):
    def testGetFilters(self):
        e = _MeanStdFilterEvaluator(sample_count=10)
        e.sample()
        obs_f, rew_f = e.get_filters(flush_after=True)
        self.assertEqual(obs_f.rs.n, 10)
        self.assertEqual(obs_f.buffer.n, 10)
        self.assertEqual(rew_f.rs.n, 10)
        self.assertEqual(rew_f.buffer.n, 10)
        obs_f, rew_f = e.get_filters()
        self.assertEqual(obs_f.rs.n, 10)
        self.assertEqual(obs_f.buffer.n, 0)
        self.assertEqual(rew_f.rs.n, 10)
        self.assertEqual(rew_f.buffer.n, 0)

    def testMergeFilter(self):
        """MergeFilter applies filter buffer onto own filter"""
        e = _MeanStdFilterEvaluator(sample_count=10)
        e.sample()
        obs_f, rew_f = e.get_filters(flush_after=True)
        obs_f, rew_f = e.get_filters()
        self.assertEqual(obs_f.rs.n, 10)
        self.assertEqual(obs_f.buffer.n, 0)
        self.assertEqual(rew_f.rs.n, 10)
        self.assertEqual(rew_f.buffer.n, 0)
        e1 = _MeanStdFilterEvaluator(sample_count=10)
        e1.sample()
        e.merge_filters(*e1.get_filters())
        obs_f, rew_f = e.get_filters()
        self.assertEqual(obs_f.rs.n, 20)
        self.assertEqual(obs_f.buffer.n, 0)
        self.assertEqual(rew_f.rs.n, 20)
        self.assertEqual(rew_f.buffer.n, 0)

    def testSyncFilter(self):
        """Show that SyncFilter rebases own buffer over input"""
        e = _MeanStdFilterEvaluator(sample_count=10)
        e.sample()
        obs_f, rew_f = e.get_filters(flush_after=True)
        e.sample()

        # Current State
        obs_f, rew_f = e.get_filters(flush_after=False)
        self.assertEqual(obs_f.rs.n, 20)
        self.assertEqual(obs_f.buffer.n, 10)
        self.assertEqual(rew_f.rs.n, 20)
        self.assertEqual(rew_f.buffer.n, 10)

        e1 = _MeanStdFilterEvaluator(sample_count=50)
        e1.sample()
        new_obsf, new_rewf = e1.get_filters(flush_after=True)
        new_obsf, new_rewf = e1.get_filters()
        self.assertEqual(new_obsf.rs.n, 50)
        self.assertEqual(new_rewf.rs.n, 50)
        self.assertEqual(new_obsf.buffer.n, 0)
        self.assertEqual(new_rewf.buffer.n, 0)
        e.sync_filters(new_obsf, new_rewf)
        obs_f, rew_f = e.get_filters()
        self.assertEqual(obs_f.rs.n, 60)
        self.assertEqual(obs_f.buffer.n, 10)
        self.assertEqual(rew_f.rs.n, 60)
        self.assertEqual(rew_f.buffer.n, 10)

    def testSyncFilter2(self):
        """Show that SyncFilter ignores input filter buffer"""
        e = _MeanStdFilterEvaluator(sample_count=10)
        e.sample()
        obs_f, rew_f = e.get_filters(flush_after=False)
        self.assertEqual(obs_f.rs.n, 10)
        self.assertEqual(obs_f.buffer.n, 10)
        self.assertEqual(rew_f.rs.n, 10)
        self.assertEqual(rew_f.buffer.n, 10)
        e1 = _MeanStdFilterEvaluator(sample_count=10)
        e1.sample()
        new_obsf, new_rewf = e1.get_filters(flush_after=True)
        self.assertEqual(new_obsf.buffer.n, 10)
        self.assertEqual(new_rewf.buffer.n, 10)
        e.sync_filters(new_obsf, new_rewf)
        obs_f, rew_f = e.get_filters()
        self.assertEqual(obs_f.rs.n, 20)
        self.assertEqual(obs_f.buffer.n, 10)
        self.assertEqual(rew_f.rs.n, 20)
        self.assertEqual(rew_f.buffer.n, 10)


if __name__ == '__main__':
    unittest.main(verbosity=2)
