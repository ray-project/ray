from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import gym
import shutil
import tempfile

import ray
from ray.rllib.test.mock import _MockEvaluator
from ray.rllib.a3c import DEFAULT_CONFIG
from ray.rllib.a3c.base_evaluator import A3CEvaluator
from ray.rllib.utils import FilterManager
from ray.rllib.utils.filter import MeanStdFilter


class A3CEvaluatorTest(unittest.TestCase):

    def setUp(self):
        ray.init(num_cpus=1)
        config = DEFAULT_CONFIG.copy()
        config["num_workers"] = 1
        config["observation_filter"] = "ConcurrentMeanStdFilter"
        config["reward_filter"] = "MeanStdFilter"
        config["batch_size"] = 2
        self._temp_dir = tempfile.mkdtemp("a3c_evaluator_test")
        self.e = A3CEvaluator(
            lambda: gym.make("Pong-v0"),
            config,
            logdir=self._temp_dir)

    def tearDown(self):
        ray.worker.cleanup()
        shutil.rmtree(self._temp_dir)

    def sample_and_flush(self):
        e = self.e
        self.e.sample()
        filters = e.get_filters(flush_after=True)
        obs_f = filters["obs_filter"]
        rew_f = filters["rew_filter"]
        self.assertNotEqual(obs_f.rs.n, 0)
        self.assertNotEqual(obs_f.buffer.n, 0)
        self.assertNotEqual(rew_f.rs.n, 0)
        self.assertNotEqual(rew_f.buffer.n, 0)
        return obs_f, rew_f

    def testGetFilters(self):
        e = self.e
        obs_f, rew_f = self.sample_and_flush()
        COUNT = obs_f.rs.n
        filters = e.get_filters(flush_after=False)
        obs_f = filters["obs_filter"]
        NEW_COUNT = obs_f.rs.n
        self.assertGreaterEqual(NEW_COUNT, COUNT)
        self.assertLessEqual(obs_f.buffer.n, NEW_COUNT - COUNT)

    def testSyncFilter(self):
        """Show that sync_filters rebases own buffer over input"""
        e = self.e
        obs_f, _ = self.sample_and_flush()

        # Current State
        filters = e.get_filters(flush_after=False)
        obs_f = filters["obs_filter"]

        self.assertLessEqual(obs_f.buffer.n, 20)

        new_obsf = obs_f.copy()
        new_obsf.rs._n = 100
        e.sync_filters(obs_filter=new_obsf)
        filters = e.get_filters(flush_after=False)
        obs_f = filters["obs_filter"]
        self.assertGreater(obs_f.rs.n, 100)
        self.assertLessEqual(obs_f.buffer.n, 20)


class FilterManagerTest(unittest.TestCase):
    def setUp(self):
        ray.init(num_cpus=1)

    def tearDown(self):
        ray.worker.cleanup()

    def testSynchronize(self):
        """Synchronize applies filter buffer onto own filter"""
        filt1 = MeanStdFilter(())
        for i in range(10):
            filt1(i)
        self.assertEqual(filt1.rs.n, 10)
        filt1.clear_buffer()
        self.assertEqual(filt1.buffer.n, 0)

        RemoteEvaluator = ray.remote(_MockEvaluator)
        remote_e = RemoteEvaluator.remote(sample_count=10)
        remote_e.sample.remote()

        manager = FilterManager(obs_filter=filt1)
        manager.synchronize([remote_e])

        filters = ray.get(remote_e.get_filters.remote())
        obs_f = filters["obs_filter"]
        self.assertEqual(filt1.rs.n, 20)
        self.assertEqual(filt1.buffer.n, 0)
        self.assertEqual(obs_f.rs.n, filt1.rs.n)
        self.assertEqual(obs_f.buffer.n, filt1.buffer.n)


if __name__ == '__main__':
    unittest.main(verbosity=2)
