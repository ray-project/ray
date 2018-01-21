from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import gym
import shutil
import tempfile

import ray
from ray.rllib.a3c import DEFAULT_CONFIG
from ray.rllib.a3c.a3c_evaluator import A3CEvaluator
from ray.rllib.dqn.dqn_evaluator import adjust_nstep
from ray.tune.registry import get_registry


class DQNEvaluatorTest(unittest.TestCase):
    def testNStep(self):
        obs = [1, 2, 3, 4, 5, 6, 7]
        actions = ["a", "b", "a", "a", "a", "b", "a"]
        rewards = [10.0, 0.0, 100.0, 100.0, 100.0, 100.0, 100000.0]
        new_obs = [2, 3, 4, 5, 6, 7, 8]
        dones = [1, 0, 0, 0, 0, 1, 0]
        adjust_nstep(3, 0.9, obs, actions, rewards, new_obs, dones)
        self.assertEqual(obs, [1, 2, 3, 4, 5])
        self.assertEqual(actions, ["a", "b", "a", "a", "a"])
        self.assertEqual(rewards, [10.0, 171.0, 271.0, 271.0, 190.0])
        self.assertEqual(new_obs, [2, 5, 6, 7, 7])
        self.assertEqual(dones, [1, 0, 0, 0, 0])


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
            get_registry(),
            lambda config: gym.make("CartPole-v0"),
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
        rew_f = filters["rew_filter"]

        self.assertLessEqual(obs_f.buffer.n, 20)

        new_obsf = obs_f.copy()
        new_obsf.rs._n = 100
        e.sync_filters({"obs_filter": new_obsf, "rew_filter": rew_f})
        filters = e.get_filters(flush_after=False)
        obs_f = filters["obs_filter"]
        self.assertGreaterEqual(obs_f.rs.n, 100)
        self.assertLessEqual(obs_f.buffer.n, 20)


if __name__ == '__main__':
    unittest.main(verbosity=2)
