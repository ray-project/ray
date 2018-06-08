from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import time
import unittest

import ray
from ray.rllib.utils.common_policy_evaluator import CommonPolicyEvaluator, \
    collect_metrics
from ray.rllib.utils.policy_graph import PolicyGraph
from ray.rllib.utils.process_rollout import compute_advantages


class MockPolicyGraph(PolicyGraph):
    def compute_actions(self, obs_batch, state_batches, is_training=False):
        return [0] * len(obs_batch), [], {}

    def postprocess_trajectory(self, batch):
        return compute_advantages(batch, 100.0, 0.9, use_gae=False)


class MockEnv(gym.Env):
    def __init__(self, episode_length):
        self.episode_length = episode_length
        self.i = 0
        self.observation_space = gym.spaces.Discrete(1)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        self.i = 0
        return self.i

    def step(self, action):
        self.i += 1
        return 0, 1, self.i >= self.episode_length, {}


class MockVectorEnv(object):
    def __init__(self, episode_length, vector_width):
        self.envs = [
            MockEnv(episode_length) for _ in range(vector_width)]

    def vector_reset(self):
        return [e.reset() for e in self.envs]

    def vector_step(self, actions):
        obs_batch, rew_batch, done_batch, info_batch = [], [], [], []
        for i in range(len(self.envs)):
            obs, rew, done, info = self.envs[i].step(actions[i])
            obs_batch.append(obs)
            rew_batch.append(rew)
            done_batch.append(done)
            info_batch.append(info)

            o
        self.i += 1
        return 0, 1, self.i >= self.episode_length, {}


class TestCommonPolicyEvaluator(unittest.TestCase):
    def testBasic(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph)
        batch = ev.sample()
        for key in ["obs", "actions", "rewards", "dones", "advantages"]:
            self.assertIn(key, batch)
        self.assertGreater(batch["advantages"][0], 1)

    def testAsync(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            sample_async=True,
            policy_graph=MockPolicyGraph)
        batch = ev.sample()
        for key in ["obs", "actions", "rewards", "dones", "advantages"]:
            self.assertIn(key, batch)
        self.assertGreater(batch["advantages"][0], 1)

    def testAutoConcat(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: MockEnv(episode_length=40),
            policy_graph=MockPolicyGraph,
            sample_async=True,
            batch_steps=10,
            batch_mode="truncate_episodes",
            observation_filter="ConcurrentMeanStdFilter")
        time.sleep(2)
        batch = ev.sample()
        self.assertEqual(batch.count, 40)  # auto-concat up to 5 episodes

    def testAutoVectorization(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: MockEnv(episode_length=20),
            policy_graph=MockPolicyGraph,
            batch_mode="truncate_episodes",
            batch_steps=10, vector_width=8)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 10)
        result = collect_metrics(ev, [])
        self.assertEqual(result.episodes_total, 0)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 10)
        result = collect_metrics(ev, [])
        self.assertEqual(result.episodes_total, 8)

    def testVectorEnvSupport(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: MockVectorEnv(
                episode_length=20, vector_width=8),
            policy_graph=MockPolicyGraph,
            batch_mode="truncate_episodes",
            batch_steps=10)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 10)
        result = collect_metrics(ev, [])
        self.assertEqual(result.episodes_total, 0)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 10)
        result = collect_metrics(ev, [])
        self.assertEqual(result.episodes_total, 8)

    def testPackEpisodes(self):
        for batch_size in [1, 10, 100, 1000]:
            ev = CommonPolicyEvaluator(
                env_creator=lambda _: gym.make("CartPole-v0"),
                policy_graph=MockPolicyGraph,
                batch_steps=batch_size,
                batch_mode="pack_episodes")
            batch = ev.sample()
            self.assertEqual(batch.count, batch_size)

    def testTruncateEpisodes(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph,
            batch_steps=2,
            batch_mode="truncate_episodes")
        batch = ev.sample()
        self.assertEqual(batch.count, 2)
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph,
            batch_steps=1000,
            batch_mode="truncate_episodes")
        self.assertLess(batch.count, 200)

    def testCompleteEpisodes(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph,
            batch_steps=2,
            batch_mode="complete_episodes")
        batch = ev.sample()
        self.assertGreater(batch.count, 2)
        self.assertTrue(batch["dones"][-1])
        batch = ev.sample()
        self.assertGreater(batch.count, 2)
        self.assertTrue(batch["dones"][-1])

    def testFilterSync(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph,
            sample_async=True,
            observation_filter="ConcurrentMeanStdFilter")
        time.sleep(2)
        ev.sample()
        filters = ev.get_filters(flush_after=True)
        obs_f = filters["obs_filter"]
        self.assertNotEqual(obs_f.rs.n, 0)
        self.assertNotEqual(obs_f.buffer.n, 0)

    def testGetFilters(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph,
            sample_async=True,
            observation_filter="ConcurrentMeanStdFilter")
        self.sample_and_flush(ev)
        filters = ev.get_filters(flush_after=False)
        time.sleep(2)
        filters2 = ev.get_filters(flush_after=False)
        obs_f = filters["obs_filter"]
        obs_f2 = filters2["obs_filter"]
        self.assertGreaterEqual(obs_f2.rs.n, obs_f.rs.n)
        self.assertGreaterEqual(obs_f2.buffer.n, obs_f.buffer.n)

    def testSyncFilter(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph,
            sample_async=True,
            observation_filter="ConcurrentMeanStdFilter")
        obs_f = self.sample_and_flush(ev)

        # Current State
        filters = ev.get_filters(flush_after=False)
        obs_f = filters["obs_filter"]

        self.assertLessEqual(obs_f.buffer.n, 20)

        new_obsf = obs_f.copy()
        new_obsf.rs._n = 100
        ev.sync_filters({"obs_filter": new_obsf})
        filters = ev.get_filters(flush_after=False)
        obs_f = filters["obs_filter"]
        self.assertGreaterEqual(obs_f.rs.n, 100)
        self.assertLessEqual(obs_f.buffer.n, 20)

    def sample_and_flush(self, ev):
        time.sleep(2)
        ev.sample()
        filters = ev.get_filters(flush_after=True)
        obs_f = filters["obs_filter"]
        self.assertNotEqual(obs_f.rs.n, 0)
        self.assertNotEqual(obs_f.buffer.n, 0)
        return obs_f


if __name__ == '__main__':
    ray.init()
    unittest.main(verbosity=2)
