from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import time
import unittest

import ray
from ray.rllib.agents.pg import PGAgent
from ray.rllib.evaluation.policy_evaluator import PolicyEvaluator
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.env.vector_env import VectorEnv
from ray.tune.registry import register_env


class MockPolicyGraph(PolicyGraph):
    def compute_actions(self, obs_batch, state_batches, is_training=False):
        return [0] * len(obs_batch), [], {}

    def postprocess_trajectory(self, batch, other_agent_batches=None):
        return compute_advantages(batch, 100.0, 0.9, use_gae=False)


class BadPolicyGraph(PolicyGraph):
    def compute_actions(self, obs_batch, state_batches, is_training=False):
        raise Exception("intentional error")

    def postprocess_trajectory(self, batch, other_agent_batches=None):
        return compute_advantages(batch, 100.0, 0.9, use_gae=False)


class MockEnv(gym.Env):
    def __init__(self, episode_length, config=None):
        self.episode_length = episode_length
        self.config = config
        self.i = 0
        self.observation_space = gym.spaces.Discrete(1)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        self.i = 0
        return self.i

    def step(self, action):
        self.i += 1
        return 0, 1, self.i >= self.episode_length, {}


class MockEnv2(gym.Env):
    def __init__(self, episode_length):
        self.episode_length = episode_length
        self.i = 0
        self.observation_space = gym.spaces.Discrete(100)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        self.i = 0
        return self.i

    def step(self, action):
        self.i += 1
        return self.i, 100, self.i >= self.episode_length, {}


class MockVectorEnv(VectorEnv):
    def __init__(self, episode_length, num_envs):
        self.envs = [MockEnv(episode_length) for _ in range(num_envs)]
        self.observation_space = gym.spaces.Discrete(1)
        self.action_space = gym.spaces.Discrete(2)
        self.num_envs = num_envs

    def vector_reset(self):
        return [e.reset() for e in self.envs]

    def reset_at(self, index):
        return self.envs[index].reset()

    def vector_step(self, actions):
        obs_batch, rew_batch, done_batch, info_batch = [], [], [], []
        for i in range(len(self.envs)):
            obs, rew, done, info = self.envs[i].step(actions[i])
            obs_batch.append(obs)
            rew_batch.append(rew)
            done_batch.append(done)
            info_batch.append(info)
        return obs_batch, rew_batch, done_batch, info_batch


class TestPolicyEvaluator(unittest.TestCase):
    def testBasic(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph)
        batch = ev.sample()
        for key in ["obs", "actions", "rewards", "dones", "advantages"]:
            self.assertIn(key, batch)
        self.assertGreater(batch["advantages"][0], 1)

    def testQueryEvaluators(self):
        register_env("test", lambda _: gym.make("CartPole-v0"))
        pg = PGAgent(
            env="test", config={
                "num_workers": 2,
                "sample_batch_size": 5
            })
        results = pg.optimizer.foreach_evaluator(lambda ev: ev.batch_steps)
        results2 = pg.optimizer.foreach_evaluator_with_index(
            lambda ev, i: (i, ev.batch_steps))
        self.assertEqual(results, [5, 5, 5])
        self.assertEqual(results2, [(0, 5), (1, 5), (2, 5)])

    def testMetrics(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: MockEnv(episode_length=10),
            policy_graph=MockPolicyGraph,
            batch_mode="complete_episodes")
        remote_ev = PolicyEvaluator.as_remote().remote(
            env_creator=lambda _: MockEnv(episode_length=10),
            policy_graph=MockPolicyGraph,
            batch_mode="complete_episodes")
        ev.sample()
        ray.get(remote_ev.sample.remote())
        result = collect_metrics(ev, [remote_ev])
        self.assertEqual(result["episodes_total"], 20)
        self.assertEqual(result["episode_reward_mean"], 10)

    def testAsync(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            sample_async=True,
            policy_graph=MockPolicyGraph)
        batch = ev.sample()
        for key in ["obs", "actions", "rewards", "dones", "advantages"]:
            self.assertIn(key, batch)
        self.assertGreater(batch["advantages"][0], 1)

    def testAutoConcat(self):
        ev = PolicyEvaluator(
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
        ev = PolicyEvaluator(
            env_creator=lambda cfg: MockEnv(episode_length=20, config=cfg),
            policy_graph=MockPolicyGraph,
            batch_mode="truncate_episodes",
            batch_steps=16,
            num_envs=8)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 16)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_total"], 0)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 16)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_total"], 8)
        indices = []
        for env in ev.async_env.vector_env.envs:
            self.assertEqual(env.unwrapped.config.worker_index, 0)
            indices.append(env.unwrapped.config.vector_index)
        self.assertEqual(indices, [0, 1, 2, 3, 4, 5, 6, 7])

    def testBatchDivisibilityCheck(self):
        self.assertRaises(
            ValueError,
            lambda: PolicyEvaluator(
                env_creator=lambda _: MockEnv(episode_length=8),
                policy_graph=MockPolicyGraph,
                batch_mode="truncate_episodes",
                batch_steps=15, num_envs=4))

    def testBatchesSmallerWhenVectorized(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: MockEnv(episode_length=8),
            policy_graph=MockPolicyGraph,
            batch_mode="truncate_episodes",
            batch_steps=16,
            num_envs=4)
        batch = ev.sample()
        self.assertEqual(batch.count, 16)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_total"], 0)
        batch = ev.sample()
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_total"], 4)

    def testVectorEnvSupport(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: MockVectorEnv(episode_length=20, num_envs=8),
            policy_graph=MockPolicyGraph,
            batch_mode="truncate_episodes",
            batch_steps=10)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 10)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_total"], 0)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 10)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_total"], 8)

    def testTruncateEpisodes(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: MockEnv(10),
            policy_graph=MockPolicyGraph,
            batch_steps=15,
            batch_mode="truncate_episodes")
        batch = ev.sample()
        self.assertEqual(batch.count, 15)

    def testCompleteEpisodes(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: MockEnv(10),
            policy_graph=MockPolicyGraph,
            batch_steps=5,
            batch_mode="complete_episodes")
        batch = ev.sample()
        self.assertEqual(batch.count, 10)

    def testCompleteEpisodesPacking(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: MockEnv(10),
            policy_graph=MockPolicyGraph,
            batch_steps=15,
            batch_mode="complete_episodes")
        batch = ev.sample()
        self.assertEqual(batch.count, 20)
        self.assertEqual(
            batch["t"].tolist(),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

    def testFilterSync(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph,
            sample_async=True,
            observation_filter="ConcurrentMeanStdFilter")
        time.sleep(2)
        ev.sample()
        filters = ev.get_filters(flush_after=True)
        obs_f = filters["default"]
        self.assertNotEqual(obs_f.rs.n, 0)
        self.assertNotEqual(obs_f.buffer.n, 0)

    def testGetFilters(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph,
            sample_async=True,
            observation_filter="ConcurrentMeanStdFilter")
        self.sample_and_flush(ev)
        filters = ev.get_filters(flush_after=False)
        time.sleep(2)
        filters2 = ev.get_filters(flush_after=False)
        obs_f = filters["default"]
        obs_f2 = filters2["default"]
        self.assertGreaterEqual(obs_f2.rs.n, obs_f.rs.n)
        self.assertGreaterEqual(obs_f2.buffer.n, obs_f.buffer.n)

    def testSyncFilter(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph,
            sample_async=True,
            observation_filter="ConcurrentMeanStdFilter")
        obs_f = self.sample_and_flush(ev)

        # Current State
        filters = ev.get_filters(flush_after=False)
        obs_f = filters["default"]

        self.assertLessEqual(obs_f.buffer.n, 20)

        new_obsf = obs_f.copy()
        new_obsf.rs._n = 100
        ev.sync_filters({"default": new_obsf})
        filters = ev.get_filters(flush_after=False)
        obs_f = filters["default"]
        self.assertGreaterEqual(obs_f.rs.n, 100)
        self.assertLessEqual(obs_f.buffer.n, 20)

    def sample_and_flush(self, ev):
        time.sleep(2)
        ev.sample()
        filters = ev.get_filters(flush_after=True)
        obs_f = filters["default"]
        self.assertNotEqual(obs_f.rs.n, 0)
        self.assertNotEqual(obs_f.buffer.n, 0)
        return obs_f


if __name__ == '__main__':
    ray.init(num_cpus=5)
    unittest.main(verbosity=2)
