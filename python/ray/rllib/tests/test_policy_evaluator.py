from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import numpy as np
import random
import time
import unittest
from collections import Counter

import ray
from ray.rllib.agents.pg import PGAgent
from ray.rllib.agents.a3c import A2CAgent
from ray.rllib.evaluation.policy_evaluator import PolicyEvaluator
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.evaluation.policy_graph import PolicyGraph
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.env.vector_env import VectorEnv
from ray.tune.registry import register_env


class MockPolicyGraph(PolicyGraph):
    def compute_actions(self,
                        obs_batch,
                        state_batches,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        episodes=None,
                        **kwargs):
        return [random.choice([0, 1])] * len(obs_batch), [], {}

    def postprocess_trajectory(self,
                               batch,
                               other_agent_batches=None,
                               episode=None):
        assert episode is not None
        return compute_advantages(batch, 100.0, 0.9, use_gae=False)


class BadPolicyGraph(PolicyGraph):
    def compute_actions(self,
                        obs_batch,
                        state_batches,
                        prev_action_batch=None,
                        prev_reward_batch=None,
                        episodes=None,
                        **kwargs):
        raise Exception("intentional error")

    def postprocess_trajectory(self,
                               batch,
                               other_agent_batches=None,
                               episode=None):
        assert episode is not None
        return compute_advantages(batch, 100.0, 0.9, use_gae=False)


class FailOnStepEnv(gym.Env):
    def __init__(self):
        self.observation_space = gym.spaces.Discrete(1)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        raise ValueError("kaboom")

    def step(self, action):
        raise ValueError("kaboom")


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

    def get_unwrapped(self):
        return self.envs


class TestPolicyEvaluator(unittest.TestCase):
    def testBasic(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph)
        batch = ev.sample()
        for key in [
                "obs", "actions", "rewards", "dones", "advantages",
                "prev_rewards", "prev_actions"
        ]:
            self.assertIn(key, batch)
            self.assertGreater(np.abs(np.mean(batch[key])), 0)

        def to_prev(vec):
            out = np.zeros_like(vec)
            for i, v in enumerate(vec):
                if i + 1 < len(out) and not batch["dones"][i]:
                    out[i + 1] = v
            return out.tolist()

        self.assertEqual(batch["prev_rewards"].tolist(),
                         to_prev(batch["rewards"]))
        self.assertEqual(batch["prev_actions"].tolist(),
                         to_prev(batch["actions"]))
        self.assertGreater(batch["advantages"][0], 1)

    # 11/23/18: Samples per second 8501.125113727468
    def testBaselinePerformance(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy_graph=MockPolicyGraph,
            batch_steps=100)
        start = time.time()
        count = 0
        while time.time() - start < 1:
            count += ev.sample().count
        print()
        print("Samples per second {}".format(count / (time.time() - start)))
        print()

    def testGlobalVarsUpdate(self):
        agent = A2CAgent(
            env="CartPole-v0",
            config={
                "lr_schedule": [[0, 0.1], [400, 0.000001]],
            })
        result = agent.train()
        self.assertGreater(result["info"]["learner"]["cur_lr"], 0.01)
        result2 = agent.train()
        self.assertLess(result2["info"]["learner"]["cur_lr"], 0.0001)

    def testNoStepOnInit(self):
        register_env("fail", lambda _: FailOnStepEnv())
        pg = PGAgent(env="fail", config={"num_workers": 1})
        self.assertRaises(Exception, lambda: pg.train())

    def testCallbacks(self):
        counts = Counter()
        pg = PGAgent(
            env="CartPole-v0", config={
                "num_workers": 0,
                "sample_batch_size": 50,
                "train_batch_size": 50,
                "callbacks": {
                    "on_episode_start": lambda x: counts.update({"start": 1}),
                    "on_episode_step": lambda x: counts.update({"step": 1}),
                    "on_episode_end": lambda x: counts.update({"end": 1}),
                    "on_sample_end": lambda x: counts.update({"sample": 1}),
                },
            })
        pg.train()
        pg.train()
        pg.train()
        pg.train()
        self.assertEqual(counts["sample"], 4)
        self.assertGreater(counts["start"], 0)
        self.assertGreater(counts["end"], 0)
        self.assertGreater(counts["step"], 200)
        self.assertLess(counts["step"], 400)

    def testQueryEvaluators(self):
        register_env("test", lambda _: gym.make("CartPole-v0"))
        pg = PGAgent(
            env="test",
            config={
                "num_workers": 2,
                "sample_batch_size": 5,
                "num_envs_per_worker": 2,
            })
        results = pg.optimizer.foreach_evaluator(
            lambda ev: ev.sample_batch_size)
        results2 = pg.optimizer.foreach_evaluator_with_index(
            lambda ev, i: (i, ev.sample_batch_size))
        results3 = pg.optimizer.foreach_evaluator(
            lambda ev: ev.foreach_env(lambda env: 1))
        self.assertEqual(results, [10, 10, 10])
        self.assertEqual(results2, [(0, 10), (1, 10), (2, 10)])
        self.assertEqual(results3, [[1, 1], [1, 1], [1, 1]])

    def testRewardClipping(self):
        # clipping on
        ev = PolicyEvaluator(
            env_creator=lambda _: MockEnv2(episode_length=10),
            policy_graph=MockPolicyGraph,
            clip_rewards=True,
            batch_mode="complete_episodes")
        self.assertEqual(max(ev.sample()["rewards"]), 1)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episode_reward_mean"], 1000)

        # clipping off
        ev2 = PolicyEvaluator(
            env_creator=lambda _: MockEnv2(episode_length=10),
            policy_graph=MockPolicyGraph,
            clip_rewards=False,
            batch_mode="complete_episodes")
        self.assertEqual(max(ev2.sample()["rewards"]), 100)
        result2 = collect_metrics(ev2, [])
        self.assertEqual(result2["episode_reward_mean"], 1000)

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
        self.assertEqual(result["episodes_this_iter"], 20)
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

    def testAutoVectorization(self):
        ev = PolicyEvaluator(
            env_creator=lambda cfg: MockEnv(episode_length=20, config=cfg),
            policy_graph=MockPolicyGraph,
            batch_mode="truncate_episodes",
            batch_steps=2,
            num_envs=8)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 16)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_this_iter"], 0)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 16)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_this_iter"], 8)
        indices = []
        for env in ev.async_env.vector_env.envs:
            self.assertEqual(env.unwrapped.config.worker_index, 0)
            indices.append(env.unwrapped.config.vector_index)
        self.assertEqual(indices, [0, 1, 2, 3, 4, 5, 6, 7])

    def testBatchesLargerWhenVectorized(self):
        ev = PolicyEvaluator(
            env_creator=lambda _: MockEnv(episode_length=8),
            policy_graph=MockPolicyGraph,
            batch_mode="truncate_episodes",
            batch_steps=4,
            num_envs=4)
        batch = ev.sample()
        self.assertEqual(batch.count, 16)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_this_iter"], 0)
        batch = ev.sample()
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_this_iter"], 4)

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
        self.assertEqual(result["episodes_this_iter"], 0)
        for _ in range(8):
            batch = ev.sample()
            self.assertEqual(batch.count, 10)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episodes_this_iter"], 8)

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


if __name__ == "__main__":
    ray.init(num_cpus=5)
    unittest.main(verbosity=2)
