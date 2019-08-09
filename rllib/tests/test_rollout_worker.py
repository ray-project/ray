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
from ray.rllib.agents.pg import PGTrainer
from ray.rllib.agents.a3c import A2CTrainer
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.policy.policy import Policy
from ray.rllib.evaluation.postprocessing import compute_advantages
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.env.vector_env import VectorEnv
from ray.tune.registry import register_env


class MockPolicy(Policy):
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


class BadPolicy(Policy):
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


class TestRolloutWorker(unittest.TestCase):
    def testBasic(self):
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"), policy=MockPolicy)
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

    def testBatchIds(self):
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"), policy=MockPolicy)
        batch1 = ev.sample()
        batch2 = ev.sample()
        self.assertEqual(len(set(batch1["unroll_id"])), 1)
        self.assertEqual(len(set(batch2["unroll_id"])), 1)
        self.assertEqual(
            len(set(SampleBatch.concat(batch1, batch2)["unroll_id"])), 2)

    def testGlobalVarsUpdate(self):
        agent = A2CTrainer(
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
        pg = PGTrainer(env="fail", config={"num_workers": 1})
        self.assertRaises(Exception, lambda: pg.train())

    def testCallbacks(self):
        counts = Counter()
        pg = PGTrainer(
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
        pg = PGTrainer(
            env="test",
            config={
                "num_workers": 2,
                "sample_batch_size": 5,
                "num_envs_per_worker": 2,
            })
        results = pg.workers.foreach_worker(lambda ev: ev.sample_batch_size)
        results2 = pg.workers.foreach_worker_with_index(
            lambda ev, i: (i, ev.sample_batch_size))
        results3 = pg.workers.foreach_worker(
            lambda ev: ev.foreach_env(lambda env: 1))
        self.assertEqual(results, [10, 10, 10])
        self.assertEqual(results2, [(0, 10), (1, 10), (2, 10)])
        self.assertEqual(results3, [[1, 1], [1, 1], [1, 1]])

    def testRewardClipping(self):
        # clipping on
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv2(episode_length=10),
            policy=MockPolicy,
            clip_rewards=True,
            batch_mode="complete_episodes")
        self.assertEqual(max(ev.sample()["rewards"]), 1)
        result = collect_metrics(ev, [])
        self.assertEqual(result["episode_reward_mean"], 1000)

        # clipping off
        ev2 = RolloutWorker(
            env_creator=lambda _: MockEnv2(episode_length=10),
            policy=MockPolicy,
            clip_rewards=False,
            batch_mode="complete_episodes")
        self.assertEqual(max(ev2.sample()["rewards"]), 100)
        result2 = collect_metrics(ev2, [])
        self.assertEqual(result2["episode_reward_mean"], 1000)

    def testHardHorizon(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(episode_length=10),
            policy=MockPolicy,
            batch_mode="complete_episodes",
            batch_steps=10,
            episode_horizon=4,
            soft_horizon=False)
        samples = ev.sample()
        # three logical episodes
        self.assertEqual(len(set(samples["eps_id"])), 3)
        # 3 done values
        self.assertEqual(sum(samples["dones"]), 3)

    def testSoftHorizon(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(episode_length=10),
            policy=MockPolicy,
            batch_mode="complete_episodes",
            batch_steps=10,
            episode_horizon=4,
            soft_horizon=True)
        samples = ev.sample()
        # three logical episodes
        self.assertEqual(len(set(samples["eps_id"])), 3)
        # only 1 hard done value
        self.assertEqual(sum(samples["dones"]), 1)

    def testMetrics(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(episode_length=10),
            policy=MockPolicy,
            batch_mode="complete_episodes")
        remote_ev = RolloutWorker.as_remote().remote(
            env_creator=lambda _: MockEnv(episode_length=10),
            policy=MockPolicy,
            batch_mode="complete_episodes")
        ev.sample()
        ray.get(remote_ev.sample.remote())
        result = collect_metrics(ev, [remote_ev])
        self.assertEqual(result["episodes_this_iter"], 20)
        self.assertEqual(result["episode_reward_mean"], 10)

    def testAsync(self):
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"),
            sample_async=True,
            policy=MockPolicy)
        batch = ev.sample()
        for key in ["obs", "actions", "rewards", "dones", "advantages"]:
            self.assertIn(key, batch)
        self.assertGreater(batch["advantages"][0], 1)

    def testAutoVectorization(self):
        ev = RolloutWorker(
            env_creator=lambda cfg: MockEnv(episode_length=20, config=cfg),
            policy=MockPolicy,
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
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(episode_length=8),
            policy=MockPolicy,
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
        ev = RolloutWorker(
            env_creator=lambda _: MockVectorEnv(episode_length=20, num_envs=8),
            policy=MockPolicy,
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
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(10),
            policy=MockPolicy,
            batch_steps=15,
            batch_mode="truncate_episodes")
        batch = ev.sample()
        self.assertEqual(batch.count, 15)

    def testCompleteEpisodes(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(10),
            policy=MockPolicy,
            batch_steps=5,
            batch_mode="complete_episodes")
        batch = ev.sample()
        self.assertEqual(batch.count, 10)

    def testCompleteEpisodesPacking(self):
        ev = RolloutWorker(
            env_creator=lambda _: MockEnv(10),
            policy=MockPolicy,
            batch_steps=15,
            batch_mode="complete_episodes")
        batch = ev.sample()
        self.assertEqual(batch.count, 20)
        self.assertEqual(
            batch["t"].tolist(),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6, 7, 8, 9])

    def testFilterSync(self):
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy=MockPolicy,
            sample_async=True,
            observation_filter="ConcurrentMeanStdFilter")
        time.sleep(2)
        ev.sample()
        filters = ev.get_filters(flush_after=True)
        obs_f = filters[DEFAULT_POLICY_ID]
        self.assertNotEqual(obs_f.rs.n, 0)
        self.assertNotEqual(obs_f.buffer.n, 0)

    def testGetFilters(self):
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy=MockPolicy,
            sample_async=True,
            observation_filter="ConcurrentMeanStdFilter")
        self.sample_and_flush(ev)
        filters = ev.get_filters(flush_after=False)
        time.sleep(2)
        filters2 = ev.get_filters(flush_after=False)
        obs_f = filters[DEFAULT_POLICY_ID]
        obs_f2 = filters2[DEFAULT_POLICY_ID]
        self.assertGreaterEqual(obs_f2.rs.n, obs_f.rs.n)
        self.assertGreaterEqual(obs_f2.buffer.n, obs_f.buffer.n)

    def testSyncFilter(self):
        ev = RolloutWorker(
            env_creator=lambda _: gym.make("CartPole-v0"),
            policy=MockPolicy,
            sample_async=True,
            observation_filter="ConcurrentMeanStdFilter")
        obs_f = self.sample_and_flush(ev)

        # Current State
        filters = ev.get_filters(flush_after=False)
        obs_f = filters[DEFAULT_POLICY_ID]

        self.assertLessEqual(obs_f.buffer.n, 20)

        new_obsf = obs_f.copy()
        new_obsf.rs._n = 100
        ev.sync_filters({DEFAULT_POLICY_ID: new_obsf})
        filters = ev.get_filters(flush_after=False)
        obs_f = filters[DEFAULT_POLICY_ID]
        self.assertGreaterEqual(obs_f.rs.n, 100)
        self.assertLessEqual(obs_f.buffer.n, 20)

    def sample_and_flush(self, ev):
        time.sleep(2)
        ev.sample()
        filters = ev.get_filters(flush_after=True)
        obs_f = filters[DEFAULT_POLICY_ID]
        self.assertNotEqual(obs_f.rs.n, 0)
        self.assertNotEqual(obs_f.buffer.n, 0)
        return obs_f


if __name__ == "__main__":
    ray.init(num_cpus=5)
    unittest.main(verbosity=2)
