from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import ray
from ray.rllib.utils.common_policy_evaluator import CommonPolicyEvaluator
from ray.rllib.utils.serving_env import ServingEnv
from ray.rllib.test.test_common_policy_evaluator import MockPolicyGraph, \
    MockEnv


class MyServingEnv(ServingEnv):
    def __init__(self, env):
        ServingEnv.__init__(self, env.action_space, env.observation_space)
        self.env = env

    def run(self):
        self.start_episode()
        obs = self.env.reset()
        while True:
            action = self.get_action(obs)
            obs, reward, done, info = self.env.step(action)
            self.log_returns(reward, info=info)
            if done:
                self.end_episode(obs)
                obs = self.env.reset()
                self.start_episode()


class TestServingEnv(unittest.TestCase):
    def testServingEnvCompleteEpisodes(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: MyServingEnv(MockEnv(25)),
            policy_graph=MockPolicyGraph,
            batch_steps=40,
            truncate_episodes=False)
        for _ in range(10):
            batch = ev.sample()
            self.assertEqual(batch.count, 50)

    def testServingEnvTruncateEpisodes(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: MyServingEnv(MockEnv(25)),
            policy_graph=MockPolicyGraph,
            batch_steps=40,
            truncate_episodes=True)
        for _ in range(10):
            batch = ev.sample()
            self.assertEqual(batch.count, 40)

    def testServingEnvHorizonNotSupported(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: MyServingEnv(MockEnv(25)),
            policy_graph=MockPolicyGraph,
            episode_horizon=20,
            batch_steps=10,
            truncate_episodes=False)
        ev.sample()
        self.assertRaises(Exception, lambda: ev.sample())


if __name__ == '__main__':
    ray.init()
    unittest.main(verbosity=2)
