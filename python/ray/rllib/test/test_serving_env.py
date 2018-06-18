from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import numpy as np
import random
import unittest
import uuid

import ray
from ray.rllib.dqn import DQNAgent
from ray.rllib.pg import PGAgent
from ray.rllib.utils.common_policy_evaluator import CommonPolicyEvaluator
from ray.rllib.utils.serving_env import ServingEnv
from ray.rllib.test.test_common_policy_evaluator import BadPolicyGraph, \
    MockPolicyGraph, MockEnv
from ray.tune.registry import register_env


class SimpleServing(ServingEnv):
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


class PartOffPolicyServing(ServingEnv):
    def __init__(self, env, off_pol_frac):
        ServingEnv.__init__(self, env.action_space, env.observation_space)
        self.env = env
        self.off_pol_frac = off_pol_frac

    def run(self):
        self.start_episode()
        obs = self.env.reset()
        while True:
            if random.random() < self.off_pol_frac:
                action = self.env.action_space.sample()
                self.log_action(obs, action)
            else:
                action = self.get_action(obs)
            obs, reward, done, info = self.env.step(action)
            self.log_returns(reward, info=info)
            if done:
                self.end_episode(obs)
                obs = self.env.reset()
                self.start_episode()


class SimpleOffPolicyServing(ServingEnv):
    def __init__(self, env):
        ServingEnv.__init__(self, env.action_space, env.observation_space)
        self.env = env

    def run(self):
        self.start_episode()
        obs = self.env.reset()
        while True:
            # Take random actions
            action = self.env.action_space.sample()
            self.log_action(obs, action)
            obs, reward, done, info = self.env.step(action)
            self.log_returns(reward, info=info)
            if done:
                self.end_episode(obs)
                obs = self.env.reset()
                self.start_episode()


class MultiServing(ServingEnv):
    def __init__(self, env_creator):
        self.env_creator = env_creator
        self.env = env_creator()
        ServingEnv.__init__(
            self, self.env.action_space, self.env.observation_space)

    def run(self):
        envs = [self.env_creator() for _ in range(5)]
        cur_obs = {}
        eids = {}
        while True:
            active = np.random.choice(range(5), 2, replace=False)
            for i in active:
                if i not in cur_obs:
                    eids[i] = uuid.uuid4().hex
                    self.start_episode(episode_id=eids[i])
                    cur_obs[i] = envs[i].reset()
            actions = [
                self.get_action(
                    cur_obs[i], episode_id=eids[i]) for i in active]
            for i, action in zip(active, actions):
                obs, reward, done, _ = envs[i].step(action)
                cur_obs[i] = obs
                self.log_returns(reward, episode_id=eids[i])
                if done:
                    self.end_episode(obs, episode_id=eids[i])
                    del cur_obs[i]


class TestServingEnv(unittest.TestCase):
    def testServingEnvCompleteEpisodes(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: SimpleServing(MockEnv(25)),
            policy_graph=MockPolicyGraph,
            batch_steps=40,
            batch_mode="complete_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 50)

    def testServingEnvTruncateEpisodes(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: SimpleServing(MockEnv(25)),
            policy_graph=MockPolicyGraph,
            batch_steps=40,
            batch_mode="truncate_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 40)

    def testServingEnvOffPolicy(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: SimpleOffPolicyServing(MockEnv(25)),
            policy_graph=MockPolicyGraph,
            batch_steps=40,
            batch_mode="complete_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 50)

    def testServingEnvBadActions(self):
        ev = CommonPolicyEvaluator(
            env_creator=lambda _: SimpleServing(MockEnv(25)),
            policy_graph=BadPolicyGraph,
            sample_async=True,
            batch_steps=40,
            batch_mode="truncate_episodes")
        self.assertRaises(Exception, lambda: ev.sample())

    def testTrainCartpoleOffPolicy(self):
        register_env(
            "test3", lambda _: PartOffPolicyServing(
                gym.make("CartPole-v0"), off_pol_frac=0.2))
        dqn = DQNAgent(env="test3", config={"exploration_fraction": 0.001})
        for i in range(100):
            result = dqn.train()
            print("Iteration {}, reward {}, timesteps {}".format(
                i, result.episode_reward_mean, result.timesteps_total))
            if result.episode_reward_mean >= 100:
                return
        raise Exception("failed to improve reward")

    def testTrainCartpole(self):
        register_env(
            "test", lambda _: SimpleServing(gym.make("CartPole-v0")))
        pg = PGAgent(env="test", config={"num_workers": 0})
        for i in range(100):
            result = pg.train()
            print("Iteration {}, reward {}, timesteps {}".format(
                i, result.episode_reward_mean, result.timesteps_total))
            if result.episode_reward_mean >= 100:
                return
        raise Exception("failed to improve reward")

    def testTrainCartpoleMulti(self):
        register_env(
            "test2", lambda _: MultiServing(lambda: gym.make("CartPole-v0")))
        pg = PGAgent(env="test2", config={"num_workers": 0})
        for i in range(100):
            result = pg.train()
            print("Iteration {}, reward {}, timesteps {}".format(
                i, result.episode_reward_mean, result.timesteps_total))
            if result.episode_reward_mean >= 100:
                return
        raise Exception("failed to improve reward")


if __name__ == '__main__':
    ray.init()
    unittest.main(verbosity=2)
