from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import numpy as np
import random
import unittest
import uuid

import ray
from ray.rllib.agents.dqn import DQNTrainer
from ray.rllib.agents.pg import PGTrainer
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.env.external_env import ExternalEnv
from ray.rllib.tests.test_rollout_worker import (BadPolicy, MockPolicy,
                                                 MockEnv)
from ray.tune.registry import register_env


def make_simple_serving(multiagent, superclass):
    class SimpleServing(superclass):
        def __init__(self, env):
            superclass.__init__(self, env.action_space, env.observation_space)
            self.env = env

        def run(self):
            eid = self.start_episode()
            obs = self.env.reset()
            while True:
                action = self.get_action(eid, obs)
                obs, reward, done, info = self.env.step(action)
                if multiagent:
                    self.log_returns(eid, reward)
                else:
                    self.log_returns(eid, reward, info=info)
                if done:
                    self.end_episode(eid, obs)
                    obs = self.env.reset()
                    eid = self.start_episode()

    return SimpleServing


# generate & register SimpleServing class
SimpleServing = make_simple_serving(False, ExternalEnv)


class PartOffPolicyServing(ExternalEnv):
    def __init__(self, env, off_pol_frac):
        ExternalEnv.__init__(self, env.action_space, env.observation_space)
        self.env = env
        self.off_pol_frac = off_pol_frac

    def run(self):
        eid = self.start_episode()
        obs = self.env.reset()
        while True:
            if random.random() < self.off_pol_frac:
                action = self.env.action_space.sample()
                self.log_action(eid, obs, action)
            else:
                action = self.get_action(eid, obs)
            obs, reward, done, info = self.env.step(action)
            self.log_returns(eid, reward, info=info)
            if done:
                self.end_episode(eid, obs)
                obs = self.env.reset()
                eid = self.start_episode()


class SimpleOffPolicyServing(ExternalEnv):
    def __init__(self, env, fixed_action):
        ExternalEnv.__init__(self, env.action_space, env.observation_space)
        self.env = env
        self.fixed_action = fixed_action

    def run(self):
        eid = self.start_episode()
        obs = self.env.reset()
        while True:
            action = self.fixed_action
            self.log_action(eid, obs, action)
            obs, reward, done, info = self.env.step(action)
            self.log_returns(eid, reward, info=info)
            if done:
                self.end_episode(eid, obs)
                obs = self.env.reset()
                eid = self.start_episode()


class MultiServing(ExternalEnv):
    def __init__(self, env_creator):
        self.env_creator = env_creator
        self.env = env_creator()
        ExternalEnv.__init__(self, self.env.action_space,
                             self.env.observation_space)

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
            actions = [self.get_action(eids[i], cur_obs[i]) for i in active]
            for i, action in zip(active, actions):
                obs, reward, done, _ = envs[i].step(action)
                cur_obs[i] = obs
                self.log_returns(eids[i], reward)
                if done:
                    self.end_episode(eids[i], obs)
                    del cur_obs[i]


class TestExternalEnv(unittest.TestCase):
    def testExternalEnvCompleteEpisodes(self):
        ev = RolloutWorker(
            env_creator=lambda _: SimpleServing(MockEnv(25)),
            policy=MockPolicy,
            batch_steps=40,
            batch_mode="complete_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 50)

    def testExternalEnvTruncateEpisodes(self):
        ev = RolloutWorker(
            env_creator=lambda _: SimpleServing(MockEnv(25)),
            policy=MockPolicy,
            batch_steps=40,
            batch_mode="truncate_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 40)

    def testExternalEnvOffPolicy(self):
        ev = RolloutWorker(
            env_creator=lambda _: SimpleOffPolicyServing(MockEnv(25), 42),
            policy=MockPolicy,
            batch_steps=40,
            batch_mode="complete_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 50)
            self.assertEqual(batch["actions"][0], 42)
            self.assertEqual(batch["actions"][-1], 42)

    def testExternalEnvBadActions(self):
        ev = RolloutWorker(
            env_creator=lambda _: SimpleServing(MockEnv(25)),
            policy=BadPolicy,
            sample_async=True,
            batch_steps=40,
            batch_mode="truncate_episodes")
        self.assertRaises(Exception, lambda: ev.sample())

    def testTrainCartpoleOffPolicy(self):
        register_env(
            "test3", lambda _: PartOffPolicyServing(
                gym.make("CartPole-v0"), off_pol_frac=0.2))
        dqn = DQNTrainer(env="test3", config={"exploration_fraction": 0.001})
        for i in range(100):
            result = dqn.train()
            print("Iteration {}, reward {}, timesteps {}".format(
                i, result["episode_reward_mean"], result["timesteps_total"]))
            if result["episode_reward_mean"] >= 100:
                return
        raise Exception("failed to improve reward")

    def testTrainCartpole(self):
        register_env("test", lambda _: SimpleServing(gym.make("CartPole-v0")))
        pg = PGTrainer(env="test", config={"num_workers": 0})
        for i in range(100):
            result = pg.train()
            print("Iteration {}, reward {}, timesteps {}".format(
                i, result["episode_reward_mean"], result["timesteps_total"]))
            if result["episode_reward_mean"] >= 100:
                return
        raise Exception("failed to improve reward")

    def testTrainCartpoleMulti(self):
        register_env("test2",
                     lambda _: MultiServing(lambda: gym.make("CartPole-v0")))
        pg = PGTrainer(env="test2", config={"num_workers": 0})
        for i in range(100):
            result = pg.train()
            print("Iteration {}, reward {}, timesteps {}".format(
                i, result["episode_reward_mean"], result["timesteps_total"]))
            if result["episode_reward_mean"] >= 100:
                return
        raise Exception("failed to improve reward")

    def testExternalEnvHorizonNotSupported(self):
        ev = RolloutWorker(
            env_creator=lambda _: SimpleServing(MockEnv(25)),
            policy=MockPolicy,
            episode_horizon=20,
            batch_steps=10,
            batch_mode="complete_episodes")
        self.assertRaises(ValueError, lambda: ev.sample())


if __name__ == "__main__":
    ray.init()
    unittest.main(verbosity=2)
