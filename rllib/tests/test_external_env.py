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
from ray.rllib.evaluation.tests.test_rollout_worker import (BadPolicy,
                                                            MockPolicy)
from ray.rllib.examples.env.mock_env import MockEnv
from ray.rllib.utils.test_utils import framework_iterator
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
    @classmethod
    def setUpClass(cls) -> None:
        ray.init(ignore_reinit_error=True)

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_external_env_complete_episodes(self):
        ev = RolloutWorker(
            env_creator=lambda _: SimpleServing(MockEnv(25)),
            policy_spec=MockPolicy,
            rollout_fragment_length=40,
            batch_mode="complete_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 50)

    def test_external_env_truncate_episodes(self):
        ev = RolloutWorker(
            env_creator=lambda _: SimpleServing(MockEnv(25)),
            policy_spec=MockPolicy,
            rollout_fragment_length=40,
            batch_mode="truncate_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 40)

    def test_external_env_off_policy(self):
        ev = RolloutWorker(
            env_creator=lambda _: SimpleOffPolicyServing(MockEnv(25), 42),
            policy_spec=MockPolicy,
            rollout_fragment_length=40,
            batch_mode="complete_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 50)
            self.assertEqual(batch["actions"][0], 42)
            self.assertEqual(batch["actions"][-1], 42)

    def test_external_env_bad_actions(self):
        ev = RolloutWorker(
            env_creator=lambda _: SimpleServing(MockEnv(25)),
            policy_spec=BadPolicy,
            sample_async=True,
            rollout_fragment_length=40,
            batch_mode="truncate_episodes")
        self.assertRaises(Exception, lambda: ev.sample())

    def test_train_cartpole_off_policy(self):
        register_env(
            "test3", lambda _: PartOffPolicyServing(
                gym.make("CartPole-v0"), off_pol_frac=0.2))
        config = {
            "num_workers": 0,
            "exploration_config": {
                "epsilon_timesteps": 100
            },
        }
        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            dqn = DQNTrainer(env="test3", config=config)
            reached = False
            for i in range(50):
                result = dqn.train()
                print("Iteration {}, reward {}, timesteps {}".format(
                    i, result["episode_reward_mean"],
                    result["timesteps_total"]))
                if result["episode_reward_mean"] >= 80:
                    reached = True
                    break
            if not reached:
                raise Exception("failed to improve reward")

    def test_train_cartpole(self):
        register_env("test", lambda _: SimpleServing(gym.make("CartPole-v0")))
        config = {"num_workers": 0}
        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            pg = PGTrainer(env="test", config=config)
            reached = False
            for i in range(80):
                result = pg.train()
                print("Iteration {}, reward {}, timesteps {}".format(
                    i, result["episode_reward_mean"],
                    result["timesteps_total"]))
                if result["episode_reward_mean"] >= 80:
                    reached = True
                    break
            if not reached:
                raise Exception("failed to improve reward")

    def test_train_cartpole_multi(self):
        register_env("test2",
                     lambda _: MultiServing(lambda: gym.make("CartPole-v0")))
        config = {"num_workers": 0}
        for _ in framework_iterator(config, frameworks=("tf", "torch")):
            pg = PGTrainer(env="test2", config=config)
            reached = False
            for i in range(80):
                result = pg.train()
                print("Iteration {}, reward {}, timesteps {}".format(
                    i, result["episode_reward_mean"],
                    result["timesteps_total"]))
                if result["episode_reward_mean"] >= 80:
                    reached = True
                    break
            if not reached:
                raise Exception("failed to improve reward")

    def test_external_env_horizon_not_supported(self):
        ev = RolloutWorker(
            env_creator=lambda _: SimpleServing(MockEnv(25)),
            policy_spec=MockPolicy,
            episode_horizon=20,
            rollout_fragment_length=10,
            batch_mode="complete_episodes")
        self.assertRaises(ValueError, lambda: ev.sample())


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
