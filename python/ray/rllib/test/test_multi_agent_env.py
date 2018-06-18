from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import ray
from ray.rllib.test.test_common_policy_evaluator import MockEnv
from ray.rllib.utils.multi_agent_env import MultiAgentEnv, \
    _MultiAgentEnvToAsync


class BasicMultiAgent(MultiAgentEnv):
    def __init__(self, num):
        self.agents = [MockEnv(25) for _ in range(num)]
        self.dones = set()

    def reset(self):
        self.dones = set()
        return {i: a.reset() for i, a in enumerate(self.agents)}

    def step(self, action_dict):
        obs, rew, done, info = {}, {}, {}, {}
        for i, action in action_dict.items():
            obs[i], rew[i], done[i], info[i] = self.agents[i].step(action)
            if done[i]:
                self.dones.add(i)
        done["__all__"] = len(self.dones) == len(self.agents)
        return obs, rew, done, info


class RoundRobinMultiAgent(MultiAgentEnv):
    def __init__(self, num):
        self.agents = [MockEnv(5) for _ in range(num)]
        self.dones = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_done = {}
        self.last_info = {}
        self.i = 0
        self.num = num

    def reset(self):
        self.dones = set()
        return {i: a.reset() for i, a in enumerate(self.agents)}

    def step(self, action_dict):
        assert len(self.dones) != len(self.agents)
        for i, action in action_dict.items():
            (self.last_obs[i], self.last_rew[i], self.last_done[i],
             self.last_info[i]) = self.agents[i].step(action)
            if self.last_done[i]:
                self.dones.add(i)
        obs = {self.i: self.last_obs[i]}
        rew = {self.i: self.last_rew[i]}
        done = {self.i: self.last_done[i]}
        info = {self.i: self.last_info[i]}
        self.i += 1
        self.i %= self.num
        done["__all__"] = len(self.dones) == len(self.agents)
        return obs, rew, done, info


class TestMultiAgentEnv(unittest.TestCase):
    def testBasicMock(self):
        env = BasicMultiAgent(4)
        obs = env.reset()
        self.assertEqual(obs, {0: 0, 1: 0, 2: 0, 3: 0})
        for _ in range(24):
            obs, rew, done, info = env.step({0: 0, 1: 0, 2: 0, 3: 0})
            self.assertEqual(obs, {0: 0, 1: 0, 2: 0, 3: 0})
            self.assertEqual(rew, {0: 1, 1: 1, 2: 1, 3: 1})
            self.assertEqual(
                done,
                {0: False, 1: False, 2: False, 3: False, "__all__": False})
        obs, rew, done, info = env.step({0: 0, 1: 0, 2: 0, 3: 0})
        self.assertEqual(
            done, {0: True, 1: True, 2: True, 3: True, "__all__": True})

    def testRoundRobinMock(self):
        env = RoundRobinMultiAgent(2)
        obs = env.reset()
        self.assertEqual(obs, {0: 0, 1: 0})
        obs, rew, done, info = env.step({0: 0, 1: 0})
        self.assertEqual(obs, {0: 0})
        for _ in range(4):
            obs, rew, done, info = env.step({0: 0})
            self.assertEqual(obs, {1: 0})
            self.assertEqual(done["__all__"], False)
            obs, rew, done, info = env.step({1: 0})
            self.assertEqual(obs, {0: 0})
        self.assertEqual(done["__all__"], True)

    def testVectorizeBasic(self):
        env = _MultiAgentEnvToAsync(lambda: BasicMultiAgent(2), [], 2)
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        self.assertEqual(rew, {0: {0: None, 1: None}, 1: {0: None, 1: None}})
        self.assertEqual(
            dones,
            {0: {0: False, 1: False, "__all__": False},
             1: {0: False, 1: False, "__all__": False}})
        for _ in range(24):
            env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
            obs, rew, dones, _, _ = env.poll()
            self.assertEqual(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
            self.assertEqual(rew, {0: {0: 1, 1: 1}, 1: {0: 1, 1: 1}})
            self.assertEqual(
                dones,
                {0: {0: False, 1: False, "__all__": False},
                 1: {0: False, 1: False, "__all__": False}})
        env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(
            dones,
            {0: {0: True, 1: True, "__all__": True},
             1: {0: True, 1: True, "__all__": True}})

        # Reset processing
        self.assertRaises(
            ValueError,
            lambda: env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}}))
        self.assertEqual(env.try_reset(0), {0: 0, 1: 0})
        self.assertEqual(env.try_reset(1), {0: 0, 1: 0})
        env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        self.assertEqual(rew, {0: {0: 1, 1: 1}, 1: {0: 1, 1: 1}})
        self.assertEqual(
            dones,
            {0: {0: False, 1: False, "__all__": False},
             1: {0: False, 1: False, "__all__": False}})

    def testVectorizeRoundRobin(self):
        env = _MultiAgentEnvToAsync(lambda: RoundRobinMultiAgent(2), [], 2)
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        self.assertEqual(rew, {0: {0: None, 1: None}, 1: {0: None, 1: None}})
        env.send_actions({0: {0: 0}, 1: {0: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0}, 1: {0: 0}})
        env.send_actions({0: {0: 0}, 1: {0: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {1: 0}, 1: {1: 0}})

if __name__ == '__main__':
    ray.init()
    unittest.main(verbosity=2)
