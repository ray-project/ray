from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import ray
from ray.rllib.agents.dqn import DQNAgent
from ray.rllib.agents.dqn.dqn_policy_graph import _adjust_nstep


class DQNTest(unittest.TestCase):
    def testNStep(self):
        obs = [1, 2, 3, 4, 5, 6, 7]
        actions = ["a", "b", "a", "a", "a", "b", "a"]
        rewards = [10.0, 0.0, 100.0, 100.0, 100.0, 100.0, 100.0]
        new_obs = [2, 3, 4, 5, 6, 7, 8]
        dones = [0, 0, 0, 0, 0, 0, 1]
        _adjust_nstep(3, 0.9, obs, actions, rewards, new_obs, dones)
        self.assertEqual(obs, [1, 2, 3, 4, 5, 6, 7])
        self.assertEqual(actions, ["a", "b", "a", "a", "a", "b", "a"])
        self.assertEqual(new_obs, [4, 5, 6, 7, 8, 8, 8])
        self.assertEqual(dones, [0, 0, 0, 0, 1, 1, 1])
        self.assertEqual(rewards,
                         [91.0, 171.0, 271.0, 271.0, 271.0, 190.0, 100.0])

    def testEvaluationOption(self):
        ray.init()
        agent = DQNAgent(env="CartPole-v0", config={"evaluation_interval": 2})
        r0 = agent.train()
        r1 = agent.train()
        r2 = agent.train()
        r3 = agent.train()
        r4 = agent.train()
        self.assertTrue("evaluation" in r0)
        self.assertTrue("episode_reward_mean" in r0["evaluation"])
        self.assertEqual(r0["evaluation"], r1["evaluation"])
        self.assertNotEqual(r1["evaluation"], r2["evaluation"])
        self.assertEqual(r2["evaluation"], r3["evaluation"])
        self.assertNotEqual(r3["evaluation"], r4["evaluation"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
