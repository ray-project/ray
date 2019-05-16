from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import numpy as np
import random
import unittest

import ray
from ray.rllib.agents.pg.pg_policy_graph import PGPolicyGraph
from ray.rllib.optimizers import SyncSamplesOptimizer
from ray.rllib.evaluation.policy_evaluator import PolicyEvaluator
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.tests.test_policy_evaluator import MockPolicyGraph
from ray.rllib.tests.test_external_env import make_simple_serving
from ray.rllib.tests.test_multi_agent_env import BasicMultiAgent, MultiCartpole
from ray.rllib.evaluation.metrics import collect_metrics

SimpleMultiServing = make_simple_serving(True, ExternalMultiAgentEnv)


class TestExternalMultiAgentEnv(unittest.TestCase):
    def testExternalMultiAgentEnvCompleteEpisodes(self):
        agents = 4
        ev = PolicyEvaluator(
            env_creator=lambda _: SimpleMultiServing(BasicMultiAgent(agents)),
            policy_graph=MockPolicyGraph,
            batch_steps=40,
            batch_mode="complete_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 40)
            self.assertEqual(len(np.unique(batch["agent_index"])), agents)

    def testExternalMultiAgentEnvTruncateEpisodes(self):
        agents = 4
        ev = PolicyEvaluator(
            env_creator=lambda _: SimpleMultiServing(BasicMultiAgent(agents)),
            policy_graph=MockPolicyGraph,
            batch_steps=40,
            batch_mode="truncate_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 160)
            self.assertEqual(len(np.unique(batch["agent_index"])), agents)

    def testExternalMultiAgentEnvSample(self):
        agents = 2
        act_space = gym.spaces.Discrete(2)
        obs_space = gym.spaces.Discrete(2)
        ev = PolicyEvaluator(
            env_creator=lambda _: SimpleMultiServing(BasicMultiAgent(agents)),
            policy_graph={
                "p0": (MockPolicyGraph, obs_space, act_space, {}),
                "p1": (MockPolicyGraph, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda agent_id: "p{}".format(agent_id % 2),
            batch_steps=50)
        batch = ev.sample()
        self.assertEqual(batch.count, 50)

    def testTrainExternalMultiCartpoleManyPolicies(self):
        n = 20
        single_env = gym.make("CartPole-v0")
        act_space = single_env.action_space
        obs_space = single_env.observation_space
        policies = {}
        for i in range(20):
            policies["pg_{}".format(i)] = (PGPolicyGraph, obs_space, act_space,
                                           {})
        policy_ids = list(policies.keys())
        ev = PolicyEvaluator(
            env_creator=lambda _: MultiCartpole(n),
            policy_graph=policies,
            policy_mapping_fn=lambda agent_id: random.choice(policy_ids),
            batch_steps=100)
        optimizer = SyncSamplesOptimizer(ev, [])
        for i in range(100):
            optimizer.step()
            result = collect_metrics(ev)
            print("Iteration {}, rew {}".format(i,
                                                result["policy_reward_mean"]))
            print("Total reward", result["episode_reward_mean"])
            if result["episode_reward_mean"] >= 25 * n:
                return
        raise Exception("failed to improve reward")


if __name__ == "__main__":
    ray.init()
    unittest.main(verbosity=2)
