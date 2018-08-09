from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import gym
import random
import unittest

import ray
from ray.rllib.agents.pg import PGAgent
from ray.rllib.agents.pg.pg_policy_graph import PGPolicyGraph
from ray.rllib.agents.dqn.dqn_policy_graph import DQNPolicyGraph
from ray.rllib.optimizers import SyncSamplesOptimizer, \
    SyncReplayOptimizer, AsyncGradientsOptimizer
from ray.rllib.test.test_policy_evaluator import MockEnv, MockEnv2, \
    MockPolicyGraph
from ray.rllib.evaluation.policy_evaluator import PolicyEvaluator
from ray.rllib.evaluation.metrics import collect_metrics
from ray.rllib.env.async_vector_env import _MultiAgentEnvToAsync
from ray.rllib.env.multi_agent_env import MultiAgentEnv
from ray.tune.registry import register_env


class BasicMultiAgent(MultiAgentEnv):
    """Env of N independent agents, each of which exits after 25 steps."""

    def __init__(self, num):
        self.agents = [MockEnv(25) for _ in range(num)]
        self.dones = set()
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)

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
    """Env of N independent agents, each of which exits after 5 steps.

    On each step() of the env, only one agent takes an action."""

    def __init__(self, num, increment_obs=False):
        if increment_obs:
            # Observations are 0, 1, 2, 3... etc. as time advances
            self.agents = [MockEnv2(5) for _ in range(num)]
        else:
            # Observations are all zeros
            self.agents = [MockEnv(5) for _ in range(num)]
        self.dones = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_done = {}
        self.last_info = {}
        self.i = 0
        self.num = num
        self.observation_space = gym.spaces.Discrete(2)
        self.action_space = gym.spaces.Discrete(2)

    def reset(self):
        self.dones = set()
        self.last_obs = {}
        self.last_rew = {}
        self.last_done = {}
        self.last_info = {}
        self.i = 0
        for i, a in enumerate(self.agents):
            self.last_obs[i] = a.reset()
            self.last_rew[i] = None
            self.last_done[i] = False
            self.last_info[i] = {}
        obs_dict = {self.i: self.last_obs[self.i]}
        self.i = (self.i + 1) % self.num
        return obs_dict

    def step(self, action_dict):
        assert len(self.dones) != len(self.agents)
        for i, action in action_dict.items():
            (self.last_obs[i], self.last_rew[i], self.last_done[i],
             self.last_info[i]) = self.agents[i].step(action)
        obs = {self.i: self.last_obs[self.i]}
        rew = {self.i: self.last_rew[self.i]}
        done = {self.i: self.last_done[self.i]}
        info = {self.i: self.last_info[self.i]}
        if done[self.i]:
            rew[self.i] = 0
            self.dones.add(self.i)
        self.i = (self.i + 1) % self.num
        done["__all__"] = len(self.dones) == len(self.agents)
        return obs, rew, done, info


class MultiCartpole(MultiAgentEnv):
    def __init__(self, num):
        self.agents = [gym.make("CartPole-v0") for _ in range(num)]
        self.dones = set()
        self.observation_space = self.agents[0].observation_space
        self.action_space = self.agents[0].action_space

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


class TestMultiAgentEnv(unittest.TestCase):
    def testBasicMock(self):
        env = BasicMultiAgent(4)
        obs = env.reset()
        self.assertEqual(obs, {0: 0, 1: 0, 2: 0, 3: 0})
        for _ in range(24):
            obs, rew, done, info = env.step({0: 0, 1: 0, 2: 0, 3: 0})
            self.assertEqual(obs, {0: 0, 1: 0, 2: 0, 3: 0})
            self.assertEqual(rew, {0: 1, 1: 1, 2: 1, 3: 1})
            self.assertEqual(done, {
                0: False,
                1: False,
                2: False,
                3: False,
                "__all__": False
            })
        obs, rew, done, info = env.step({0: 0, 1: 0, 2: 0, 3: 0})
        self.assertEqual(done, {
            0: True,
            1: True,
            2: True,
            3: True,
            "__all__": True
        })

    def testRoundRobinMock(self):
        env = RoundRobinMultiAgent(2)
        obs = env.reset()
        self.assertEqual(obs, {0: 0})
        for _ in range(5):
            obs, rew, done, info = env.step({0: 0})
            self.assertEqual(obs, {1: 0})
            self.assertEqual(done["__all__"], False)
            obs, rew, done, info = env.step({1: 0})
            self.assertEqual(obs, {0: 0})
            self.assertEqual(done["__all__"], False)
        obs, rew, done, info = env.step({0: 0})
        self.assertEqual(done["__all__"], True)

    def testVectorizeBasic(self):
        env = _MultiAgentEnvToAsync(lambda v: BasicMultiAgent(2), [], 2)
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        self.assertEqual(rew, {0: {0: None, 1: None}, 1: {0: None, 1: None}})
        self.assertEqual(
            dones, {
                0: {
                    0: False,
                    1: False,
                    "__all__": False
                },
                1: {
                    0: False,
                    1: False,
                    "__all__": False
                }
            })
        for _ in range(24):
            env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
            obs, rew, dones, _, _ = env.poll()
            self.assertEqual(obs, {0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
            self.assertEqual(rew, {0: {0: 1, 1: 1}, 1: {0: 1, 1: 1}})
            self.assertEqual(
                dones, {
                    0: {
                        0: False,
                        1: False,
                        "__all__": False
                    },
                    1: {
                        0: False,
                        1: False,
                        "__all__": False
                    }
                })
        env.send_actions({0: {0: 0, 1: 0}, 1: {0: 0, 1: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(
            dones, {
                0: {
                    0: True,
                    1: True,
                    "__all__": True
                },
                1: {
                    0: True,
                    1: True,
                    "__all__": True
                }
            })

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
            dones, {
                0: {
                    0: False,
                    1: False,
                    "__all__": False
                },
                1: {
                    0: False,
                    1: False,
                    "__all__": False
                }
            })

    def testVectorizeRoundRobin(self):
        env = _MultiAgentEnvToAsync(lambda v: RoundRobinMultiAgent(2), [], 2)
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0}, 1: {0: 0}})
        self.assertEqual(rew, {0: {0: None}, 1: {0: None}})
        env.send_actions({0: {0: 0}, 1: {0: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {1: 0}, 1: {1: 0}})
        env.send_actions({0: {1: 0}, 1: {1: 0}})
        obs, rew, dones, _, _ = env.poll()
        self.assertEqual(obs, {0: {0: 0}, 1: {0: 0}})

    def testMultiAgentSample(self):
        act_space = gym.spaces.Discrete(2)
        obs_space = gym.spaces.Discrete(2)
        ev = PolicyEvaluator(
            env_creator=lambda _: BasicMultiAgent(5),
            policy_graph={
                "p0": (MockPolicyGraph, obs_space, act_space, {}),
                "p1": (MockPolicyGraph, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda agent_id: "p{}".format(agent_id % 2),
            batch_steps=50)
        batch = ev.sample()
        self.assertEqual(batch.count, 50)
        self.assertEqual(batch.policy_batches["p0"].count, 150)
        self.assertEqual(batch.policy_batches["p1"].count, 100)
        self.assertEqual(batch.policy_batches["p0"]["t"].tolist(),
                         list(range(25)) * 6)

    def testMultiAgentSampleWithHorizon(self):
        act_space = gym.spaces.Discrete(2)
        obs_space = gym.spaces.Discrete(2)
        ev = PolicyEvaluator(
            env_creator=lambda _: BasicMultiAgent(5),
            policy_graph={
                "p0": (MockPolicyGraph, obs_space, act_space, {}),
                "p1": (MockPolicyGraph, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda agent_id: "p{}".format(agent_id % 2),
            episode_horizon=10,  # test with episode horizon set
            batch_steps=50)
        batch = ev.sample()
        self.assertEqual(batch.count, 50)

    def testMultiAgentSampleRoundRobin(self):
        act_space = gym.spaces.Discrete(2)
        obs_space = gym.spaces.Discrete(2)
        ev = PolicyEvaluator(
            env_creator=lambda _: RoundRobinMultiAgent(5, increment_obs=True),
            policy_graph={
                "p0": (MockPolicyGraph, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda agent_id: "p0",
            batch_steps=50)
        batch = ev.sample()
        self.assertEqual(batch.count, 50)
        # since we round robin introduce agents into the env, some of the env
        # steps don't count as proper transitions
        self.assertEqual(batch.policy_batches["p0"].count, 42)
        self.assertEqual(batch.policy_batches["p0"]["obs"].tolist()[:10],
                         [0, 1, 2, 3, 4] * 2)
        self.assertEqual(batch.policy_batches["p0"]["new_obs"].tolist()[:10],
                         [1, 2, 3, 4, 5] * 2)
        self.assertEqual(batch.policy_batches["p0"]["rewards"].tolist()[:10],
                         [100, 100, 100, 100, 0] * 2)
        self.assertEqual(batch.policy_batches["p0"]["dones"].tolist()[:10],
                         [False, False, False, False, True] * 2)
        self.assertEqual(batch.policy_batches["p0"]["t"].tolist()[:10],
                         [4, 9, 14, 19, 24, 5, 10, 15, 20, 25])

    def testTrainMultiCartpoleSinglePolicy(self):
        n = 10
        register_env("multi_cartpole", lambda _: MultiCartpole(n))
        pg = PGAgent(env="multi_cartpole", config={"num_workers": 0})
        for i in range(100):
            result = pg.train()
            print("Iteration {}, reward {}, timesteps {}".format(
                i, result["episode_reward_mean"], result["timesteps_total"]))
            if result["episode_reward_mean"] >= 50 * n:
                return
        raise Exception("failed to improve reward")

    def testTrainMultiCartpoleMultiPolicy(self):
        n = 10
        register_env("multi_cartpole", lambda _: MultiCartpole(n))
        single_env = gym.make("CartPole-v0")

        def gen_policy():
            config = {
                "gamma": random.choice([0.5, 0.8, 0.9, 0.95, 0.99]),
                "n_step": random.choice([1, 2, 3, 4, 5]),
            }
            obs_space = single_env.observation_space
            act_space = single_env.action_space
            return (PGPolicyGraph, obs_space, act_space, config)

        pg = PGAgent(
            env="multi_cartpole",
            config={
                "num_workers": 0,
                "multiagent": {
                    "policy_graphs": {
                        "policy_1": gen_policy(),
                        "policy_2": gen_policy(),
                    },
                    "policy_mapping_fn": lambda agent_id: "policy_1",
                },
            })

        # Just check that it runs without crashing
        for i in range(10):
            result = pg.train()
            print("Iteration {}, reward {}, timesteps {}".format(
                i, result["episode_reward_mean"], result["timesteps_total"]))
        self.assertTrue(
            pg.compute_action([0, 0, 0, 0], policy_id="policy_1") in [0, 1])
        self.assertTrue(
            pg.compute_action([0, 0, 0, 0], policy_id="policy_2") in [0, 1])
        self.assertRaises(
            KeyError,
            lambda: pg.compute_action([0, 0, 0, 0], policy_id="policy_3"))

    def _testWithOptimizer(self, optimizer_cls):
        n = 3
        env = gym.make("CartPole-v0")
        act_space = env.action_space
        obs_space = env.observation_space
        dqn_config = {"gamma": 0.95, "n_step": 3}
        if optimizer_cls == SyncReplayOptimizer:
            # TODO: support replay with non-DQN graphs. Currently this can't
            # happen since the replay buffer doesn't encode extra fields like
            # "advantages" that PG uses.
            policies = {
                "p1": (DQNPolicyGraph, obs_space, act_space, dqn_config),
                "p2": (DQNPolicyGraph, obs_space, act_space, dqn_config),
            }
        else:
            policies = {
                "p1": (PGPolicyGraph, obs_space, act_space, {}),
                "p2": (DQNPolicyGraph, obs_space, act_space, dqn_config),
            }
        ev = PolicyEvaluator(
            env_creator=lambda _: MultiCartpole(n),
            policy_graph=policies,
            policy_mapping_fn=lambda agent_id: ["p1", "p2"][agent_id % 2],
            batch_steps=50)
        if optimizer_cls == AsyncGradientsOptimizer:

            def policy_mapper(agent_id):
                return ["p1", "p2"][agent_id % 2]

            remote_evs = [
                PolicyEvaluator.as_remote().remote(
                    env_creator=lambda _: MultiCartpole(n),
                    policy_graph=policies,
                    policy_mapping_fn=policy_mapper,
                    batch_steps=50)
            ]
        else:
            remote_evs = []
        optimizer = optimizer_cls(ev, remote_evs, {})
        for i in range(200):
            ev.foreach_policy(
                lambda p, _: p.set_epsilon(max(0.02, 1 - i * .02))
                if isinstance(p, DQNPolicyGraph) else None)
            optimizer.step()
            result = collect_metrics(ev, remote_evs)
            if i % 20 == 0:
                ev.foreach_policy(
                    lambda p, _: p.update_target()
                    if isinstance(p, DQNPolicyGraph) else None)
                print("Iter {}, rew {}".format(i,
                                               result["policy_reward_mean"]))
                print("Total reward", result["episode_reward_mean"])
            if result["episode_reward_mean"] >= 25 * n:
                return
        print(result)
        raise Exception("failed to improve reward")

    def testMultiAgentSyncOptimizer(self):
        self._testWithOptimizer(SyncSamplesOptimizer)

    def testMultiAgentAsyncGradientsOptimizer(self):
        self._testWithOptimizer(AsyncGradientsOptimizer)

    def testMultiAgentReplayOptimizer(self):
        self._testWithOptimizer(SyncReplayOptimizer)

    def testTrainMultiCartpoleManyPolicies(self):
        n = 20
        env = gym.make("CartPole-v0")
        act_space = env.action_space
        obs_space = env.observation_space
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
        optimizer = SyncSamplesOptimizer(ev, [], {})
        for i in range(100):
            optimizer.step()
            result = collect_metrics(ev)
            print("Iteration {}, rew {}".format(i,
                                                result["policy_reward_mean"]))
            print("Total reward", result["episode_reward_mean"])
            if result["episode_reward_mean"] >= 25 * n:
                return
        raise Exception("failed to improve reward")


if __name__ == '__main__':
    ray.init()
    unittest.main(verbosity=2)
