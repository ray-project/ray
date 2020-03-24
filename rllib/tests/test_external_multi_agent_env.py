import gym
import numpy as np
import random
import unittest

import ray
from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy
from ray.rllib.optimizers import SyncSamplesOptimizer
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.tests.test_rollout_worker import MockPolicy
from ray.rllib.tests.test_external_env import make_simple_serving
from ray.rllib.tests.test_multi_agent_env import BasicMultiAgent, MultiCartpole
from ray.rllib.evaluation.metrics import collect_metrics

SimpleMultiServing = make_simple_serving(True, ExternalMultiAgentEnv)


class TestExternalMultiAgentEnv(unittest.TestCase):
    def setUp(self) -> None:
        ray.init()

    def tearDown(self) -> None:
        ray.shutdown()

    def test_external_multi_agent_env_complete_episodes(self):
        agents = 4
        ev = RolloutWorker(
            env_creator=lambda _: SimpleMultiServing(BasicMultiAgent(agents)),
            policy=MockPolicy,
            rollout_fragment_length=40,
            batch_mode="complete_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 40)
            self.assertEqual(len(np.unique(batch["agent_index"])), agents)

    def test_external_multi_agent_env_truncate_episodes(self):
        agents = 4
        ev = RolloutWorker(
            env_creator=lambda _: SimpleMultiServing(BasicMultiAgent(agents)),
            policy=MockPolicy,
            rollout_fragment_length=40,
            batch_mode="truncate_episodes")
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 160)
            self.assertEqual(len(np.unique(batch["agent_index"])), agents)

    def test_external_multi_agent_env_sample(self):
        agents = 2
        act_space = gym.spaces.Discrete(2)
        obs_space = gym.spaces.Discrete(2)
        ev = RolloutWorker(
            env_creator=lambda _: SimpleMultiServing(BasicMultiAgent(agents)),
            policy={
                "p0": (MockPolicy, obs_space, act_space, {}),
                "p1": (MockPolicy, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda agent_id: "p{}".format(agent_id % 2),
            rollout_fragment_length=50)
        batch = ev.sample()
        self.assertEqual(batch.count, 50)

    def test_train_external_multi_cartpole_many_policies(self):
        n = 20
        single_env = gym.make("CartPole-v0")
        act_space = single_env.action_space
        obs_space = single_env.observation_space
        policies = {}
        for i in range(20):
            policies["pg_{}".format(i)] = (PGTFPolicy, obs_space, act_space,
                                           {})
        policy_ids = list(policies.keys())
        ev = RolloutWorker(
            env_creator=lambda _: MultiCartpole(n),
            policy=policies,
            policy_mapping_fn=lambda agent_id: random.choice(policy_ids),
            rollout_fragment_length=100)
        optimizer = SyncSamplesOptimizer(WorkerSet._from_existing(ev))
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
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
