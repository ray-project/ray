import gym
import numpy as np
import unittest

import ray
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.env.tests.test_external_env import make_simple_serving
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.evaluation.tests.test_rollout_worker import MockPolicy
from ray.rllib.examples.env.multi_agent import BasicMultiAgent
from ray.rllib.policy.sample_batch import SampleBatch

SimpleMultiServing = make_simple_serving(True, ExternalMultiAgentEnv)


class TestExternalMultiAgentEnv(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_external_multi_agent_env_complete_episodes(self):
        agents = 4
        ev = RolloutWorker(
            env_creator=lambda _: SimpleMultiServing(BasicMultiAgent(agents)),
            policy_spec=MockPolicy,
            rollout_fragment_length=40,
            batch_mode="complete_episodes",
        )
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 40)
            self.assertEqual(len(np.unique(batch[SampleBatch.AGENT_INDEX])), agents)

    def test_external_multi_agent_env_truncate_episodes(self):
        agents = 4
        ev = RolloutWorker(
            env_creator=lambda _: SimpleMultiServing(BasicMultiAgent(agents)),
            policy_spec=MockPolicy,
            rollout_fragment_length=40,
            batch_mode="truncate_episodes",
        )
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 160)
            self.assertEqual(len(np.unique(batch[SampleBatch.AGENT_INDEX])), agents)

    def test_external_multi_agent_env_sample(self):
        agents = 2
        act_space = gym.spaces.Discrete(2)
        obs_space = gym.spaces.Discrete(2)
        ev = RolloutWorker(
            env_creator=lambda _: SimpleMultiServing(BasicMultiAgent(agents)),
            policy_spec={
                "p0": (MockPolicy, obs_space, act_space, {}),
                "p1": (MockPolicy, obs_space, act_space, {}),
            },
            policy_mapping_fn=lambda aid, **kwargs: "p{}".format(aid % 2),
            rollout_fragment_length=50,
        )
        batch = ev.sample()
        self.assertEqual(batch.count, 50)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
