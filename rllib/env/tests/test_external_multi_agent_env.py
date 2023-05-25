import numpy as np
import unittest

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.env.external_multi_agent_env import ExternalMultiAgentEnv
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.evaluation.tests.test_rollout_worker import MockPolicy
from ray.rllib.examples.env.multi_agent import BasicMultiAgent
from ray.rllib.policy.sample_batch import SampleBatch


class SimpleMultiServing(ExternalMultiAgentEnv):
    def __init__(self, env):
        ExternalMultiAgentEnv.__init__(self, env.action_space, env.observation_space)
        self.env = env

    def run(self):
        eid = self.start_episode()
        obs, info = self.env.reset()
        while True:
            action = self.get_action(eid, obs)
            obs, reward, terminated, truncated, info = self.env.step(action)
            self.log_returns(eid, reward)
            if terminated or truncated:
                self.end_episode(eid, obs)
                obs, info = self.env.reset()
                eid = self.start_episode()


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
            default_policy_class=MockPolicy,
            config=AlgorithmConfig().rollouts(
                rollout_fragment_length=40,
                num_rollout_workers=0,
                batch_mode="complete_episodes",
                enable_connectors=False,
            ),
        )
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 40)
            self.assertEqual(len(np.unique(batch[SampleBatch.AGENT_INDEX])), agents)

    def test_external_multi_agent_env_truncate_episodes(self):
        agents = 4
        ev = RolloutWorker(
            env_creator=lambda _: SimpleMultiServing(BasicMultiAgent(agents)),
            default_policy_class=MockPolicy,
            config=AlgorithmConfig().rollouts(
                rollout_fragment_length=40,
                num_rollout_workers=0,
                enable_connectors=False,
            ),
        )
        for _ in range(3):
            batch = ev.sample()
            self.assertEqual(batch.count, 160)
            self.assertEqual(len(np.unique(batch[SampleBatch.AGENT_INDEX])), agents)

    def test_external_multi_agent_env_sample(self):
        agents = 2
        ev = RolloutWorker(
            env_creator=lambda _: SimpleMultiServing(BasicMultiAgent(agents)),
            default_policy_class=MockPolicy,
            config=AlgorithmConfig()
            .rollouts(
                rollout_fragment_length=50,
                num_rollout_workers=0,
                batch_mode="complete_episodes",
                enable_connectors=False,
            )
            .multi_agent(
                policies={"p0", "p1"},
                policy_mapping_fn=(
                    lambda agent_id, episode, worker, **kwargs: "p{}".format(
                        agent_id % 2
                    )
                ),
            ),
        )
        batch = ev.sample()
        self.assertEqual(batch.count, 50)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
