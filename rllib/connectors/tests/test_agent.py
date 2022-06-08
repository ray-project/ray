import gym
import numpy as np
import unittest

from ray.rllib.connectors.agent.clip_reward import ClipRewardAgentConnector
from ray.rllib.connectors.agent.env_to_agent import EnvToAgentDataConnector
from ray.rllib.connectors.agent.lambdas import FlattenDataAgentConnector
from ray.rllib.connectors.agent.obs_preproc import ObsPreprocessorConnector
from ray.rllib.connectors.agent.pipeline import AgentConnectorPipeline
from ray.rllib.connectors.connector import (
    ConnectorContext,
    get_connector,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.typing import (
    AgentConnectorDataType,
)


class TestAgentConnector(unittest.TestCase):
    def test_connector_pipeline(self):
        ctx = ConnectorContext()
        connectors = [ClipRewardAgentConnector(ctx, False, 1.0)]
        pipeline = AgentConnectorPipeline(ctx, connectors)
        name, params = pipeline.to_config()
        restored = get_connector(ctx, name, params)
        self.assertTrue(isinstance(restored, AgentConnectorPipeline))
        self.assertTrue(isinstance(restored.connectors[0], ClipRewardAgentConnector))

    def test_env_to_per_agent_data_connector(self):
        vrs = {
            "infos": ViewRequirement(
                "infos",
                used_for_training=True,
                used_for_compute_actions=False,
            )
        }
        ctx = ConnectorContext(view_requirements=vrs)

        c = EnvToAgentDataConnector(ctx)

        name, params = c.to_config()
        restored = get_connector(ctx, name, params)
        self.assertTrue(isinstance(restored, EnvToAgentDataConnector))

        d = AgentConnectorDataType(
            0,
            None,
            [
                # obs
                {1: [8, 8], 2: [9, 9]},
                # rewards
                {
                    1: 8.8,
                    2: 9.9,
                },
                # dones
                {
                    1: False,
                    2: False,
                },
                # infos
                {
                    1: {"random": "info"},
                    2: {},
                },
                # training_episode_info
                {
                    1: {SampleBatch.DONES: True},
                },
            ],
        )
        per_agent = c(d)

        self.assertEqual(len(per_agent), 2)

        batch1 = per_agent[0].data
        self.assertEqual(batch1[SampleBatch.NEXT_OBS], [8, 8])
        self.assertTrue(batch1[SampleBatch.DONES])  # from training_episode_info
        self.assertTrue(SampleBatch.INFOS in batch1)
        self.assertEqual(batch1[SampleBatch.INFOS]["random"], "info")

        batch2 = per_agent[1].data
        self.assertEqual(batch2[SampleBatch.NEXT_OBS], [9, 9])
        self.assertFalse(batch2[SampleBatch.DONES])

    def test_obs_preprocessor_connector(self):
        obs_space = gym.spaces.Dict(
            {
                "a": gym.spaces.Box(low=0, high=1, shape=(1,)),
                "b": gym.spaces.Tuple(
                    [gym.spaces.Discrete(2), gym.spaces.MultiDiscrete(nvec=[2, 3])]
                ),
            }
        )
        ctx = ConnectorContext(config={}, observation_space=obs_space)

        c = ObsPreprocessorConnector(ctx)
        name, params = c.to_config()

        restored = get_connector(ctx, name, params)
        self.assertTrue(isinstance(restored, ObsPreprocessorConnector))

        obs = obs_space.sample()
        # Fake deterministic data.
        obs["a"][0] = 0.5
        obs["b"] = (1, np.array([0, 2]))

        d = AgentConnectorDataType(
            0,
            1,
            {
                SampleBatch.OBS: obs,
            },
        )
        preprocessed = c(d)

        # obs is completely flattened.
        self.assertTrue(
            (preprocessed[0].data[SampleBatch.OBS] == [0.5, 0, 1, 1, 0, 0, 0, 1]).all()
        )

    def test_clip_reward_connector(self):
        ctx = ConnectorContext()

        c = ClipRewardAgentConnector(ctx, limit=2.0)
        name, params = c.to_config()

        self.assertEqual(name, "ClipRewardAgentConnector")
        self.assertAlmostEqual(params["limit"], 2.0)

        restored = get_connector(ctx, name, params)
        self.assertTrue(isinstance(restored, ClipRewardAgentConnector))

        d = AgentConnectorDataType(
            0,
            1,
            {
                SampleBatch.REWARDS: 5.8,
            },
        )
        clipped = restored(ac_data=d)

        self.assertEqual(len(clipped), 1)
        self.assertEqual(clipped[0].data[SampleBatch.REWARDS], 2.0)

    def test_flatten_data_connector(self):
        ctx = ConnectorContext()

        c = FlattenDataAgentConnector(ctx)

        name, params = c.to_config()
        restored = get_connector(ctx, name, params)
        self.assertTrue(isinstance(restored, FlattenDataAgentConnector))

        d = AgentConnectorDataType(
            0,
            1,
            {
                SampleBatch.NEXT_OBS: {
                    "sensor1": [[1, 1], [2, 2]],
                    "sensor2": 8.8,
                },
                SampleBatch.REWARDS: 5.8,
                SampleBatch.ACTIONS: [[1, 1], [2]],
                SampleBatch.INFOS: {"random": "info"},
            },
        )

        flattened = c(d)
        self.assertEqual(len(flattened), 1)

        batch = flattened[0].data
        self.assertTrue((batch[SampleBatch.NEXT_OBS] == [1, 1, 2, 2, 8.8]).all())
        self.assertEqual(batch[SampleBatch.REWARDS][0], 5.8)
        # Not flattened.
        self.assertEqual(len(batch[SampleBatch.ACTIONS]), 2)
        self.assertEqual(batch[SampleBatch.INFOS]["random"], "info")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
