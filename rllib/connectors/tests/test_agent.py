import gym
import numpy as np
import unittest

from ray.rllib.connectors.agent.clip_reward import ClipRewardAgentConnector
from ray.rllib.connectors.agent.lambdas import FlattenDataAgentConnector
from ray.rllib.connectors.agent.obs_preproc import ObsPreprocessorConnector
from ray.rllib.connectors.agent.pipeline import AgentConnectorPipeline
from ray.rllib.connectors.agent.state_buffer import StateBufferConnector
from ray.rllib.connectors.agent.view_requirement import ViewRequirementAgentConnector
from ray.rllib.connectors.connector import ConnectorContext, get_connector
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.typing import (
    ActionConnectorDataType,
    AgentConnectorDataType,
    AgentConnectorsOutput,
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
        preprocessed = c([d])

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
        clipped = restored([d])

        self.assertEqual(len(clipped), 1)
        self.assertEqual(clipped[0].data[SampleBatch.REWARDS], 2.0)

    def test_flatten_data_connector(self):
        ctx = ConnectorContext()

        c = FlattenDataAgentConnector(ctx)

        name, params = c.to_config()
        restored = get_connector(ctx, name, params)
        self.assertTrue(isinstance(restored, FlattenDataAgentConnector))

        sample_batch = {
            SampleBatch.NEXT_OBS: {
                "sensor1": [[1, 1], [2, 2]],
                "sensor2": 8.8,
            },
            SampleBatch.REWARDS: 5.8,
            SampleBatch.ACTIONS: [[1, 1], [2]],
            SampleBatch.INFOS: {"random": "info"},
        }

        d = AgentConnectorDataType(
            0,
            1,
            # FlattenDataAgentConnector does NOT touch for_training dict,
            # so simply pass None here.
            AgentConnectorsOutput(None, sample_batch),
        )

        flattened = c([d])
        self.assertEqual(len(flattened), 1)

        batch = flattened[0].data.for_action
        self.assertTrue((batch[SampleBatch.NEXT_OBS] == [1, 1, 2, 2, 8.8]).all())
        self.assertEqual(batch[SampleBatch.REWARDS][0], 5.8)
        # Not flattened.
        self.assertEqual(len(batch[SampleBatch.ACTIONS]), 2)
        self.assertEqual(batch[SampleBatch.INFOS]["random"], "info")

    def test_state_buffer_connector(self):
        ctx = ConnectorContext(
            action_space=gym.spaces.Box(low=-1.0, high=1.0, shape=(3,)),
        )
        c = StateBufferConnector(ctx)

        # Reset without any buffered data should do nothing.
        c.reset(env_id=0)

        d = AgentConnectorDataType(
            0,
            1,
            {
                SampleBatch.NEXT_OBS: {
                    "sensor1": [[1, 1], [2, 2]],
                    "sensor2": 8.8,
                },
            },
        )

        with_buffered = c([d])
        self.assertEqual(len(with_buffered), 1)
        self.assertTrue((with_buffered[0].data[SampleBatch.ACTIONS] == [0, 0, 0]).all())

        c.on_policy_output(ActionConnectorDataType(0, 1, ([1, 2, 3], [], {})))

        with_buffered = c([d])
        self.assertEqual(len(with_buffered), 1)
        self.assertEqual(with_buffered[0].data[SampleBatch.ACTIONS], [1, 2, 3])

    def test_view_requirement_connector(self):
        view_requirements = {
            "obs": ViewRequirement(
                used_for_training=True, used_for_compute_actions=True
            ),
            "prev_actions": ViewRequirement(
                data_col="actions",
                shift=-1,
                used_for_training=True,
                used_for_compute_actions=True,
            ),
        }
        ctx = ConnectorContext(view_requirements=view_requirements)

        c = ViewRequirementAgentConnector(ctx)
        f = FlattenDataAgentConnector(ctx)

        d = AgentConnectorDataType(
            0,
            1,
            {
                SampleBatch.NEXT_OBS: {
                    "sensor1": [[1, 1], [2, 2]],
                    "sensor2": 8.8,
                },
                SampleBatch.ACTIONS: np.array(0),
            },
        )
        # ViewRequirementAgentConnector then FlattenAgentConnector.
        processed = f(c([d]))

        self.assertTrue("obs" in processed[0].data.for_action)
        self.assertTrue("prev_actions" in processed[0].data.for_action)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
