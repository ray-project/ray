import unittest

import gymnasium as gym
import numpy as np

from ray.rllib.connectors.into_env.clip_actions import ClipActions
from ray.rllib.connectors.into_env.unsquash_actions import UnsquashActions
from ray.rllib.connectors.connector import ConnectorContextV2
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import check


class TestFromModuleConnectors(unittest.TestCase):
    def test_connector_pipeline(self):
        ctx = ConnectorContext()
        connectors = [ConvertToNumpyConnector(ctx)]
        pipeline = ActionConnectorPipeline(ctx, connectors)
        name, params = pipeline.serialize()
        restored = get_connector(name, ctx, params)
        self.assertTrue(isinstance(restored, ActionConnectorPipeline))
        self.assertTrue(isinstance(restored.connectors[0], ConvertToNumpyConnector))
        # There should not be any timer yet
        self.assertFalse(bool(pipeline.timers.values()))
        pipeline(ActionConnectorDataType(0, 0, {}, ([1], [], None)))
        # After a first input, there should be one timer
        self.assertEquals(len(pipeline.timers.values()), 1)

    def test_clip_actions_connector(self):
        ctx = ConnectorContextV2()

        connector = ClipActions(
            action_space=gym.spaces.Box(low=0.0, high=6.0, shape=(1,))
        )

        # name, params = connector.serialize()
        # self.assertEqual(name, "ClipActions")

        # restored = get_connector(name, ctx, params)
        # self.assertTrue(isinstance(restored, ClipActionsConnector))

        for action in [8.8, 6.0, -0.2, 0.0, 5.9999, 3.2, 6.1]:
            output = connector(
                {SampleBatch.ACTIONS: np.array([action])},
                ctx,
            )
            check(output[SampleBatch.ACTIONS], np.clip(action, 0.0, 6.0))

        connector = ClipActions(
            action_space=gym.spaces.Dict(
                {
                    "a": gym.spaces.Box(low=-1.0, high=1.0, shape=(2,)),
                    "b": gym.spaces.Discrete(3),
                }
            )
        )
        for action in [
            {"a": np.array([8.8, 8.9]), "b": 1},
            {"a": np.array([9.0, -1.0]), "b": 0},
            {"a": np.array([100.0, 200.0]), "b": 2},
            {"a": np.array([-1000, 0.0001]), "b": 2},
            {"a": np.array([0.4, 1.2]), "b": 0},
            {"a": np.array([1.0, -1.0]), "b": 1},
        ]:
            output = connector({SampleBatch.ACTIONS: action}, ctx)
            check(
                output[SampleBatch.ACTIONS],
                {"a": np.clip(action["a"], -1.0, 1.0), "b": action["b"]},
            )

    def test_unsquash_actions_connector(self):
        ctx = ConnectorContextV2()

        connector = UnsquashActions(
            action_space=gym.spaces.Box(low=-2.0, high=6.0, shape=(2,))
        )

        # name, params = connector.serialize()
        # self.assertEqual(name, "UnsquashActions")

        # restored = get_connector(name, ctx, params)
        # self.assertTrue(isinstance(restored, NormalizeActionsConnector))

        for action in [
            [1.8, 1.8],
            [1.0, -1.0],
            [-1.0, 1.1],
            [0.0, 0.0],
            [10.0, 0.5],
            [0.5, -0.5],
        ]:
            action = np.array(action)
            output = connector(
                {SampleBatch.ACTIONS: action},
                ctx,
            )
            check(
                output[SampleBatch.ACTIONS],
                np.clip((action + 1.0) * 4.0 - 2.0, -2.0, 6.0),
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
