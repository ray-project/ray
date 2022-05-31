import gym
import numpy as np
import unittest

from ray.rllib.connectors.action import (
    ActionConnectorPipeline,
    ClipActionsConnector,
    ConvertToNumpyConnector,
    NormalizeActionsConnector,
    UnbatchActionsConnector,
)
from ray.rllib.connectors.connector import (
    ConnectorContext,
    get_connector,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.typing import ActionConnectorDataType

torch, _ = try_import_torch()


class TestActionConnector(unittest.TestCase):
    def test_connector_pipeline(self):
        ctx = ConnectorContext()
        connectors = [ConvertToNumpyConnector(ctx)]
        pipeline = ActionConnectorPipeline(ctx, connectors)
        name, params = pipeline.to_config()
        restored = get_connector(ctx, name, params)
        self.assertTrue(isinstance(restored, ActionConnectorPipeline))
        self.assertTrue(isinstance(restored.connectors[0], ConvertToNumpyConnector))

    def test_convert_to_numpy_connector(self):
        ctx = ConnectorContext()
        c = ConvertToNumpyConnector(ctx)

        name, params = c.to_config()

        self.assertEqual(name, "ConvertToNumpyConnector")

        restored = get_connector(ctx, name, params)
        self.assertTrue(isinstance(restored, ConvertToNumpyConnector))

        action = torch.Tensor([8, 9])
        states = torch.Tensor([[1, 1, 1], [2, 2, 2]])
        ac_data = ActionConnectorDataType(0, 1, (action, states, {}))

        converted = c(ac_data)
        self.assertTrue(isinstance(converted.output[0], np.ndarray))
        self.assertTrue(isinstance(converted.output[1], np.ndarray))

    def test_unbatch_action_connector(self):
        ctx = ConnectorContext()
        c = UnbatchActionsConnector(ctx)

        name, params = c.to_config()

        self.assertEqual(name, "UnbatchActionsConnector")

        restored = get_connector(ctx, name, params)
        self.assertTrue(isinstance(restored, UnbatchActionsConnector))

        # TODO(jungong) : bring this test online once I understand how it works.
        # ac_data = ActionConnectorDataType(
        #    0, 1, ({"a": [1, 2, 3], "b": ([4, 5, 6], [7, 8, 9])}, [], {})
        # )
        #
        # unbatched = c(ctx={}, ac_data=ac_data)
        # self.assertEqual(len(unbatched.output), 3)

    def test_normalize_action_connector(self):
        ctx = ConnectorContext(
            action_space=gym.spaces.Box(low=0.0, high=6.0, shape=[1])
        )
        c = NormalizeActionsConnector(ctx)

        name, params = c.to_config()
        self.assertEqual(name, "NormalizeActionsConnector")

        restored = get_connector(ctx, name, params)
        self.assertTrue(isinstance(restored, NormalizeActionsConnector))

        ac_data = ActionConnectorDataType(0, 1, (0.5, [], {}))

        normalized = c(ac_data)
        self.assertEqual(normalized.output[0], 4.5)

    def test_clip_action_connector(self):
        ctx = ConnectorContext(
            action_space=gym.spaces.Box(low=0.0, high=6.0, shape=[1])
        )
        c = ClipActionsConnector(ctx)

        name, params = c.to_config()
        self.assertEqual(name, "ClipActionsConnector")

        restored = get_connector(ctx, name, params)
        self.assertTrue(isinstance(restored, ClipActionsConnector))

        ac_data = ActionConnectorDataType(0, 1, (8.8, [], {}))

        clipped = c(ac_data)
        self.assertEqual(clipped.output[0], 6.0)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
