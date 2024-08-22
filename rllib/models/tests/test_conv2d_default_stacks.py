import gymnasium as gym
import unittest

from ray.rllib.models.catalog import ModelCatalog, MODEL_DEFAULTS
from ray.rllib.models.tf.visionnet import VisionNetwork
from ray.rllib.models.torch.visionnet import VisionNetwork as TorchVision
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class TestConv2DDefaultStacks(unittest.TestCase):
    """Tests our ConvTranspose2D Stack modules/layers."""

    def test_conv2d_default_stacks(self):
        """Tests, whether conv2d defaults are available for img obs spaces."""
        action_space = gym.spaces.Discrete(2)

        shapes = [
            (96, 96, 3),
            (84, 84, 3),
            (42, 42, 3),
            (10, 10, 3),
        ]
        for shape in shapes:
            print(f"shape={shape}")
            obs_space = gym.spaces.Box(-1.0, 1.0, shape=shape)
            model = ModelCatalog.get_model_v2(
                obs_space, action_space, 2, MODEL_DEFAULTS.copy(), framework="torch"
            )
            self.assertTrue(isinstance(model, (VisionNetwork, TorchVision)))
            output, _ = model({"obs": torch.from_numpy(obs_space.sample()[None])})
            # B x [action logits]
            self.assertTrue(output.shape == (1, 2))
            print("ok")


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
