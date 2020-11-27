import cv2
import gym
import numpy as np
import os
from pathlib import Path
import unittest

from ray.rllib.models.preprocessors import GenericPixelPreprocessor
from ray.rllib.models.torch.modules.convtranspose2d_stack import \
    ConvTranspose2DStack
from ray.rllib.utils.framework import try_import_torch, try_import_tf

torch, nn = try_import_torch()
tf1, tf, tfv = try_import_tf()


class TestConvTranspose2DStack(unittest.TestCase):
    """Tests our ConvTranspose2D Stack modules/layers."""

    def test_convtranspose2d_stack(self):
        """Tests, whether the conv2d stack can be trained to predict an image.
        """
        batch_size = 128
        input_size = 1
        module = ConvTranspose2DStack(input_size=input_size)
        preprocessor = GenericPixelPreprocessor(
            gym.spaces.Box(0, 255, (64, 64, 3), np.uint8), options={"dim": 64})
        optim = torch.optim.Adam(module.parameters(), lr=0.0001)

        rllib_dir = Path(__file__).parent.parent.parent
        img_file = os.path.join(rllib_dir,
                                "tests/data/images/obstacle_tower.png")
        img = cv2.imread(img_file).astype(np.float32)
        # Preprocess.
        img = preprocessor.transform(img)
        # Make channels first.
        img = np.transpose(img, (2, 0, 1))
        # Add batch rank and repeat.
        imgs = np.reshape(img, (1, ) + img.shape)
        imgs = np.repeat(imgs, batch_size, axis=0)
        # Move to torch.
        imgs = torch.from_numpy(imgs)
        init_loss = loss = None
        for _ in range(10):
            # Random inputs.
            inputs = torch.from_numpy(
                np.random.normal(0.0, 1.0, (batch_size, input_size))).float()
            distribution = module(inputs)
            # Construct a loss.
            loss = -torch.mean(distribution.log_prob(imgs))
            if init_loss is None:
                init_loss = loss
            print("loss={}".format(loss))
            # Minimize loss.
            loss.backward()
            optim.step()
        self.assertLess(loss, init_loss)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
