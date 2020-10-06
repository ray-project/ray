import cv2
import numpy as np
import unittest

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
        optim = torch.optim.Adam(module.parameters(), lr=0.0001)
        img = cv2.imread("../../tests/data/images/obstacle_tower.png")
        # Make channels first.
        img = np.transpose(img, (2, 0, 1))
        # Add batch rank and repeat.
        imgs = np.reshape(img, (1, ) + img.shape)
        imgs = np.repeat(imgs, batch_size, axis=0)
        # Normalize (-1.0 to 1.0).
        imgs = (imgs / 255.0) * 2.0 - 1.0
        imgs = torch.from_numpy(imgs)
        init_loss = loss = None
        for _ in range(1000):
            # Random inputs.
            inputs = torch.from_numpy(
                np.random.normal(0.0, 1.0, (batch_size, input_size))).float()
            distribution = module(inputs)
            # Construct a loss.
            #predictions = distribution.rsample()
            #loss = torch.mean(torch.pow(predictions - imgs, 2.0))
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
