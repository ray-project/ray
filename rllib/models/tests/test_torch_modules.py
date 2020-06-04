from functools import partial
import numpy as np
from gym.spaces import Box, Dict, Tuple
from scipy.stats import beta, norm
import unittest

from ray.rllib.models.tf.tf_action_dist import Beta, Categorical, \
    DiagGaussian, GumbelSoftmax, MultiActionDistribution, MultiCategorical, \
    SquashedGaussian
from ray.rllib.models.torch.torch_action_dist import TorchBeta, \
    TorchCategorical, TorchDiagGaussian, TorchMultiActionDistribution, \
    TorchMultiCategorical, TorchSquashedGaussian
from ray.rllib.utils import try_import_tree
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.models.torch.modules.multi_head_attention import MultiHeadAttention
from ray.rllib.models.torch.modules.gru_gate import GRUGate
from ray.rllib.models.torch.modules.relative_multi_head_attention import RelativeMultiHeadAttention
from ray.rllib.models.torch.modules.skip_connection import SkipConnection

tf = try_import_tf()
torch, nn = try_import_torch()
tree = try_import_tree()


class TestDistributions(unittest.TestCase):
    """Tests models/torch/modules classes."""

    def test_categorical(self):
        """Tests the Categorical ActionDistribution (tf only)."""
        # TEST MULTIHEAD ATTENTION

        N, D_in, D_out = 64, 32, 10

        # Create random Tensors to hold inputs and outputs
        x = torch.randn(N, D_in)
        y = torch.randn(N, D_out)

        model = MultiHeadAttention(in_dim=D_in, out_dim=D_out, num_heads=2,
                                   head_dim=32)

        criterion = torch.nn.MSELoss(reduction='sum')
        optimizer = torch.optim.SGD(model.parameters(), lr=1e-4)
        for t in range(500):
            y_pred = model(x)

            loss = criterion(y_pred, y)
            if t % 100 == 99:
                print(t, loss.item())

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
