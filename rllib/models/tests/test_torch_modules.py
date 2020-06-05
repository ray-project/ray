import unittest
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.torch.modules.multi_head_attention import \
    MultiHeadAttention
from ray.rllib.models.torch.modules.gru_gate import GRUGate
from ray.rllib.models.torch.modules.relative_multi_head_attention import \
    RelativeMultiHeadAttention
from ray.rllib.models.torch.modules.skip_connection import SkipConnection

torch, nn = try_import_torch()


class TestDistributions(unittest.TestCase):
    """Tests models/torch/modules helper classes for attention net."""

    def test_MultiHeadAttention(self):
        """Tests the MultiHeadAttention mechanism of Vaswani et al."""

        N, D_in, D_out = 64, 32, 10

        # Create random Tensors to hold inputs and outputs
        x = torch.randn(N, D_in)
        y = torch.randn(N, D_out)

        # Create a single attention layer with 2 heads
        model = MultiHeadAttention(in_dim=D_in, out_dim=D_out, num_heads=2,
                                   head_dim=32)

        # Check that the layer is instantiated correctly
        criterion = torch.nn.MSELoss(reduction='sum')
        optimizer = torch.optim.SGD(model.parameters(), lr=1e-4)

        # Check that the layer trains correctly
        for t in range(500):
            y_pred = model(x)

            loss = criterion(y_pred, y)
            if t % 100 == 99:
                print(t, loss.item())

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

        # The final layer has trained correctly to have nearly zero loss
        assert abs(loss.item()) < 10e-3

if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
