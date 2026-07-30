import unittest

import torch

from ray.rllib.core.distribution.torch.torch_distribution import TorchSquashedGaussian


class TestTorchSquashedGaussian(unittest.TestCase):
    def test_logp_is_positive_for_a_saturated_policy(self):
        # Regression test for #65036
        # A confident policy (large mean, small std) puts almost all of its mass
        # near the action boundary, so its own samples have a large *positive*
        # log-density. If we regress, we return a large
        # negative value instead. The fix must return a finite, positive logp.
        loc = torch.full((1024, 6), 10.0)  # mean well past tanh's linear range
        log_std = torch.full((1024, 6), -2.0)  # small std -> confident policy
        dist = TorchSquashedGaussian.from_logits(torch.cat([loc, log_std], dim=-1))

        _, logp = dist.rsample_and_logp()
        print(logp)
        print(_)

        self.assertTrue(torch.isfinite(logp).all())
        self.assertGreater(logp.mean().item(), 0.0)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
