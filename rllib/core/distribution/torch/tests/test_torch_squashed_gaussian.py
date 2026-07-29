import unittest

import torch

from ray.rllib.core.distribution.torch.torch_distribution import TorchSquashedGaussian


class TestTorchSquashedGaussian(unittest.TestCase):
    """Regression tests for the squashed-Gaussian log-prob fix.

    The bug: ``logp(action)`` recovered the pre-squash sample with
    ``atanh(clamp(action, -1 + 1e-6, 1 - 1e-6))``. That clamp caps the recovered
    value at ``atanh(1 - 1e-6) ~= 7.25``, so once the policy mean saturates
    ``tanh`` the Gaussian log-prob was evaluated far from the true mean and
    collapsed to a large *negative* value -- a sign flip that then corrupted
    alpha tuning and the SAC/TQC critic target.

    The fix: ``rsample_and_logp()`` / ``sample_and_logp()`` compute the log-prob
    from the pre-squash sample directly (no ``atanh`` round-trip), so they are
    correct even when the policy saturates.
    """

    def test_logp_is_positive_for_a_saturated_policy(self):
        # A confident policy (large mean, small std) puts almost all of its mass
        # near the action boundary, so its own samples have a large *positive*
        # log-density. This is exactly where the bug bit: it returned a large
        # negative value instead. The fix must return a finite, positive logp.
        loc = torch.full((1024, 6), 10.0)  # mean well past tanh's linear range
        log_std = torch.full((1024, 6), -2.0)  # small std -> confident policy
        dist = TorchSquashedGaussian.from_logits(torch.cat([loc, log_std], dim=-1))

        _, logp = dist.rsample_and_logp()

        self.assertTrue(torch.isfinite(logp).all())
        self.assertGreater(logp.mean().item(), 0.0)  # the bug produced ~ -1000s here

    def test_logp_matches_the_squashed_gaussian_formula(self):
        # The squashed-Gaussian log-prob of a pre-squash sample x is:
        #   log N(x | loc, scale)  -  sum_i log(1 - tanh(x_i)^2)
        # Check rsample_and_logp() against this formula in the unsaturated
        # regime, where recovering x via atanh is accurate and the naive tanh
        # Jacobian above is numerically well-behaved.
        torch.manual_seed(0)
        loc = torch.zeros(256, 4)
        log_std = torch.full((256, 4), -0.5)
        scale = log_std.exp()
        dist = TorchSquashedGaussian.from_logits(torch.cat([loc, log_std], dim=-1))

        action, logp = dist.rsample_and_logp()

        x = torch.atanh(action.clamp(-1 + 1e-6, 1 - 1e-6))  # pre-squash sample
        expected = torch.distributions.Normal(loc, scale).log_prob(x).sum(
            -1
        ) - torch.log(1 - torch.tanh(x) ** 2).sum(-1)
        self.assertTrue(torch.allclose(logp, expected, atol=1e-3))


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
