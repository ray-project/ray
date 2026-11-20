import unittest

import torch

from ray.rllib.core.distribution.torch.torch_distribution import TorchSquashedGaussian


class TestTorchSquashedGaussian(unittest.TestCase):
    def test_deterministic_action_respects_squashing_and_bounds(self):
        loc = torch.tensor([2.0, -1.0])
        low = torch.tensor([-2.0, 0.0])
        high = torch.tensor([2.0, 6.0])
        dist = TorchSquashedGaussian(loc, torch.ones_like(loc), low=low, high=high)
        expected = (torch.tanh(loc) + 1) * (high - low) / 2 + low
        torch.testing.assert_close(dist.to_deterministic().sample(), expected)
        scalar = TorchSquashedGaussian(2.0, 1.0)
        torch.testing.assert_close(
            scalar.to_deterministic().sample(), torch.tanh(torch.tensor(2.0))
        )

    def test_logp_includes_affine_action_scaling(self):
        loc = torch.tensor([0.3, -0.2], requires_grad=True)
        scale = torch.tensor([0.8, 1.2])
        low = torch.tensor([-2.0, 0.0], requires_grad=True)
        high = torch.tensor([2.0, 6.0], requires_grad=True)
        action = torch.tensor([0.5, 1.0])
        dist = TorchSquashedGaussian(loc, scale, low=low, high=high)
        reference = torch.distributions.TransformedDistribution(
            torch.distributions.Normal(loc, scale),
            [
                torch.distributions.transforms.TanhTransform(),
                torch.distributions.transforms.AffineTransform(
                    loc=(high + low) / 2, scale=(high - low) / 2
                ),
            ],
        )
        torch.testing.assert_close(dist.logp(action), reference.log_prob(action).sum())
        actual_grad = torch.autograd.grad(dist.logp(action), (loc, low, high))
        expected_grad = torch.autograd.grad(
            reference.log_prob(action).sum(), (loc, low, high)
        )
        for actual, expected in zip(actual_grad, expected_grad):
            torch.testing.assert_close(actual, expected)

    def test_sample_and_logp_match_with_nondefault_bounds(self):
        torch.manual_seed(42)
        dist = TorchSquashedGaussian(
            torch.zeros((3, 2)), torch.ones((3, 2)), low=-2.0, high=4.0
        )
        for sample_fn in (dist.sample_and_logp, dist.rsample_and_logp):
            sample, logp = sample_fn()
            assert ((sample >= -2.0) & (sample <= 4.0)).all()
            torch.testing.assert_close(logp, dist.logp(sample))

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

        self.assertTrue(torch.isfinite(logp).all())
        self.assertGreater(logp.mean().item(), 0.0)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
