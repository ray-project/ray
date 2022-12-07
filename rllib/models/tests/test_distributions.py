from copy import copy
import numpy as np
import unittest
import math

from ray.rllib.models.torch.torch_distributions import (
    TorchCategorical,
    TorchDiagGaussian,
    TorchDeterministic,
)
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.numpy import (
    softmax,
    SMALL_NUMBER,
    LARGE_INTEGER,
)
from ray.rllib.utils.test_utils import check

torch, _ = try_import_torch()


def check_stability(dist_class, *, sample_input=None, constraints=None):
    max_tries = 100
    extreme_values = [
        0.0,
        float(LARGE_INTEGER),
        -float(LARGE_INTEGER),
        1.1e-34,
        1.1e34,
        -1.1e-34,
        -1.1e34,
        SMALL_NUMBER,
        -SMALL_NUMBER,
    ]

    input_kwargs = copy(sample_input)
    for key, array in input_kwargs.items():
        arr_sampled = np.random.choice(extreme_values, replace=True, size=array.shape)
        input_kwargs[key] = torch.from_numpy(arr_sampled).float()

        if constraints:
            constraint = constraints.get(key, None)
            if constraint:
                if constraint == "positive_not_inf":
                    input_kwargs[key] = torch.minimum(
                        SMALL_NUMBER + torch.log(1 + torch.exp(input_kwargs[key])),
                        torch.tensor([LARGE_INTEGER]),
                    )
                elif constraint == "probability":
                    input_kwargs[key] = torch.softmax(input_kwargs[key], dim=-1)

    dist = dist_class(**input_kwargs)
    for _ in range(max_tries):
        sample = dist.sample()

        assert not torch.isnan(sample).any()
        assert torch.all(torch.isfinite(sample))

        logp = dist.logp(sample)
        assert not torch.isnan(logp).any()
        assert torch.all(torch.isfinite(logp))


class TestDistributions(unittest.TestCase):
    """Tests Distribution classes."""

    @classmethod
    def setUpClass(cls) -> None:
        # Set seeds for deterministic tests (make sure we don't fail
        # because of "bad" sampling).
        np.random.seed(42)
        torch.manual_seed(42)

    def test_categorical(self):
        batch_size = 10000
        num_categories = 4
        sample_shape = 2

        # Create categorical distribution with n categories.
        logits = np.random.randn(batch_size, num_categories)
        probs = torch.from_numpy(softmax(logits)).float()
        logits = torch.from_numpy(logits).float()

        # check stability against skewed inputs
        check_stability(TorchCategorical, sample_input={"logits": logits})
        check_stability(
            TorchCategorical,
            sample_input={"probs": logits},
            constraints={"probs": "probability"},
        )

        dist_with_logits = TorchCategorical(logits=logits)
        dist_with_probs = TorchCategorical(probs=probs)

        samples = dist_with_logits.sample(sample_shape=(sample_shape,))

        # check shape of samples
        self.assertEqual(
            samples.shape,
            (
                sample_shape,
                batch_size,
            ),
        )
        self.assertEqual(samples.dtype, torch.int64)
        # check that none of the samples are nan
        self.assertFalse(torch.isnan(samples).any())
        # check that all samples are in the range of the number of categories
        self.assertTrue((samples >= 0).all())
        self.assertTrue((samples < num_categories).all())

        # resample to remove the first batch dim
        samples = dist_with_logits.sample()
        # check that the two distributions are the same
        check(dist_with_logits.logp(samples), dist_with_probs.logp(samples))

        # check logp values
        expected = probs.log().gather(dim=-1, index=samples.view(-1, 1)).view(-1)
        check(dist_with_logits.logp(samples), expected)

        # check entropy
        expected = -(probs * probs.log()).sum(dim=-1)
        check(dist_with_logits.entropy(), expected)

        # check kl
        probs2 = softmax(np.random.randn(batch_size, num_categories))
        probs2 = torch.from_numpy(probs2).float()
        dist2 = TorchCategorical(probs=probs2)
        expected = (probs * (probs / probs2).log()).sum(dim=-1)
        check(dist_with_probs.kl(dist2), expected)

        # check rsample
        self.assertRaises(NotImplementedError, dist_with_logits.rsample)

        # test temperature
        dist_with_logits = TorchCategorical(logits=logits, temperature=1e-20)
        samples = dist_with_logits.sample()
        # expected is armax of logits
        expected = logits.argmax(dim=-1)
        check(samples, expected)

    def test_diag_gaussian(self):
        batch_size = 128
        ndim = 4
        sample_shape = 100000

        loc = np.random.randn(batch_size, ndim)
        scale = np.exp(np.random.randn(batch_size, ndim))

        loc_tens = torch.from_numpy(loc).float()
        scale_tens = torch.from_numpy(scale).float()

        dist = TorchDiagGaussian(loc=loc_tens, scale=scale_tens)
        sample = dist.sample(sample_shape=(sample_shape,))

        # check shape of samples
        self.assertEqual(sample.shape, (sample_shape, batch_size, ndim))
        self.assertEqual(sample.dtype, torch.float32)
        # check that none of the samples are nan
        self.assertFalse(torch.isnan(sample).any())

        # check that mean and std are approximately correct
        check(sample.mean(0), loc, decimals=1)
        check(sample.std(0), scale, decimals=1)

        # check logp values
        expected = (
            -0.5 * ((sample - loc_tens) / scale_tens).pow(2).sum(-1)
            + -0.5 * ndim * math.log(2 * math.pi)
            - scale_tens.log().sum(-1)
        )
        check(dist.logp(sample), expected)

        # check entropy
        expected = 0.5 * ndim * (1 + math.log(2 * math.pi)) + scale_tens.log().sum(-1)
        check(dist.entropy(), expected)

        # check kl
        loc2 = torch.from_numpy(np.random.randn(batch_size, ndim)).float()
        scale2 = torch.from_numpy(np.exp(np.random.randn(batch_size, ndim)))
        dist2 = TorchDiagGaussian(loc=loc2, scale=scale2)
        expected = (
            scale2.log()
            - scale_tens.log()
            + (scale_tens.pow(2) + (loc_tens - loc2).pow(2)) / (2 * scale2.pow(2))
            - 0.5
        ).sum(-1)
        check(dist.kl(dist2), expected, decimals=4)

        # check rsample
        loc_tens.requires_grad = True
        scale_tens.requires_grad = True
        dist = TorchDiagGaussian(loc=2 * loc_tens, scale=2 * scale_tens)
        sample1 = dist.rsample()
        sample2 = dist.sample()

        self.assertRaises(
            RuntimeError, lambda: sample2.mean().backward(retain_graph=True)
        )
        sample1.mean().backward(retain_graph=True)

        # check stablity against skewed inputs
        check_stability(
            TorchDiagGaussian,
            sample_input={"loc": loc_tens, "scale": scale_tens},
            constraints={"scale": "positive_not_inf"},
        )

    def test_determinstic(self):
        batch_size = 128
        ndim = 4
        sample_shape = 100000

        loc = np.random.randn(batch_size, ndim)

        loc_tens = torch.from_numpy(loc).float()

        dist = TorchDeterministic(loc=loc_tens)
        sample = dist.sample(sample_shape=(sample_shape,))
        sample2 = dist.sample(sample_shape=(sample_shape,))
        check(sample, sample2)

        # check shape of samples
        self.assertEqual(sample.shape, (sample_shape, batch_size, ndim))
        self.assertEqual(sample.dtype, torch.float32)
        # check that none of the samples are nan
        self.assertFalse(torch.isnan(sample).any())


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
