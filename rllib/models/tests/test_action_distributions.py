import unittest

import numpy as np
from gymnasium.spaces import Box
from scipy.stats import norm

from ray.rllib.models.torch.torch_action_dist import (
    TorchCategorical,
    TorchDiagGaussian,
)
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import (
    LARGE_INTEGER,
    SMALL_NUMBER,
    softmax,
)
from ray.rllib.utils.test_utils import check

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class TestActionDistributions(unittest.TestCase):
    """Tests ActionDistribution classes."""

    @classmethod
    def setUpClass(cls) -> None:
        # Set seeds for deterministic tests (make sure we don't fail
        # because of "bad" sampling).
        np.random.seed(42 + 1)
        torch.manual_seed(42 + 1)

    def _stability_test(
        self,
        distribution_cls,
        network_output_shape,
        fw,
        sess=None,
        bounds=None,
        extra_kwargs=None,
    ):
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
        inputs = np.zeros(shape=network_output_shape, dtype=np.float32)
        for batch_item in range(network_output_shape[0]):
            for num in range(len(inputs[batch_item]) // 2):
                inputs[batch_item][num] = np.random.choice(extreme_values)
            else:
                # For Gaussians, the second half of the vector is
                # log standard deviations, and should therefore be
                # the log of a positive number >= 1.
                inputs[batch_item][num] = np.log(
                    max(1, np.random.choice((extreme_values)))
                )

        dist = distribution_cls(inputs, {}, **(extra_kwargs or {}))
        for _ in range(100):
            sample = dist.sample()
            sample_check = sample.numpy()
            assert not np.any(np.isnan(sample_check))
            assert np.all(np.isfinite(sample_check))
            if bounds:
                assert np.min(sample_check) >= bounds[0]
                assert np.max(sample_check) <= bounds[1]
                # Make sure bounds make sense and are actually also being
                # sampled.
                if isinstance(bounds[0], int):
                    assert isinstance(bounds[1], int)
                    assert bounds[0] in sample_check
                    assert bounds[1] in sample_check
            logp = dist.logp(sample)
            logp_check = logp.numpy()
            assert not np.any(np.isnan(logp_check))
            assert np.all(np.isfinite(logp_check))

    def test_categorical(self):
        batch_size = 10000
        num_categories = 4
        # Create categorical distribution with n categories.
        inputs_space = Box(
            -1.0, 2.0, shape=(batch_size, num_categories), dtype=np.float32
        )
        inputs_space.seed(42)
        values_space = Box(0, num_categories - 1, shape=(batch_size,), dtype=np.int32)
        values_space.seed(42)

        inputs = inputs_space.sample()

        # Create the correct distribution object.
        cls = TorchCategorical
        categorical = cls(inputs, {})

        # Do a stability test using extreme NN outputs to see whether
        # sampling and logp'ing result in NaN or +/-inf values.
        self._stability_test(
            cls,
            inputs_space.shape,
            fw="torch",
            sess=None,
            bounds=(0, num_categories - 1),
        )

        # Batch of size=3 and deterministic (True).
        expected = np.transpose(np.argmax(inputs, axis=-1))
        # Sample, expect always max value
        # (max likelihood for deterministic draw).
        out = categorical.deterministic_sample()
        check(out, expected)

        # Batch of size=3 and non-deterministic -> expect roughly the mean.
        out = categorical.sample()
        check(torch.mean(out.float()), 1.0, decimals=0)

        # Test log-likelihood outputs.
        probs = softmax(inputs)
        values = values_space.sample()

        out = categorical.logp(torch.Tensor(values))
        expected = []
        for i in range(batch_size):
            expected.append(np.sum(np.log(np.array(probs[i][values[i]]))))
        check(out, expected, decimals=4)

        # Test entropy outputs.
        out = categorical.entropy()
        expected_entropy = -np.sum(probs * np.log(probs), -1)
        check(out, expected_entropy)

    def test_diag_gaussian(self):
        """Tests the DiagGaussian ActionDistribution for all frameworks."""
        input_space = Box(-2.0, 1.0, shape=(2000, 10))
        input_space.seed(42)

        cls = TorchDiagGaussian

        # Do a stability test using extreme NN outputs to see whether
        # sampling and logp'ing result in NaN or +/-inf values.
        self._stability_test(cls, input_space.shape, fw="torch")

        # Batch of size=n and deterministic.
        inputs = input_space.sample()
        means, _ = np.split(inputs, 2, axis=-1)
        diag_distribution = cls(inputs, {})
        expected = means
        # Sample n times, expect always mean value (deterministic draw).
        out = diag_distribution.deterministic_sample()
        check(out, expected)

        # Batch of size=n and non-deterministic -> expect roughly the mean.
        inputs = input_space.sample()
        means, log_stds = np.split(inputs, 2, axis=-1)
        diag_distribution = cls(inputs, {})
        expected = means
        values = diag_distribution.sample()
        values = values.numpy()
        check(np.mean(values), expected.mean(), decimals=1)

        # NN output.
        means = np.array(
            [[0.1, 0.2, 0.3, 0.4, 50.0], [-0.1, -0.2, -0.3, -0.4, -1.0]],
            dtype=np.float32,
        )
        log_stds = np.array(
            [[0.8, -0.2, 0.3, -1.0, 2.0], [0.7, -0.3, 0.4, -0.9, 2.0]],
            dtype=np.float32,
        )

        diag_distribution = cls(
            inputs=np.concatenate([means, log_stds], axis=-1), model={}
        )
        # Convert to parameters for distr.
        stds = np.exp(log_stds)
        # Values to get log-likelihoods for.
        values = np.array(
            [[0.9, 0.2, 0.4, -0.1, -1.05], [-0.9, -0.2, 0.4, -0.1, -1.05]]
        )

        # get log-llh from regular gaussian.
        log_prob = np.sum(np.log(norm.pdf(values, means, stds)), -1)

        outs = diag_distribution.logp(torch.Tensor(values))
        check(outs, log_prob, decimals=4)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
