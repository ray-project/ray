from functools import partial
from gym.spaces import Box, Dict, Tuple
import numpy as np
from scipy.stats import beta, norm
import tree  # pip install dm_tree
import unittest

from ray.rllib.models.jax.jax_action_dist import JAXCategorical
from ray.rllib.models.tf.tf_action_dist import (
    Beta,
    Categorical,
    DiagGaussian,
    GumbelSoftmax,
    MultiActionDistribution,
    MultiCategorical,
    SquashedGaussian,
)
from ray.rllib.models.torch.torch_action_dist import (
    TorchBeta,
    TorchCategorical,
    TorchDiagGaussian,
    TorchMultiActionDistribution,
    TorchMultiCategorical,
    TorchSquashedGaussian,
)
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import (
    MIN_LOG_NN_OUTPUT,
    MAX_LOG_NN_OUTPUT,
    softmax,
    SMALL_NUMBER,
    LARGE_INTEGER,
)
from ray.rllib.utils.test_utils import check, framework_iterator

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


class TestDistributions(unittest.TestCase):
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
            if fw == "jax":
                sample_check = sample
            elif fw != "tf":
                sample_check = sample.numpy()
            else:
                sample_check = sess.run(sample)
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
            if fw == "jax":
                logp_check = logp
            elif fw != "tf":
                logp_check = logp.numpy()
            else:
                logp_check = sess.run(logp)
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

        for fw, sess in framework_iterator(session=True):
            # Create the correct distribution object.
            cls = (
                JAXCategorical
                if fw == "jax"
                else Categorical
                if fw != "torch"
                else TorchCategorical
            )
            categorical = cls(inputs, {})

            # Do a stability test using extreme NN outputs to see whether
            # sampling and logp'ing result in NaN or +/-inf values.
            self._stability_test(
                cls,
                inputs_space.shape,
                fw=fw,
                sess=sess,
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
            check(
                np.mean(out)
                if fw == "jax"
                else tf.reduce_mean(out)
                if fw != "torch"
                else torch.mean(out.float()),
                1.0,
                decimals=0,
            )

            # Test log-likelihood outputs.
            probs = softmax(inputs)
            values = values_space.sample()

            out = categorical.logp(values if fw != "torch" else torch.Tensor(values))
            expected = []
            for i in range(batch_size):
                expected.append(np.sum(np.log(np.array(probs[i][values[i]]))))
            check(out, expected, decimals=4)

            # Test entropy outputs.
            out = categorical.entropy()
            expected_entropy = -np.sum(probs * np.log(probs), -1)
            check(out, expected_entropy)

    def test_multi_categorical(self):
        batch_size = 100
        num_categories = 3
        num_sub_distributions = 5
        # Create 5 categorical distributions of 3 categories each.
        inputs_space = Box(
            -1.0, 2.0, shape=(batch_size, num_sub_distributions * num_categories)
        )
        inputs_space.seed(42)
        values_space = Box(
            0,
            num_categories - 1,
            shape=(num_sub_distributions, batch_size),
            dtype=np.int32,
        )
        values_space.seed(42)

        inputs = inputs_space.sample()
        input_lengths = [num_categories] * num_sub_distributions
        inputs_split = np.split(inputs, num_sub_distributions, axis=1)

        for fw, sess in framework_iterator(session=True):
            # Create the correct distribution object.
            cls = MultiCategorical if fw != "torch" else TorchMultiCategorical
            multi_categorical = cls(inputs, None, input_lengths)

            # Do a stability test using extreme NN outputs to see whether
            # sampling and logp'ing result in NaN or +/-inf values.
            self._stability_test(
                cls,
                inputs_space.shape,
                fw=fw,
                sess=sess,
                bounds=(0, num_categories - 1),
                extra_kwargs={"input_lens": input_lengths},
            )

            # Batch of size=3 and deterministic (True).
            expected = np.transpose(np.argmax(inputs_split, axis=-1))
            # Sample, expect always max value
            # (max likelihood for deterministic draw).
            out = multi_categorical.deterministic_sample()
            check(out, expected)

            # Batch of size=3 and non-deterministic -> expect roughly the mean.
            out = multi_categorical.sample()
            check(
                tf.reduce_mean(out) if fw != "torch" else torch.mean(out.float()),
                1.0,
                decimals=0,
            )

            # Test log-likelihood outputs.
            probs = softmax(inputs_split)
            values = values_space.sample()

            out = multi_categorical.logp(
                values
                if fw != "torch"
                else [torch.Tensor(values[i]) for i in range(num_sub_distributions)]
            )  # v in np.stack(values, 1)])
            expected = []
            for i in range(batch_size):
                expected.append(
                    np.sum(
                        np.log(
                            np.array(
                                [
                                    probs[j][i][values[j][i]]
                                    for j in range(num_sub_distributions)
                                ]
                            )
                        )
                    )
                )
            check(out, expected, decimals=4)

            # Test entropy outputs.
            out = multi_categorical.entropy()
            expected_entropy = -np.sum(np.sum(probs * np.log(probs), 0), -1)
            check(out, expected_entropy)

    def test_squashed_gaussian(self):
        """Tests the SquashedGaussian ActionDistribution for all frameworks."""
        input_space = Box(-2.0, 2.0, shape=(2000, 10))
        input_space.seed(42)

        low, high = -2.0, 1.0

        for fw, sess in framework_iterator(session=True):
            cls = SquashedGaussian if fw != "torch" else TorchSquashedGaussian

            # Do a stability test using extreme NN outputs to see whether
            # sampling and logp'ing result in NaN or +/-inf values.
            self._stability_test(
                cls, input_space.shape, fw=fw, sess=sess, bounds=(low, high)
            )

            # Batch of size=n and deterministic.
            inputs = input_space.sample()
            means, _ = np.split(inputs, 2, axis=-1)
            squashed_distribution = cls(inputs, {}, low=low, high=high)
            expected = ((np.tanh(means) + 1.0) / 2.0) * (high - low) + low
            # Sample n times, expect always mean value (deterministic draw).
            out = squashed_distribution.deterministic_sample()
            check(out, expected)

            # Batch of size=n and non-deterministic -> expect roughly the mean.
            inputs = input_space.sample()
            means, log_stds = np.split(inputs, 2, axis=-1)
            squashed_distribution = cls(inputs, {}, low=low, high=high)
            expected = ((np.tanh(means) + 1.0) / 2.0) * (high - low) + low
            values = squashed_distribution.sample()
            if sess:
                values = sess.run(values)
            else:
                values = values.numpy()
            self.assertTrue(np.max(values) <= high)
            self.assertTrue(np.min(values) >= low)

            check(np.mean(values), expected.mean(), decimals=1)

            # Test log-likelihood outputs.
            sampled_action_logp = squashed_distribution.logp(
                values if fw != "torch" else torch.Tensor(values)
            )
            if sess:
                sampled_action_logp = sess.run(sampled_action_logp)
            else:
                sampled_action_logp = sampled_action_logp.numpy()
            # Convert to parameters for distr.
            stds = np.exp(np.clip(log_stds, MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT))
            # Unsquash values, then get log-llh from regular gaussian.
            # atanh_in = np.clip((values - low) / (high - low) * 2.0 - 1.0,
            #   -1.0 + SMALL_NUMBER, 1.0 - SMALL_NUMBER)
            normed_values = (values - low) / (high - low) * 2.0 - 1.0
            save_normed_values = np.clip(
                normed_values, -1.0 + SMALL_NUMBER, 1.0 - SMALL_NUMBER
            )
            unsquashed_values = np.arctanh(save_normed_values)
            log_prob_unsquashed = np.sum(
                np.log(norm.pdf(unsquashed_values, means, stds)), -1
            )
            log_prob = log_prob_unsquashed - np.sum(
                np.log(1 - np.tanh(unsquashed_values) ** 2), axis=-1
            )
            check(np.sum(sampled_action_logp), np.sum(log_prob), rtol=0.05)

            # NN output.
            means = np.array(
                [[0.1, 0.2, 0.3, 0.4, 50.0], [-0.1, -0.2, -0.3, -0.4, -1.0]]
            )
            log_stds = np.array(
                [[0.8, -0.2, 0.3, -1.0, 2.0], [0.7, -0.3, 0.4, -0.9, 2.0]]
            )
            squashed_distribution = cls(
                inputs=np.concatenate([means, log_stds], axis=-1),
                model={},
                low=low,
                high=high,
            )
            # Convert to parameters for distr.
            stds = np.exp(log_stds)
            # Values to get log-likelihoods for.
            values = np.array(
                [[0.9, 0.2, 0.4, -0.1, -1.05], [-0.9, -0.2, 0.4, -0.1, -1.05]]
            )

            # Unsquash values, then get log-llh from regular gaussian.
            unsquashed_values = np.arctanh((values - low) / (high - low) * 2.0 - 1.0)
            log_prob_unsquashed = np.sum(
                np.log(norm.pdf(unsquashed_values, means, stds)), -1
            )
            log_prob = log_prob_unsquashed - np.sum(
                np.log(1 - np.tanh(unsquashed_values) ** 2), axis=-1
            )

            outs = squashed_distribution.logp(
                values if fw != "torch" else torch.Tensor(values)
            )
            if sess:
                outs = sess.run(outs)
            check(outs, log_prob, decimals=4)

    def test_diag_gaussian(self):
        """Tests the DiagGaussian ActionDistribution for all frameworks."""
        input_space = Box(-2.0, 1.0, shape=(2000, 10))
        input_space.seed(42)

        for fw, sess in framework_iterator(session=True):
            cls = DiagGaussian if fw != "torch" else TorchDiagGaussian

            # Do a stability test using extreme NN outputs to see whether
            # sampling and logp'ing result in NaN or +/-inf values.
            self._stability_test(cls, input_space.shape, fw=fw, sess=sess)

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
            if sess:
                values = sess.run(values)
            else:
                values = values.numpy()
            check(np.mean(values), expected.mean(), decimals=1)

            # Test log-likelihood outputs.
            sampled_action_logp = diag_distribution.logp(
                values if fw != "torch" else torch.Tensor(values)
            )
            if sess:
                sampled_action_logp = sess.run(sampled_action_logp)
            else:
                sampled_action_logp = sampled_action_logp.numpy()

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

            outs = diag_distribution.logp(
                values if fw != "torch" else torch.Tensor(values)
            )
            if sess:
                outs = sess.run(outs)
            check(outs, log_prob, decimals=4)

    def test_beta(self):
        input_space = Box(-2.0, 1.0, shape=(2000, 10))
        input_space.seed(42)
        low, high = -1.0, 2.0
        plain_beta_value_space = Box(0.0, 1.0, shape=(2000, 5))
        plain_beta_value_space.seed(42)

        for fw, sess in framework_iterator(session=True):
            cls = TorchBeta if fw == "torch" else Beta
            inputs = input_space.sample()
            beta_distribution = cls(inputs, {}, low=low, high=high)

            inputs = beta_distribution.inputs
            if sess:
                inputs = sess.run(inputs)
            else:
                inputs = inputs.numpy()
            alpha, beta_ = np.split(inputs, 2, axis=-1)

            # Mean for a Beta distribution: 1 / [1 + (beta/alpha)]
            expected = (1.0 / (1.0 + beta_ / alpha)) * (high - low) + low
            # Sample n times, expect always mean value (deterministic draw).
            out = beta_distribution.deterministic_sample()
            check(out, expected, rtol=0.01)

            # Batch of size=n and non-deterministic -> expect roughly the mean.
            values = beta_distribution.sample()
            if sess:
                values = sess.run(values)
            else:
                values = values.numpy()
            self.assertTrue(np.max(values) <= high)
            self.assertTrue(np.min(values) >= low)

            check(np.mean(values), expected.mean(), decimals=1)

            # Test log-likelihood outputs (against scipy).
            inputs = input_space.sample()
            beta_distribution = cls(inputs, {}, low=low, high=high)
            inputs = beta_distribution.inputs
            if sess:
                inputs = sess.run(inputs)
            else:
                inputs = inputs.numpy()
            alpha, beta_ = np.split(inputs, 2, axis=-1)

            values = plain_beta_value_space.sample()
            values_scaled = values * (high - low) + low
            if fw == "torch":
                values_scaled = torch.Tensor(values_scaled)
            print(values_scaled)
            out = beta_distribution.logp(values_scaled)
            check(out, np.sum(np.log(beta.pdf(values, alpha, beta_)), -1), rtol=0.01)

            # TODO(sven): Test entropy outputs (against scipy).

    def test_gumbel_softmax(self):
        """Tests the GumbelSoftmax ActionDistribution (tf + eager only)."""
        for fw, sess in framework_iterator(frameworks=("tf2", "tf"), session=True):
            batch_size = 1000
            num_categories = 5
            input_space = Box(-1.0, 1.0, shape=(batch_size, num_categories))
            input_space.seed(42)

            # Batch of size=n and deterministic.
            inputs = input_space.sample()
            gumbel_softmax = GumbelSoftmax(inputs, {}, temperature=1.0)

            expected = softmax(inputs)
            # Sample n times, expect always mean value (deterministic draw).
            out = gumbel_softmax.deterministic_sample()
            check(out, expected)

            # Batch of size=n and non-deterministic -> expect roughly that
            # the max-likelihood (argmax) ints are output (most of the time).
            inputs = input_space.sample()
            gumbel_softmax = GumbelSoftmax(inputs, {}, temperature=1.0)
            expected_mean = np.mean(np.argmax(inputs, -1)).astype(np.float32)
            outs = gumbel_softmax.sample()
            if sess:
                outs = sess.run(outs)
            check(np.mean(np.argmax(outs, -1)), expected_mean, rtol=0.08)

    def test_multi_action_distribution(self):
        """Tests the MultiActionDistribution (across all frameworks)."""
        batch_size = 1000
        input_space = Tuple(
            [
                Box(-10.0, 10.0, shape=(batch_size, 4)),
                Box(
                    -2.0,
                    2.0,
                    shape=(
                        batch_size,
                        6,
                    ),
                ),
                Dict({"a": Box(-1.0, 1.0, shape=(batch_size, 4))}),
            ]
        )
        input_space.seed(42)
        std_space = Box(
            -0.05,
            0.05,
            shape=(
                batch_size,
                3,
            ),
        )
        std_space.seed(42)

        low, high = -1.0, 1.0
        value_space = Tuple(
            [
                Box(0, 3, shape=(batch_size,), dtype=np.int32),
                Box(-2.0, 2.0, shape=(batch_size, 3), dtype=np.float32),
                Dict({"a": Box(0.0, 1.0, shape=(batch_size, 2), dtype=np.float32)}),
            ]
        )
        value_space.seed(42)

        for fw, sess in framework_iterator(session=True):
            if fw == "torch":
                cls = TorchMultiActionDistribution
                child_distr_cls = [
                    TorchCategorical,
                    TorchDiagGaussian,
                    partial(TorchBeta, low=low, high=high),
                ]
            else:
                cls = MultiActionDistribution
                child_distr_cls = [
                    Categorical,
                    DiagGaussian,
                    partial(Beta, low=low, high=high),
                ]

            inputs = list(input_space.sample())
            distr = cls(
                np.concatenate([inputs[0], inputs[1], inputs[2]["a"]], axis=1),
                model={},
                action_space=value_space,
                child_distributions=child_distr_cls,
                input_lens=[4, 6, 4],
            )

            # Adjust inputs for the Beta distr just as Beta itself does.
            inputs[2]["a"] = np.clip(
                inputs[2]["a"], np.log(SMALL_NUMBER), -np.log(SMALL_NUMBER)
            )
            inputs[2]["a"] = np.log(np.exp(inputs[2]["a"]) + 1.0) + 1.0
            # Sample deterministically.
            expected_det = [
                np.argmax(inputs[0], axis=-1),
                inputs[1][:, :3],  # [:3]=Mean values.
                # Mean for a Beta distribution:
                # 1 / [1 + (beta/alpha)] * range + low
                (1.0 / (1.0 + inputs[2]["a"][:, 2:] / inputs[2]["a"][:, 0:2]))
                * (high - low)
                + low,
            ]
            out = distr.deterministic_sample()
            if sess:
                out = sess.run(out)
            check(out[0], expected_det[0])
            check(out[1], expected_det[1])
            check(out[2]["a"], expected_det[2])

            # Stochastic sampling -> expect roughly the mean.
            inputs = list(input_space.sample())
            # Fix categorical inputs (not needed for distribution itself, but
            # for our expectation calculations).
            inputs[0] = softmax(inputs[0], -1)
            # Fix std inputs (shouldn't be too large for this test).
            inputs[1][:, 3:] = std_space.sample()
            # Adjust inputs for the Beta distr just as Beta itself does.
            inputs[2]["a"] = np.clip(
                inputs[2]["a"], np.log(SMALL_NUMBER), -np.log(SMALL_NUMBER)
            )
            inputs[2]["a"] = np.log(np.exp(inputs[2]["a"]) + 1.0) + 1.0
            distr = cls(
                np.concatenate([inputs[0], inputs[1], inputs[2]["a"]], axis=1),
                model={},
                action_space=value_space,
                child_distributions=child_distr_cls,
                input_lens=[4, 6, 4],
            )
            expected_mean = [
                np.mean(np.sum(inputs[0] * np.array([0, 1, 2, 3]), -1)),
                inputs[1][:, :3],  # [:3]=Mean values.
                # Mean for a Beta distribution:
                # 1 / [1 + (beta/alpha)] * range + low
                (1.0 / (1.0 + inputs[2]["a"][:, 2:] / inputs[2]["a"][:, :2]))
                * (high - low)
                + low,
            ]
            out = distr.sample()
            if sess:
                out = sess.run(out)
            out = list(out)
            if fw == "torch":
                out[0] = out[0].numpy()
                out[1] = out[1].numpy()
                out[2]["a"] = out[2]["a"].numpy()
            check(np.mean(out[0]), expected_mean[0], decimals=1)
            check(np.mean(out[1], 0), np.mean(expected_mean[1], 0), decimals=1)
            check(np.mean(out[2]["a"], 0), np.mean(expected_mean[2], 0), decimals=1)

            # Test log-likelihood outputs.
            # Make sure beta-values are within 0.0 and 1.0 for the numpy
            # calculation (which doesn't have scaling).
            inputs = list(input_space.sample())
            # Adjust inputs for the Beta distr just as Beta itself does.
            inputs[2]["a"] = np.clip(
                inputs[2]["a"], np.log(SMALL_NUMBER), -np.log(SMALL_NUMBER)
            )
            inputs[2]["a"] = np.log(np.exp(inputs[2]["a"]) + 1.0) + 1.0
            distr = cls(
                np.concatenate([inputs[0], inputs[1], inputs[2]["a"]], axis=1),
                model={},
                action_space=value_space,
                child_distributions=child_distr_cls,
                input_lens=[4, 6, 4],
            )
            inputs[0] = softmax(inputs[0], -1)
            values = list(value_space.sample())
            log_prob_beta = np.log(
                beta.pdf(values[2]["a"], inputs[2]["a"][:, :2], inputs[2]["a"][:, 2:])
            )
            # Now do the up-scaling for [2] (beta values) to be between
            # low/high.
            values[2]["a"] = values[2]["a"] * (high - low) + low
            inputs[1][:, 3:] = np.exp(inputs[1][:, 3:])
            expected_log_llh = np.sum(
                np.concatenate(
                    [
                        np.expand_dims(
                            np.log([i[values[0][j]] for j, i in enumerate(inputs[0])]),
                            -1,
                        ),
                        np.log(norm.pdf(values[1], inputs[1][:, :3], inputs[1][:, 3:])),
                        log_prob_beta,
                    ],
                    -1,
                ),
                -1,
            )

            values[0] = np.expand_dims(values[0], -1)
            if fw == "torch":
                values = tree.map_structure(lambda s: torch.Tensor(s), values)
            # Test all flattened input.
            concat = np.concatenate(tree.flatten(values), -1).astype(np.float32)
            out = distr.logp(concat)
            if sess:
                out = sess.run(out)
            check(out, expected_log_llh, atol=15)
            # Test structured input.
            out = distr.logp(values)
            if sess:
                out = sess.run(out)
            check(out, expected_log_llh, atol=15)
            # Test flattened input.
            out = distr.logp(tree.flatten(values))
            if sess:
                out = sess.run(out)
            check(out, expected_log_llh, atol=15)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
