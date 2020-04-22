import numpy as np
from gym.spaces import Box, Tuple
from scipy.stats import norm, beta
import unittest

from ray.rllib.models.tf.tf_action_dist import Categorical, \
    DiagGaussian, GumbelSoftmax, MultiActionDistribution, MultiCategorical, \
    SquashedGaussian
from ray.rllib.models.torch.torch_action_dist import TorchMultiCategorical, \
    TorchSquashedGaussian, TorchBeta, TorchCategorical, \
    TorchMultiActionDistribution, TorchDiagGaussian
from ray.rllib.utils import try_import_tree
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT, \
    softmax, SMALL_NUMBER, LARGE_INTEGER
from ray.rllib.utils.test_utils import check, framework_iterator

tf = try_import_tf()
torch, _ = try_import_torch()
tree = try_import_tree()


class TestDistributions(unittest.TestCase):
    """Tests ActionDistribution classes."""

    def _stability_test(self,
                        distribution_cls,
                        network_output_shape,
                        fw,
                        sess=None,
                        bounds=None):
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
            for num in range(len(inputs[batch_item])):
                inputs[batch_item][num] = np.random.choice(extreme_values)
        dist = distribution_cls(inputs, {})
        for _ in range(100):
            sample = dist.sample()
            if fw != "tf":
                sample_check = sample.numpy()
            else:
                sample_check = sess.run(sample)
            assert not np.any(np.isnan(sample_check))
            assert np.all(np.isfinite(sample_check))
            if bounds:
                assert np.min(sample_check) >= bounds[0]
                assert np.max(sample_check) <= bounds[1]
            logp = dist.logp(sample)
            if fw != "tf":
                logp_check = logp.numpy()
            else:
                logp_check = sess.run(logp)
            assert not np.any(np.isnan(logp_check))
            assert np.all(np.isfinite(logp_check))

    def test_categorical(self):
        """Tests the Categorical ActionDistribution (tf only)."""
        num_samples = 100000
        logits = tf.placeholder(tf.float32, shape=(None, 10))
        z = 8 * (np.random.rand(10) - 0.5)
        data = np.tile(z, (num_samples, 1))
        c = Categorical(logits, {})  # dummy config dict
        sample_op = c.sample()
        sess = tf.Session()
        sess.run(tf.global_variables_initializer())
        samples = sess.run(sample_op, feed_dict={logits: data})
        counts = np.zeros(10)
        for sample in samples:
            counts[sample] += 1.0
        probs = np.exp(z) / np.sum(np.exp(z))
        self.assertTrue(np.sum(np.abs(probs - counts / num_samples)) <= 0.01)

    def test_multi_categorical(self):
        batch_size = 100
        num_categories = 3
        num_sub_distributions = 5
        # Create 5 categorical distributions of 3 categories each.
        inputs_space = Box(
            -1.0,
            2.0,
            shape=(batch_size, num_sub_distributions * num_categories))
        values_space = Box(
            0,
            num_categories - 1,
            shape=(num_sub_distributions, batch_size),
            dtype=np.int32)

        inputs = inputs_space.sample()
        input_lengths = [num_categories] * num_sub_distributions
        inputs_split = np.split(inputs, num_sub_distributions, axis=1)

        for fw in framework_iterator():
            # Create the correct distribution object.
            cls = MultiCategorical if fw != "torch" else TorchMultiCategorical
            multi_categorical = cls(inputs, None, input_lengths)

            # Batch of size=3 and deterministic (True).
            expected = np.transpose(np.argmax(inputs_split, axis=-1))
            # Sample, expect always max value
            # (max likelihood for deterministic draw).
            out = multi_categorical.deterministic_sample()
            check(out, expected)

            # Batch of size=3 and non-deterministic -> expect roughly the mean.
            out = multi_categorical.sample()
            check(
                tf.reduce_mean(out)
                if fw != "torch" else torch.mean(out.float()),
                1.0,
                decimals=0)

            # Test log-likelihood outputs.
            probs = softmax(inputs_split)
            values = values_space.sample()

            out = multi_categorical.logp(values if fw != "torch" else [
                torch.Tensor(values[i]) for i in range(num_sub_distributions)
            ])  # v in np.stack(values, 1)])
            expected = []
            for i in range(batch_size):
                expected.append(
                    np.sum(
                        np.log(
                            np.array([
                                probs[j][i][values[j][i]]
                                for j in range(num_sub_distributions)
                            ]))))
            check(out, expected, decimals=4)

            # Test entropy outputs.
            out = multi_categorical.entropy()
            expected_entropy = -np.sum(np.sum(probs * np.log(probs), 0), -1)
            check(out, expected_entropy)

    def test_squashed_gaussian(self):
        """Tests the SquashedGaussian ActionDistribution for all frameworks."""
        input_space = Box(-2.0, 2.0, shape=(200, 10))
        low, high = -2.0, 1.0

        for fw, sess in framework_iterator(
                frameworks=("torch", "tf", "eager"), session=True):
            cls = SquashedGaussian if fw != "torch" else TorchSquashedGaussian

            # Do a stability test using extreme NN outputs to see whether
            # sampling and logp'ing result in NaN or +/-inf values.
            self._stability_test(
                cls, input_space.shape, fw=fw, sess=sess, bounds=(low, high))

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
                values if fw != "torch" else torch.Tensor(values))
            if sess:
                sampled_action_logp = sess.run(sampled_action_logp)
            else:
                sampled_action_logp = sampled_action_logp.numpy()
            # Convert to parameters for distr.
            stds = np.exp(
                np.clip(log_stds, MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT))
            # Unsquash values, then get log-llh from regular gaussian.
            # atanh_in = np.clip((values - low) / (high - low) * 2.0 - 1.0,
            #   -1.0 + SMALL_NUMBER, 1.0 - SMALL_NUMBER)
            normed_values = (values - low) / (high - low) * 2.0 - 1.0
            save_normed_values = np.clip(normed_values, -1.0 + SMALL_NUMBER,
                                         1.0 - SMALL_NUMBER)
            unsquashed_values = np.arctanh(save_normed_values)
            log_prob_unsquashed = np.sum(
                np.log(norm.pdf(unsquashed_values, means, stds)), -1)
            log_prob = log_prob_unsquashed - \
                np.sum(np.log(1 - np.tanh(unsquashed_values) ** 2),
                       axis=-1)
            check(np.sum(sampled_action_logp), np.sum(log_prob), rtol=0.05)

            # NN output.
            means = np.array([[0.1, 0.2, 0.3, 0.4, 50.0],
                              [-0.1, -0.2, -0.3, -0.4, -1.0]])
            log_stds = np.array([[0.8, -0.2, 0.3, -1.0, 2.0],
                                 [0.7, -0.3, 0.4, -0.9, 2.0]])
            squashed_distribution = cls(
                inputs=np.concatenate([means, log_stds], axis=-1),
                model={},
                low=low,
                high=high)
            # Convert to parameters for distr.
            stds = np.exp(log_stds)
            # Values to get log-likelihoods for.
            values = np.array([[0.9, 0.2, 0.4, -0.1, -1.05],
                               [-0.9, -0.2, 0.4, -0.1, -1.05]])

            # Unsquash values, then get log-llh from regular gaussian.
            unsquashed_values = np.arctanh((values - low) /
                                           (high - low) * 2.0 - 1.0)
            log_prob_unsquashed = \
                np.sum(np.log(norm.pdf(unsquashed_values, means, stds)), -1)
            log_prob = log_prob_unsquashed - \
                np.sum(np.log(1 - np.tanh(unsquashed_values) ** 2),
                       axis=-1)

            outs = squashed_distribution.logp(values if fw != "torch" else
                                              torch.Tensor(values))
            if sess:
                outs = sess.run(outs)
            check(outs, log_prob, decimals=4)

    def test_beta(self):
        input_space = Box(-2.0, 1.0, shape=(200, 10))
        low, high = -1.0, 2.0
        plain_beta_value_space = Box(0.0, 1.0, shape=(200, 5))

        for fw, sess in framework_iterator(frameworks="torch", session=True):
            cls = TorchBeta
            inputs = input_space.sample()
            beta_distribution = cls(inputs, {}, low=low, high=high)

            inputs = beta_distribution.inputs
            alpha, beta_ = np.split(inputs.numpy(), 2, axis=-1)

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
            alpha, beta_ = np.split(inputs.numpy(), 2, axis=-1)

            values = plain_beta_value_space.sample()
            values_scaled = values * (high - low) + low
            out = beta_distribution.logp(torch.Tensor(values_scaled))
            check(
                out,
                np.sum(np.log(beta.pdf(values, alpha, beta_)), -1),
                rtol=0.001)

            # TODO(sven): Test entropy outputs (against scipy).

    def test_gumbel_softmax(self):
        """Tests the GumbelSoftmax ActionDistribution (tf + eager only)."""
        for fw, sess in framework_iterator(
                frameworks=["tf", "eager"], session=True):
            batch_size = 1000
            num_categories = 5
            input_space = Box(-1.0, 1.0, shape=(batch_size, num_categories))

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
        """Tests the MultiActionDistribution (only torch so far)."""
        batch_size = 1000
        input_space = Tuple([
            Box(-10.0, 10.0, shape=(batch_size, 4)),
            Box(-2.0, 2.0, shape=(
                batch_size,
                6,
            ))
        ])
        std_space = Box(
            -0.05, 0.05, shape=(
                batch_size,
                3,
            ))

        value_space = Tuple([
            Box(0, 3, shape=(batch_size, ), dtype=np.int32),
            Box(-2.0, 2.0, shape=(batch_size, 3), dtype=np.float32)
        ])

        for fw, sess in framework_iterator(frameworks="torch", session=True):
            if fw == "torch":
                cls = TorchMultiActionDistribution
                child_distr_cls = [TorchCategorical, TorchDiagGaussian]
            else:
                cls = MultiActionDistribution
                child_distr_cls = [Categorical, DiagGaussian]

            inputs = list(input_space.sample())
            distr = cls(
                np.concatenate([inputs[0], inputs[1]], axis=1),
                model={},
                action_space=value_space,
                child_distributions=child_distr_cls,
                input_lens=[4, 6])

            # Sample deterministically.
            expected_det = [
                np.argmax(inputs[0], axis=-1),
                inputs[1][:, :3],  # [:3]=Mean values.
            ]
            out = distr.deterministic_sample()
            if sess:
                out = sess.run(out)
            check(out[0], expected_det[0])
            check(out[1], expected_det[1])

            # Stochastic sampling -> expect roughly the mean.
            inputs = list(input_space.sample())
            # Fix categorical inputs (not needed for distribution itself, but
            # for our expectation calculations).
            inputs[0] = softmax(inputs[0], -1)
            # Fix std inputs (shouldn't be too large for this test).
            inputs[1][:, 3:] = std_space.sample()
            distr = cls(
                np.concatenate([inputs[0], inputs[1]], axis=1),
                model={},
                action_space=value_space,
                child_distributions=child_distr_cls,
                input_lens=[4, 6])
            expected_mean = [
                np.mean(np.sum(inputs[0] * np.array([0, 1, 2, 3]), -1)),
                inputs[1][:, :3],  # [:3]=Mean values.
            ]
            out = distr.sample()
            if sess:
                out = sess.run(out)
            out = list(out)
            if fw == "torch":
                out[0] = out[0].numpy()
                out[1] = out[1].numpy()
            check(np.mean(out[0]), expected_mean[0], decimals=1)
            check(np.mean(out[1], 0), np.mean(expected_mean[1], 0), decimals=1)

            # Test log-likelihood outputs.
            # Make sure beta-values are within 0.0 and 1.0 for the numpy
            # calculation (which doesn't have scaling).
            inputs = list(input_space.sample())
            distr = cls(
                np.concatenate([inputs[0], inputs[1]], axis=1),
                model={},
                action_space=value_space,
                child_distributions=child_distr_cls,
                input_lens=[4, 6])
            inputs[0] = softmax(inputs[0], -1)
            values = list(value_space.sample())
            inputs[1][:, 3:] = np.exp(inputs[1][:, 3:])
            expected_log_llh = np.sum(
                np.concatenate([
                    np.expand_dims(
                        np.log(
                            [i[values[0][j]]
                             for j, i in enumerate(inputs[0])]), -1),
                    np.log(
                        norm.pdf(values[1], inputs[1][:, :3],
                                 inputs[1][:, 3:]))
                ], -1), -1)

            values[0] = np.expand_dims(values[0], -1)
            if fw == "torch":
                values = tree.map_structure(lambda s: torch.Tensor(s), values)
            # Test all flattened input.
            concat = np.concatenate(tree.flatten(values),
                                    -1).astype(np.float32)
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
