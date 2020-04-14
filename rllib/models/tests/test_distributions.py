from functools import partial
import numpy as np
from gym.spaces import Box, Tuple
from scipy.stats import beta, norm
import unittest

from ray.rllib.models.tf.tf_action_dist import Beta, Categorical, \
    DiagGaussian, GumbelSoftmax, MultiActionDistribution, MultiCategorical, \
    SquashedGaussian
from ray.rllib.models.torch.torch_action_dist import TorchMultiCategorical, \
    TorchCategorical, TorchDiagGaussian, TorchBeta, \
    TorchMultiActionDistribution
from ray.rllib.utils import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT, \
    SMALL_NUMBER, softmax
from ray.rllib.utils.test_utils import check, framework_iterator

tf = try_import_tf()
torch, _ = try_import_torch()


class TestDistributions(unittest.TestCase):
    """Tests ActionDistribution classes."""

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
        """Tests the SquashedGaussia ActionDistribution (tf-eager only)."""
        for fw, sess in framework_iterator(
                frameworks=["tf", "eager"], session=True):
            input_space = Box(-1.0, 1.0, shape=(200, 10))
            low, high = -2.0, 1.0

            # Batch of size=n and deterministic.
            inputs = input_space.sample()
            means, _ = np.split(inputs, 2, axis=-1)
            squashed_distribution = SquashedGaussian(
                inputs, {}, low=low, high=high)
            expected = ((np.tanh(means) + 1.0) / 2.0) * (high - low) + low
            # Sample n times, expect always mean value (deterministic draw).
            out = squashed_distribution.deterministic_sample()
            check(out, expected)

            # Batch of size=n and non-deterministic -> expect roughly the mean.
            inputs = input_space.sample()
            means, log_stds = np.split(inputs, 2, axis=-1)
            squashed_distribution = SquashedGaussian(
                inputs, {}, low=low, high=high)
            expected = ((np.tanh(means) + 1.0) / 2.0) * (high - low) + low
            values = squashed_distribution.sample()
            if sess:
                values = sess.run(values)
            self.assertTrue(np.max(values) < high)
            self.assertTrue(np.min(values) > low)

            check(np.mean(values), expected.mean(), decimals=1)

            # Test log-likelihood outputs.
            sampled_action_logp = squashed_distribution.logp(values)
            if sess:
                sampled_action_logp = sess.run(sampled_action_logp)
            # Convert to parameters for distr.
            stds = np.exp(
                np.clip(log_stds, MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT))
            # Unsquash values, then get log-llh from regular gaussian.
            unsquashed_values = np.arctanh((values - low) /
                                           (high - low) * 2.0 - 1.0)
            log_prob_unsquashed = \
                np.sum(np.log(norm.pdf(unsquashed_values, means, stds)), -1)
            log_prob = log_prob_unsquashed - \
                np.sum(np.log(1 - np.tanh(unsquashed_values) ** 2),
                       axis=-1)
            check(np.mean(sampled_action_logp), np.mean(log_prob), rtol=0.01)

            # NN output.
            means = np.array([[0.1, 0.2, 0.3, 0.4, 50.0],
                              [-0.1, -0.2, -0.3, -0.4, -1.0]])
            log_stds = np.array([[0.8, -0.2, 0.3, -1.0, 2.0],
                                 [0.7, -0.3, 0.4, -0.9, 2.0]])
            squashed_distribution = SquashedGaussian(
                np.concatenate([means, log_stds], axis=-1), {},
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

            outs = squashed_distribution.logp(values)
            if sess:
                outs = sess.run(outs)
            check(outs, log_prob)

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
        """Tests the MultiActionDistribution (across all frameworks)."""
        batch_size = 1000
        input_space = Tuple([
            Box(-10.0, 10.0, shape=(batch_size, 4)),
            Box(-2.0, 2.0, shape=(batch_size, 6,)),
            Box(-1.0, 1.0, shape=(batch_size, 4)),
        ])
        std_space = Box(-0.05, 0.05, shape=(batch_size, 3,))

        low, high = -1.0, 1.0
        values_space = Tuple([
            Box(0, 3, shape=(batch_size, ), dtype=np.int32),
            Box(-2.0, 2.0, shape=(batch_size, 3)),
            Box(0.0, 1.0, shape=(batch_size, 2)),
        ])

        for fw, sess in framework_iterator(session=True):
            if fw == "torch":
                cls = TorchMultiActionDistribution
                child_distr_cls = [TorchCategorical, TorchDiagGaussian,
                                   partial(TorchBeta, low=low, high=high)]
            else:
                cls = MultiActionDistribution
                child_distr_cls = [Categorical, DiagGaussian,
                                   partial(Beta, low=low, high=high)]

            inputs = list(input_space.sample())
            distr = cls(np.concatenate(inputs, axis=1), {}, None,
                        child_distributions=child_distr_cls,
                        input_lens=[4, 6, 4])

            # Adjust inputs for the Beta distr just as Beta itself does.
            inputs[2] = np.clip(
                inputs[2], np.log(SMALL_NUMBER), -np.log(SMALL_NUMBER))
            inputs[2] = np.log(np.exp(inputs[2]) + 1.0) + 1.0
            # Sample deterministically.
            expected_det = [
                np.argmax(inputs[0], axis=-1),
                inputs[1][:,:3],  # [:3]=Mean values.
                # Mean for a Beta distribution: 1 / [1 + (beta/alpha)] * range + low
                (1.0 / (1.0 + inputs[2][:,2:] / inputs[2][:,0:2])) * (high - low) + low,
            ]
            out = distr.deterministic_sample()
            if sess:
                out = sess.run(out)
            check(out.batches[0], expected_det[0])
            check(out.batches[1], expected_det[1])
            check(out.batches[2], expected_det[2])

            # Stochastic sampling -> expect roughly the mean.
            inputs = list(input_space.sample())
            # Fix categorical inputs (not needed for distribution itself, but
            # for our expectation calculations).
            inputs[0] = softmax(inputs[0], -1)
            # Fix std inputs (shouldn't be too large for this test).
            inputs[1][:,3:] = std_space.sample()
            # Adjust inputs for the Beta distr just as Beta itself does.
            inputs[2] = np.clip(
                inputs[2], np.log(SMALL_NUMBER), -np.log(SMALL_NUMBER))
            inputs[2] = np.log(np.exp(inputs[2]) + 1.0) + 1.0
            distr = cls(np.concatenate(inputs, axis=1), {}, None,
                        child_distributions=child_distr_cls,
                        input_lens=[4, 6, 4])
            expected_mean = [
                np.mean(np.sum(inputs[0] * np.array([0, 1, 2, 3]), -1)),
                inputs[1][:,:3],  # [:3]=Mean values.
                # Mean for a Beta distribution: 1 / [1 + (beta/alpha)] * range + low
                (1.0 / (1.0 + inputs[2][:,2:] / inputs[2][:,:2])) * (high - low) + low,
            ]
            out = distr.sample()
            if sess:
                out = sess.run(out)
            if fw == "torch":
                out.batches[0] = out.batches[0].numpy()
                out.batches[1] = out.batches[1].numpy()
                out.batches[2] = out.batches[2].numpy()
            check(np.mean(out.batches[0]), expected_mean[0], decimals=1)
            check(np.mean(out.batches[1], 0), np.mean(expected_mean[1], 0), decimals=1)
            check(np.mean(out.batches[2], 0), np.mean(expected_mean[2], 0), decimals=1)

            # Test log-likelihood outputs.
            # Make sure beta-values are within 0.0 and 1.0 for the numpy
            # calculation (which doesn't have scaling).
            inputs = list(input_space.sample())
            # Adjust inputs for the Beta distr just as Beta itself does.
            inputs[2] = np.clip(
                inputs[2], np.log(SMALL_NUMBER), -np.log(SMALL_NUMBER))
            inputs[2] = np.log(np.exp(inputs[2]) + 1.0) + 1.0
            distr = cls(np.concatenate(inputs, axis=1), {}, None,
                        child_distributions=child_distr_cls,
                        input_lens=[4, 6, 4])
            inputs[0] = softmax(inputs[0], -1)
            values = list(values_space.sample())
            log_prob_beta = np.log(beta.pdf(values[2], inputs[2][:,:2], inputs[2][:,2:]))
            # Now do the up-scaling for [2] (beta values) to be between
            # low/high.
            values[2] = values[2] * (high - low) + low
            inputs[1][:,3:] = np.exp(inputs[1][:,3:])
            expected_log_llh = np.sum(np.concatenate([
                np.expand_dims(np.log([i[values[0][j]] for j, i in enumerate(inputs[0])]), -1),
                np.log(norm.pdf(values[1], inputs[1][:,:3], inputs[1][:,3:])),
                log_prob_beta], -1), -1)

            values[0] = np.expand_dims(values[0], -1)
            values = np.concatenate(values, axis=1).astype(np.float32)
            if fw == "torch":
                values = torch.Tensor(values)
            out = distr.logp(values)
            if sess:
                out = sess.run(out)
            check(out, expected_log_llh, atol=15)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
