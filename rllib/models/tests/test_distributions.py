import numpy as np
from gym.spaces import Box
from scipy.stats import norm
from tensorflow.python.eager.context import eager_mode
import unittest

from ray.rllib.models.tf.tf_action_dist import Categorical, SquashedGaussian
from ray.rllib.utils import try_import_tf
from ray.rllib.utils.numpy import MIN_LOG_NN_OUTPUT, MAX_LOG_NN_OUTPUT
from ray.rllib.utils.test_utils import check

tf = try_import_tf()


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

    def test_squashed_gaussian(self):
        """Tests the SquashedGaussia ActionDistribution (tf-eager only)."""
        with eager_mode():
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
            self.assertTrue(np.max(values) < high)
            self.assertTrue(np.min(values) > low)

            check(np.mean(values), expected.mean(), decimals=1)

            # Test log-likelihood outputs.
            sampled_action_logp = squashed_distribution.sampled_action_logp()
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

            out = squashed_distribution.logp(values)
            check(out, log_prob)


if __name__ == "__main__":
    import unittest
    unittest.main(verbosity=1)
