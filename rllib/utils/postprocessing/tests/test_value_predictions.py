import unittest

import numpy as np

from ray.rllib.utils.postprocessing.value_predictions import (
    compute_value_targets,
    compute_value_targets_with_bootstrap,
    extract_bootstrapped_values,
)
from ray.rllib.utils.test_utils import check


class TestComputeValueTargetsWithBootstrap(unittest.TestCase):
    """Tests for compute_value_targets_with_bootstrap."""

    def test_terminated_episode_lambda1(self):
        """Lambda=1 + terminated episode: targets equal discounted returns."""
        values = np.array([1.0, 2.0, 3.0])
        rewards = np.array([10.0, 20.0, 30.0])
        terminateds = np.array([False, False, True])
        bootstrap = 0.0
        gamma = 0.9
        lambda_ = 1.0

        result = compute_value_targets_with_bootstrap(
            values, rewards, terminateds, bootstrap, gamma, lambda_
        )
        # Hand-computed discounted returns (lambda=1):
        # G_2 = 30                           = 30.0
        # G_1 = 20 + 0.9 * 30               = 47.0
        # G_0 = 10 + 0.9 * 47               = 52.3
        np.testing.assert_allclose(result, [52.3, 47.0, 30.0], atol=1e-5)

    def test_truncated_episode_with_bootstrap(self):
        """Truncated episode: bootstrap value propagates through GAE."""
        values = np.array([1.0, 2.0, 3.0])
        rewards = np.array([10.0, 20.0, 30.0])
        terminateds = np.array([False, False, False])
        bootstrap = 4.0
        gamma = 0.9
        lambda_ = 1.0

        result = compute_value_targets_with_bootstrap(
            values, rewards, terminateds, bootstrap, gamma, lambda_
        )
        # Hand-computed discounted returns (lambda=1):
        # G_2 = 30 + 0.9 * 4.0              = 33.6
        # G_1 = 20 + 0.9 * 33.6             = 50.24
        # G_0 = 10 + 0.9 * 50.24            = 55.216
        np.testing.assert_allclose(result, [55.216, 50.24, 33.6], atol=1e-5)

    def test_lambda0_gives_one_step_td_targets(self):
        """Lambda=0: targets collapse to one-step TD targets r + gamma*V(next)."""
        values = np.array([1.0, 2.0, 3.0])
        rewards = np.array([10.0, 20.0, 30.0])
        terminateds = np.array([False, False, False])
        bootstrap = 4.0
        gamma = 0.9
        lambda_ = 0.0

        result = compute_value_targets_with_bootstrap(
            values, rewards, terminateds, bootstrap, gamma, lambda_
        )
        # target_t = r_t + gamma * V(s_{t+1})   (lambda=0 => no GAE propagation)
        # target_0 = 10 + 0.9 * 2  = 11.8
        # target_1 = 20 + 0.9 * 3  = 22.7
        # target_2 = 30 + 0.9 * 4  = 33.6
        np.testing.assert_allclose(result, [11.8, 22.7, 33.6], atol=1e-5)

    def test_gamma0_gives_raw_rewards(self):
        """Gamma=0: no discounting, targets equal raw rewards."""
        values = np.array([1.0, 2.0, 3.0])
        rewards = np.array([10.0, 20.0, 30.0])
        terminateds = np.array([False, False, False])
        bootstrap = 100.0  # should be irrelevant
        gamma = 0.0
        lambda_ = 0.95

        result = compute_value_targets_with_bootstrap(
            values, rewards, terminateds, bootstrap, gamma, lambda_
        )
        np.testing.assert_allclose(result, [10.0, 20.0, 30.0], atol=1e-5)

    def test_single_timestep_terminated(self):
        """Single terminated timestep: target = reward."""
        values = np.array([5.0])
        rewards = np.array([10.0])
        terminateds = np.array([True])
        bootstrap = 0.0
        gamma = 0.99
        lambda_ = 0.95

        result = compute_value_targets_with_bootstrap(
            values, rewards, terminateds, bootstrap, gamma, lambda_
        )
        np.testing.assert_allclose(result, [10.0], atol=1e-5)

    def test_single_timestep_truncated(self):
        """Single truncated timestep: target = r + gamma * bootstrap."""
        values = np.array([5.0])
        rewards = np.array([10.0])
        terminateds = np.array([False])
        bootstrap = 3.0
        gamma = 0.99
        lambda_ = 0.95

        result = compute_value_targets_with_bootstrap(
            values, rewards, terminateds, bootstrap, gamma, lambda_
        )
        # delta = 10 + 0.99*3 - 5 = 7.97;  A = 7.97;  target = 12.97
        np.testing.assert_allclose(result, [12.97], atol=1e-5)

    def test_intermediate_lambda(self):
        """GAE with 0 < lambda < 1 matches the reference implementation."""
        values = np.array([1.0, 2.0, 3.0, 4.0])
        rewards = np.array([0.5, 1.5, 2.5, 3.5])
        terminateds = np.array([False, False, False, True])
        bootstrap = 0.0
        gamma = 0.99
        lambda_ = 0.95

        result = compute_value_targets_with_bootstrap(
            values, rewards, terminateds, bootstrap, gamma, lambda_
        )
        np.testing.assert_allclose(result, [1.0, 2.0, 3.0, 4.0], atol=1e-5)

    def test_all_zeros(self):
        """Zero values, rewards, and bootstrap should give zero targets."""
        T = 5
        result = compute_value_targets_with_bootstrap(
            values=np.zeros(T),
            rewards=np.zeros(T),
            terminateds=np.zeros(T, dtype=bool),
            bootstrap_value=0.0,
            gamma=0.99,
            lambda_=0.95,
        )
        np.testing.assert_allclose(result, np.zeros(T), atol=1e-7)

    def test_matches_old_function_terminated_lambda1(self):
        """For terminated episodes with lambda=1 both functions must agree.

        With lambda=1 the (1-lambda)*V term vanishes, so the value-zeroing
        difference in the old function does not matter.
        """
        values = np.array([1.0, 2.0, 3.0, 4.0, 5.0])
        rewards = np.array([0.5, 1.5, 2.5, 3.5, 4.5])
        terminateds = np.array([0.0, 0.0, 0.0, 0.0, 1.0])
        truncateds = np.zeros(5)
        gamma = 0.99
        lambda_ = 1.0

        old_targets = compute_value_targets(
            values, rewards, terminateds, truncateds, gamma, lambda_
        )
        new_targets = compute_value_targets_with_bootstrap(
            values, rewards, terminateds, 0.0, gamma, lambda_
        )
        np.testing.assert_allclose(new_targets, old_targets, atol=1e-5)

    def test_shape_preserved(self):
        """Output shape must equal input shape."""
        for T in [1, 5, 20]:
            result = compute_value_targets_with_bootstrap(
                values=np.ones(T),
                rewards=np.ones(T),
                terminateds=np.zeros(T, dtype=bool),
                bootstrap_value=1.0,
                gamma=0.99,
                lambda_=0.95,
            )
            self.assertEqual(result.shape, (T,))


class TestExtractBootstrappedValues(unittest.TestCase):
    def test_extract_bootstrapped_values(self):
        """Tests, whether the extract_bootstrapped_values utility works properly."""

        # Fake vf_preds sequence.
        # Spaces = denote (elongated-by-one-artificial-ts) episode boundaries.
        # digits = timesteps within the actual episode.
        # [lower case letters] = bootstrap values at episode truncations.
        # '-' = bootstrap values at episode terminals (these values are simply zero).
        sequence = "012345678a 01234A 0- 0123456b 01c 012- 012345e 012-"
        sequence = sequence.replace(" ", "")
        sequence = list(sequence)
        # The actual, non-elongated, episode lengths.
        episode_lengths = [9, 5, 1, 7, 2, 3, 6, 3]
        T = 4
        result = extract_bootstrapped_values(
            vf_preds=sequence,
            episode_lengths=episode_lengths,
            T=T,
        )
        check(result, [4, 8, 3, 1, 5, "c", 1, 5, "-"])

        # Another example.
        sequence = "0123a 012345b 01234567- 012- 012- 012- 012345- 0123456c"
        sequence = sequence.replace(" ", "")
        sequence = list(sequence)
        episode_lengths = [4, 6, 8, 3, 3, 3, 6, 7]
        T = 5
        result = extract_bootstrapped_values(
            vf_preds=sequence,
            episode_lengths=episode_lengths,
            T=T,
        )
        check(result, [1, "b", 5, 2, 1, 3, 2, "c"])


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
