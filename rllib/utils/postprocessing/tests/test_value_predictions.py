import numpy as np

from ray.rllib.utils.postprocessing.value_predictions import (
    compute_value_targets_with_bootstrap,
)
from ray.rllib.utils.test_utils import check


class TestComputeValueTargetsWithBootstrap:
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
        check(result, [52.3, 47.0, 30.0], atol=1e-5)

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
        check(result, [55.216, 50.24, 33.6], atol=1e-5)

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
        check(result, [11.8, 22.7, 33.6], atol=1e-5)

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
        check(result, [10.0, 20.0, 30.0], atol=1e-5)

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
        check(result, [10.0], atol=1e-5)

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
        check(result, [12.97], atol=1e-5)

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
        # Hand-computed GAE targets (gamma=0.99, lambda=0.95, terminated at t=3):
        #
        # TD deltas: delta_t = r_t + gamma * V(t+1) * (1 - term_t) - V(t)
        #   delta_3 = 3.5 + 0.99 * 0.0 * (1-1) - 4.0     = -0.5
        #   delta_2 = 2.5 + 0.99 * 4.0 * (1-0) - 3.0     =  3.46
        #   delta_1 = 1.5 + 0.99 * 3.0 * (1-0) - 2.0     =  2.47
        #   delta_0 = 0.5 + 0.99 * 2.0 * (1-0) - 1.0     =  1.48
        #
        # GAE advantages: A_t = delta_t + gamma * lambda * (1 - term_t) * A_{t+1}
        #   A_3 = -0.5
        #   A_2 = 3.46  + 0.99 * 0.95 * 1 * (-0.5)       =  2.98975
        #   A_1 = 2.47  + 0.99 * 0.95 * 1 * 2.98975      =  5.281860
        #   A_0 = 1.48  + 0.99 * 0.95 * 1 * 5.281860     =  6.447589
        #
        # Value targets: target_t = A_t + V(t)
        #   target_3 = -0.5     + 4.0 =  3.5
        #   target_2 =  2.98975 + 3.0 =  5.98975
        #   target_1 =  5.28186 + 2.0 =  7.28186
        #   target_0 =  6.44759 + 1.0 =  7.44759
        check(result, [7.447589, 7.281860, 5.989750, 3.5], atol=1e-5)

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
        check(result, np.zeros(T), atol=1e-7)


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
