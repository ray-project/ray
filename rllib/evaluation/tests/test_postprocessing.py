import numpy as np
import unittest

import ray
from ray.rllib.evaluation.postprocessing import adjust_nstep, discount_cumsum
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import check


class TestPostprocessing(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_n_step_3(self):
        """Tests, whether n-step adjustments of trajectories work."""
        # n-step = 3
        gamma = 0.9
        obs = [1, 2, 3, 4, 5, 6, 7]
        actions = ["ac1", "ac2", "ac1", "ac1", "ac1", "ac2", "ac1"]
        rewards = [10.0, 0.0, 100.0, 100.0, 100.0, 100.0, 100.0]
        terminateds = [0, 0, 0, 0, 0, 0, 1]
        truncateds = [0, 0, 0, 0, 0, 0, 0]
        next_obs = [2, 3, 4, 5, 6, 7, 8]
        batch = SampleBatch(
            {
                SampleBatch.OBS: obs,
                SampleBatch.ACTIONS: actions,
                SampleBatch.REWARDS: rewards,
                SampleBatch.TERMINATEDS: terminateds,
                SampleBatch.TRUNCATEDS: truncateds,
                SampleBatch.NEXT_OBS: next_obs,
            }
        )
        adjust_nstep(3, gamma, batch)
        check(batch[SampleBatch.OBS], [1, 2, 3, 4, 5, 6, 7])
        check(
            batch[SampleBatch.ACTIONS],
            ["ac1", "ac2", "ac1", "ac1", "ac1", "ac2", "ac1"],
        )
        check(batch[SampleBatch.NEXT_OBS], [4, 5, 6, 7, 8, 8, 8])
        check(batch[SampleBatch.TERMINATEDS], [0, 0, 0, 0, 1, 1, 1])
        check(batch[SampleBatch.TRUNCATEDS], [0, 0, 0, 0, 0, 0, 0])
        check(
            batch[SampleBatch.REWARDS], [91.0, 171.0, 271.0, 271.0, 271.0, 190.0, 100.0]
        )

    def test_n_step_4(self):
        """Tests, whether n-step adjustments of trajectories work."""
        # n-step = 4
        gamma = 0.99
        obs = np.arange(0, 7)
        actions = np.random.randint(-1, 3, size=(7,))
        check_actions = actions.copy()
        rewards = [10.0, 0.0, 100.0, 50.0, 60.0, 10.0, 100.0]
        terminateds = [False, False, False, False, False, False, True]
        truncateds = [False, False, False, False, False, False, False]
        next_obs = np.arange(1, 8)
        batch = SampleBatch(
            {
                SampleBatch.OBS: obs,
                SampleBatch.ACTIONS: actions,
                SampleBatch.REWARDS: rewards,
                SampleBatch.TERMINATEDS: terminateds,
                SampleBatch.TRUNCATEDS: truncateds,
                SampleBatch.NEXT_OBS: next_obs,
            }
        )
        adjust_nstep(4, gamma, batch)
        check(batch[SampleBatch.OBS], [0, 1, 2, 3, 4, 5, 6])
        check(batch[SampleBatch.ACTIONS], check_actions)
        check(batch[SampleBatch.NEXT_OBS], [4, 5, 6, 7, 7, 7, 7])
        check(
            batch[SampleBatch.TERMINATEDS],
            [False, False, False, True, True, True, True],
        )
        check(
            batch[SampleBatch.TRUNCATEDS],
            [False, False, False, False, False, False, False],
        )
        check(
            batch[SampleBatch.REWARDS],
            [
                discount_cumsum(np.array(rewards[0:4]), gamma)[0],
                discount_cumsum(np.array(rewards[1:5]), gamma)[0],
                discount_cumsum(np.array(rewards[2:6]), gamma)[0],
                discount_cumsum(np.array(rewards[3:7]), gamma)[0],
                discount_cumsum(np.array(rewards[4:]), gamma)[0],
                discount_cumsum(np.array(rewards[5:]), gamma)[0],
                discount_cumsum(np.array(rewards[6:]), gamma)[0],
            ],
        )

    def test_n_step_malformed_terminateds(self):
        # Test bad input (trajectory has `terminateds` in middle).
        # Re-use same batch, but change terminateds.
        gamma = 1.0
        obs = np.arange(0, 7)
        actions = np.random.randint(-1, 3, size=(7,))
        rewards = [10.0, 0.0, 100.0, 50.0, 60.0, 10.0, 100.0]
        next_obs = np.arange(1, 8)
        batch = SampleBatch(
            {
                SampleBatch.OBS: obs,
                SampleBatch.ACTIONS: actions,
                SampleBatch.REWARDS: rewards,
                SampleBatch.TERMINATEDS: [
                    False,
                    False,
                    True,
                    False,
                    False,
                    False,
                    True,
                ],
                SampleBatch.TRUNCATEDS: [
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                ],
                SampleBatch.NEXT_OBS: next_obs,
            }
        )
        self.assertRaisesRegex(
            AssertionError,
            "Unexpected terminated\\|truncated in middle",
            lambda: adjust_nstep(5, gamma, batch),
        )

        batch = SampleBatch(
            {
                SampleBatch.OBS: obs,
                SampleBatch.ACTIONS: actions,
                SampleBatch.REWARDS: rewards,
                SampleBatch.TERMINATEDS: [
                    False,
                    False,
                    False,
                    False,
                    False,
                    False,
                    True,
                ],
                SampleBatch.TRUNCATEDS: [
                    False,
                    True,
                    False,
                    True,
                    False,
                    False,
                    False,
                ],
                SampleBatch.NEXT_OBS: next_obs,
            }
        )
        self.assertRaisesRegex(
            AssertionError,
            "Unexpected terminated\\|truncated in middle",
            lambda: adjust_nstep(5, gamma, batch),
        )

    def test_n_step_very_short_trajectory(self):
        """Tests, whether n-step also works for very small trajectories."""
        gamma = 1.0
        obs = np.arange(0, 2)
        actions = np.random.randint(-100, 300, size=(2,))
        check_actions = actions.copy()
        rewards = [10.0, 100.0]
        next_obs = np.arange(1, 3)
        batch = SampleBatch(
            {
                SampleBatch.OBS: obs,
                SampleBatch.ACTIONS: actions,
                SampleBatch.REWARDS: rewards,
                SampleBatch.TERMINATEDS: [False, False],
                SampleBatch.TRUNCATEDS: [False, False],
                SampleBatch.NEXT_OBS: next_obs,
            }
        )
        adjust_nstep(3, gamma, batch)
        check(batch[SampleBatch.OBS], [0, 1])
        check(batch[SampleBatch.ACTIONS], check_actions)
        check(batch[SampleBatch.TERMINATEDS], [False, False])
        check(batch[SampleBatch.TRUNCATEDS], [False, False])
        check(batch[SampleBatch.REWARDS], [10.0 + gamma * 100.0, 100.0])
        check(batch[SampleBatch.NEXT_OBS], [2, 2])

    def test_n_step_from_same_obs_source_array(self):
        """Tests, whether n-step also works on a shared obs/new-obs array."""
        gamma = 0.99
        # The underlying observation data. Both obs and next_obs will
        # be references into that same np.array.
        underlying_obs = np.arange(0, 8)
        obs = underlying_obs[:7]
        next_obs = underlying_obs[1:]

        actions = np.random.randint(-1, 3, size=(7,))
        check_actions = actions.copy()
        rewards = [10.0, 0.0, 100.0, 50.0, 60.0, 10.0, 100.0]
        terminateds = [False, False, False, False, False, False, False]
        truncateds = [False, False, False, False, False, False, True]

        batch = SampleBatch(
            {
                SampleBatch.OBS: obs,
                SampleBatch.ACTIONS: actions,
                SampleBatch.REWARDS: rewards,
                SampleBatch.TERMINATEDS: terminateds,
                SampleBatch.TRUNCATEDS: truncateds,
                SampleBatch.NEXT_OBS: next_obs,
            }
        )
        adjust_nstep(4, gamma, batch)

        check(batch[SampleBatch.OBS], [0, 1, 2, 3, 4, 5, 6])
        check(batch[SampleBatch.ACTIONS], check_actions)
        check(batch[SampleBatch.NEXT_OBS], [4, 5, 6, 7, 7, 7, 7])
        check(
            batch[SampleBatch.TERMINATEDS],
            [False, False, False, False, False, False, False],
        )
        check(
            batch[SampleBatch.TRUNCATEDS],
            [False, False, False, True, True, True, True],
        )
        check(
            batch[SampleBatch.REWARDS],
            [
                discount_cumsum(np.array(rewards[0:4]), gamma)[0],
                discount_cumsum(np.array(rewards[1:5]), gamma)[0],
                discount_cumsum(np.array(rewards[2:6]), gamma)[0],
                discount_cumsum(np.array(rewards[3:7]), gamma)[0],
                discount_cumsum(np.array(rewards[4:]), gamma)[0],
                discount_cumsum(np.array(rewards[5:]), gamma)[0],
                discount_cumsum(np.array(rewards[6:]), gamma)[0],
            ],
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
