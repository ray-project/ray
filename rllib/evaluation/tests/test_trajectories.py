from gym.spaces import Box, Discrete
import numpy as np
import unittest

from ray.rllib.evaluation.trajectory import Trajectory


class TestTrajectories(unittest.TestCase):
    """Tests Trajectory classes."""

    def test_trajectory(self):
        """Tests the Trajectory class."""

        buffer_size = 5

        # Small trajecory object for testing purposes.
        trajectory = Trajectory(buffer_size=buffer_size)
        self.assertEqual(trajectory.cursor, 0)
        self.assertEqual(trajectory.timestep, 0)
        self.assertEqual(trajectory.sample_batch_offset, 0)
        self.assertEqual(trajectory.has_initial_obs, False)
        assert not trajectory.buffers
        observation_space = Box(-1.0, 1.0, shape=(3, ))
        action_space = Discrete(2)
        trajectory.add_init_obs(
            env_id=0,
            agent_id="agent",
            policy_id="policy",
            init_obs=observation_space.sample())
        self.assertEqual(trajectory.cursor, 0)
        self.assertEqual(trajectory.has_initial_obs, True)
        self.assertEqual(len(trajectory.buffers["obs"]), buffer_size + 1)

        # Fill up the buffer and make it extend if it hits the limit.
        for i in range(buffer_size):
            trajectory.add_action_reward_next_obs(
                env_id=0,
                agent_id="agent",
                policy_id="policy",
                values=dict(
                    actions=action_space.sample(),
                    rewards=1.0,
                    new_obs=observation_space.sample(),
                    action_logp=-0.5,
                    action_dist_inputs=np.array([[0.5, 0.5]]),
                ))
            self.assertEqual(trajectory.cursor, i + 1)
            self.assertEqual(trajectory.timestep, i + 1)
            self.assertEqual(trajectory.sample_batch_offset, 0)
            self.assertEqual(len(trajectory.buffers["obs"]), buffer_size + 1)
            self.assertEqual(len(trajectory.buffers["rewards"]), buffer_size)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
