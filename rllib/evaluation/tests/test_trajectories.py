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
        assert not trajectory.buffers
        observation_space = Box(-1.0, 1.0, shape=(3, ))
        action_space = Discrete(2)
        trajectory.add_init_obs(
            env_id=0,
            agent_id="agent",
            policy_id="policy",
            init_obs=observation_space.sample())
        self.assertEqual(trajectory.cursor, 0)
        self.assertEqual(trajectory.initial_obs.shape, observation_space.shape)

        # Fill up the buffer and make it extend if it hits the limit.
        cur_buffer_size = buffer_size
        for i in range(buffer_size + 1):
            trajectory.add_action_reward_next_obs(
                env_id=0,
                agent_id="agent",
                policy_id="policy",
                values=dict(
                    t=i,
                    actions=action_space.sample(),
                    rewards=1.0,
                    dones=i == buffer_size,
                    new_obs=observation_space.sample(),
                    action_logp=-0.5,
                    action_dist_inputs=np.array([[0.5, 0.5]]),
                ))
            self.assertEqual(trajectory.cursor, i + 1)
            self.assertEqual(trajectory.timestep, i + 1)
            self.assertEqual(trajectory.sample_batch_offset, 0)
            if i == buffer_size - 1:
                cur_buffer_size *= 2
            self.assertEqual(
                len(trajectory.buffers["new_obs"]), cur_buffer_size)
            self.assertEqual(
                len(trajectory.buffers["rewards"]), cur_buffer_size)

        # Create a SampleBatch from the Trajectory and reset it.
        batch = trajectory.get_sample_batch_and_reset()
        self.assertEqual(batch.count, buffer_size + 1)
        # Make sure, Trajectory was reset properly.
        self.assertEqual(trajectory.cursor, buffer_size + 1)
        self.assertEqual(trajectory.timestep, 0)
        self.assertEqual(trajectory.sample_batch_offset, buffer_size + 1)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
