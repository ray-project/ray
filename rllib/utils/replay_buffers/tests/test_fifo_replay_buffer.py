import unittest
import numpy as np
from ray.rllib.policy.sample_batch import SampleBatch

from ray.rllib.utils.replay_buffers.fifo_replay_buffer import FifoReplayBuffer


class TestFifoReplayBuffer(unittest.TestCase):
    def test_empty_buffer(self):
        buffer = FifoReplayBuffer()
        batch = buffer.sample()
        self.assertEqual(len(batch), 0)

    def test_sample(self):
        buffer = FifoReplayBuffer()

        buffer.add(
            SampleBatch(
                {
                    SampleBatch.T: [1],
                    SampleBatch.ACTIONS: [np.random.choice([0, 1])],
                    SampleBatch.REWARDS: [np.random.rand()],
                    SampleBatch.OBS: [np.random.random((4,))],
                    SampleBatch.NEXT_OBS: [np.random.random((4,))],
                    SampleBatch.DONES: [np.random.choice([False, True])],
                }
            )
        )
        buffer.add(
            SampleBatch(
                {
                    SampleBatch.T: [2],
                    SampleBatch.ACTIONS: [np.random.choice([0, 1])],
                    SampleBatch.REWARDS: [np.random.rand()],
                    SampleBatch.OBS: [np.random.random((4,))],
                    SampleBatch.NEXT_OBS: [np.random.random((4,))],
                    SampleBatch.DONES: [np.random.choice([False, True])],
                }
            )
        )

        batch = buffer.sample()
        self.assertEqual(batch[SampleBatch.T][0], 1)
        batch = buffer.sample()
        self.assertEqual(batch[SampleBatch.T][0], 2)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
