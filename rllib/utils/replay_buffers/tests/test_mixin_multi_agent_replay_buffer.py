import numpy as np
import unittest

from ray.rllib.utils.replay_buffers.mixin_multi_agent_replay_buffer import \
    MixInMultiAgentReplayBuffer
from ray.rllib.policy.sample_batch import SampleBatch


class TestMixInMultiAgentReplayBuffer(unittest.TestCase):
    """Tests insertion and mixed sampling of the MixInMultiAgentReplayBuffer."""

    capacity = 10

    def _generate_data(self):
        return SampleBatch(
            {
                "obs": [np.random.random((4,))],
                "action": [np.random.choice([0, 1])],
                "reward": [np.random.rand()],
                "new_obs": [np.random.random((4,))],
                "done": [np.random.choice([False, True])],
            }
        )

    def test_mixin_sampling(self):
        # 50% replay ratio.
        buffer = MixInMultiAgentReplayBuffer(capacity=self.capacity,
                                             replay_ratio=0.5)

        # If we insert and replay n times, expect roughly return batches of
        # len 2 (replay_ratio=0.5 -> 50% replayed samples -> 1 new and 1 old sample
        # on average in each returned value).
        results = []
        batch = self._generate_data()
        for _ in range(100):
            buffer.add(batch)
            sample = buffer.sample(2)
            results.append(len(sample))
        self.assertAlmostEqual(np.mean(results), 2.0)

        # 33% replay ratio.
        buffer = MixInMultiAgentReplayBuffer(capacity=self.capacity,
                                             replay_ratio=0.333)
        # Expect exactly 0 samples to be returned (buffer empty).
        sample = buffer.sample(10)
        self.assertTrue(sample is None)

        # If we insert-2x and replay n times, expect roughly return batches of
        # len 3 (replay_ratio=0.33 -> 33% replayed samples -> 2 new and 1 old sample
        # on average in each returned value).
        results = []
        for _ in range(100):
            buffer.add(batch)
            buffer.add(batch)
            sample = buffer.sample(3)
            results.append(len(sample))
        self.assertAlmostEqual(np.mean(results), 3.0, delta=0.2)

        # If we insert-1x and replay n times, expect roughly return batches of
        # len 1.5 (replay_ratio=0.33 -> 33% replayed samples -> 1 new and 0.5
        # old
        # samples on average in each returned value).
        results = []
        for _ in range(100):
            buffer.add(batch)
            sample = buffer.sample(5)
            results.append(len(sample))
        self.assertAlmostEqual(np.mean(results), 1.5, delta=0.2)

        # 90% replay ratio.
        buffer = MixInMultiAgentReplayBuffer(capacity=self.capacity,
                                             replay_ratio=0.9)

        # If we insert and replay n times, expect roughly return batches of
        # len 10 (replay_ratio=0.9 -> 90% replayed samples -> 1 new and 9 old
        # samples on average in each returned value).
        results = []
        for _ in range(100):
            buffer.add(batch)
            sample = buffer.sample(10)
            results.append(len(sample))
        self.assertAlmostEqual(np.mean(results), 10.0, delta=0.2)

        # 0% replay ratio -> Only new samples.
        buffer = MixInMultiAgentReplayBuffer(capacity=self.capacity,
                                             replay_ratio=0.0)
        # Add a new batch.
        batch = self._generate_data()
        buffer.add(batch)
        # Expect exactly 1 sample to be returned.
        sample = buffer.sample(1)
        self.assertTrue(len(sample) == 1)
        # Expect exactly 0 sample to be returned (nothing new to be returned;
        # no replay allowed (replay_ratio=0.0)).
        sample = buffer.sample(1)
        self.assertTrue(sample is None)
        # If we insert and replay n times, expect roughly return batches of
        # len 1 (replay_ratio=0.0 -> 0% replayed samples -> 1 new and 0 old samples
        # on average in each returned value).
        results = []
        for _ in range(100):
            buffer.add(batch)
            sample = buffer.sample(1)
            results.append(len(sample))
        self.assertAlmostEqual(np.mean(results), 1.0, delta=0.2)

        # 100% replay ratio -> Only new samples.
        buffer = MixInMultiAgentReplayBuffer(capacity=self.capacity, replay_ratio=1.0)
        # Expect exactly 0 samples to be returned (buffer empty).
        sample = buffer.sample(1)
        self.assertTrue(sample is None)
        # Add a new batch.
        batch = self._generate_data()
        buffer.add(batch)
        # Expect exactly 1 sample to be returned (the new batch).
        sample = buffer.sample(1)
        self.assertTrue(len(sample) == 1)
        # Another replay -> Expect exactly 1 sample to be returned.
        sample = buffer.sample(1)
        self.assertTrue(len(sample) == 1)
        # If we replay n times, expect roughly return batches of
        # len 1 (replay_ratio=1.0 -> 100% replayed samples -> 0 new and 1 old samples
        # on average in each returned value).
        results = []
        for _ in range(100):
            sample = buffer.sample(1)
            results.append(len(sample))
        self.assertAlmostEqual(np.mean(results), 1.0)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
