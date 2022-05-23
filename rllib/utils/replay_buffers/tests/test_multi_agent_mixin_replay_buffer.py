import numpy as np
import unittest

from ray.rllib.utils.replay_buffers.multi_agent_mixin_replay_buffer import (
    MultiAgentMixInReplayBuffer,
)
from ray.rllib.policy.sample_batch import (
    SampleBatch,
    DEFAULT_POLICY_ID,
    MultiAgentBatch,
)


class TestMixInMultiAgentReplayBuffer(unittest.TestCase):
    batch_id = 0
    capacity = 10

    def _generate_episodes(self):
        return SampleBatch(
            {
                SampleBatch.T: [1, 0, 1],
                SampleBatch.ACTIONS: 3 * [np.random.choice([0, 1])],
                SampleBatch.REWARDS: 3 * [np.random.rand()],
                SampleBatch.OBS: 3 * [np.random.random((4,))],
                SampleBatch.NEXT_OBS: 3 * [np.random.random((4,))],
                SampleBatch.DONES: [True, False, True],
                SampleBatch.SEQ_LENS: [1, 2],
                SampleBatch.EPS_ID: [-1, self.batch_id, self.batch_id],
                SampleBatch.AGENT_INDEX: 3 * [0],
            }
        )

    def _generate_single_timesteps(self):
        return SampleBatch(
            {
                SampleBatch.T: [0],
                SampleBatch.ACTIONS: [np.random.choice([0, 1])],
                SampleBatch.REWARDS: [np.random.rand()],
                SampleBatch.OBS: [np.random.random((4,))],
                SampleBatch.NEXT_OBS: [np.random.random((4,))],
                SampleBatch.DONES: [True],
                SampleBatch.EPS_ID: [self.batch_id],
                SampleBatch.AGENT_INDEX: [0],
            }
        )

    def test_mixin_sampling_episodes(self):
        """Test sampling of episodes."""
        # 50% replay ratio.
        buffer = MultiAgentMixInReplayBuffer(
            capacity=self.capacity,
            storage_unit="episodes",
            replay_ratio=0.5,
            learning_starts=0,
        )

        # If we insert and replay n times, expect roughly return batches of
        # len 5 (replay_ratio=0.5 -> 50% replayed samples -> 1 new and 1
        # old sample, each of length two on average in each returned value).
        results = []
        batch = self._generate_episodes()
        for _ in range(20):
            buffer.add(batch)
            sample = buffer.sample(2)
            assert type(sample) == MultiAgentBatch
            results.append(len(sample.policy_batches[DEFAULT_POLICY_ID]))
        # One sample in the episode does not belong the the episode on thus
        # gets dropped. Full episodes are of length two.
        self.assertAlmostEqual(np.mean(results), 2 * (len(batch) - 1))

    def test_mixin_sampling_sequences(self):
        """Test sampling of sequences."""
        # 50% replay ratio.
        buffer = MultiAgentMixInReplayBuffer(
            capacity=100,
            storage_unit="sequences",
            replay_ratio=0.5,
            learning_starts=0,
            replay_sequence_length=2,
            replay_sequence_override=True,
        )

        # If we insert and replay n times, expect roughly return batches of
        # len 6 (replay_ratio=0.5 -> 50% replayed samples -> 2 new and 2
        # old sequences with an average length of 1.5 each.
        results = []
        batch = self._generate_episodes()
        for _ in range(400):
            buffer.add(batch)
            sample = buffer.sample(10)
            assert type(sample) == MultiAgentBatch
            results.append(len(sample.policy_batches[DEFAULT_POLICY_ID]))
        self.assertAlmostEqual(np.mean(results), 2 * len(batch), delta=0.1)

    def test_mixin_sampling_timesteps(self):
        """Test different mixin ratios with timesteps."""
        # 33% replay ratio.
        buffer = MultiAgentMixInReplayBuffer(
            capacity=self.capacity,
            storage_unit="timesteps",
            replay_ratio=0.333,
            learning_starts=0,
        )
        # Expect exactly 0 samples to be returned (buffer empty).
        sample = buffer.sample(10)
        assert len(sample.policy_batches) == 0

        batch = self._generate_single_timesteps()
        # If we insert-2x and replay n times, expect roughly return batches of
        # len 5 (replay_ratio=0.33 -> 33% replayed samples -> 2 new and 1
        # old sample on average in each returned value).
        results = []
        for _ in range(100):
            buffer.add(batch)
            buffer.add(batch)
            sample = buffer.sample(3)
            assert type(sample) == MultiAgentBatch
            results.append(len(sample.policy_batches[DEFAULT_POLICY_ID]))
        self.assertAlmostEqual(np.mean(results), 3.0, delta=0.2)

        # If we insert-1x and replay n times, expect roughly return batches of
        # len 1.5 (replay_ratio=0.33 -> 33% replayed samples -> 1 new and 0.5
        # old
        # samples on average in each returned value).
        results = []
        for _ in range(100):
            buffer.add(batch)
            sample = buffer.sample(5)
            assert type(sample) == MultiAgentBatch
            results.append(len(sample.policy_batches[DEFAULT_POLICY_ID]))
        self.assertAlmostEqual(np.mean(results), 1.5, delta=0.2)

        # 90% replay ratio.
        buffer = MultiAgentMixInReplayBuffer(
            capacity=self.capacity, replay_ratio=0.9, learning_starts=0
        )

        # If we insert and replay n times, expect roughly return batches of
        # len 10 (replay_ratio=0.9 -> 90% replayed samples -> 1 new and 9 old
        # samples on average in each returned value).
        results = []
        for _ in range(100):
            buffer.add(batch)
            sample = buffer.sample(10)
            assert type(sample) == MultiAgentBatch
            results.append(len(sample.policy_batches[DEFAULT_POLICY_ID]))
        self.assertAlmostEqual(np.mean(results), 10.0, delta=0.2)

        # 0% replay ratio -> Only new samples.
        buffer = MultiAgentMixInReplayBuffer(
            capacity=self.capacity, replay_ratio=0.0, learning_starts=0
        )
        # Add a new batch.
        batch = self._generate_single_timesteps()
        buffer.add(batch)
        # Expect exactly 1 batch to be returned.
        sample = buffer.sample(1)
        assert type(sample) == MultiAgentBatch
        self.assertTrue(len(sample) == 1)
        # Expect exactly 0 sample to be returned (nothing new to be returned;
        # no replay allowed (replay_ratio=0.0)).
        sample = buffer.sample(1)
        assert type(sample) == MultiAgentBatch
        assert len(sample.policy_batches) == 0
        # If we insert and replay n times, expect roughly return batches of
        # len 1 (replay_ratio=0.0 -> 0% replayed samples -> 1 new and 0 old samples
        # on average in each returned value).
        results = []
        for _ in range(100):
            buffer.add(batch)
            sample = buffer.sample(1)
            assert type(sample) == MultiAgentBatch
            results.append(len(sample.policy_batches[DEFAULT_POLICY_ID]))
        self.assertAlmostEqual(np.mean(results), 1.0, delta=0.2)

        # 100% replay ratio -> Only new samples.
        buffer = MultiAgentMixInReplayBuffer(
            capacity=self.capacity, replay_ratio=1.0, learning_starts=0
        )
        # Expect exactly 0 samples to be returned (buffer empty).
        sample = buffer.sample(1)
        assert len(sample.policy_batches) == 0
        # Add a new batch.
        batch = self._generate_single_timesteps()
        buffer.add(batch)
        # Expect exactly 1 sample to be returned (the new batch).
        sample = buffer.sample(1)
        assert type(sample) == MultiAgentBatch
        self.assertTrue(len(sample) == 1)
        # Another replay -> Expect exactly 1 sample to be returned.
        sample = buffer.sample(1)
        assert type(sample) == MultiAgentBatch
        self.assertTrue(len(sample) == 1)
        # If we replay n times, expect roughly return batches of
        # len 1 (replay_ratio=1.0 -> 100% replayed samples -> 0 new and 1 old samples
        # on average in each returned value).
        results = []
        for _ in range(100):
            sample = buffer.sample(1)
            assert type(sample) == MultiAgentBatch
            results.append(len(sample.policy_batches[DEFAULT_POLICY_ID]))
        self.assertAlmostEqual(np.mean(results), 1.0)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
