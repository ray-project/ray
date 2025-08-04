from collections import Counter
import numpy as np
import unittest

from ray.rllib.utils.replay_buffers.prioritized_replay_buffer import (
    PrioritizedReplayBuffer,
)
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch, concat_samples
from ray.rllib.utils.test_utils import check


class TestPrioritizedReplayBuffer(unittest.TestCase):
    """
    Tests insertion and (weighted) sampling of the PrioritizedReplayBuffer.
    """

    capacity = 10
    alpha = 1.0
    beta = 1.0

    def _generate_data(self):
        return SampleBatch(
            {
                SampleBatch.T: [np.random.random((4,))],
                SampleBatch.ACTIONS: [np.random.choice([0, 1])],
                SampleBatch.REWARDS: [np.random.rand()],
                SampleBatch.OBS: [np.random.random((4,))],
                SampleBatch.NEXT_OBS: [np.random.random((4,))],
                SampleBatch.TERMINATEDS: [np.random.choice([False, True])],
                SampleBatch.TRUNCATEDS: [np.random.choice([False, False])],
            }
        )

    def test_multi_agent_batches(self):
        """Tests buffer with storage of MultiAgentBatches."""
        self.batch_id = 0

        def _add_multi_agent_batch_to_buffer(
            buffer, num_policies, num_batches=5, seq_lens=False, **kwargs
        ):
            def _generate_data(policy_id):
                batch = SampleBatch(
                    {
                        SampleBatch.T: [0, 1],
                        SampleBatch.ACTIONS: 2 * [np.random.choice([0, 1])],
                        SampleBatch.REWARDS: 2 * [np.random.rand()],
                        SampleBatch.OBS: 2 * [np.random.random((4,))],
                        SampleBatch.NEXT_OBS: 2 * [np.random.random((4,))],
                        SampleBatch.TERMINATEDS: [False, False],
                        SampleBatch.TRUNCATEDS: [False, True],
                        SampleBatch.EPS_ID: 2 * [self.batch_id],
                        SampleBatch.AGENT_INDEX: 2 * [0],
                        SampleBatch.SEQ_LENS: [2],
                        "batch_id": 2 * [self.batch_id],
                        "policy_id": 2 * [policy_id],
                    }
                )
                if not seq_lens:
                    del batch[SampleBatch.SEQ_LENS]
                self.batch_id += 1
                return batch

            for i in range(num_batches):
                # genera a few policy batches
                policy_batches = {
                    idx: _generate_data(idx)
                    for idx, _ in enumerate(range(num_policies))
                }
                batch = MultiAgentBatch(policy_batches, num_batches * 2)
                buffer.add(batch, **kwargs)

        buffer = PrioritizedReplayBuffer(
            capacity=100, storage_unit="fragments", alpha=0.5
        )

        # Test add/sample
        _add_multi_agent_batch_to_buffer(buffer, num_policies=2, num_batches=2)

        # After adding a single batch to a buffer, it should not be full
        assert len(buffer) == 2
        assert buffer._num_timesteps_added == 8
        assert buffer._num_timesteps_added_wrap == 8
        assert buffer._next_idx == 2
        assert buffer._eviction_started is False

        # Sampling three times should yield 3 batches of 5 timesteps each
        buffer.sample(3, beta=0.5)
        assert buffer._num_timesteps_sampled == 12

        _add_multi_agent_batch_to_buffer(
            buffer, batch_size=100, num_policies=3, num_batches=3
        )

        # After adding two more batches, the buffer should be full
        assert len(buffer) == 5
        assert buffer._num_timesteps_added == 26
        assert buffer._num_timesteps_added_wrap == 26
        assert buffer._next_idx == 5

    def test_sequence_size(self):
        # Seq-len=1.
        buffer = PrioritizedReplayBuffer(
            capacity=100, alpha=0.1, storage_unit="fragments"
        )
        for _ in range(200):
            buffer.add(self._generate_data())
        assert len(buffer._storage) == 100, len(buffer._storage)
        assert buffer.stats()["added_count"] == 200, buffer.stats()
        # Test get_state/set_state.
        state = buffer.get_state()
        new_memory = PrioritizedReplayBuffer(capacity=100, alpha=0.1)
        new_memory.set_state(state)
        assert len(new_memory._storage) == 100, len(new_memory._storage)
        assert new_memory.stats()["added_count"] == 200, new_memory.stats()

        # Seq-len=5.
        buffer = PrioritizedReplayBuffer(
            capacity=100, alpha=0.1, storage_unit="fragments"
        )
        for _ in range(40):
            buffer.add(concat_samples([self._generate_data() for _ in range(5)]))
        assert len(buffer._storage) == 20, len(buffer._storage)
        assert buffer.stats()["added_count"] == 200, buffer.stats()
        # Test get_state/set_state.
        state = buffer.get_state()
        new_memory = PrioritizedReplayBuffer(capacity=100, alpha=0.1)
        new_memory.set_state(state)
        assert len(new_memory._storage) == 20, len(new_memory._storage)
        assert new_memory.stats()["added_count"] == 200, new_memory.stats()

    def test_add(self):
        buffer = PrioritizedReplayBuffer(capacity=2, alpha=self.alpha)

        # Assert indices 0 before insert.
        self.assertEqual(len(buffer), 0)
        self.assertEqual(buffer._next_idx, 0)

        # Insert single record.
        data = self._generate_data()
        buffer.add(data, weight=0.5)
        self.assertTrue(len(buffer) == 1)
        self.assertTrue(buffer._next_idx == 1)

        # Insert single record.
        data = self._generate_data()
        buffer.add(data, weight=0.1)
        self.assertTrue(len(buffer) == 2)
        self.assertTrue(buffer._next_idx == 0)

        # Insert over capacity.
        data = self._generate_data()
        buffer.add(data, weight=1.0)
        self.assertTrue(len(buffer) == 2)
        self.assertTrue(buffer._next_idx == 1)

        # Test get_state/set_state.
        state = buffer.get_state()
        new_memory = PrioritizedReplayBuffer(capacity=2, alpha=self.alpha)
        new_memory.set_state(state)
        self.assertTrue(len(new_memory) == 2)
        self.assertTrue(new_memory._next_idx == 1)

    def test_update_priorities(self):
        buffer = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)

        # Insert n samples.
        num_records = 5
        for i in range(num_records):
            data = self._generate_data()
            buffer.add(data, weight=1.0)
            self.assertTrue(len(buffer) == i + 1)
            self.assertTrue(buffer._next_idx == i + 1)

        # Test get_state/set_state.
        state = buffer.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)
        new_memory.set_state(state)
        self.assertTrue(len(new_memory) == num_records)
        self.assertTrue(new_memory._next_idx == num_records)

        # Fetch records, their indices and weights.
        batch = buffer.sample(3, beta=self.beta)
        weights = batch["weights"]
        indices = batch["batch_indexes"]
        check(weights, np.ones(shape=(3,)))
        self.assertEqual(3, len(indices))
        self.assertTrue(len(buffer) == num_records)
        self.assertTrue(buffer._next_idx == num_records)

        # Update weight of indices 0, 2, 3, 4 to very small.
        buffer.update_priorities(
            np.array([0, 2, 3, 4]), np.array([0.01, 0.01, 0.01, 0.01])
        )
        # Expect to sample almost only index 1
        # (which still has a weight of 1.0).
        for _ in range(10):
            batch = buffer.sample(1000, beta=self.beta)
            indices = batch["batch_indexes"]
            self.assertTrue(970 < np.sum(indices) < 1100)
        # Test get_state/set_state.
        state = buffer.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)
        new_memory.set_state(state)
        batch = new_memory.sample(1000, beta=self.beta)
        indices = batch["batch_indexes"]
        self.assertTrue(970 < np.sum(indices) < 1100)

        # Update weight of indices 0 and 1 to >> 0.01.
        # Expect to sample 0 and 1 equally (and some 2s, 3s, and 4s).
        for _ in range(10):
            rand = np.random.random() + 0.2
            buffer.update_priorities(np.array([0, 1]), np.array([rand, rand]))
            batch = buffer.sample(1000, beta=self.beta)
            indices = batch["batch_indexes"]
            # Expect biased to higher values due to some 2s, 3s, and 4s.
            self.assertTrue(400 < np.sum(indices) < 800)
        # Test get_state/set_state.
        state = buffer.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)
        new_memory.set_state(state)
        batch = new_memory.sample(1000, beta=self.beta)
        indices = batch["batch_indexes"]
        self.assertTrue(400 < np.sum(indices) < 800)

        # Update weights to be 1:2.
        # Expect to sample double as often index 1 over index 0
        # plus very few times indices 2, 3, or 4.
        for _ in range(10):
            rand = np.random.random() + 0.2
            buffer.update_priorities(np.array([0, 1]), np.array([rand, rand * 2]))
            batch = buffer.sample(1000, beta=self.beta)
            indices = batch["batch_indexes"]
            self.assertTrue(600 < np.sum(indices) < 850)
        # Test get_state/set_state.
        state = buffer.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)
        new_memory.set_state(state)
        batch = new_memory.sample(1000, beta=self.beta)
        indices = batch["batch_indexes"]
        self.assertTrue(600 < np.sum(indices) < 850)

        # Update weights to be 1:4.
        # Expect to sample quadruple as often index 1 over index 0
        # plus very few times indices 2, 3, or 4.
        for _ in range(10):
            rand = np.random.random() + 0.2
            buffer.update_priorities(np.array([0, 1]), np.array([rand, rand * 4]))
            batch = buffer.sample(1000, beta=self.beta)
            indices = batch["batch_indexes"]
            self.assertTrue(750 < np.sum(indices) < 950)
        # Test get_state/set_state.
        state = buffer.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)
        new_memory.set_state(state)
        batch = new_memory.sample(1000, beta=self.beta)
        indices = batch["batch_indexes"]
        self.assertTrue(750 < np.sum(indices) < 950)

        # Update weights to be 1:9.
        # Expect to sample 9 times as often index 1 over index 0.
        # plus very few times indices 2, 3, or 4.
        for _ in range(10):
            rand = np.random.random() + 0.2
            buffer.update_priorities(np.array([0, 1]), np.array([rand, rand * 9]))
            batch = buffer.sample(1000, beta=self.beta)
            indices = batch["batch_indexes"]
            self.assertTrue(850 < np.sum(indices) < 1100)
        # Test get_state/set_state.
        state = buffer.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)
        new_memory.set_state(state)
        batch = new_memory.sample(1000, beta=self.beta)
        indices = batch["batch_indexes"]
        self.assertTrue(850 < np.sum(indices) < 1100)

        # Insert n more samples.
        num_records = 5
        for i in range(num_records):
            data = self._generate_data()
            buffer.add(data, weight=1.0)
            self.assertTrue(len(buffer) == i + 6)
            self.assertTrue(buffer._next_idx == (i + 6) % self.capacity)

        # Update all weights to be 1.0 to 10.0 and sample a >100 batch.
        buffer.update_priorities(
            np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            np.array([0.001, 0.1, 2.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0]),
        )
        counts = Counter()
        for _ in range(10):
            batch = buffer.sample(np.random.randint(100, 600), beta=self.beta)
            indices = batch["batch_indexes"]
            for i in indices:
                counts[i] += 1

        # Expect an approximately correct distribution of indices.
        self.assertTrue(
            counts[9]
            >= counts[8]
            >= counts[7]
            >= counts[6]
            >= counts[5]
            >= counts[4]
            >= counts[3]
            >= counts[2]
            >= counts[1]
            >= counts[0]
        )
        # Test get_state/set_state.
        state = buffer.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)
        new_memory.set_state(state)
        counts = Counter()
        for _ in range(10):
            batch = new_memory.sample(np.random.randint(100, 600), beta=self.beta)
            indices = batch["batch_indexes"]
            for i in indices:
                counts[i] += 1

        self.assertTrue(
            counts[9]
            >= counts[8]
            >= counts[7]
            >= counts[6]
            >= counts[5]
            >= counts[4]
            >= counts[3]
            >= counts[2]
            >= counts[1]
            >= counts[0]
        )

    def test_alpha_parameter(self):
        # Test sampling from a PR with a very small alpha (should behave just
        # like a regular ReplayBuffer).
        buffer = PrioritizedReplayBuffer(self.capacity, alpha=0.01)

        # Insert n samples.
        num_records = 5
        for i in range(num_records):
            data = self._generate_data()
            buffer.add(data, weight=float(np.random.rand()))
            self.assertTrue(len(buffer) == i + 1)
            self.assertTrue(buffer._next_idx == i + 1)
        # Test get_state/set_state.
        state = buffer.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=0.01)
        new_memory.set_state(state)
        self.assertTrue(len(new_memory) == num_records)
        self.assertTrue(new_memory._next_idx == num_records)

        # Fetch records, their indices and weights.
        batch = buffer.sample(1000, beta=self.beta)
        indices = batch["batch_indexes"]
        counts = Counter()
        for i in indices:
            counts[i] += 1

        # Expect an approximately uniform distribution of indices.
        self.assertTrue(any(100 < i < 300 for i in counts.values()))
        # Test get_state/set_state.
        state = buffer.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=0.01)
        new_memory.set_state(state)
        batch = new_memory.sample(1000, beta=self.beta)
        indices = batch["batch_indexes"]
        counts = Counter()
        for i in indices:
            counts[i] += 1
        self.assertTrue(any(100 < i < 300 for i in counts.values()))

    def test_sequences_unit(self):
        """Tests adding, sampling and eviction of sequences."""
        # We do not test the mathematical correctness of our prioritization
        # here but rather if it works together with sequence mode at all

        buffer = PrioritizedReplayBuffer(capacity=10, storage_unit="sequences")

        batches = [
            SampleBatch(
                {
                    SampleBatch.T: i * [np.random.random((4,))],
                    SampleBatch.ACTIONS: i * [np.random.choice([0, 1])],
                    SampleBatch.REWARDS: i * [np.random.rand()],
                    SampleBatch.TERMINATEDS: i * [np.random.choice([False, True])],
                    SampleBatch.TRUNCATEDS: i * [np.random.choice([False, True])],
                    SampleBatch.SEQ_LENS: [i],
                    "batch_id": i * [i],
                }
            )
            for i in range(1, 4)
        ]

        # Add some batches with sequences of low priority
        for batch in batches:
            buffer.add(batch, weight=0.01)

        # Add two high priority sequences
        buffer.add(
            SampleBatch(
                {
                    SampleBatch.T: 4 * [np.random.random((4,))],
                    SampleBatch.ACTIONS: 4 * [np.random.choice([0, 1])],
                    SampleBatch.REWARDS: 4 * [np.random.rand()],
                    SampleBatch.TERMINATEDS: 4 * [np.random.choice([False, True])],
                    SampleBatch.TRUNCATEDS: 4 * [np.random.choice([False, True])],
                    SampleBatch.SEQ_LENS: [2, 2],
                    "batch_id": 4 * [4],
                }
            ),
            weight=1,
        )

        num_sampled_dict = {_id: 0 for _id in range(1, 5)}
        num_samples = 200
        for i in range(num_samples):
            sample = buffer.sample(1, beta=self.beta)
            _id = sample["batch_id"][0]
            assert len(sample[SampleBatch.SEQ_LENS]) == 1
            num_sampled_dict[_id] += 1

        # Out of five sequences, we want to sequences from the last batch to
        # be sampled almost always
        assert np.allclose(
            np.array(list(num_sampled_dict.values())) / num_samples,
            [0.1, 0.1, 0.1, 0.8],
            atol=0.2,
        )

        # Add another batch to evict
        buffer.add(
            SampleBatch(
                {
                    SampleBatch.T: 5 * [np.random.random((4,))],
                    SampleBatch.ACTIONS: 5 * [np.random.choice([0, 1])],
                    SampleBatch.REWARDS: 5 * [np.random.rand()],
                    SampleBatch.TERMINATEDS: 5 * [np.random.choice([False, True])],
                    SampleBatch.TRUNCATEDS: 5 * [np.random.choice([False, True])],
                    SampleBatch.SEQ_LENS: [5],
                    "batch_id": 5 * [5],
                }
            ),
            weight=1,
        )

        # After adding 1 more batch, eviction has started with 15
        # timesteps added in total
        assert len(buffer) == 5
        assert buffer._num_timesteps_added == sum(range(1, 6))
        assert buffer._num_timesteps_added_wrap == 5
        assert buffer._next_idx == 1
        assert buffer._eviction_started is True

        num_sampled_dict = {_id: 0 for _id in range(1, 6)}
        num_samples = 200
        for i in range(num_samples):
            sample = buffer.sample(1, beta=self.beta)
            _id = sample["batch_id"][0]
            assert len(sample[SampleBatch.SEQ_LENS]) == 1
            num_sampled_dict[_id] += 1

        # Out of all six sequences, we want sequences from batches 4 and 5
        # to be sampled with equal probability
        assert np.allclose(
            np.array(list(num_sampled_dict.values())) / num_samples,
            [0, 0, 0, 0.5, 0.5],
            atol=0.25,
        )

    def test_episodes_unit(self):
        """Tests adding, sampling, and eviction of episodes."""
        # We do not test the mathematical correctness of our prioritization
        # here but rather if it works together with episode mode at all
        buffer = PrioritizedReplayBuffer(capacity=18, storage_unit="episodes")

        batches = [
            SampleBatch(
                {
                    SampleBatch.T: [0, 1, 2, 3],
                    SampleBatch.ACTIONS: 4 * [np.random.choice([0, 1])],
                    SampleBatch.REWARDS: 4 * [np.random.rand()],
                    SampleBatch.TERMINATEDS: [False, False, False, True],
                    SampleBatch.TRUNCATEDS: [False, False, False, False],
                    SampleBatch.SEQ_LENS: [4],
                    SampleBatch.EPS_ID: 4 * [i],
                }
            )
            for i in range(3)
        ]

        # Add some batches with episodes of low priority
        for batch in batches:
            buffer.add(batch, weight=0.01)

        # Add two high priority episodes
        buffer.add(
            SampleBatch(
                {
                    SampleBatch.T: [0, 1, 0, 1],
                    SampleBatch.ACTIONS: 4 * [np.random.choice([0, 1])],
                    SampleBatch.REWARDS: 4 * [np.random.rand()],
                    SampleBatch.TERMINATEDS: [False, True, False, True],
                    SampleBatch.TRUNCATEDS: [False, False, False, True],
                    SampleBatch.SEQ_LENS: [2, 2],
                    SampleBatch.EPS_ID: [3, 3, 4, 4],
                }
            ),
            weight=1,
        )

        num_sampled_dict = {_id: 0 for _id in range(5)}
        num_samples = 200
        for i in range(num_samples):
            sample = buffer.sample(1, beta=self.beta)
            _id = sample[SampleBatch.EPS_ID][0]
            assert len(sample[SampleBatch.SEQ_LENS]) == 1
            num_sampled_dict[_id] += 1

        # All episodes, even though in different batches should be sampled
        # equally often
        assert np.allclose(
            np.array(list(num_sampled_dict.values())) / num_samples,
            [0, 0, 0, 0.5, 0.5],
            atol=0.1,
        )

        # Episode 6 is not entirely inside this batch, it should not be added
        # to the buffer
        buffer.add(
            SampleBatch(
                {
                    SampleBatch.T: [0, 1, 0, 1],
                    SampleBatch.ACTIONS: 4 * [np.random.choice([0, 1])],
                    SampleBatch.REWARDS: 4 * [np.random.rand()],
                    SampleBatch.TERMINATEDS: [False, True, False, False],
                    SampleBatch.TRUNCATEDS: [False, False, False, False],
                    SampleBatch.SEQ_LENS: [2, 2],
                    SampleBatch.EPS_ID: [5, 5, 6, 6],
                }
            ),
            weight=1,
        )

        num_sampled_dict = {_id: 0 for _id in range(7)}
        num_samples = 200
        for i in range(num_samples):
            sample = buffer.sample(1, beta=self.beta)
            _id = sample[SampleBatch.EPS_ID][0]
            assert len(sample[SampleBatch.SEQ_LENS]) == 1
            num_sampled_dict[_id] += 1

        # Episode 7 should be dropped for not ending inside the batch
        assert np.allclose(
            np.array(list(num_sampled_dict.values())) / num_samples,
            [0, 0, 0, 1 / 3, 1 / 3, 1 / 3, 0],
            atol=0.1,
        )

        # Add another batch to evict the first batch.
        buffer.add(
            SampleBatch(
                {
                    SampleBatch.T: [0, 1, 2, 3],
                    SampleBatch.ACTIONS: 4 * [np.random.choice([0, 1])],
                    SampleBatch.REWARDS: 4 * [np.random.rand()],
                    SampleBatch.TERMINATEDS: [False, False, False, True],
                    SampleBatch.TRUNCATEDS: [False, False, False, True],
                    SampleBatch.SEQ_LENS: [4],
                    SampleBatch.EPS_ID: 4 * [7],
                }
            ),
            weight=0.01,
        )

        # After adding 1 more batch, eviction has started with 24
        # timesteps added in total, 2 of which were discarded
        assert len(buffer) == 6
        assert buffer._num_timesteps_added == 4 * 6 - 2
        assert buffer._num_timesteps_added_wrap == 4
        assert buffer._next_idx == 1
        assert buffer._eviction_started is True

        num_sampled_dict = {_id: 0 for _id in range(8)}
        num_samples = 200
        for i in range(num_samples):
            sample = buffer.sample(1, beta=self.beta)
            _id = sample[SampleBatch.EPS_ID][0]
            assert len(sample[SampleBatch.SEQ_LENS]) == 1
            num_sampled_dict[_id] += 1

        assert np.allclose(
            np.array(list(num_sampled_dict.values())) / num_samples,
            [0, 0, 0, 1 / 3, 1 / 3, 1 / 3, 0, 0],
            atol=0.1,
        )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
