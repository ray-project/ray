from collections import Counter
import numpy as np
import unittest

from ray.rllib.execution.buffers.replay_buffer import PrioritizedReplayBuffer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import check


class TestPrioritizedReplayBuffer(unittest.TestCase):
    """
    Tests insertion and (weighted) sampling of the PrioritizedReplayBuffer.
    """

    capacity = 10
    alpha = 1.0
    beta = 1.0
    max_priority = 1.0

    def _generate_data(self):
        return SampleBatch(
            {
                "obs_t": [np.random.random((4,))],
                "action": [np.random.choice([0, 1])],
                "reward": [np.random.rand()],
                "obs_tp1": [np.random.random((4,))],
                "done": [np.random.choice([False, True])],
            }
        )

    def test_sequence_size(self):
        # Seq-len=1.
        memory = PrioritizedReplayBuffer(capacity=100, alpha=0.1)
        for _ in range(200):
            memory.add(self._generate_data(), weight=None)
        assert len(memory._storage) == 100, len(memory._storage)
        assert memory.stats()["added_count"] == 200, memory.stats()
        # Test get_state/set_state.
        state = memory.get_state()
        new_memory = PrioritizedReplayBuffer(capacity=100, alpha=0.1)
        new_memory.set_state(state)
        assert len(new_memory._storage) == 100, len(new_memory._storage)
        assert new_memory.stats()["added_count"] == 200, new_memory.stats()

        # Seq-len=5.
        memory = PrioritizedReplayBuffer(capacity=100, alpha=0.1)
        for _ in range(40):
            memory.add(
                SampleBatch.concat_samples([self._generate_data() for _ in range(5)]),
                weight=None,
            )
        assert len(memory._storage) == 20, len(memory._storage)
        assert memory.stats()["added_count"] == 200, memory.stats()
        # Test get_state/set_state.
        state = memory.get_state()
        new_memory = PrioritizedReplayBuffer(capacity=100, alpha=0.1)
        new_memory.set_state(state)
        assert len(new_memory._storage) == 20, len(new_memory._storage)
        assert new_memory.stats()["added_count"] == 200, new_memory.stats()

    def test_add(self):
        memory = PrioritizedReplayBuffer(capacity=2, alpha=self.alpha)

        # Assert indices 0 before insert.
        self.assertEqual(len(memory), 0)
        self.assertEqual(memory._next_idx, 0)

        # Insert single record.
        data = self._generate_data()
        memory.add(data, weight=0.5)
        self.assertTrue(len(memory) == 1)
        self.assertTrue(memory._next_idx == 1)

        # Insert single record.
        data = self._generate_data()
        memory.add(data, weight=0.1)
        self.assertTrue(len(memory) == 2)
        self.assertTrue(memory._next_idx == 0)

        # Insert over capacity.
        data = self._generate_data()
        memory.add(data, weight=1.0)
        self.assertTrue(len(memory) == 2)
        self.assertTrue(memory._next_idx == 1)

        # Test get_state/set_state.
        state = memory.get_state()
        new_memory = PrioritizedReplayBuffer(capacity=2, alpha=self.alpha)
        new_memory.set_state(state)
        self.assertTrue(len(new_memory) == 2)
        self.assertTrue(new_memory._next_idx == 1)

    def test_update_priorities(self):
        memory = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)

        # Insert n samples.
        num_records = 5
        for i in range(num_records):
            data = self._generate_data()
            memory.add(data, weight=1.0)
            self.assertTrue(len(memory) == i + 1)
            self.assertTrue(memory._next_idx == i + 1)

        # Test get_state/set_state.
        state = memory.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)
        new_memory.set_state(state)
        self.assertTrue(len(new_memory) == num_records)
        self.assertTrue(new_memory._next_idx == num_records)

        # Fetch records, their indices and weights.
        batch = memory.sample(3, beta=self.beta)
        weights = batch["weights"]
        indices = batch["batch_indexes"]
        check(weights, np.ones(shape=(3,)))
        self.assertEqual(3, len(indices))
        self.assertTrue(len(memory) == num_records)
        self.assertTrue(memory._next_idx == num_records)

        # Update weight of indices 0, 2, 3, 4 to very small.
        memory.update_priorities(
            np.array([0, 2, 3, 4]), np.array([0.01, 0.01, 0.01, 0.01])
        )
        # Expect to sample almost only index 1
        # (which still has a weight of 1.0).
        for _ in range(10):
            batch = memory.sample(1000, beta=self.beta)
            indices = batch["batch_indexes"]
            self.assertTrue(970 < np.sum(indices) < 1100)
        # Test get_state/set_state.
        state = memory.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)
        new_memory.set_state(state)
        batch = new_memory.sample(1000, beta=self.beta)
        indices = batch["batch_indexes"]
        self.assertTrue(970 < np.sum(indices) < 1100)

        # Update weight of indices 0 and 1 to >> 0.01.
        # Expect to sample 0 and 1 equally (and some 2s, 3s, and 4s).
        for _ in range(10):
            rand = np.random.random() + 0.2
            memory.update_priorities(np.array([0, 1]), np.array([rand, rand]))
            batch = memory.sample(1000, beta=self.beta)
            indices = batch["batch_indexes"]
            # Expect biased to higher values due to some 2s, 3s, and 4s.
            self.assertTrue(400 < np.sum(indices) < 800)
        # Test get_state/set_state.
        state = memory.get_state()
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
            memory.update_priorities(np.array([0, 1]), np.array([rand, rand * 2]))
            batch = memory.sample(1000, beta=self.beta)
            indices = batch["batch_indexes"]
            # print(np.sum(indices))
            self.assertTrue(600 < np.sum(indices) < 850)
        # Test get_state/set_state.
        state = memory.get_state()
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
            memory.update_priorities(np.array([0, 1]), np.array([rand, rand * 4]))
            batch = memory.sample(1000, beta=self.beta)
            indices = batch["batch_indexes"]
            self.assertTrue(750 < np.sum(indices) < 950)
        # Test get_state/set_state.
        state = memory.get_state()
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
            memory.update_priorities(np.array([0, 1]), np.array([rand, rand * 9]))
            batch = memory.sample(1000, beta=self.beta)
            indices = batch["batch_indexes"]
            self.assertTrue(850 < np.sum(indices) < 1100)
        # Test get_state/set_state.
        state = memory.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)
        new_memory.set_state(state)
        batch = new_memory.sample(1000, beta=self.beta)
        indices = batch["batch_indexes"]
        self.assertTrue(850 < np.sum(indices) < 1100)

        # Insert n more samples.
        num_records = 5
        for i in range(num_records):
            data = self._generate_data()
            memory.add(data, weight=1.0)
            self.assertTrue(len(memory) == i + 6)
            self.assertTrue(memory._next_idx == (i + 6) % self.capacity)

        # Update all weights to be 1.0 to 10.0 and sample a >100 batch.
        memory.update_priorities(
            np.array([0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            np.array([0.001, 0.1, 2.0, 8.0, 16.0, 32.0, 64.0, 128.0, 256.0, 512.0]),
        )
        counts = Counter()
        for _ in range(10):
            batch = memory.sample(np.random.randint(100, 600), beta=self.beta)
            indices = batch["batch_indexes"]
            for i in indices:
                counts[i] += 1
        print(counts)
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
        state = memory.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=self.alpha)
        new_memory.set_state(state)
        counts = Counter()
        for _ in range(10):
            batch = new_memory.sample(np.random.randint(100, 600), beta=self.beta)
            indices = batch["batch_indexes"]
            for i in indices:
                counts[i] += 1
        print(counts)
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
        memory = PrioritizedReplayBuffer(self.capacity, alpha=0.01)

        # Insert n samples.
        num_records = 5
        for i in range(num_records):
            data = self._generate_data()
            memory.add(data, weight=np.random.rand())
            self.assertTrue(len(memory) == i + 1)
            self.assertTrue(memory._next_idx == i + 1)
        # Test get_state/set_state.
        state = memory.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=0.01)
        new_memory.set_state(state)
        self.assertTrue(len(new_memory) == num_records)
        self.assertTrue(new_memory._next_idx == num_records)

        # Fetch records, their indices and weights.
        batch = memory.sample(1000, beta=self.beta)
        indices = batch["batch_indexes"]
        counts = Counter()
        for i in indices:
            counts[i] += 1
        print(counts)
        # Expect an approximately uniform distribution of indices.
        self.assertTrue(any(100 < i < 300 for i in counts.values()))
        # Test get_state/set_state.
        state = memory.get_state()
        new_memory = PrioritizedReplayBuffer(self.capacity, alpha=0.01)
        new_memory.set_state(state)
        batch = new_memory.sample(1000, beta=self.beta)
        indices = batch["batch_indexes"]
        counts = Counter()
        for i in indices:
            counts[i] += 1
        self.assertTrue(any(100 < i < 300 for i in counts.values()))


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
