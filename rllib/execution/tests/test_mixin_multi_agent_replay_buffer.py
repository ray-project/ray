from collections import Counter
import numpy as np
import unittest

from ray.rllib.execution.buffers.mixin_replay_buffer import \
    MixInMultiAgentReplayBuffer
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.test_utils import check


class TestMixInMultiAgentReplayBuffer(unittest.TestCase):
    """Tests insertion and mixed sampling of the MixInMultiAgentReplayBuffer.
    """

    capacity = 10

    def _generate_data(self):
        return SampleBatch({
            "obs": [np.random.random((4, ))],
            "action": [np.random.choice([0, 1])],
            "reward": [np.random.rand()],
            "new_obs": [np.random.random((4, ))],
            "done": [np.random.choice([False, True])],
        })

    def test_mixin_sampling(self):
        # 50% repay ratio.
        buffer = MixInMultiAgentReplayBuffer(
            capacity=self.capacity, replay_ratio=0.5)

        # Add a new batch.
        batch = self._generate_data()
        buffer.add_batch(batch)

        sample = buffer.replay()

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


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
