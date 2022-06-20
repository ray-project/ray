import unittest
import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.replay_buffers.reservoir_replay_buffer import ReservoirReplayBuffer


class TestReservoirBuffer(unittest.TestCase):
    def test_timesteps_unit(self):
        """Tests adding, sampling, get-/set state, and eviction with
        experiences stored by timesteps."""
        self.batch_id = 0

        def _add_data_to_buffer(_buffer, batch_size, num_batches=5, **kwargs):
            def _generate_data():
                return SampleBatch(
                    {
                        SampleBatch.T: [np.random.random((4,))],
                        SampleBatch.ACTIONS: [np.random.choice([0, 1])],
                        SampleBatch.OBS: [np.random.random((4,))],
                        SampleBatch.NEXT_OBS: [np.random.random((4,))],
                        SampleBatch.REWARDS: [np.random.rand()],
                        SampleBatch.DONES: [np.random.choice([False, True])],
                        "batch_id": [self.batch_id],
                    }
                )

            for i in range(num_batches):
                data = [_generate_data() for _ in range(batch_size)]
                self.batch_id += 1
                batch = SampleBatch.concat_samples(data)
                _buffer.add(batch, **kwargs)

        batch_size = 1
        buffer_size = 100

        buffer = ReservoirReplayBuffer(capacity=buffer_size)
        # Put 1000 batches in a buffer with capacity 100
        _add_data_to_buffer(buffer, batch_size=batch_size, num_batches=1000)

        # Expect the batch id to be ~500 on average
        batch_id_sum = 0
        for i in range(200):
            num_ts_sampled = np.random.randint(1, 10)
            sample = buffer.sample(num_ts_sampled)
            batch_id_sum += sum(sample["batch_id"]) / num_ts_sampled

        self.assertAlmostEqual(batch_id_sum / 200, 500, delta=100)

    def test_episodes_unit(self):
        """Tests adding, sampling, get-/set state, and eviction with
        experiences stored by timesteps."""
        self.batch_id = 0

        def _add_data_to_buffer(_buffer, batch_size, num_batches=5, **kwargs):
            def _generate_data():
                return SampleBatch(
                    {
                        SampleBatch.T: [0, 1],
                        SampleBatch.ACTIONS: 2 * [np.random.choice([0, 1])],
                        SampleBatch.REWARDS: 2 * [np.random.rand()],
                        SampleBatch.OBS: 2 * [np.random.random((4,))],
                        SampleBatch.NEXT_OBS: 2 * [np.random.random((4,))],
                        SampleBatch.DONES: [False, True],
                        SampleBatch.AGENT_INDEX: 2 * [0],
                        "batch_id": 2 * [self.batch_id],
                    }
                )

            for i in range(num_batches):
                data = [_generate_data() for _ in range(batch_size)]
                self.batch_id += 1
                batch = SampleBatch.concat_samples(data)
                _buffer.add(batch, **kwargs)

        batch_size = 1
        buffer_size = 100

        buffer = ReservoirReplayBuffer(capacity=buffer_size, storage_unit="fragments")
        # Put 1000 batches in a buffer with capacity 100
        _add_data_to_buffer(buffer, batch_size=batch_size, num_batches=1000)

        # Expect the batch id to be ~500 on average
        batch_id_sum = 0
        for i in range(200):
            num_episodes_sampled = np.random.randint(1, 10)
            sample = buffer.sample(num_episodes_sampled)
            num_ts_sampled = num_episodes_sampled * 2
            batch_id_sum += sum(sample["batch_id"]) / num_ts_sampled

        self.assertAlmostEqual(batch_id_sum / 200, 500, delta=100)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
