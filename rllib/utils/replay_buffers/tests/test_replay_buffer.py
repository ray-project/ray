import unittest

import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch

from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer


class TestReplayBuffers(unittest.TestCase):

    batch_id = 0

    def _add_data_to_buffer(self, buffer, batch_size, num_batches=5, **kwargs):
        def _generate_data():
            return SampleBatch(
                {
                    "obs_t": [np.random.random((4,))],
                    "action": [np.random.choice([0, 1])],
                    "reward": [np.random.rand()],
                    "obs_tp1": [np.random.random((4,))],
                    "done": [np.random.choice([False, True])],
                    "batch_id": [self.batch_id],
                }
            )

        batches = list()
        for i in range(num_batches):
            data = [_generate_data() for _ in range(batch_size)]
            self.batch_id += 1
            batch = SampleBatch.concat_samples(data)
            buffer.add(batch, **kwargs)

    def test_replay_buffer(self):
        batch_size = 5
        buffer_size = 15

        buffer = ReplayBuffer(capacity=buffer_size)

        # Test add/sample
        self._add_data_to_buffer(buffer,
                                      batch_size=batch_size,
                                      num_batches=1)

        # After adding a single batch to a buffer, it should not be full
        assert len(buffer) == 1
        assert buffer._num_timesteps_added == 5
        assert buffer._num_timesteps_added_wrap == 5
        assert buffer._next_idx == 1
        assert buffer._eviction_started is False

        # Sampling from it now should yield the first batch
        assert buffer.sample(1)["batch_id"][0] == 0

        self._add_data_to_buffer(buffer, batch_size=batch_size, num_batches=2)

        # After adding two more batches, the buffer should be full
        assert len(buffer) == 3
        assert buffer._num_timesteps_added == 15
        assert buffer._num_timesteps_added_wrap == 0
        assert buffer._next_idx == 0
        assert buffer._eviction_started is True

        # Sampling from it now should yield our first batch 1/3 of the time
        num_sampled_dict = {_id: 0 for _id in range(self.batch_id)}
        num_samples = 200
        for i in range(num_samples):
            _id = buffer.sample(1)["batch_id"][0]
            num_sampled_dict[_id] += 1
        assert np.allclose(
            np.array(list(num_sampled_dict.values()))/num_samples,
                len(num_sampled_dict) * [1/3],
                    atol=0.1)

        # TODO (artur): Test adding batches of different sizes

        # Test set/get state
        state = buffer.get_state()
        other_buffer = ReplayBuffer(capacity=buffer_size)
        self._add_data_to_buffer(other_buffer, 1)
        other_buffer.set_state(state)

        assert other_buffer._storage == buffer._storage
        assert other_buffer._next_idx == buffer._next_idx
        assert other_buffer._num_timesteps_added == buffer._num_timesteps_added
        assert other_buffer._num_timesteps_added_wrap == \
               buffer._num_timesteps_added_wrap
        assert other_buffer._num_timesteps_sampled == \
               buffer._num_timesteps_sampled
        assert other_buffer._eviction_started == buffer._eviction_started
        assert other_buffer._est_size_bytes == buffer._est_size_bytes
        assert len(other_buffer) == len(other_buffer)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
