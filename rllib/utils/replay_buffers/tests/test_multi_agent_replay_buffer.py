import unittest

import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch, concat_samples
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID

from ray.rllib.utils.replay_buffers.multi_agent_replay_buffer import (
    MultiAgentReplayBuffer,
)
from ray.rllib.utils.replay_buffers.prioritized_replay_buffer import (
    PrioritizedReplayBuffer,
)
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer


def get_batch_id(batch, policy_id=DEFAULT_POLICY_ID):
    return batch.policy_batches[policy_id]["batch_id"][0]


class TestMultiAgentReplayBuffer(unittest.TestCase):
    batch_id = 0

    def _add_sample_batch_to_buffer(self, buffer, batch_size, num_batches=5, **kwargs):
        self.eps_id = 0

        def _generate_data():
            self.eps_id += 1
            return SampleBatch(
                {
                    SampleBatch.T: [0, 1],
                    SampleBatch.ACTIONS: 2 * [np.random.choice([0, 1])],
                    SampleBatch.REWARDS: 2 * [np.random.rand()],
                    SampleBatch.OBS: 2 * [np.random.random((4,))],
                    SampleBatch.NEXT_OBS: 2 * [np.random.random((4,))],
                    SampleBatch.TERMINATEDS: 2 * [np.random.choice([False, True])],
                    SampleBatch.TRUNCATEDS: 2 * [np.random.choice([False, True])],
                    SampleBatch.EPS_ID: 2 * [self.eps_id],
                    SampleBatch.AGENT_INDEX: 2 * [0],
                    "batch_id": 2 * [self.batch_id],
                }
            )

        for i in range(num_batches):
            data = [_generate_data() for _ in range(batch_size)]
            self.batch_id += 1
            batch = concat_samples(data)
            buffer.add(batch, **kwargs)

    def _add_multi_agent_batch_to_buffer(
        self, buffer, num_policies, num_batches=5, seq_lens=False, **kwargs
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
                idx: _generate_data(idx) for idx, _ in enumerate(range(num_policies))
            }
            batch = MultiAgentBatch(policy_batches, 1)
            buffer.add(batch, **kwargs)

    def test_policy_id_of_multi_agent_batches_independent(self):
        """Test if indepent sampling yields a MultiAgentBatch with the
        correct policy id."""
        self.batch_id = 0

        # Test lockstep mode with different policy ids using MultiAgentBatches
        buffer = MultiAgentReplayBuffer(
            capacity_ts=10, replay_mode="independent", num_shards=1
        )

        self._add_multi_agent_batch_to_buffer(buffer, num_policies=1, num_batches=1)

        mabatch = buffer.sample(1)
        assert list(mabatch.policy_batches.keys())[0] == 0

    def test_lockstep_mode(self):
        """Test the lockstep mode by only adding SampleBatches.

        Such SampleBatches are converted to MultiAgent Batches as if there
        was only one policy."""
        self.batch_id = 0
        batch_size = 5
        buffer_size = 30

        buffer = MultiAgentReplayBuffer(
            capacity_ts=buffer_size,
            replay_mode="lockstep",
            num_shards=1,
        )

        # Test add/sample
        self._add_sample_batch_to_buffer(buffer, batch_size=batch_size, num_batches=1)

        # Sampling from it now should yield the first batch
        assert get_batch_id(buffer.sample(1)) == 0

        self._add_sample_batch_to_buffer(buffer, batch_size=batch_size, num_batches=2)

        # Sampling from it now should yield our first batch 1/3 of the time
        num_sampled_dict = {_id: 0 for _id in range(self.batch_id)}
        num_samples = 200
        for i in range(num_samples):
            _id = get_batch_id(buffer.sample(1))
            num_sampled_dict[_id] += 1
        assert np.allclose(
            np.array(list(num_sampled_dict.values())) / num_samples,
            len(num_sampled_dict) * [1 / 3],
            atol=0.1,
        )

    def test_independent_mode_sequences_storage_unit(self):
        """Test the independent mode with sequences as a storage unit.

        Such SampleBatches are converted to MultiAgentBatches as if there
        was only one policy."""
        buffer_size = 15
        self.batch_id = 0

        buffer = MultiAgentReplayBuffer(
            capacity_ts=buffer_size,
            replay_mode="independent",
            storage_unit="sequences",
            replay_sequence_length=2,
            num_shards=1,
        )

        # Test add/sample
        self._add_multi_agent_batch_to_buffer(
            buffer, num_policies=2, num_batches=1, seq_lens=True
        )

        # Sampling from it now should yield the first batch
        assert get_batch_id(buffer.sample(1), 0) == 0

        self._add_multi_agent_batch_to_buffer(
            buffer, num_policies=2, num_batches=2, seq_lens=True
        )

        # Sampling from it now should yield each batch that went into a
        # multiagent batch 1/6th of the time
        num_sampled_dict = {_id: 0 for _id in range(self.batch_id)}
        num_samples = 200
        for i in range(num_samples):
            sample = buffer.sample(1)
            # Count one of both policy batches
            _id = get_batch_id(sample, np.random.choice([0, 1]))
            num_sampled_dict[_id] += 1
            # See if a random batch has the desired sequence length of two
            assert len(sample.policy_batches[np.random.choice([0, 1])]) == 2
        assert np.allclose(
            np.array(list(num_sampled_dict.values())) / num_samples,
            len(num_sampled_dict) * [1 / 6],
            atol=0.1,
        )

    def test_independent_mode_multiple_policies(self):
        """Test the lockstep mode by adding batches from multiple policies."""

        num_batches = 3
        buffer_size = 15
        num_policies = 2
        # Test lockstep mode with different policy ids using MultiAgentBatches

        self.batch_id = 0

        buffer = MultiAgentReplayBuffer(
            capacity_ts=buffer_size,
            replay_mode="independent",
            num_steps_sampled_before_learning_starts=0,
            num_shards=1,
        )

        self._add_multi_agent_batch_to_buffer(
            buffer, num_policies=num_policies, num_batches=num_batches
        )

        # Sample 4 SampleBatches from only one policy
        for _id in range(num_policies):
            for __id in buffer.sample(4, policy_id=_id).policy_batches[_id][
                "policy_id"
            ]:
                assert __id == _id

        # Sample without specifying the policy should yield the same number
        # of batches from each policy
        num_sampled_dict = {_id: 0 for _id in range(num_policies)}
        num_samples = 200
        for i in range(num_samples):
            num_items = np.random.randint(0, 5)
            for _id, batch in buffer.sample(num_items=num_items).policy_batches.items():
                num_sampled_dict[_id] += 1
                assert len(batch) == num_items
        assert np.allclose(
            np.array(list(num_sampled_dict.values())),
            len(num_sampled_dict) * [200],
            atol=0.1,
        )

    def test_lockstep_with_underlying_replay_buffer(self):
        """Test this the buffer with different underlying buffers.

        Test if we can initialize a simple underlying buffer without
        additional arguments and lockstep sampling.
        """
        # Test with ReplayBuffer, no args for c'tor, add and sample
        replay_buffer_config = {"type": ReplayBuffer}

        num_policies = 2
        buffer_size = 200
        num_batches = 20

        buffer = MultiAgentReplayBuffer(
            capacity_ts=buffer_size,
            replay_mode="lockstep",
            num_shards=1,
            underlying_buffer_config=replay_buffer_config,
        )

        self._add_multi_agent_batch_to_buffer(
            buffer, num_policies=num_policies - 1, num_batches=num_batches
        )

        # Only test if we can sample and if samples belong to a single policy
        sample = buffer.sample(2)
        assert len(sample) == 2
        assert len(sample.policy_batches) == 1

        self._add_multi_agent_batch_to_buffer(
            buffer, num_policies=num_policies, num_batches=num_batches
        )

        # Only test if we can sample from multiple policies, out of 100
        # samples, some should be of each policy
        sample = buffer.sample(100)
        assert len(sample) == 100
        assert len(sample.policy_batches) == 2

    def test_independent_with_underlying_prioritized_replay_buffer(self):
        """Test this the buffer with different underlying buffers.

        Test if we can initialize a more complex underlying buffer with
        additional arguments and independent sampling.
        This does not test updating priorities and using weights as
        implemented in MultiAgentPrioritizedReplayBuffer.
        """
        # Test with PrioritizedReplayBuffer, args for c'tor, add and sample
        prioritized_replay_buffer_config = {
            "type": PrioritizedReplayBuffer,
            "alpha": 0.6,
            "beta": 0.4,
        }

        num_policies = 2
        buffer_size = 15
        num_batches = 1

        buffer = MultiAgentReplayBuffer(
            capacity_ts=buffer_size,
            replay_mode="independent",
            num_shards=1,
            underlying_buffer_config=prioritized_replay_buffer_config,
        )

        self._add_multi_agent_batch_to_buffer(
            buffer, num_policies=num_policies, num_batches=num_batches
        )

        # Only test if we can sample from multiple policies
        sample = buffer.sample(2)
        assert len(sample) == 4
        assert len(sample.policy_batches) == 2

    def test_set_get_state(self):
        num_policies = 2
        buffer_size = 15
        num_batches = 1

        buffer = MultiAgentReplayBuffer(
            capacity_ts=buffer_size,
            replay_mode="independent",
            num_shards=1,
        )

        self._add_multi_agent_batch_to_buffer(
            buffer, num_policies=num_policies, num_batches=num_batches
        )

        state = buffer.get_state()

        another_buffer = MultiAgentReplayBuffer(
            capacity_ts=buffer_size,
            replay_mode="independent",
            num_steps_sampled_before_learning_starts=0,
            num_shards=1,
        )

        another_buffer.set_state(state)

        # State is equal to set of states of underlying buffers
        for _id, _buffer in buffer.replay_buffers.items():
            assert _buffer.get_state() == another_buffer.replay_buffers[_id].get_state()

        assert buffer._num_added == another_buffer._num_added


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
