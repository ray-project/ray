import unittest

import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID

from ray.rllib.utils.replay_buffers.multi_agent_replay_buffer import \
    MultiAgentReplayBuffer
from ray.rllib.utils.replay_buffers.prioritized_replay_buffer import \
    PrioritizedReplayBuffer
from ray.rllib.utils.replay_buffers.replay_buffer import ReplayBuffer


def get_batch_id(batch, policy_id=DEFAULT_POLICY_ID):
    return batch.policy_batches[policy_id][
        "batch_id"][0]


class TestMultiAgentReplayBuffer(unittest.TestCase):
    batch_id = 0

    def _add_sample_batch_to_buffer(self, buffer, batch_size, num_batches=5,
                                    **kwargs):
        def _generate_data():
            return SampleBatch(
                {
                    "obs_t": [np.random.random((4,))],
                    "action": [np.random.choice([0, 1])],
                    "reward": [np.random.rand()],
                    "obs_tp1": [np.random.random((4,))],
                    "done": [np.random.choice([False, True])],
                    "batch_id": [self.batch_id],
                    "eps_id": [self.batch_id],
                    "t": [0],
                    "agent_index": [0],
                }
            )

        for i in range(num_batches):
            data = [_generate_data() for _ in range(batch_size)]
            self.batch_id += 1
            batch = SampleBatch.concat_samples(data)
            buffer.add(batch, **kwargs)

    def _add_multi_agent_batch_to_buffer(self, buffer, num_policies,
                                         num_batches=5, **kwargs):

        def _generate_data(policy_id):
            batch = SampleBatch(
                {
                    "obs_t": [np.random.random((4,))],
                    "action": [np.random.choice([0, 1])],
                    "reward": [np.random.rand()],
                    "obs_tp1": [np.random.random((4,))],
                    "done": [np.random.choice([False, True])],
                    "batch_id": [self.batch_id],
                    "eps_id": [self.batch_id],
                    "t": [0],
                    "policy_id": [policy_id]
                }
            )
            return batch

        for i in range(num_batches):
            # genera a few policy batches
            policy_batches = {idx: _generate_data(idx) for idx,
                                                           _ in
                              enumerate(range(
                                  num_policies))}
            self.batch_id += 1
            batch = MultiAgentBatch(policy_batches, 1)
            buffer.add(batch, **kwargs)

    def test_lockstep_mode_single_policy(self):
        """Test the lockstep mode by only adding SampleBatches.

        Such SampleBatches are converted to MultiAgent Batches as if there
        was only one policy."""
        batch_size = 5
        buffer_size = 15

        buffer = MultiAgentReplayBuffer(capacity=buffer_size,
                                        replay_mode="lockstep",
                                        learning_starts=0,
                                        num_shards=1)

        # Test add/sample
        self._add_sample_batch_to_buffer(buffer,
                                         batch_size=batch_size,
                                         num_batches=1)

        # Sampling from it now should yield the first batch
        assert get_batch_id(buffer.sample(1)) == 0

        self._add_sample_batch_to_buffer(buffer, batch_size=batch_size,
                                         num_batches=2)

        # Sampling from it now should yield our first batch 1/3 of the time
        num_sampled_dict = {_id: 0 for _id in range(self.batch_id)}
        num_samples = 200
        for i in range(num_samples):
            _id = get_batch_id(buffer.sample(1))
            num_sampled_dict[_id] += 1
        assert np.allclose(
            np.array(list(num_sampled_dict.values())) / num_samples,
            len(num_sampled_dict) * [1 / 3],
            atol=0.1)

    def test_lockstep_mode_multiple_policies(self):
        """Test the lockstep mode by adding batches from multiple policies."""

        num_batches = 3
        buffer_size = 15
        num_policies = 2
        # Test lockstep mode with different policy ids using MultiAgentBatches

        self.batch_id = 0

        buffer = MultiAgentReplayBuffer(capacity=buffer_size,
                                        replay_mode="lockstep",
                                        learning_starts=0,
                                        num_shards=1)

        self._add_multi_agent_batch_to_buffer(buffer,
                                              num_policies=num_policies,
                                              num_batches=num_batches)

        self._add_multi_agent_batch_to_buffer(buffer,
                                              num_policies=num_policies - 1,
                                              num_batches=num_batches)

        # With uneven distribution of batches per policy, we want all
        # policies to be in each batch
        num_sampled_dict = {_id: 0 for _id in range(num_policies)}
        num_samples = 200
        for i in range(num_samples):
            for _id in buffer.sample(1).policy_batches.keys():
                num_sampled_dict[_id] += 1
        assert np.allclose(
            np.array(list(num_sampled_dict.values())),
            len(num_sampled_dict) * [200],
            atol=0.1)

        # With uneven distribution of batches per policy, we want our first
        # batch ids to be sampled three times as often
        num_sampled_dict = {_id: 0 for _id in range(self.batch_id)}
        for i in range(num_samples):
            policy_batches = list(buffer.sample(1).policy_batches.values())
            for batch in policy_batches:
                _id = int(batch["batch_id"])
                num_sampled_dict[_id] += 1
        assert np.allclose(
            np.array(list(num_sampled_dict.values())) / num_samples,
            num_batches * [1 / 2] + num_batches * [1 / 6],
            atol=0.2)

    def test_independent_mode_multiple_policies(self):
        """Test the lockstep mode by adding batches from multiple policies."""

        num_batches = 3
        buffer_size = 15
        num_policies = 2
        # Test lockstep mode with different policy ids using MultiAgentBatches

        self.batch_id = 0

        buffer = MultiAgentReplayBuffer(capacity=buffer_size,
                                        replay_mode="independent",
                                        learning_starts=0,
                                        num_shards=1)

        self._add_multi_agent_batch_to_buffer(buffer,
                                              num_policies=num_policies,
                                              num_batches=num_batches)

        # Sample 4 SampleBatches from only one policy and put it into a
        # MultiAgentBatch
        for _id in range(num_policies):
            for __id in buffer.sample(4, policy_id=_id)["policy_id"]:
                assert __id == _id

        # Sample without specifying the policy should yield the same number
        # of batches from each policy
        num_sampled_dict = {_id: 0 for _id in range(num_policies)}
        num_samples = 200
        for i in range(num_samples):
            num_items = np.random.randint(0, 5)
            for _id, batch in buffer.sample(
                num_items=num_items).policy_batches.items():
                num_sampled_dict[_id] += 1
                assert len(batch) == num_items
        assert np.allclose(
            np.array(list(num_sampled_dict.values())),
            len(num_sampled_dict) * [200],
            atol=0.1)

    def test_with_underlying_replaybuffer(self):
        """Test this the buffer with different underlying buffers.

        Test if we can initialize a simple underlying buffer without
        additional arguments and lockstep sampling.
        """
        # Test with ReplayBuffer, no args for c'tor, add and sample
        reservoir_buffer_config = {
            "type": ReplayBuffer
        }

        num_policies = 2
        buffer_size = 15
        num_batches = 1

        buffer = MultiAgentReplayBuffer(capacity=buffer_size,
                                        replay_mode="lockstep",
                                        learning_starts=0,
                                        num_shards=1,
                                        underlying_buffer_config=reservoir_buffer_config)

        self._add_multi_agent_batch_to_buffer(buffer,
                                              num_policies=num_policies - 1,
                                              num_batches=num_batches)
        # Only test if we can sample and if samples belong to a single policy
        sample = buffer.sample(2)
        assert len(sample) == 1
        assert len(sample.policy_batches) == 1

        self._add_multi_agent_batch_to_buffer(buffer,
                                              num_policies=num_policies,
                                              num_batches=num_batches)

        # Only test if we can sample from multiple policies
        sample = buffer.sample(2)
        assert len(sample) == 1
        assert len(sample.policy_batches) == 2

    def test_with_underlying_prioritized_replay_buffer(self):
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

        buffer = MultiAgentReplayBuffer(capacity=buffer_size,
                                        replay_mode="independent",
                                        learning_starts=0,
                                        num_shards=1,
                                        underlying_buffer_config=prioritized_replay_buffer_config)

        self._add_multi_agent_batch_to_buffer(buffer,
                                              num_policies=num_policies,
                                              num_batches=num_batches)

        # Only test if we can sample from multiple policies
        sample = buffer.sample(2)
        assert len(sample) == 1
        assert len(sample.policy_batches) == 2

    def test_set_get_state(self):
        num_policies = 2
        buffer_size = 15
        num_batches = 1

        buffer = MultiAgentReplayBuffer(capacity=buffer_size,
                                        replay_mode="independent",
                                        learning_starts=0,
                                        num_shards=1)

        self._add_multi_agent_batch_to_buffer(buffer,
                                              num_policies=num_policies,
                                              num_batches=num_batches)

        state = buffer.get_state()

        another_buffer = MultiAgentReplayBuffer(capacity=buffer_size,
                                                replay_mode="independent",
                                                learning_starts=0,
                                                num_shards=1)

        another_buffer.set_state(state)

        # State is equal to set of states of underlying buffers
        for _id, _buffer in buffer.replay_buffers.items():
            assert _buffer.get_state() == \
                   another_buffer.replay_buffers[_id].get_state()

        assert buffer._num_added == another_buffer._num_added


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
