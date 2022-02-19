import unittest

import numpy as np

from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch, \
    DEFAULT_POLICY_ID

from ray.rllib.utils.replay_buffers.multi_agent_prioritized_replay_buffer \
    import MultiAgentPrioritizedReplayBuffer
from ray.rllib.utils.replay_buffers.tests.test_multi_agent_replay_buffer \
    import get_batch_id
from ray.rllib.utils.test_utils import check


class TestMultiAgentPrioritizedReplayBuffer(unittest.TestCase):
    batch_id = 0
    alpha = 1.0
    beta = 1.0

    def _generate_data(self):
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

    def _add_sample_batch_to_buffer(self, buffer, batch_size, num_batches=5,
                                    **kwargs):

        for i in range(num_batches):
            data = [self._generate_data() for _ in range(batch_size)]
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

    def test_independent_mode(self):
        """Test the lockstep mode by adding batches from multiple policies."""
        self.batch_id = 0

        num_batches = 3
        buffer_size = 15
        num_policies = 2

        # Test lockstep mode with different policy ids using MultiAgentBatches
        buffer = MultiAgentPrioritizedReplayBuffer(capacity=buffer_size,
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

        # Sample without specifying the policy should yield approx. the same
        # number of batches from each policy
        num_sampled_dict = {_id: 0 for _id in range(num_policies)}
        num_samples = 200
        for i in range(num_samples):
            num_items = np.random.randint(1, 5)
            for _id, batch in buffer.sample(
                num_items=num_items).policy_batches.items():
                num_sampled_dict[_id] += 1
                assert len(batch) == num_items
        assert np.allclose(
            np.array(list(num_sampled_dict.values())),
            len(num_sampled_dict) * [200],
            atol=0.1)

    def test_update_priorities(self):
        num_batches = 5
        buffer_size = 15

        # Buffer needs to be in independent mode, lockstep is not supported
        buffer = MultiAgentPrioritizedReplayBuffer(capacity=buffer_size,
                                                   prioritized_replay_alpha=self.alpha,
                                                   prioritized_replay_beta=self.beta,
                                                   replay_mode="independent",
                                                   learning_starts=0,
                                                   num_shards=1)

        # Insert n samples
        for i in range(num_batches):
            data = self._generate_data()
            buffer.add(data, weight=1.0)
            assert len(buffer) == i + 1

        # Fetch records, their indices and weights.
        batch = buffer.sample(3).policy_batches[DEFAULT_POLICY_ID]
        weights = batch["weights"]
        indices = batch["batch_indexes"]
        check(weights, np.ones(shape=(3,)))
        assert 3 == len(indices)
        assert len(buffer) == num_batches
        policy_buffer = buffer.replay_buffers[DEFAULT_POLICY_ID]
        assert policy_buffer._next_idx == num_batches
        # Update weight of indices 0, 2, 3, 4, like in our
        # PrioritizedReplayBuffer tests
        priority_dict = {DEFAULT_POLICY_ID: (np.array([0, 2, 3, 4]), np.array([
            0.01, 0.01, 0.01, 0.01]))}

        buffer.update_priorities(priority_dict)

        # Expect to sample almost only index 1
        # (which still has a weight of 1.0).
        for _ in range(10):
            batch = buffer.sample(1000).policy_batches[
                DEFAULT_POLICY_ID]
            indices = batch["batch_indexes"]
            self.assertTrue(970 < np.sum(indices) < 1100)
        # Test get_state/set_state.
        state = buffer.get_state()
        new_buffer = MultiAgentPrioritizedReplayBuffer(capacity=buffer_size,
                                                       prioritized_replay_alpha=self.alpha,
                                                       prioritized_replay_beta=self.beta,
                                                       replay_mode="independent",
                                                       learning_starts=0,
                                                       num_shards=1)
        new_buffer.set_state(state)
        batch = new_buffer.sample(1000).policy_batches[
                DEFAULT_POLICY_ID]
        indices = batch["batch_indexes"]
        self.assertTrue(970 < np.sum(indices) < 1100)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
