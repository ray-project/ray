import unittest
from typing import Union, List

import numpy as np

import ray
from ray.rllib.algorithms.dt.segmentation_buffer import (
    SegmentationBuffer,
    MultiAgentSegmentationBuffer,
)
from ray.rllib.policy.sample_batch import (
    SampleBatch,
    MultiAgentBatch,
    concat_samples,
    DEFAULT_POLICY_ID,
)
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.typing import PolicyID

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


def _generate_episode_batch(max_ep_len, eps_id, obs_dim=8, act_dim=3):
    """Generate a batch containing one episode."""
    batch = SampleBatch(
        {
            SampleBatch.OBS: np.full((max_ep_len, obs_dim), eps_id, dtype=np.float32),
            SampleBatch.ACTIONS: np.full(
                (max_ep_len, act_dim), eps_id + 100, dtype=np.float32
            ),
            SampleBatch.RETURNS_TO_GO: np.full(
                (max_ep_len,), eps_id + 200, dtype=np.float32
            ),
            SampleBatch.EPS_ID: np.full((max_ep_len,), eps_id, dtype=np.int32),
            SampleBatch.T: np.arange(max_ep_len, dtype=np.int32),
        }
    )
    return batch


def _assert_sample_batch_equals(original: SampleBatch, sample: SampleBatch):
    """Assert that the sampled batch is the same as the original."""
    for key in original.keys():
        assert key in sample.keys()
        original_val = original[key]
        sample_val = sample[key]
        assert (
            original_val.shape == sample_val.shape
        ), f"Key {key} have different shapes."
        assert np.allclose(original_val, sample_val), f"Key {key} are different."


def _assert_sample_batch_keys(batch: SampleBatch):
    """Assert sampled batch has the requisite keys."""
    assert SampleBatch.OBS in batch
    assert SampleBatch.ACTIONS in batch
    assert SampleBatch.RETURNS_TO_GO in batch
    assert SampleBatch.T in batch
    assert SampleBatch.ATTENTION_MASKS in batch


def _assert_sample_batch_not_equal(b1: SampleBatch, b2: SampleBatch):
    """Assert that the two batches are not equal."""
    for key in b1.keys() & b2.keys():
        if b1[key].shape == b2[key].shape:
            assert not np.allclose(
                b1[key], b2[key]
            ), f"Key {key} contain the same value when they should not."


def _assert_is_segment(segment: SampleBatch, episode: SampleBatch):
    """Assert that the sampled segment is a segment of episode."""
    timesteps = segment[SampleBatch.T]
    masks = segment[SampleBatch.ATTENTION_MASKS] > 0.5
    seq_len = timesteps.shape[0]
    episode_segment = episode.slice(timesteps[0], timesteps[-1] + 1)
    assert np.allclose(
        segment[SampleBatch.OBS][masks], episode_segment[SampleBatch.OBS]
    )
    assert np.allclose(
        segment[SampleBatch.ACTIONS][masks], episode_segment[SampleBatch.ACTIONS]
    )
    assert np.allclose(
        segment[SampleBatch.RETURNS_TO_GO][:seq_len].reshape(-1)[masks],
        episode_segment[SampleBatch.RETURNS_TO_GO],
    )


def _get_internal_buffer(
    buffer: Union[SegmentationBuffer, MultiAgentSegmentationBuffer],
    policy_id: PolicyID = DEFAULT_POLICY_ID,
) -> List[SampleBatch]:
    """Get the internal buffer list from the buffer. If MultiAgent then return the
    internal buffer corresponding to the given policy_id.
    """
    if type(buffer) == SegmentationBuffer:
        return buffer._buffer
    elif type(buffer) == MultiAgentSegmentationBuffer:
        return buffer.buffers[policy_id]._buffer
    else:
        raise NotImplementedError


def _as_sample_batch(
    batch: Union[SampleBatch, MultiAgentBatch],
    policy_id: PolicyID = DEFAULT_POLICY_ID,
) -> SampleBatch:
    """Returns a SampleBatch. If MultiAgentBatch then return the SampleBatch
    corresponding to the given policy_id.
    """
    if type(batch) == SampleBatch:
        return batch
    elif type(batch) == MultiAgentBatch:
        return batch.policy_batches[policy_id]
    else:
        raise NotImplementedError


class TestSegmentationBuffer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_add(self):
        """Test adding to segmentation buffer."""
        for buffer_cls in [SegmentationBuffer, MultiAgentSegmentationBuffer]:
            max_seq_len = 3
            max_ep_len = 10
            capacity = 1
            buffer = buffer_cls(capacity, max_seq_len, max_ep_len)

            # generate batch
            episode_batches = []
            for i in range(4):
                episode_batches.append(_generate_episode_batch(max_ep_len, i))
            batch = concat_samples(episode_batches)

            # add to buffer and check that only last one is kept (due to replacement)
            buffer.add(batch)

            assert len(_get_internal_buffer(buffer)) == 1, (
                "The internal buffer should only contain one SampleBatch since"
                " the capacity is 1."
            )
            _assert_sample_batch_equals(
                episode_batches[-1], _get_internal_buffer(buffer)[0]
            )

            # add again
            buffer.add(episode_batches[0])

            _assert_sample_batch_equals(
                episode_batches[0], _get_internal_buffer(buffer)[0]
            )

            # make buffer of enough capacity
            capacity = len(episode_batches)
            buffer = buffer_cls(capacity, max_seq_len, max_ep_len)

            # add to buffer and make sure all are in
            buffer.add(batch)
            assert len(_get_internal_buffer(buffer)) == len(episode_batches)
            for i in range(len(episode_batches)):
                _assert_sample_batch_equals(
                    episode_batches[i], _get_internal_buffer(buffer)[i]
                )

            # add another one and make sure it replaced one of them
            new_batch = _generate_episode_batch(max_ep_len, 12345)
            buffer.add(new_batch)
            assert len(_get_internal_buffer(buffer)) == len(episode_batches)
            found = False
            for episode_batch in _get_internal_buffer(buffer):
                if episode_batch[SampleBatch.EPS_ID][0] == 12345:
                    _assert_sample_batch_equals(episode_batch, new_batch)
                    found = True
                    break
            assert found, "new_batch not added to buffer."

    def test_sample_basic(self):
        """Test sampling from a segmentation buffer."""
        for buffer_cls in (SegmentationBuffer, MultiAgentSegmentationBuffer):
            max_seq_len = 5
            max_ep_len = 15
            capacity = 4
            obs_dim = 10
            act_dim = 2

            buffer = buffer_cls(capacity, max_seq_len, max_ep_len)

            # generate batch and add to buffer
            episode_batches = []
            for i in range(8):
                episode_batches.append(
                    _generate_episode_batch(max_ep_len, i, obs_dim, act_dim)
                )
            batch = concat_samples(episode_batches)
            buffer.add(batch)

            # sample a few times and check shape
            for bs in range(10, 20):
                batch = _as_sample_batch(buffer.sample(bs))
                # check the keys exist
                _assert_sample_batch_keys(batch)

                # check the shapes
                assert batch[SampleBatch.OBS].shape == (bs, max_seq_len, obs_dim)
                assert batch[SampleBatch.ACTIONS].shape == (bs, max_seq_len, act_dim)
                assert batch[SampleBatch.RETURNS_TO_GO].shape == (
                    bs,
                    max_seq_len + 1,
                    1,
                )
                assert batch[SampleBatch.T].shape == (bs, max_seq_len)
                assert batch[SampleBatch.ATTENTION_MASKS].shape == (bs, max_seq_len)

    def test_sample_content(self):
        """Test that the content of the sampling are valid."""
        for buffer_cls in (SegmentationBuffer, MultiAgentSegmentationBuffer):
            max_seq_len = 5
            max_ep_len = 200
            capacity = 1
            obs_dim = 11
            act_dim = 1

            buffer = buffer_cls(capacity, max_seq_len, max_ep_len)

            # generate single episode and add to buffer
            episode = _generate_episode_batch(max_ep_len, 123, obs_dim, act_dim)
            buffer.add(episode)

            # sample twice and make sure they are not equal.
            # with a 200 max_ep_len and 200 samples, the probability that the two
            # samples are equal by chance is (1/200)**200 which is basically zero.
            sample1 = _as_sample_batch(buffer.sample(200))
            sample2 = _as_sample_batch(buffer.sample(200))
            _assert_sample_batch_keys(sample1)
            _assert_sample_batch_keys(sample2)
            _assert_sample_batch_not_equal(sample1, sample2)

            # sample and make sure the segments are actual segments of the episode
            batch = _as_sample_batch(buffer.sample(1000))
            _assert_sample_batch_keys(batch)
            for elem in batch.rows():
                _assert_is_segment(SampleBatch(elem), episode)

    def test_sample_capacity(self):
        """Test that sampling from buffer of capacity > 1 works."""
        for buffer_cls in (SegmentationBuffer, MultiAgentSegmentationBuffer):
            max_seq_len = 3
            max_ep_len = 10
            capacity = 100
            obs_dim = 1
            act_dim = 1

            buffer = buffer_cls(capacity, max_seq_len, max_ep_len)

            # Generate batch and add to buffer
            episode_batches = []
            for i in range(capacity):
                episode_batches.append(
                    _generate_episode_batch(max_ep_len, i, obs_dim, act_dim)
                )
            buffer.add(concat_samples(episode_batches))

            # Sample 100 times and check that samples are from at least 2 different
            # episodes. The [robability of all sampling from 1 episode by chance is
            # (1/100)**99 which is basically zero.
            batch = _as_sample_batch(buffer.sample(100))
            eps_ids = set()
            for i in range(100):
                # obs generated by _generate_episode_batch contains eps_id
                # use -1 because there might be front padding
                eps_id = int(batch[SampleBatch.OBS][i, -1, 0])
                eps_ids.add(eps_id)

            assert len(eps_ids) > 1

    def test_multi_agent(self):
        max_seq_len = 5
        max_ep_len = 20
        capacity = 10
        obs_dim = 3
        act_dim = 5

        ma_buffer = MultiAgentSegmentationBuffer(capacity, max_seq_len, max_ep_len)

        policy_id1 = "1"
        policy_id2 = "2"
        policy_id3 = "3"
        policy_ids = {policy_id1, policy_id2, policy_id3}

        policy1_batches = []
        for i in range(0, 10):
            policy1_batches.append(
                _generate_episode_batch(max_ep_len, i, obs_dim, act_dim)
            )
        policy2_batches = []
        for i in range(10, 20):
            policy2_batches.append(
                _generate_episode_batch(max_ep_len, i, obs_dim, act_dim)
            )
        policy3_batches = []
        for i in range(20, 30):
            policy3_batches.append(
                _generate_episode_batch(max_ep_len, i, obs_dim, act_dim)
            )

        batches_mapping = {
            policy_id1: policy1_batches,
            policy_id2: policy2_batches,
            policy_id3: policy3_batches,
        }

        ma_batch = MultiAgentBatch(
            {
                policy_id1: concat_samples(policy1_batches),
                policy_id2: concat_samples(policy2_batches),
                policy_id3: concat_samples(policy3_batches),
            },
            max_ep_len * 10,
        )

        ma_buffer.add(ma_batch)

        # check all are added properly
        for policy_id in policy_ids:
            assert policy_id in ma_buffer.buffers.keys()

        for policy_id, buffer in ma_buffer.buffers.items():
            assert policy_id in policy_ids
            for i in range(10):
                _assert_sample_batch_equals(
                    batches_mapping[policy_id][i], _get_internal_buffer(buffer)[i]
                )

        # check that sampling are proper
        for _ in range(50):
            ma_sample = ma_buffer.sample(100)
            for policy_id in policy_ids:
                assert policy_id in ma_sample.policy_batches.keys()

            for policy_id, batch in ma_sample.policy_batches.items():
                eps_id_start = (int(policy_id) - 1) * 10
                eps_id_end = eps_id_start + 10

                _assert_sample_batch_keys(batch)

                for i in range(100):
                    # obs generated by _generate_episode_batch contains eps_id
                    # use -1 because there might be front padding
                    eps_id = int(batch[SampleBatch.OBS][i, -1, 0])
                    assert eps_id_start <= eps_id < eps_id_end

        # sample twice and make sure they are not equal (probability equal almost zero)
        ma_sample1 = ma_buffer.sample(200)
        ma_sample2 = ma_buffer.sample(200)
        for policy_id in policy_ids:
            _assert_sample_batch_not_equal(
                ma_sample1.policy_batches[policy_id],
                ma_sample2.policy_batches[policy_id],
            )


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
