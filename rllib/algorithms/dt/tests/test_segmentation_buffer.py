import unittest

import numpy as np

import ray
from ray.rllib.algorithms.dt.segmentation_buffer import (
    SegmentationBuffer,
    MultiAgentSegmentationBuffer,
)
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch, concat_samples
from ray.rllib.utils.framework import try_import_tf, try_import_torch

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
            SampleBatch.EPS_ID: np.full((max_ep_len,), eps_id, dtype=np.long),
            SampleBatch.T: np.arange(max_ep_len),
        }
    )
    return batch


def _assert_sample_batch_equals(original: SampleBatch, sample: SampleBatch):
    """Assert that the sampled batch is the same as the original."""
    for key in original.keys():
        assert key in sample.keys()
        original_val = original[key]
        sample_val = sample[key]
        assert original_val.shape == sample_val.shape
        assert np.allclose(original_val, sample_val)


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
            assert not np.allclose(b1[key], b2[key])


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


class TestSegmentationBuffer(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        ray.init()

    @classmethod
    def tearDownClass(cls):
        ray.shutdown()

    def test_add(self):
        """Test adding to segmentation buffer."""
        max_seq_len = 3
        max_ep_len = 10
        capacity = 1
        buffer = SegmentationBuffer(capacity, max_seq_len, max_ep_len)

        # generate batch
        episode_batches = []
        for i in range(4):
            episode_batches.append(_generate_episode_batch(max_ep_len, i))
        batch = concat_samples(episode_batches)

        # add to buffer and check that only last one is kept (due to replacement)
        buffer.add(batch)

        assert len(buffer._buffer) == 1
        _assert_sample_batch_equals(episode_batches[-1], buffer._buffer[0])

        # add again
        buffer.add(episode_batches[0])

        _assert_sample_batch_equals(episode_batches[0], buffer._buffer[0])

        # make buffer of enough capacity
        capacity = len(episode_batches)
        buffer = SegmentationBuffer(capacity, max_seq_len, max_ep_len)

        # add to buffer and make sure all are in
        buffer.add(batch)
        assert len(buffer._buffer) == len(episode_batches)
        for i in range(len(episode_batches)):
            _assert_sample_batch_equals(episode_batches[i], buffer._buffer[i])

        # add another one and make sure it replaced one of them
        new_batch = _generate_episode_batch(max_ep_len, 12345)
        buffer.add(new_batch)
        assert len(buffer._buffer) == len(episode_batches)
        found = False
        for episode_batch in buffer._buffer:
            if episode_batch[SampleBatch.EPS_ID][0] == 12345:
                _assert_sample_batch_equals(episode_batch, new_batch)
                found = True
                break
        assert found, "new_batch not added to buffer."

    def test_sample_basic(self):
        """Test sampling from a segmentation buffer."""
        max_seq_len = 5
        max_ep_len = 15
        capacity = 4
        obs_dim = 10
        act_dim = 2

        buffer = SegmentationBuffer(capacity, max_seq_len, max_ep_len)

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
            batch = buffer.sample(bs)
            # check the keys exist
            _assert_sample_batch_keys(batch)

            # check the shapes
            assert batch[SampleBatch.OBS].shape == (bs, max_seq_len, obs_dim)
            assert batch[SampleBatch.ACTIONS].shape == (bs, max_seq_len, act_dim)
            assert batch[SampleBatch.RETURNS_TO_GO].shape == (bs, max_seq_len + 1, 1)
            assert batch[SampleBatch.T].shape == (bs, max_seq_len)
            assert batch[SampleBatch.ATTENTION_MASKS].shape == (bs, max_seq_len)

    def test_sample_content(self):
        """Test that the content of the sampling are valid."""
        max_seq_len = 5
        max_ep_len = 200
        capacity = 1
        obs_dim = 11
        act_dim = 1

        buffer = SegmentationBuffer(capacity, max_seq_len, max_ep_len)

        # generate single episode and add to buffer
        episode = _generate_episode_batch(max_ep_len, 123, obs_dim, act_dim)
        buffer.add(episode)

        # sample twice and make sure they are not equal.
        # with a 200 max_ep_len and 200 samples, the probability that the two samples
        # are equal by chance is (1/200)**200 which is basically zero.
        sample1 = buffer.sample(200)
        sample2 = buffer.sample(200)
        _assert_sample_batch_keys(sample1)
        _assert_sample_batch_keys(sample2)
        _assert_sample_batch_not_equal(sample1, sample2)

        # sample and make sure the segments are actual segments of the episode
        batch = buffer.sample(1000)
        _assert_sample_batch_keys(batch)
        for elem in batch.rows():
            _assert_is_segment(SampleBatch(elem), episode)

    def test_sample_capacity(self):
        """Test that sampling from buffer of capacity > 1 works."""
        max_seq_len = 3
        max_ep_len = 10
        capacity = 100
        obs_dim = 1
        act_dim = 1

        buffer = SegmentationBuffer(capacity, max_seq_len, max_ep_len)

        # Generate batch and add to buffer
        episode_batches = []
        for i in range(capacity):
            episode_batches.append(
                _generate_episode_batch(max_ep_len, i, obs_dim, act_dim)
            )
        buffer.add(concat_samples(episode_batches))

        # Sample 100 times and check that samples from at least 2 different episodes.
        # Probability of all sampling from 1 episode by chance is (1/100)**99 which
        # is basically zero.
        batch = buffer.sample(100)
        eps_ids = set()
        for i in range(100):
            # obs generated by _generate_episode_batch contains eps_id
            eps_id = int(batch[SampleBatch.OBS][i, 0, 0])
            eps_ids.add(eps_id)

        assert len(eps_ids) > 1


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
