import functools
from pathlib import Path
import os
import unittest

import numpy as np

import ray
from ray.rllib.algorithms.dt import DTConfig
from ray.rllib.offline.json_reader import JsonReader
from ray.rllib.algorithms.dt.segmentation_buffer import (
    SegmentationBuffer,
    MultiAgentSegmentationBuffer,
)
from ray.rllib.policy.sample_batch import SampleBatch, MultiAgentBatch, concat_samples
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import (
    check_compute_single_action,
    check_train_results,
)

tf1, tf, tfv = try_import_tf()
torch, _ = try_import_torch()


def _generate_episode_batch(max_ep_len, eps_id, obs_dim=8, act_dim=3):
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
        }
    )
    return batch


def _assert_sample_batch_equals(original: SampleBatch, sample: SampleBatch):
    for key in original.keys():
        assert key in sample.keys()
        original_val = original[key]
        sample_val = sample[key]
        assert original_val.shape == sample_val.shape
        assert np.allclose(original_val, sample_val)


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

    def test_sample(self):
        """Test sampling from a segmentation buffer."""

        max_seq_len = 5
        max_ep_len = 15
        capacity = 2

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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
