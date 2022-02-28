import numpy as np
import unittest

import ray
from ray.rllib.policy.rnn_sequencing import pad_batch_to_sequences_of_same_size
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.test_utils import check


class TestRNNSequencing(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_pad_batch_dynamic_max(self):
        """Test pad_batch_to_sequences_of_same_size when dynamic_max = True"""
        view_requirements = {
            "state_in_0": ViewRequirement(
                "state_out_0",
                shift=[-1],
                used_for_training=False,
                used_for_compute_actions=True,
                batch_repeat_value=1,
            )
        }
        max_seq_len = 20
        num_seqs = np.random.randint(1, 20)
        seq_lens = np.random.randint(1, max_seq_len, size=(num_seqs))
        max_len = np.max(seq_lens)
        sum_seq_lens = np.sum(seq_lens)

        s1 = SampleBatch(
            {
                "a": np.arange(sum_seq_lens),
                "b": np.arange(sum_seq_lens),
                "seq_lens": seq_lens,
                "state_in_0": [[0]] * num_seqs,
            },
            _max_seq_len=max_seq_len,
        )

        pad_batch_to_sequences_of_same_size(
            s1,
            max_seq_len=max_seq_len,
            feature_keys=["a", "b"],
            view_requirements=view_requirements,
        )
        check(s1.max_seq_len, max_len)
        check(s1["a"].shape[0], max_len * num_seqs)
        check(s1["b"].shape[0], max_len * num_seqs)

    def test_pad_batch_fixed_max(self):
        """Test pad_batch_to_sequences_of_same_size when dynamic_max = False"""
        view_requirements = {
            "state_in_0": ViewRequirement(
                "state_out_0",
                shift="-3:-1",
                used_for_training=False,
                used_for_compute_actions=True,
                batch_repeat_value=1,
            )
        }
        max_seq_len = 20
        num_seqs = np.random.randint(1, 20)
        seq_lens = np.random.randint(1, max_seq_len, size=(num_seqs))
        sum_seq_lens = np.sum(seq_lens)
        s1 = SampleBatch(
            {
                "a": np.arange(sum_seq_lens),
                "b": np.arange(sum_seq_lens),
                "seq_lens": seq_lens,
                "state_in_0": [[0]] * num_seqs,
            },
            _max_seq_len=max_seq_len,
        )

        pad_batch_to_sequences_of_same_size(
            s1,
            max_seq_len=max_seq_len,
            feature_keys=["a", "b"],
            view_requirements=view_requirements,
        )
        check(s1.max_seq_len, max_seq_len)
        check(s1["a"].shape[0], max_seq_len * num_seqs)
        check(s1["b"].shape[0], max_seq_len * num_seqs)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
