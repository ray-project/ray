import numpy as np
import unittest

import ray
from ray.rllib.policy.rnn_sequencing import (
    pad_batch_to_sequences_of_same_size,
    add_time_dimension,
    chop_into_sequences,
)
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.view_requirement import ViewRequirement
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.test_utils import check


tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()


class TestRNNSequencing(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        ray.init()

    @classmethod
    def tearDownClass(cls) -> None:
        ray.shutdown()

    def test_chop_into_sequences_long_seq(self):
        """Test pad_batch where episodes are longer than max_seq_len. The long
        seq should be split into two smaller seqs that are less than max_seq_len"""
        max_seq_len = 2
        # Input seq lens, corresponding to ep_ids, unroll_ids, etc.
        seq_lens = [2, 3, 1]  # noqa: F841
        ep_ids = [0, 0, 1, 1, 1, 2]
        unroll_ids = [2, 2, 3, 3, 3, 4]
        feats = [[1, 1, 2, 2, 2, 3]]
        # Input states, ie states[3] is the input state at
        # t = 3 and the output state at t = 2
        states = [[1, 2, 3, 4, 5, 6]]
        agent = [0, 0, 0, 0, 0, 0]
        f_pad, s_init, s_lens = chop_into_sequences(
            feature_columns=feats,
            state_columns=states,
            max_seq_len=max_seq_len,
            episode_ids=ep_ids,
            unroll_ids=unroll_ids,
            agent_indices=agent,
            dynamic_max=False,
        )
        expected_f_pad = [[1, 1, 2, 2, 2, 0, 3, 0]]
        expected_seq_lens = [2, 2, 1, 1]
        expected_states = [[1, 3, 5, 6]]
        check(f_pad, expected_f_pad)
        check(s_lens, expected_seq_lens)
        check(s_init, expected_states)

        # Try again with dynamic max
        f_pad, s_init, s_lens = chop_into_sequences(
            feature_columns=feats,
            state_columns=states,
            max_seq_len=max_seq_len,
            episode_ids=ep_ids,
            unroll_ids=unroll_ids,
            agent_indices=agent,
            dynamic_max=True,
        )
        check(f_pad, expected_f_pad)
        check(s_lens, expected_seq_lens)
        check(s_init, expected_states)

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

    def test_add_time_dimension(self):
        """Test add_time_dimension gives sequential data along the time dimension"""

        B, T, F = np.random.choice(
            np.asarray(list(range(8, 32)), dtype=np.int32),  # use int32 for seq_lens
            size=3,
            replace=False,
        )

        inputs_numpy = np.repeat(
            np.arange(B * T)[:, np.newaxis], repeats=F, axis=-1
        ).astype(np.int32)
        check(inputs_numpy.shape, (B * T, F))
        seq_lens = np.random.randint(1, T + 1, (B,))

        time_shift_diff_batch_major = np.ones(shape=(B, T - 1, F), dtype=np.int32)
        time_shift_diff_time_major = np.ones(shape=(T - 1, B, F), dtype=np.int32)

        if tf is not None:
            # Test tensorflow batch-major
            padded_inputs = tf.constant(inputs_numpy)
            batch_major_outputs = add_time_dimension(
                padded_inputs, seq_lens=seq_lens, framework="tf", time_major=False
            )
            check(batch_major_outputs.shape.as_list(), [B, T, F])
            time_shift_diff = batch_major_outputs[:, 1:] - batch_major_outputs[:, :-1]
            check(time_shift_diff, time_shift_diff_batch_major)

        if torch is not None:
            # Test torch batch-major
            padded_inputs = torch.from_numpy(inputs_numpy)
            batch_major_outputs = add_time_dimension(
                padded_inputs, seq_lens=seq_lens, framework="torch", time_major=False
            )
            check(batch_major_outputs.shape, (B, T, F))
            time_shift_diff = batch_major_outputs[:, 1:] - batch_major_outputs[:, :-1]
            check(time_shift_diff, time_shift_diff_batch_major)

            # Test torch time-major
            padded_inputs = torch.from_numpy(inputs_numpy)
            time_major_outputs = add_time_dimension(
                padded_inputs, seq_lens=seq_lens, framework="torch", time_major=True
            )
            check(time_major_outputs.shape, (T, B, F))
            time_shift_diff = time_major_outputs[1:] - time_major_outputs[:-1]
            check(time_shift_diff, time_shift_diff_time_major)


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
