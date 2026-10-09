import unittest

import numpy as np

from ray.rllib.policy.rnn_sequencing import chop_into_sequences
from ray.rllib.utils.test_utils import check


class TestLSTMUtils(unittest.TestCase):
    def test_basic(self):
        eps_ids = [1, 1, 1, 5, 5, 5, 5, 5]
        agent_ids = [1, 1, 1, 1, 1, 1, 1, 1]
        f = [
            [101, 102, 103, 201, 202, 203, 204, 205],
            [[101], [102], [103], [201], [202], [203], [204], [205]],
        ]
        s = [[209, 208, 207, 109, 108, 107, 106, 105]]
        f_pad, s_init, seq_lens = chop_into_sequences(
            episode_ids=eps_ids,
            unroll_ids=np.ones_like(eps_ids),
            agent_indices=agent_ids,
            feature_columns=f,
            state_columns=s,
            max_seq_len=4,
        )
        self.assertEqual(
            [f.tolist() for f in f_pad],
            [
                [101, 102, 103, 0, 201, 202, 203, 204, 205, 0, 0, 0],
                [
                    [101],
                    [102],
                    [103],
                    [0],
                    [201],
                    [202],
                    [203],
                    [204],
                    [205],
                    [0],
                    [0],
                    [0],
                ],
            ],
        )
        self.assertEqual([s.tolist() for s in s_init], [[209, 109, 105]])
        self.assertEqual(seq_lens.tolist(), [3, 4, 1])

    def test_nested(self):
        eps_ids = [1, 1, 1, 5, 5, 5, 5, 5]
        agent_ids = [1, 1, 1, 1, 1, 1, 1, 1]
        f = [
            {
                "a": np.array([1, 2, 3, 4, 13, 14, 15, 16]),
                "b": {"ba": np.array([5, 6, 7, 8, 9, 10, 11, 12])},
            }
        ]
        s = [[209, 208, 207, 109, 108, 107, 106, 105]]

        f_pad, s_init, seq_lens = chop_into_sequences(
            episode_ids=eps_ids,
            unroll_ids=np.ones_like(eps_ids),
            agent_indices=agent_ids,
            feature_columns=f,
            state_columns=s,
            max_seq_len=4,
            handle_nested_data=True,
        )
        check(
            f_pad,
            [
                [
                    [1, 2, 3, 0, 4, 13, 14, 15, 16, 0, 0, 0],
                    [5, 6, 7, 0, 8, 9, 10, 11, 12, 0, 0, 0],
                ]
            ],
        )
        self.assertEqual([s.tolist() for s in s_init], [[209, 109, 105]])
        self.assertEqual(seq_lens.tolist(), [3, 4, 1])

    def test_multi_dim(self):
        eps_ids = [1, 1, 1]
        agent_ids = [1, 1, 1]
        obs = np.ones((84, 84, 4))
        f = [[obs, obs * 2, obs * 3]]
        s = [[209, 208, 207]]
        f_pad, s_init, seq_lens = chop_into_sequences(
            episode_ids=eps_ids,
            unroll_ids=np.ones_like(eps_ids),
            agent_indices=agent_ids,
            feature_columns=f,
            state_columns=s,
            max_seq_len=4,
        )
        self.assertEqual(
            [f.tolist() for f in f_pad],
            [
                np.array([obs, obs * 2, obs * 3]).tolist(),
            ],
        )
        self.assertEqual([s.tolist() for s in s_init], [[209]])
        self.assertEqual(seq_lens.tolist(), [3])

    def test_batch_id(self):
        eps_ids = [1, 1, 1, 5, 5, 5, 5, 5]
        batch_ids = [1, 1, 2, 2, 3, 3, 4, 4]
        agent_ids = [1, 1, 1, 1, 1, 1, 1, 1]
        f = [
            [101, 102, 103, 201, 202, 203, 204, 205],
            [[101], [102], [103], [201], [202], [203], [204], [205]],
        ]
        s = [[209, 208, 207, 109, 108, 107, 106, 105]]
        _, _, seq_lens = chop_into_sequences(
            episode_ids=eps_ids,
            unroll_ids=batch_ids,
            agent_indices=agent_ids,
            feature_columns=f,
            state_columns=s,
            max_seq_len=4,
        )
        self.assertEqual(seq_lens.tolist(), [2, 1, 1, 2, 2])

    def test_multi_agent(self):
        eps_ids = [1, 1, 1, 5, 5, 5, 5, 5]
        agent_ids = [1, 1, 2, 1, 1, 2, 2, 3]
        f = [
            [101, 102, 103, 201, 202, 203, 204, 205],
            [[101], [102], [103], [201], [202], [203], [204], [205]],
        ]
        s = [[209, 208, 207, 109, 108, 107, 106, 105]]
        f_pad, s_init, seq_lens = chop_into_sequences(
            episode_ids=eps_ids,
            unroll_ids=np.ones_like(eps_ids),
            agent_indices=agent_ids,
            feature_columns=f,
            state_columns=s,
            max_seq_len=4,
            dynamic_max=False,
        )
        self.assertEqual(seq_lens.tolist(), [2, 1, 2, 2, 1])
        self.assertEqual(len(f_pad[0]), 20)
        self.assertEqual(len(s_init[0]), 5)

    def test_dynamic_max_len(self):
        eps_ids = [5, 2, 2]
        agent_ids = [2, 2, 2]
        f = [[1, 1, 1]]
        s = [[1, 1, 1]]
        f_pad, s_init, seq_lens = chop_into_sequences(
            episode_ids=eps_ids,
            unroll_ids=np.ones_like(eps_ids),
            agent_indices=agent_ids,
            feature_columns=f,
            state_columns=s,
            max_seq_len=4,
        )
        self.assertEqual([f.tolist() for f in f_pad], [[1, 0, 1, 1]])
        self.assertEqual([s.tolist() for s in s_init], [[1, 1]])
        self.assertEqual(seq_lens.tolist(), [1, 2])


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
