from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

from ray.rllib.models.lstm import chop_into_sequences


class LSTMUtilsTest(unittest.TestCase):
    def testBasic(self):
        eps_ids = [1, 1, 1, 5, 5, 5, 5, 5]
        f = [[101, 102, 103, 201, 202, 203, 204, 205],
             [[101], [102], [103], [201], [202], [203], [204], [205]]]
        s = [[209, 208, 207, 109, 108, 107, 106, 105]]
        f_pad, s_init, seq_lens = chop_into_sequences(eps_ids, f, s, 4)
        self.assertEqual([f.tolist() for f in f_pad], [
            [101, 102, 103, 0, 201, 202, 203, 204, 205, 0, 0, 0],
            [[101], [102], [103], [0], [201], [202], [203], [204], [205], [0],
             [0], [0]],
        ])
        self.assertEqual([s.tolist() for s in s_init], [[209, 109, 105]])
        self.assertEqual(seq_lens.tolist(), [3, 4, 1])

    def testDynamicMaxLen(self):
        eps_ids = [5, 2, 2]
        f = [[1, 1, 1]]
        s = [[1, 1, 1]]
        f_pad, s_init, seq_lens = chop_into_sequences(eps_ids, f, s, 4)
        self.assertEqual([f.tolist() for f in f_pad], [[1, 0, 1, 1]])
        self.assertEqual([s.tolist() for s in s_init], [[1, 1]])
        self.assertEqual(seq_lens.tolist(), [1, 2])


if __name__ == "__main__":
    unittest.main(verbosity=2)
