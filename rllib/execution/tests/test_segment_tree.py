import numpy as np
import timeit
import unittest

from ray.rllib.execution.segment_tree import SumSegmentTree, MinSegmentTree
from ray.rllib.utils.test_utils import check


class TestSegmentTree(unittest.TestCase):
    def test_tree_set(self):
        tree = SumSegmentTree(4)

        tree[2] = 1.0
        tree[3] = 3.0

        assert np.isclose(tree.sum(), 4.0)
        assert np.isclose(tree.sum(0, 2), 0.0)
        assert np.isclose(tree.sum(0, 3), 1.0)
        assert np.isclose(tree.sum(2, 3), 1.0)
        assert np.isclose(tree.sum(2, -1), 1.0)
        assert np.isclose(tree.sum(2, 4), 4.0)
        assert np.isclose(tree.sum(2), 4.0)

    def test_tree_set_overlap(self):
        tree = SumSegmentTree(4)

        tree[2] = 1.0
        tree[2] = 3.0

        assert np.isclose(tree.sum(), 3.0)
        assert np.isclose(tree.sum(2, 3), 3.0)
        assert np.isclose(tree.sum(2, -1), 3.0)
        assert np.isclose(tree.sum(2, 4), 3.0)
        assert np.isclose(tree.sum(2), 3.0)
        assert np.isclose(tree.sum(1, 2), 0.0)

    def test_prefixsum_idx(self):
        tree = SumSegmentTree(4)

        tree[2] = 1.0
        tree[3] = 3.0

        assert tree.find_prefixsum_idx(0.0) == 2
        assert tree.find_prefixsum_idx(0.5) == 2
        assert tree.find_prefixsum_idx(0.99) == 2
        assert tree.find_prefixsum_idx(1.01) == 3
        assert tree.find_prefixsum_idx(3.00) == 3
        assert tree.find_prefixsum_idx(4.00) == 3

    def test_prefixsum_idx2(self):
        tree = SumSegmentTree(4)

        tree[0] = 0.5
        tree[1] = 1.0
        tree[2] = 1.0
        tree[3] = 3.0

        assert tree.find_prefixsum_idx(0.00) == 0
        assert tree.find_prefixsum_idx(0.55) == 1
        assert tree.find_prefixsum_idx(0.99) == 1
        assert tree.find_prefixsum_idx(1.51) == 2
        assert tree.find_prefixsum_idx(3.00) == 3
        assert tree.find_prefixsum_idx(5.50) == 3

    def test_max_interval_tree(self):
        tree = MinSegmentTree(4)

        tree[0] = 1.0
        tree[2] = 0.5
        tree[3] = 3.0

        assert np.isclose(tree.min(), 0.5)
        assert np.isclose(tree.min(0, 2), 1.0)
        assert np.isclose(tree.min(0, 3), 0.5)
        assert np.isclose(tree.min(0, -1), 0.5)
        assert np.isclose(tree.min(2, 4), 0.5)
        assert np.isclose(tree.min(3, 4), 3.0)

        tree[2] = 0.7

        assert np.isclose(tree.min(), 0.7)
        assert np.isclose(tree.min(0, 2), 1.0)
        assert np.isclose(tree.min(0, 3), 0.7)
        assert np.isclose(tree.min(0, -1), 0.7)
        assert np.isclose(tree.min(2, 4), 0.7)
        assert np.isclose(tree.min(3, 4), 3.0)

        tree[2] = 4.0

        assert np.isclose(tree.min(), 1.0)
        assert np.isclose(tree.min(0, 2), 1.0)
        assert np.isclose(tree.min(0, 3), 1.0)
        assert np.isclose(tree.min(0, -1), 1.0)
        assert np.isclose(tree.min(2, 4), 3.0)
        assert np.isclose(tree.min(2, 3), 4.0)
        assert np.isclose(tree.min(2, -1), 4.0)
        assert np.isclose(tree.min(3, 4), 3.0)

    def test_microbenchmark_vs_old_version(self):
        """
        Results from March 2020 (capacity=1048576):

        New tree:
        0.049599366000000256s
        results = timeit.timeit("tree.sum(5, 60000)",
            setup="from ray.rllib.execution.segment_tree import
            SumSegmentTree; tree = SumSegmentTree({})".format(capacity),
            number=10000)

        Old tree:
        0.13390400999999974s
        results = timeit.timeit("tree.sum(5, 60000)",
            setup="from ray.rllib.execution.tests.old_segment_tree import
            OldSumSegmentTree; tree = OldSumSegmentTree({})".format(capacity),
            number=10000)
        """
        capacity = 2**20
        # Expect reductions to be much faster now.
        new = timeit.timeit(
            "tree.sum(5, 60000)",
            setup="from ray.rllib.execution.segment_tree import "
            "SumSegmentTree; tree = SumSegmentTree({})".format(capacity),
            number=10000)
        old = timeit.timeit(
            "tree.sum(5, 60000)",
            setup="from ray.rllib.execution.tests.old_segment_tree import "
            "OldSumSegmentTree; tree = OldSumSegmentTree({})".format(capacity),
            number=10000)
        print("Sum performance (time spent) old={} new={}".format(old, new))
        self.assertGreater(old, new)

        # Expect insertions to be roughly the same.
        new = timeit.timeit(
            "tree[50000] = 10; tree[50001] = 11",
            setup="from ray.rllib.execution.segment_tree import "
            "SumSegmentTree; tree = SumSegmentTree({})".format(capacity),
            number=100000)
        old = timeit.timeit(
            "tree[50000] = 10; tree[50001] = 11",
            setup="from ray.rllib.execution.tests.old_segment_tree import "
            "OldSumSegmentTree; tree = OldSumSegmentTree({})".format(capacity),
            number=100000)
        print("Insertion performance (time spent) "
              "old={} new={}".format(old, new))
        check(old, new, rtol=0.15)


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
