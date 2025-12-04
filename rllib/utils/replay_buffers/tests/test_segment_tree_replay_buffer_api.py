import unittest

import numpy as np

from ray.rllib.env.single_agent_episode import SingleAgentEpisode
from ray.rllib.execution.segment_tree import MinSegmentTree, SumSegmentTree
from ray.rllib.utils.replay_buffers import PrioritizedEpisodeReplayBuffer


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

    @staticmethod
    def _get_episode(episode_len=None, id_=None, with_extra_model_outs=False):
        eps = SingleAgentEpisode(id_=id_, observations=[0.0], infos=[{}])
        ts = np.random.randint(1, 200) if episode_len is None else episode_len
        for t in range(ts):
            eps.add_env_step(
                observation=float(t + 1),
                action=int(t),
                reward=0.1 * (t + 1),
                infos={},
                extra_model_outputs=(
                    {k: k for k in range(2)} if with_extra_model_outs else None
                ),
            )
        eps.is_terminated = np.random.random() > 0.5
        eps.is_truncated = False if eps.is_terminated else np.random.random() > 0.8
        return eps

    def test_find_prefixsum_idx(self, buffer_size=80):
        """Fix edge case related to https://github.com/ray-project/ray/issues/54284"""
        replay_buffer = PrioritizedEpisodeReplayBuffer(capacity=buffer_size)
        sum_segment = replay_buffer._sum_segment

        for i in range(10):
            replay_buffer.add(self._get_episode(id_=str(i), episode_len=10))

        self.assertTrue(sum_segment.capacity >= buffer_size)

        # standard cases
        for sample in np.linspace(0, sum_segment.sum(), 50):
            prefixsum_idx = sum_segment.find_prefixsum_idx(sample)
            self.assertTrue(
                prefixsum_idx in replay_buffer._tree_idx_to_sample_idx,
                f"{sum_segment.sum()=}, {sample=}, {prefixsum_idx=}",
            )

        # Edge cases (at the boundary then the binary tree can "clip" into invalid regions)
        #   Therefore, testing using values close to or above the max valid number
        for sample in [
            sum_segment.sum() - 0.00001,
            sum_segment.sum(),
            sum_segment.sum() + 0.00001,
        ]:
            prefixsum_idx = sum_segment.find_prefixsum_idx(sample)
            self.assertTrue(
                prefixsum_idx in replay_buffer._tree_idx_to_sample_idx,
                f"{sum_segment.sum()=}, {sample=}, {prefixsum_idx=}",
            )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
