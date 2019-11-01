# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest
import sys

from ray.tune.checkpoint_manager import Checkpoint, CheckpointManager


if sys.version_info >= (3, 3):
    from unittest.mock import patch
else:
    from mock import patch


class CheckpointManagerTest(unittest.TestCase):

    @staticmethod
    def mock_result(i):
        return {"i": i}

    def testOnCheckpoint(self):
        """Tests monotonically increasing, then decreasing priorities."""
        keep_checkpoints_num = 2
        checkpoint_manager = CheckpointManager(keep_checkpoints_num, "i")

        results = [Checkpoint(Checkpoint.DISK, {i}, self.mock_result(i))
                   for i in range(3)]
        results += [Checkpoint(Checkpoint.DISK, {i}, self.mock_result(i))
                    for i in range(6, 3, -1)]

        with patch("shutil.rmtree") as rmtree_mock, patch("os.path"):

            for j in range(3):
                checkpoint_manager.on_checkpoint(results[j])
                expected_deletes = max(0, j - keep_checkpoints_num + 1)
                self.assertEqual(rmtree_mock.call_count, expected_deletes)
                self.assertEqual(checkpoint_manager.newest_checkpoint,
                                 results[j])

            best_checkpoints = checkpoint_manager.best_checkpoints()
            self.assertNotIn(results[0], best_checkpoints)
            self.assertIn(results[1], best_checkpoints)
            self.assertIn(results[2], best_checkpoints)

            for j in range(3, len(results)):
                checkpoint_manager.on_checkpoint(results[j])
                print(rmtree_mock.call_count)
                # expected_call_count = max(0, j - keep_checkpoints_num + 1)
                self.assertEqual(checkpoint_manager.newest_checkpoint,
                                 results[j])

        best_checkpoints = checkpoint_manager.best_checkpoints()
        for j in range(3):
            self.assertNotIn(results[j], best_checkpoints)
        self.assertIn(results[3], best_checkpoints)
        self.assertIn(results[4], best_checkpoints)
        self.assertNotIn(results[5], best_checkpoints)
