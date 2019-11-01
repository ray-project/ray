# coding: utf-8
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import random
import sys
import unittest

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

        checkpoints = [Checkpoint(Checkpoint.DISK, {i}, self.mock_result(i))
                       for i in range(3)]
        checkpoints += [Checkpoint(Checkpoint.DISK, {i}, self.mock_result(i))
                        for i in range(6, 3, -1)]

        with patch("shutil.rmtree") as rmtree_mock, patch("os.path"):

            for j in range(3):
                checkpoint_manager.on_checkpoint(checkpoints[j])
                expected_deletes = 0 if j != 2 else 1
                self.assertEqual(rmtree_mock.call_count, expected_deletes)
                self.assertEqual(checkpoint_manager.newest_checkpoint,
                                 checkpoints[j])

            best_checkpoints = checkpoint_manager.best_checkpoints()
            self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
            self.assertIn(checkpoints[1], best_checkpoints)
            self.assertIn(checkpoints[2], best_checkpoints)

            for j in range(3, len(checkpoints)):
                checkpoint_manager.on_checkpoint(checkpoints[j])
                expected_deletes = 2 if j == 3 else 3
                self.assertEqual(rmtree_mock.call_count, expected_deletes)
                self.assertEqual(checkpoint_manager.newest_checkpoint,
                                 checkpoints[j])

        best_checkpoints = checkpoint_manager.best_checkpoints()
        self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
        self.assertIn(checkpoints[3], best_checkpoints)
        self.assertIn(checkpoints[4], best_checkpoints)

    def testBestCheckpoints(self):
        keep_checkpoints_num = 4
        checkpoint_manager = CheckpointManager(keep_checkpoints_num, "i")
        checkpoints = [Checkpoint(Checkpoint.MEMORY, i, self.mock_result(i))
                       for i in range(16)]
        random.shuffle(checkpoints)

        for checkpoint in checkpoints:
            checkpoint_manager.on_checkpoint(checkpoint)

        best_checkpoints = checkpoint_manager.best_checkpoints()
        self.assertEqual(len(best_checkpoints), keep_checkpoints_num)
        for i in range(len(best_checkpoints)):
            self.assertEqual(best_checkpoints[i].value, i + 12)
